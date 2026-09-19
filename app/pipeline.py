import os, hashlib, time, zipfile
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from . import store, media, database, storage
from .providers import demo_image
from .planning import plan, demo_plan, publishing
from . import production
from . import elevenlabs_tts

pool=ThreadPoolExecutor(max_workers=1,thread_name_prefix='render')
active=set(); lock=Lock()
class Cancelled(Exception): pass

def schedule_safe_provider_retry(ident, folder, message):
    """Retry only requests Replicate definitively rejected before creating a prediction."""
    journals=list(folder.glob('*.operation.json'))
    safe=any(store.read(path).get('retry_safe') for path in journals)
    if not safe:return False
    current=store.get(ident); attempts=int(current.get('retry_count') or 0)
    maximum=int(os.getenv('REPLICATE_SAFE_RETRY_LIMIT','3'))
    if attempts>=maximum:return False
    delay=int(os.getenv('REPLICATE_SAFE_RETRY_DELAY_SECONDS','900'))*(2**attempts)
    store.update(ident,status='queued',stage='Waiting to retry a rejected Replicate request',error=message[:2400],retry_count=attempts+1,retry_at=time.time()+delay,enqueued=time.time())
    return True

def enqueue(ident):
    if database.postgres():
        import time
        with store.db() as c:
            row=c.execute('SELECT status,owner FROM jobs WHERE id=? FOR UPDATE',(ident,)).fetchone()
            if not row or row['status']=='running':return False
            c.execute('SELECT username FROM users WHERE username=? FOR UPDATE',(row['owner'],))
            count=c.execute("SELECT COUNT(*) FROM jobs WHERE owner=? AND id<>? AND status IN ('running','queued')",(row['owner'],ident)).fetchone()[0]
            if count>=int(os.getenv('MAX_JOBS_PER_USER','3')):raise ValueError('Your production queue is full.')
            c.execute("UPDATE jobs SET status='queued',cancel=0,error=NULL,enqueued=? WHERE id=?",(time.time(),ident))
        return True
    with lock:
        if ident in active: return False
        active.add(ident)
        store.update(ident,status='queued',cancel=0,error=None)
        pool.submit(work,ident)
    return True

def work(ident,lease=None):
    folder=store.folder(ident); request=store.get(ident)['request']; demo=request['mode']=='demo'
    def check():
        current=store.get(ident)
        if lease and (lease[1].is_set() or current['worker']!=lease[0]):raise Cancelled()
        if current['cancel']: raise Cancelled()
    def progress(stage,percent):
        check(); storage.sync(ident);store.update(ident,status='running',stage=stage,progress=percent)
    try:
        progress('Checking production tools',1)
        if not media.available(): raise RuntimeError('Install FFmpeg and FFprobe, then resume.')
        provider=None
        if not demo:
            if not os.getenv('REPLICATE_API_TOKEN','').strip():
                raise RuntimeError('REPLICATE_API_TOKEN is missing. No generation was started.')
            if os.getenv('ALLOW_PAID_GENERATION','false').lower()!='true': raise RuntimeError('Live generation needs paid Replicate access. Set ALLOW_PAID_GENERATION=true in .env after reviewing Pruna pricing.')
            from .replicate_provider import Replicate
            provider=Replicate(os.getenv('REPLICATE_API_TOKEN','').strip(),check)
        if demo:
            p=demo_plan(request)
            for k,v in p.items(): store.save(folder/(k+'.json'),v)
        else: p=plan(provider,folder,request,progress)
        store.save(folder/'plan.json',p)
        assets={}
        for i,c in enumerate(p['cast']['characters']):
            progress('Creating locked reference: '+c['name'],30+i*2)
            path=folder/f'character-{i+1}.png'; assets[c['tag']]=path
            if demo:
                if not path.exists():demo_image(path,c['name'],i+1)
            else: production.reviewed_asset(provider,path,[],c['reference_prompt'],request['aspect'],folder/f'review-character-{i+1}.json',c['look'])
        for i,b in enumerate(p['locations']['backgrounds']):
            progress('Creating background: '+b['location'],37+i*2)
            for a,angle in enumerate(b['angles']):
                path=folder/f'background-{i+1}-{a+1}.png'
                if a==0: assets[b['tag']]=path
                assets[b['tag']+f':{a+1}']=path
                if demo:
                    if not path.exists():demo_image(path,b['location'],a+1,request['aspect'])
                else: production.reviewed_asset(provider,path,[] if a==0 else [(b['tag'],assets[b['tag']])],b['prompt']+' Camera: '+angle,request['aspect'],folder/f'review-background-{i+1}-{a+1}.json','Empty background, no people. '+b['prompt']+' Camera: '+angle)
        from .replicate_provider import media_models
        manifest={'models':media_models(),'mode':request['mode'],'aspect':request['aspect'],'reference_url':'https://www.youtube.com/shorts/lcekXpC7jRc','assets':{tag:{'file':path.name,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()} for tag,path in assets.items()}}
        if (folder/'manifest.json').exists():
            old=store.read(folder/'manifest.json')
            if any(manifest['assets'].get(tag)!=value for tag,value in old['assets'].items()):
                raise RuntimeError('Canonical reference files changed. Restore the original assets before resuming.')
        else: store.save(folder/'manifest.json',manifest)
        clips=[]; subtitles=[]
        for i,(line,scene) in enumerate(zip(p['story']['lines'],p['scenes']['scenes'],strict=True)):
            n=i+1; progress(f'Scene {n}/{len(p["scenes"]["scenes"])}: {scene["title"]}',45+i*5)
            still=folder/f'scene-{n:02d}.png'; video=folder/f'raw-{n:02d}.mp4'
            elevenlabs=not demo
            speech=folder/f'speech-{n:02d}.mp3'; fitted=folder/f'voice-{n:02d}.wav'; clip=folder/f'clip-{n:02d}.mp4'
            location=next(b for b in p['locations']['backgrounds'] if b['tag']==scene['background_tag'])
            packet=production.packet(request,line,scene,p['cast'],location,n)
            store.save(folder/f'packet-{n:02d}.json',packet)
            corrections=store.read(folder/'corrections.json') if (folder/'corrections.json').exists() else {}
            correction=corrections.get(str(n),'')
            if not still.exists() or not demo:
                bg_tag=scene['background_tag']+':'+str(scene.get('background_angle',1))
                refs=[(tag,assets[tag]) for tag in [bg_tag,*scene['character_tags']]]
                if demo: demo_image(still,scene['title'],n,request['aspect'])
                else:
                    identities='\n'.join(c['tag']+': '+c['look'] for c in p['cast']['characters'] if c['tag'] in scene['character_tags'])
                    production.reviewed_asset(provider,still,refs,packet['still_prompt']+' '+correction,request['aspect'],folder/f'review-still-{n:02d}.json',packet['still_prompt'])
            speaker=next((c for c in p['cast']['characters'] if c['name']==line['speaker']),None)
            timing=None
            if demo:
                if not speech.exists():media.tone(speech,6,'none')
                if not fitted.exists():media.fit_speech(speech,fitted)
                if not video.exists():media.make_demo_video(still,video)
            else:
                video,_=production.verified_video(provider,folder,n,still,[(tag,assets[tag]) for tag in scene['character_tags']],packet,request['aspect'])
                if not speech.exists():elevenlabs_tts.synthesize(line['text'],speech,check)
                if not fitted.exists():media.fit_speech(speech,fitted)
            if not clip.exists(): media.render_scene(video,fitted if (demo or elevenlabs) else None,clip,request['aspect'],False,timing)
            clips.append(clip)
            def timestamp(t):
                ms=round(t*1000); return f'{ms//3600000:02d}:{ms//60000%60:02d}:{ms//1000%60:02d},{ms%1000:03d}'
            caption_end=7.5 if not timing else min(7.5,(timing[1]-timing[0])/max(1,(timing[1]-timing[0])/7.32))
            subtitles.append(f'{n}\n{timestamp(i*7.5)} --> {timestamp(i*7.5+caption_end)}\n{line["text"]}\n')
        (folder/'captions.srt').write_text('\n'.join(subtitles),encoding='utf-8')
        progress('Mixing music and rendering locked six-act master',88)
        report=media.finish(folder,clips,[s['sound'] for s in p['scenes']['scenes']],scene_seconds=7.5)
        report.update({'demo':demo,'narration':'silent test track' if demo else 'ElevenLabs Hindi narration','character_consistency':'Canonical references are supplied to scene composition; no external frame-by-frame review is performed','subtitles':'Scene-level SRT','speech_verification':'Not performed'})
        store.save(folder/'quality-report.json',report)
        progress('Creating thumbnail and publishing package',95)
        if not demo:
            p['publishing']=publishing(provider,folder,request,p)
            store.save(folder/'plan.json',p)
        thumb=folder/'thumbnail.png'
        if not thumb.exists():
            if demo: demo_image(thumb,request['title'],1)
            else:
                artwork=folder/'thumbnail-art.png'
                if not artwork.exists():provider.image(p['publishing']['thumbnail_prompts'][0]+' Leave the lower third clear for the title. Do not draw text.',artwork,list(assets.items())[:len(p['cast']['characters'])])
                media.thumbnail(artwork,thumb,request['title'])
        with zipfile.ZipFile(folder/'production-kit.zip','w',zipfile.ZIP_DEFLATED) as z:
            for path in folder.iterdir():
                if path.suffix in {'.json','.png','.srt','.wav','.mp3','.mp4'} and '.operation.' not in path.name and not path.name.startswith(('clip-','raw-')) and path.name!='joined.mp4': z.write(path,path.name)
        progress('Complete',100); store.update(ident,status='complete')
    except Cancelled:
        if not lease or store.get(ident)['worker']==lease[0]:store.update(ident,status='cancelled',stage='Stopped; saved assets can be resumed')
    except Exception as exc:
        # Never persist SDK request dumps or API credentials.
        message=str(exc)
        for secret in (os.getenv('REPLICATE_API_TOKEN'),os.getenv('OPENAI_API_KEY'),os.getenv('ELEVENLABS_API_KEY')):
            if secret: message=message.replace(secret,'[redacted]')
        if not schedule_safe_provider_retry(ident,folder,message):
            store.update(ident,status='failed',stage='Needs attention',error=message[:2400])
    finally:
        try:
            if not lease or (not lease[1].is_set() and store.get(ident)['worker']==lease[0]):storage.sync(ident)
        except Exception:store.update(ident,status='failed',stage='Storage sync interrupted',error='Some artifacts could not be uploaded. Retry with storage available.')
        with lock: active.discard(ident)


def regenerate(ident,number):
    """Archive generated outputs within this job; canonical references remain immutable."""
    from datetime import datetime, timezone
    with lock:
        job=store.get(ident)
        if not job or job['status'] not in ('complete','failed','paused','cancelled') or ident in active:
            raise ValueError('Stop the production before regenerating a scene')
        if database.postgres():
            for suffix in ('','-retry'):
                review=storage.read_json(ident,f'raw-{number:02d}{suffix}.operation.json')
                if review and not review.get('done'):raise ValueError('Resume the unresolved video operation first.')
            with store.db() as c:
                row=c.execute('SELECT status FROM jobs WHERE id=? FOR UPDATE',(ident,)).fetchone()
                if row[0] not in ('complete','failed','paused','cancelled'):raise ValueError('Production is active')
                c.execute('UPDATE jobs SET regenerate_scene=? WHERE id=?',(number,ident))
            enqueue(ident);return
    archive_scene(ident,number)
    enqueue(ident)

def archive_scene(ident,number):
        from datetime import datetime,timezone
        root=store.folder(ident).resolve()
        for suffix in ('','-retry'):
            journal=root/f'raw-{number:02d}{suffix}.operation.json'
            if journal.exists() and not store.read(journal).get('done') and not (root/f'raw-{number:02d}{suffix}.mp4').exists():
                raise ValueError('This scene has an unresolved video operation. Resume it first to avoid duplicate billing.')
        archive=root/'history'/datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
        archive.mkdir(parents=True)
        correction_path=root/'corrections.json'
        corrections=store.read(correction_path) if correction_path.exists() else {}
        for kind in ('still','motion'):
            review=root/f'review-{kind}-{number:02d}.json'
            if review.exists() and store.read(review).get('correction'):
                corrections[str(number)]=store.read(review)['correction']
        store.save(correction_path,corrections)
        names=[f'{prefix}-{number:02d}.{ext}' for prefix,ext in [('scene','png'),('raw','mp4'),('raw','operation.json'),('speech','wav'),('speech','mp3'),('voice','wav'),('clip','mp4'),('review-still','json'),('review-motion','json'),('check','png')]]
        names+=['final.mp4','production-kit.zip','quality-report.json','joined.mp4']
        names += [f'check-{number:02d}-{i}.png' for i in range(4)]
        import re
        names+= [p.name for p in root.iterdir() if p.is_file() and re.fullmatch(rf'(scene|raw|speech|voice|clip|review-still|review-motion|review-audio|review-narration|review-performance|check|packet)-{number:02d}(?:-[a-z0-9]+)*\.(?:png|mp4|wav|json|operation\.json)',p.name)]
        names=list(set(names))
        for name in names:
            target=(root/name).resolve()
            if target.parent!=root or not archive.resolve().is_relative_to(root): raise ValueError('Invalid artifact path')
            if target.exists(): target.replace(archive/name)
        storage.sync(ident)
        storage.forget(ident,names)
