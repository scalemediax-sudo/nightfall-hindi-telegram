"""Private-chat Telegram interface, durable job routing and resumable delivery.

Polling is single-leader; video rendering remains in the existing worker queue.
No raw Telegram payloads, API keys, or Bot API URLs are logged or persisted.
"""
import hashlib,json,logging,os,re,secrets,tempfile,threading,time
from pathlib import Path
import httpx
from . import auth,database,media,pipeline,storage,store,zernio
from .models import Request

STATE='not_configured'

HELP='Send a story title to create a six-act horror film. I will update a progress message and send the finished video here.\n\n/status — latest production\n/cancel — stop latest production\n/resume — continue saved work\n/scene 3 — regenerate scene 3 (paid in live mode)\n/video — send latest finished video again\n/script — script and publishing metadata\n/approve — approve the finished video for YouTube\n/reject — reject the finished video\n/regenerate — choose a scene to regenerate\n/keep — keep a rejected video without regenerating\n/id — your chat ID\n/help — show commands'
VIDEO_ONLY='This Telegram is only for generating video. Please give us your idea and we will create a video.'
CASUAL_MESSAGES={
    'hi','hello','hey','hey there','hello there','hi there','how are you',
    'hey how are you','hello how are you','good morning','good afternoon','good evening',
    'namaste','kaise ho','कैसे हो','नमस्ते','क्या हाल है',
}
READY_VIDEO_REQUEST='pick any topic and generate video'
READY_VIDEO_TOPIC='The Last Bus That Never Reached Home'

class BotError(Exception):
    def __init__(self,code=0,retry_after=10,uncertain=False):
        super().__init__(f'Telegram request failed (code {code})')
        self.code=code;self.retry_after=retry_after;self.uncertain=uncertain

class API:
    def __init__(self,token):
        logging.getLogger('httpx').setLevel(logging.WARNING)
        self.client=httpx.Client(base_url='https://api.telegram.org/bot'+token+'/',timeout=40)
    def call(self,method,fields=None,files=None):
        try:
            response=self.client.post(method,data=fields if files else None,json=None if files else fields,files=files,timeout=180 if files else 40)
            body=response.json()
        except (httpx.HTTPError,ValueError):raise BotError(uncertain=True) from None
        if not body.get('ok'):
            if method=='editMessageText' and 'message is not modified' in body.get('description',''):return True
            raise BotError(body.get('error_code',response.status_code),body.get('parameters',{}).get('retry_after',10))
        return body['result']
    def text(self,chat,text):return self.call('sendMessage',{'chat_id':chat,'text':text[:4000]})
    def close(self):self.client.close()

def init():
    with store.db() as c:
        if database.postgres():c.execute('SELECT pg_advisory_xact_lock(7128503)')
        c.execute('CREATE TABLE IF NOT EXISTS telegram_cursor (bot TEXT PRIMARY KEY, next_update BIGINT NOT NULL)')
        c.execute("CREATE TABLE IF NOT EXISTS telegram_jobs (job_id TEXT PRIMARY KEY, bot TEXT NOT NULL, chat_id TEXT NOT NULL, user_id TEXT NOT NULL, message_id BIGINT, last_text TEXT, notified DOUBLE PRECISION DEFAULT 0, delivery TEXT DEFAULT 'pending', video_message BIGINT)")
        c.execute("CREATE TABLE IF NOT EXISTS telegram_reviews (job_id TEXT PRIMARY KEY, state TEXT NOT NULL DEFAULT 'awaiting_approval', prompt_message BIGINT, youtube_status TEXT NOT NULL DEFAULT 'not_requested', youtube_post_id TEXT, error TEXT)")
        c.execute('CREATE INDEX IF NOT EXISTS telegram_jobs_bot ON telegram_jobs(bot,delivery)')

def allowed(user,chat):
    allowed_ids={value.strip() for value in os.getenv('TELEGRAM_ALLOWED_USER_IDS','').split(',') if value.strip()}
    # A literal wildcard makes the public bot mode explicit. Groups and
    # channels are still rejected by Service.handle before this check.
    return str(chat)==str(user) and ('*' in allowed_ids or str(user) in allowed_ids)

def is_casual_message(text):
    normalized=re.sub(r'[^\w\s]', ' ', text.casefold(), flags=re.UNICODE)
    normalized=' '.join(normalized.split())
    return normalized in CASUAL_MESSAGES

def owner(bot,user):return 'tg_'+bot+'_'+str(user)

def ensure_user(name):
    with store.db() as c:c.execute('INSERT INTO users(username,password_hash,role) VALUES (?,?,?) ON CONFLICT(username) DO NOTHING',(name,'telegram-only','member'))

def latest(bot,chat,complete=False):
    with store.db() as c:
        row=c.execute("SELECT j.id FROM jobs j JOIN telegram_jobs t ON t.job_id=j.id WHERE t.bot=? AND t.chat_id=? "+("AND j.status='complete' " if complete else '')+'ORDER BY j.created DESC LIMIT 1',(bot,str(chat))).fetchone()
    return store.get(row[0]) if row else None

def review(job_id):
    with store.db() as c:
        row=c.execute('SELECT * FROM telegram_reviews WHERE job_id=?',(job_id,)).fetchone()
    return dict(row) if row else None

def status_text(job,detailed=False):
    if job['status'] in ('queued','running'):
        text='Generating your script.' if job['stage'] in ('Waiting for worker','Checking production tools') or job['stage'].startswith('Writing ') else 'Your script is completed, scenes are generated. Now generating your video.'
    else:text=f"{job['request']['title']}\n{job['progress']}% · {job['stage']}"
    if detailed and job['status'] in ('queued','running'):text=job['request']['title']+'\n'+text
    if job['status'] in ('failed','paused'):text+='\nProduction paused. '+(job.get('error') or '')[:1200]+'\nUse /resume to retry saved work. Check Replicate billing, quota, and the saved operation.'
    if job['status']=='cancelled':text+='\nStopped. Use /resume to continue.'
    if job['status']=='complete':text+='\nYour film is ready.'
    return text[:3900]

class Service:
    def __init__(self,api,bot):self.api=api;self.bot=str(bot)
    def handle(self,update):
        message=update.get('message',{})
        chat=message.get('chat',{});user=message.get('from',{})
        if chat.get('type')!='private' or user.get('is_bot') or not isinstance(message.get('text'),str):return
        cid=chat['id'];uid=user['id'];text=message['text'].strip()
        command=text.split(maxsplit=1)[0].split('@')[0].lower() if text else ''
        if not allowed(uid,cid):return
        if command in ('/start','/id'):
            self.api.text(cid,HELP)
            return
        if not command.startswith('/') and text.casefold()==READY_VIDEO_REQUEST:
            text=READY_VIDEO_TOPIC
            self.api.text(cid,f"Okay, I'm going with this topic: {text}.\n\nGenerating your script.")
        if not command.startswith('/') and is_casual_message(text):
            self.api.text(cid,VIDEO_ONLY)
            return
        name=owner(self.bot,uid);ensure_user(name)
        if command=='/help':self.api.text(cid,HELP);return
        if command.startswith('/'):
            job=latest(self.bot,cid,command in ('/video','/approve','/reject','/regenerate','/keep'))
            if command not in ('/status','/cancel','/resume','/video','/script','/scene','/approve','/reject','/regenerate','/keep'):self.api.text(cid,HELP);return
            if not job:self.api.text(cid,'No matching production yet. Send a story title.');return
            if command in ('/approve','/reject','/regenerate','/keep'):
                if job['status']!='complete':self.api.text(cid,'The latest video is not ready for review yet.');return
                current=review(job['id'])
                if not current:self.api.text(cid,'This finished video has not been delivered for review yet.');return
                if command=='/approve':
                    if current['state']=='published':self.api.text(cid,'This video has already been published to YouTube.');return
                    with store.db() as c:c.execute("UPDATE telegram_reviews SET state='approved',error=NULL WHERE job_id=?",(job['id'],))
                    if not zernio.publishing_enabled():self.api.text(cid,'Approved. YouTube publishing is disabled, so nothing has been posted.');return
                    self.publish_approved_video(job,cid);return
                if command=='/reject':
                    with store.db() as c:c.execute("UPDATE telegram_reviews SET state='rejected' WHERE job_id=?",(job['id'],))
                    self.api.text(cid,'Not approved. Should I regenerate the video? Reply /regenerate to choose a scene, or /keep to keep this version.');return
                if command=='/regenerate':
                    if current['state']!='rejected':self.api.text(cid,'Reject the video first with /reject, then choose what to regenerate.');return
                    self.api.text(cid,'Choose the scene to regenerate: /scene 1 through /scene 6. Live regeneration uses paid APIs.');return
                if command=='/keep':
                    with store.db() as c:c.execute("UPDATE telegram_reviews SET state='kept' WHERE job_id=?",(job['id'],))
                    self.api.text(cid,'Kept this version. It will not be published unless you approve it later.');return
            if command=='/status':self.api.text(cid,status_text(job,detailed=True));return
            if command=='/scene':
                number=text.partition(' ')[2].strip()
                if number not in {str(n) for n in range(1,7)}:self.api.text(cid,'Use /scene followed by a number from 1 to 6. Live regeneration uses paid APIs.');return
                try:pipeline.regenerate(job['id'],int(number))
                except ValueError as exc:self.api.text(cid,str(exc));return
                with store.db() as c:c.execute("UPDATE telegram_jobs SET delivery='pending',last_text=NULL WHERE job_id=?",(job['id'],))
                with store.db() as c:c.execute("UPDATE telegram_reviews SET state='awaiting_approval',prompt_message=NULL,youtube_status='not_requested',youtube_post_id=NULL,error=NULL WHERE job_id=?",(job['id'],))
                self.api.text(cid,'Scene regeneration queued. I will send the updated film when ready.');return
            if command=='/cancel':
                with store.db() as c:c.execute("UPDATE jobs SET cancel=1,status=CASE WHEN status='queued' THEN 'cancelled' ELSE status END WHERE id=? AND status IN ('running','queued')",(job['id'],))
                self.api.text(cid,'Stop requested. Saved work is retained.');return
            if command=='/resume':
                if job['status'] not in ('failed','paused','cancelled'):self.api.text(cid,'This production is already active or complete.');return
                pipeline.enqueue(job['id'])
                with store.db() as c:c.execute("UPDATE telegram_jobs SET delivery='pending',last_text=NULL WHERE job_id=?",(job['id'],))
                self.api.text(cid,'Resuming the saved production.');return
            if command=='/video':
                with store.db() as c:c.execute("UPDATE telegram_jobs SET delivery='pending' WHERE job_id=?",(job['id'],))
                self.api.text(cid,'I will send the finished video again.');return
            if command=='/script':
                package={k:storage.read_json(job['id'],k+'.json') for k in ('story','publishing')}
                self.api.call('sendDocument',{'chat_id':cid},files={'document':('script-and-publishing.json',json.dumps(package,ensure_ascii=False,indent=2).encode(),'application/json')});return
        try:request=Request(title=text,language=os.getenv('TELEGRAM_LANGUAGE','Hindi'),mode=os.getenv('TELEGRAM_PRODUCTION_MODE','live')).model_dump()
        except ValueError:self.api.text(cid,'Send a title between 2 and 120 characters.');return
        if request['mode']=='live' and os.getenv('ALLOW_PAID_GENERATION','false').lower()!='true':
            self.api.text(cid,'Live production needs Replicate billing and operator approval.');return
        ident=hashlib.sha256(f'{self.bot}:{update["update_id"]}'.encode()).hexdigest()[:32]
        try:store.create(request,name,ident=ident)
        except ValueError as exc:self.api.text(cid,str(exc));return
        with store.db() as c:c.execute('INSERT INTO telegram_jobs(job_id,bot,chat_id,user_id) VALUES (?,?,?,?) ON CONFLICT(job_id) DO NOTHING',(ident,self.bot,str(cid),str(uid)))
        store.folder(ident).mkdir(parents=True,exist_ok=True)
        if not database.postgres() and store.get(ident)['status']=='queued':pipeline.enqueue(ident)

    def notify(self):
        with store.db() as c:rows=[dict(r) for r in c.execute("SELECT t.* FROM telegram_jobs t JOIN jobs j ON j.id=t.job_id LEFT JOIN telegram_reviews r ON r.job_id=t.job_id WHERE t.bot=? AND t.delivery NOT IN ('blocked') AND (t.delivery IN ('pending','sending') OR j.status IN ('running','queued') OR (j.status='complete' AND t.delivery='sent' AND r.prompt_message IS NULL)) ORDER BY t.notified LIMIT 100",(self.bot,))]
        for row in rows:
            try:self.notify_one(row)
            except BotError as exc:
                if exc.code==429:raise
                if exc.code==403:
                    with store.db() as c:c.execute("UPDATE telegram_jobs SET delivery='blocked' WHERE job_id=?",(row['job_id'],))
                logging.warning('Telegram notification deferred (code %s)',exc.code)
            except Exception:logging.error('Telegram artifact delivery deferred; film remains saved')

    def notify_one(self,row):
        if not allowed(row['user_id'],row['chat_id']):return
        job=store.get(row['job_id']);text=status_text(job)
        if row['delivery']=='sending':
            # A prior process may have sent the video without saving the response.
            with store.db() as c:c.execute("UPDATE telegram_jobs SET delivery='uncertain' WHERE job_id=?",(job['id'],))
            self.api.text(row['chat_id'],'Video delivery was interrupted. If it did not arrive, use /video to request it again.');return
        if row['last_text']!=text and (time.time()-row['notified']>=10 or job['status'] not in ('running','queued')):
            if row['message_id']:
                try:self.api.call('editMessageText',{'chat_id':row['chat_id'],'message_id':row['message_id'],'text':text})
                except BotError as exc:
                    if exc.code!=400:raise
                    row['message_id']=None
            if not row['message_id']:row['message_id']=self.api.text(row['chat_id'],text)['message_id']
            with store.db() as c:c.execute('UPDATE telegram_jobs SET message_id=?,last_text=?,notified=? WHERE job_id=?',(row['message_id'],text,time.time(),job['id']))
        if job['status']=='complete' and row['delivery']=='pending':self.deliver(row,job)
        elif job['status']=='complete' and row['delivery']=='sent':self.ask_for_review(row,job)
        elif job['status'] in ('failed','paused','cancelled'):
            with store.db() as c:c.execute("UPDATE telegram_jobs SET delivery='stopped' WHERE job_id=?",(job['id'],))

    def deliver(self,row,job):
        with tempfile.TemporaryDirectory(prefix='nightfall-telegram-') as temp:
            video=Path(temp)/'film.mp4'
            if storage.enabled():
                with store.db() as c:record=c.execute("SELECT object_key FROM artifacts WHERE job_id=? AND name='final.mp4'",(job['id'],)).fetchone()
                if not record:raise RuntimeError('Final film is not available in storage')
                storage.client().download_file(os.environ['S3_BUCKET'],record[0],str(video))
            else:
                import shutil
                shutil.copyfile(store.folder(job['id'])/'final.mp4',video)
            if video.stat().st_size>48_000_000:
                compressed=Path(temp)/'telegram.mp4'
                media.ff('-i',video,'-c:v','libx264','-preset','fast','-b:v','3500k','-maxrate','4000k','-bufsize','8000k','-c:a','aac','-b:a','128k','-movflags','+faststart',compressed)
                video=compressed
            if video.stat().st_size>48_000_000:raise RuntimeError('Film exceeds the Telegram upload budget after compression')
            with store.db() as c:c.execute("UPDATE telegram_jobs SET delivery='sending' WHERE job_id=?",(job['id'],))
            try:
                with video.open('rb') as stream:result=self.api.call('sendVideo',{'chat_id':row['chat_id'],'caption':job['request']['title']+'\nSix acts · 16:9'+ ('\nDEMO: test cards, no AI footage.' if job['request']['mode']=='demo' else ''),'supports_streaming':'true'},files={'video':('film.mp4',stream,'video/mp4')})
            except BotError as exc:
                with store.db() as c:c.execute('UPDATE telegram_jobs SET delivery=? WHERE job_id=?',('sending' if exc.uncertain else 'pending',job['id']))
                raise
            with store.db() as c:c.execute("UPDATE telegram_jobs SET delivery='sent',video_message=? WHERE job_id=?",(result['message_id'],job['id']))
            with store.db() as c:c.execute("INSERT INTO telegram_reviews(job_id) VALUES (?) ON CONFLICT(job_id) DO NOTHING",(job['id'],))
            self.ask_for_review(row,job)

    def ask_for_review(self,row,job):
        current=review(job['id'])
        if not current or current['prompt_message'] is not None:return
        result=self.api.text(row['chat_id'],'Your video is ready for review. Is it approved for YouTube?\n\n/approve — approve for YouTube\n/reject — not approved')
        with store.db() as c:c.execute('UPDATE telegram_reviews SET prompt_message=? WHERE job_id=?',(result['message_id'],job['id']))

    def publish_approved_video(self,job,cid):
        """The only route that can call Zernio's publishing endpoint."""
        try:
            with tempfile.TemporaryDirectory(prefix='nightfall-youtube-') as temp:
                video=Path(temp)/'film.mp4'
                if storage.enabled():
                    with store.db() as c:record=c.execute("SELECT object_key FROM artifacts WHERE job_id=? AND name='final.mp4'",(job['id'],)).fetchone()
                    if not record:raise RuntimeError('Final film is not available in storage')
                    storage.client().download_file(os.environ['S3_BUCKET'],record[0],str(video))
                else:
                    import shutil
                    shutil.copyfile(store.folder(job['id'])/'final.mp4',video)
                result=zernio.publish_youtube(video,storage.read_json(job['id'],'publishing.json'),job['id'])
            post=result.get('post',result)
            post_id=post.get('_id',post.get('id',''))
            with store.db() as c:c.execute("UPDATE telegram_reviews SET state='published',youtube_status='published',youtube_post_id=?,error=NULL WHERE job_id=?",(post_id,job['id']))
            self.api.text(cid,'Approved and posted to YouTube.')
        except (RuntimeError,zernio.ZernioError) as exc:
            with store.db() as c:c.execute("UPDATE telegram_reviews SET youtube_status='failed',error=? WHERE job_id=?",(str(exc)[:500],job['id']))
            self.api.text(cid,'The video was approved, but YouTube publishing did not complete. Nothing will be retried automatically.')

def run(stop):
    global STATE
    token=os.getenv('TELEGRAM_BOT_TOKEN','').strip()
    if not token or os.getenv('TELEGRAM_ENABLED','true').lower()!='true':return
    STATE='connecting'
    api=API(token);leader=None
    try:
        bot=str(api.call('getMe')['id']);service=Service(api,bot)
        if database.postgres():
            import psycopg
            leader=psycopg.connect(os.environ['DATABASE_URL'],autocommit=True)
            while not leader.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0))',('telegram:'+bot,)).fetchone()[0]:
                STATE='standby'
                if stop.wait(3):return
        if api.call('getWebhookInfo').get('url'):raise RuntimeError('Remove the existing bot webhook before enabling polling')
        with store.db() as c:c.execute('INSERT INTO telegram_cursor VALUES (?,0) ON CONFLICT(bot) DO NOTHING',(bot,))
        STATE='polling'
        while not stop.is_set():
            try:
                if leader:leader.execute('SELECT 1') # Lost leader connection must stop polling.
                with store.db() as c:offset=c.execute('SELECT next_update FROM telegram_cursor WHERE bot=?',(bot,)).fetchone()[0]
                updates=api.call('getUpdates',{'offset':offset,'timeout':10,'allowed_updates':['message'],'limit':20})
                for update in updates:
                    try:service.handle(update)
                    except BotError as exc:
                        if exc.code!=403:raise
                    with store.db() as c:c.execute('UPDATE telegram_cursor SET next_update=? WHERE bot=?',(update['update_id']+1,bot))
                service.notify()
            except BotError as exc:
                logging.warning('Telegram temporarily unavailable (code %s)',exc.code)
                stop.wait(min(300,max(3,exc.retry_after)))
            except Exception:
                logging.error('Telegram processing paused; saved jobs are retained')
                if leader and leader.closed:break
                stop.wait(10)
    except Exception:logging.error('Telegram startup incomplete; check token, webhook and database settings')
    finally:
        STATE='stopped'
        api.close()
        if leader:leader.close()

def supervise(stop):
    if not os.getenv('TELEGRAM_BOT_TOKEN') or os.getenv('TELEGRAM_ENABLED','true').lower()!='true':return
    while not stop.is_set():
        run(stop)
        stop.wait(10)
