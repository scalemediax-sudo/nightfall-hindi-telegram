import json
import pytest
from fastapi.testclient import TestClient
from app import store,auth,pipeline,storage,telegram_bot as tg
from app.main import app

class FakeAPI:
    def __init__(self):self.calls=[];self.fail_video=False;self.block_chat=None
    def call(self,method,fields=None,files=None):
        self.calls.append((method,fields,files is not None))
        if fields and str(fields.get('chat_id'))==str(self.block_chat):raise tg.BotError(403)
        if method=='sendVideo' and self.fail_video:raise tg.BotError(uncertain=True)
        if method=='sendVideo':assert files['video'][1].read()
        return {'message_id':len(self.calls)}
    def text(self,chat,text):return self.call('sendMessage',{'chat_id':chat,'text':text})

@pytest.fixture
def service(tmp_path,monkeypatch):
    monkeypatch.setattr(store,'DATA',tmp_path)
    monkeypatch.delenv('DATABASE_URL',raising=False)
    monkeypatch.delenv('S3_BUCKET',raising=False)
    monkeypatch.setenv('TELEGRAM_PRODUCTION_MODE','demo')
    monkeypatch.setenv('TELEGRAM_BOT_TOKEN','')
    monkeypatch.setenv('TELEGRAM_ALLOWED_USER_IDS','101,202,303')
    monkeypatch.setattr(pipeline,'enqueue',lambda ident:True)
    store.init();auth.init();tg.init()
    return tg.Service(FakeAPI(),'900')

def update(text='Haunted road',uid=101,ident=1,kind='private'):
    return {'update_id':ident,'message':{'message_id':ident,'text':text,'chat':{'id':uid,'type':kind},'from':{'id':uid}}}

def test_title_routes_once_and_preserves_chat_owner(service):
    service.handle(update());service.handle(update())
    jobs=store.all_jobs();assert len(jobs)==1 and jobs[0]['owner']=='tg_900_101'
    assert jobs[0]['request']['title']=='Haunted road'
    assert jobs[0]['request']['audio_mode']=='elevenlabs'
    service.notify();service.notify()
    assert len(service.api.calls)==1

def test_casual_message_gets_video_only_reply_without_a_job(service):
    service.handle(update('Hey, how are you?'))
    assert store.all_jobs()==[]
    assert service.api.calls[-1][1]['text']==tg.VIDEO_ONLY

def test_ready_video_request_creates_a_real_production(service):
    service.handle(update('Pick any topic and generate video'))
    texts=[call[1].get('text','') for call in service.api.calls if call[0]=='sendMessage']
    assert texts==["Okay, I'm going with this topic: The Last Bus That Never Reached Home.\n\nGenerating your script."]
    jobs=store.all_jobs()
    assert len(jobs)==1 and jobs[0]['request']['title']==tg.READY_VIDEO_TOPIC

def test_allowed_private_chat_can_start_and_groups_are_ignored(service):
    service.handle(update(uid=303));service.handle(update(kind='group',ident=2))
    assert len(store.all_jobs())==1
    assert store.all_jobs()[0]['owner']=='tg_900_303'
    service.handle(update('/start',uid=303,ident=3))
    assert 'Send a story title' in service.api.calls[-1][1]['text']

def test_unlisted_private_chat_cannot_create_a_job(service):
    service.handle(update(uid=404))
    assert store.all_jobs()==[]

def test_wildcard_allows_any_private_chat_but_not_groups(service,monkeypatch):
    monkeypatch.setenv('TELEGRAM_ALLOWED_USER_IDS','*')
    service.handle(update(uid=404))
    service.handle(update(uid=405,ident=2,kind='group'))
    assert [job['owner'] for job in store.all_jobs()]==['tg_900_404']

def test_cancel_and_status_are_chat_scoped(service):
    service.handle(update());service.handle(update('Second film',202,2))
    service.handle(update('/cancel',101,3))
    assert tg.latest('900',101)['cancel']==1
    assert tg.latest('900',202)['cancel']==0
    service.handle(update('/status',202,4))
    assert 'Second film' in service.api.calls[-1][1]['text']

def complete(service,uid=101,ident=1):
    service.handle(update(uid=uid,ident=ident));job=tg.latest('900',uid)
    (store.folder(job['id'])/'final.mp4').write_bytes(b'test-video')
    store.update(job['id'],status='complete',stage='Complete',progress=100)
    return job

def test_video_delivery_and_restart_do_not_resend(service):
    complete(service)
    service.notify();tg.Service(service.api,'900').notify()
    assert [c[0] for c in service.api.calls].count('sendVideo')==1
    service.handle(update('/video',ident=2));service.notify()
    assert [c[0] for c in service.api.calls].count('sendVideo')==2

def test_uncertain_upload_requires_explicit_resend(service):
    complete(service);service.api.fail_video=True
    service.notify();service.notify();service.notify()
    assert [c[0] for c in service.api.calls].count('sendVideo')==1
    assert any('/video' in c[1].get('text','') for c in service.api.calls)

def test_blocked_chat_does_not_block_other_delivery(service):
    complete(service);complete(service,202,2);service.api.block_chat='101'
    service.notify()
    assert any(c[0]=='sendVideo' and c[1]['chat_id']=='202' for c in service.api.calls)

def test_no_dashboard_or_browser_api(service):
    with TestClient(app) as client:
        assert client.get('/healthz').json()['interface']=='telegram'
        for path in ('/','/login','/signup','/api/jobs','/static/app.js'):
            assert client.get(path).status_code==404

def test_live_requires_operator_approval_before_creating_job(service,monkeypatch):
    monkeypatch.setenv('TELEGRAM_PRODUCTION_MODE','live')
    monkeypatch.setenv('ALLOW_PAID_GENERATION','false')
    service.handle(update())
    assert store.all_jobs()==[]

def test_private_owner_receives_updates(service):
    complete(service);service.notify()
    assert any(call[0]=='sendVideo' for call in service.api.calls)

def test_scene_regeneration_cannot_target_another_chat(service,monkeypatch):
    own=complete(service);complete(service,202,2)
    regenerated=[]
    monkeypatch.setattr(pipeline,'regenerate',lambda ident,number:regenerated.append((ident,number)))
    service.handle(update('/scene 3',ident=3))
    service.handle(update('/scene 99',ident=4))
    assert regenerated==[(own['id'],3)]

def test_title_to_real_demo_video_delivery(service):
    from app import media
    if not media.available():pytest.skip('FFmpeg unavailable')
    service.handle(update('Telegram end-to-end demo'))
    job=tg.latest('900',101)
    pipeline.work(job['id'])
    assert store.get(job['id'])['status']=='complete',store.get(job['id'])['error']
    assert abs(media.duration(store.folder(job['id'])/'final.mp4')-45)<.15
    service.notify()
    assert any(call[0]=='sendVideo' for call in service.api.calls)
