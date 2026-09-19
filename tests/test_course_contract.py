import copy
from types import SimpleNamespace
import pytest
from app import production, store, pipeline
from app.models import Request, Story, validate_links
from app.planning import demo_plan
from app.replicate_provider import Replicate

def test_resume_repairs_saved_invalid_script_before_review(tmp_path,monkeypatch):
    from app import auth,story_engine
    monkeypatch.setattr(store,'DATA',tmp_path)
    monkeypatch.delenv('DATABASE_URL',raising=False)
    store.init();auth.init()
    ident=store.create(Request(title='The last hotel').model_dump())
    folder=store.folder(ident)
    invalid={'hook':'A hotel waits.','entity':'The bellman.','lines':[{'speaker':'NARRATOR','text':'Too short.','emotion':'fear'} for _ in range(6)]}
    store.save(folder/'script-draft-1.json',invalid)
    class Provider:
        def __init__(self):self.story_calls=0
        def structured(self,prompt,schema):
            if schema is story_engine.Concept:return story_engine.Concept(hindi_title='होटल',english_title='The last hotel',transliteration='Hotel',category='Horror',hook_type='DREAD',hook='A bell rings.',brief='A traveler is trapped.',emotional_core='grief',entity_archetype='bellman',input_fit='hotel',originality_signature='traveler, hotel, bell rule, old wound, key ritual, final twist').model_dump()
            if schema is story_engine.Blueprint:return story_engine.Blueprint(authenticity_anchor='hotel',protagonist_need='safety',wrongness_signal='bell',isolation_lock='locked doors',entity_rule='answer no bells',historical_wound='abandoned guest',time_gap='twenty years',survival_by_wit_or_ritual='break the bell',grievance_named='betrayal',twist_recontextualizes='the exit',acts=[story_engine.Act(act=act,scenes=[i+1],action='action',audience_question='why',payoff_or_clue='clue') for i,act in enumerate(story_engine.ACTS)]).model_dump()
            if schema is Story:
                self.story_calls+=1
                return {'hook':'A hotel waits.','entity':'The bellman.','lines':[{'speaker':'Narrator' if i==0 else 'Meera','text':'one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen','emotion':'fear'} for i in range(6)]}
            if schema is story_engine.Critique:return {'criteria':[{'name':name,'passed':True,'evidence':'ok','correction':''} for name in ('hook','rupture','escalation','rule','truth','climax','twist','dialogue','fiction','originality')]}
            raise AssertionError(schema)
    provider=Provider()
    story_engine.build(provider,folder,Request(title='The last hotel').model_dump(),lambda *args:None)
    assert store.read(folder/'script-draft-1.json')==invalid
    assert (folder/'script-repair-1.json').exists()
    assert (folder/'script-repair-1-review.json').exists()
    assert provider.story_calls==1
    assert store.read(folder/'story.json')['lines'][0]['speaker']=='NARRATOR'

def test_six_beats_and_landscape_only():
    p=demo_plan({'title':'The last ride'})
    Story.model_validate(p['story'])
    with pytest.raises(ValueError):
        Story.model_validate({**p['story'],'lines':p['story']['lines']*2})
    with pytest.raises(ValueError):Request(title='Test',aspect='9:16')

def test_course_document_uses_selected_plate_and_exact_words():
    p=demo_plan({'title':'Test'})
    scene=p['scenes']['scenes'][2]
    location=p['locations']['backgrounds'][1]
    packet=production.packet(Request(title='Test').model_dump(),p['story']['lines'][2],scene,p['cast'],location,3)
    assert '00:15.0 – 00:22.5' in packet['document']
    assert location['angles'][2] in packet['still_prompt']
    assert p['cast']['characters'][1]['look'] in packet['still_prompt']
    assert 'Studio audio will be layered' in packet['animation_prompt']
    for label in ('ASSETS:', 'SPEAKER:', 'STILL IMAGE PROMPT', 'SYSTEM & ENVIRONMENT', 'PHYSICS/EFFECTS', 'NEGATIVE RESTRICTIONS'):
        assert label in packet['document']

def test_valid_continuing_shot_and_placeholder_rejection():
    p=demo_plan({'title':'Test'})
    p['scenes']['scenes'][1]=copy.deepcopy(p['scenes']['scenes'][0])
    validate_links(p['story'],p['cast'],p['locations'],p['scenes'])
    p['scenes']['scenes'][1]['still_prompt']='placeholder'
    with pytest.raises(ValueError,match='placeholder'):
        validate_links(p['story'],p['cast'],p['locations'],p['scenes'])

def test_uncertain_pruna_prediction_is_never_resubmitted(tmp_path):
    calls=[]
    def submit(**kwargs):
        calls.append(kwargs)
        raise TimeoutError('ambiguous submission')
    provider=object.__new__(Replicate)
    provider.checkpoint=lambda:None
    provider.client=SimpleNamespace(models=SimpleNamespace(get=lambda model:SimpleNamespace(latest_version=SimpleNamespace(id='v1'))),predictions=SimpleNamespace(create=submit))
    path=tmp_path/'image.png'
    with pytest.raises(TimeoutError):provider._predict('model',{},path,{'prompt':'test'})
    assert store.read(path.with_suffix('.operation.json'))['status']=='submitting'
    with pytest.raises(RuntimeError,match='uncertain'):provider._predict('model',{},path,{'prompt':'test'})
    assert len(calls)==1

def test_definitively_rejected_prediction_is_preserved_and_can_retry(tmp_path):
    from replicate.exceptions import ReplicateError
    calls=[]
    def submit(**kwargs):
        calls.append(kwargs)
        if len(calls)==1:raise ReplicateError(status=402,title='Insufficient credit')
        return SimpleNamespace(id='prediction-2',status='failed',output=None,error='rejected again')
    provider=object.__new__(Replicate)
    provider.checkpoint=lambda:None
    provider.client=SimpleNamespace(models=SimpleNamespace(get=lambda model:SimpleNamespace(latest_version=SimpleNamespace(id='v1'))),predictions=SimpleNamespace(create=submit))
    path=tmp_path/'image.png'
    with pytest.raises(ReplicateError):provider._predict('model',{},path,{'prompt':'test'})
    journal=path.with_suffix('.operation.json')
    assert store.read(journal)['retry_safe'] is True
    with pytest.raises(RuntimeError,match='rejected again'):provider._predict('model',{},path,{'prompt':'test'})
    assert len(calls)==2
    assert list((tmp_path/'history').rglob('*.operation.json'))

def test_safe_replicate_rejection_is_scheduled_without_manual_resume(tmp_path,monkeypatch):
    monkeypatch.setattr(store,'DATA',tmp_path)
    monkeypatch.delenv('DATABASE_URL',raising=False)
    monkeypatch.setenv('REPLICATE_SAFE_RETRY_DELAY_SECONDS','1')
    store.init(); ident=store.create(Request(title='Test').model_dump())
    folder=store.folder(ident)
    store.save(folder/'character-1.operation.json',{'retry_safe':True,'status':'rejected','done':True})
    assert pipeline.schedule_safe_provider_retry(ident,folder,'Replicate image generation failed')
    job=store.get(ident)
    assert job['status']=='queued' and job['retry_count']==1 and job['retry_at'] is not None

def test_semantic_plan_failure_stops_before_media(tmp_path,monkeypatch):
    from app import planning,story_engine
    p=demo_plan({'title':'Test'})
    monkeypatch.setattr(story_engine,'build',lambda *args:p['story'])
    for name in ('cast','locations','scenes'):store.save(tmp_path/(name+'.json'),p[name])
    class Reviewer:
        def structured(self,prompt,schema):
            return {'consistent':False,'issues':['Missing final location'],'correction':'Extract the home exterior.'}
    with pytest.raises(RuntimeError,match='before media generation'):
        planning.plan(Reviewer(),tmp_path,Request(title='Test').model_dump(),lambda *args:None)
    assert not store.read(tmp_path/'review-plan.json')['consistent']
