import json, uuid, os, time
from datetime import datetime, timezone
from .config import DATA
from . import database

def db():
    return database.connection(DATA)

def init():
    with db() as c:
        if database.postgres(): c.execute('SELECT pg_advisory_xact_lock(7128501)')
        else: c.execute('PRAGMA journal_mode=WAL')
        c.execute('CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, request TEXT, status TEXT, stage TEXT, progress INTEGER, error TEXT, created TEXT, cancel INTEGER DEFAULT 0)')
        columns=[r[0] for r in c.execute("SELECT column_name FROM information_schema.columns WHERE table_name='jobs' AND table_schema=current_schema()")] if database.postgres() else [r[1] for r in c.execute('PRAGMA table_info(jobs)')]
        for name,definition in [('owner',"TEXT NOT NULL DEFAULT 'admin'"),('heartbeat','DOUBLE PRECISION'),('worker','TEXT'),('regenerate_scene','INTEGER'),('enqueued','DOUBLE PRECISION'),('retry_at','DOUBLE PRECISION'),('retry_count','INTEGER NOT NULL DEFAULT 0')]:
            if name not in columns:c.execute('ALTER TABLE jobs ADD COLUMN '+name+' '+definition)
        c.execute('CREATE INDEX IF NOT EXISTS jobs_owner_created ON jobs(owner,created)')
        c.execute('CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(status,enqueued)')
        c.execute('CREATE TABLE IF NOT EXISTS artifacts (job_id TEXT NOT NULL, name TEXT NOT NULL, object_key TEXT NOT NULL, digest TEXT NOT NULL, size BIGINT NOT NULL, PRIMARY KEY(job_id,name))')
        c.execute('CREATE TABLE IF NOT EXISTS script_history (job_id TEXT PRIMARY KEY, owner TEXT NOT NULL, concept TEXT NOT NULL, script TEXT NOT NULL, fingerprint TEXT NOT NULL, created DOUBLE PRECISION NOT NULL)')
        c.execute('CREATE INDEX IF NOT EXISTS script_history_owner ON script_history(owner,created)')
        if not database.postgres(): c.execute("UPDATE jobs SET status='paused', stage='Interrupted. Resume to continue.' WHERE status IN ('running','queued')")

def create(request, owner='admin',ident=None):
    ident=ident or uuid.uuid4().hex
    folder(ident) # Validate any supplied id before using it.
    with db() as c:
        if database.postgres():c.execute('SELECT username FROM users WHERE username=? FOR UPDATE',(owner,))
        existing=c.execute('SELECT owner FROM jobs WHERE id=?',(ident,)).fetchone()
        if existing:
            if existing[0]!=owner:raise ValueError('Job identity conflict')
            return ident
        count=c.execute("SELECT COUNT(*) FROM jobs WHERE owner=? AND status IN ('running','queued')",(owner,)).fetchone()[0]
        if count>=int(os.getenv('MAX_JOBS_PER_USER','3')): raise ValueError('You already have three active productions. Wait for one to finish.')
        if database.postgres() and c.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('running','queued')").fetchone()[0]>=int(os.getenv('MAX_QUEUED_JOBS','10000')):raise ValueError('The production queue is full. Please retry later.')
        c.execute('INSERT INTO jobs (id,request,status,stage,progress,error,created,cancel,owner) VALUES (?,?,?,?,?,?,?,?,?)',(ident,json.dumps(request,ensure_ascii=False),'queued','Waiting for worker',0,None,datetime.now(timezone.utc).isoformat(),0,owner))
        c.execute('UPDATE jobs SET enqueued=? WHERE id=?',(time.time(),ident))
    folder(ident).mkdir(parents=True)
    return ident

def folder(ident):
    if len(ident)!=32 or any(c not in '0123456789abcdef' for c in ident): raise ValueError('Invalid job id')
    return DATA/ident

def update(ident, **values):
    assert set(values)<= {'status','stage','progress','error','cancel','heartbeat','worker','regenerate_scene','enqueued','retry_at','retry_count'}
    with db() as c: c.execute('UPDATE jobs SET '+','.join(k+'=?' for k in values)+' WHERE id=?', (*values.values(),ident))

def get(ident):
    with db() as c: row=c.execute('SELECT * FROM jobs WHERE id=?',(ident,)).fetchone()
    if not row: return None
    item=dict(row); item['request']=json.loads(item['request']); return item

def all_jobs(owner=None):
    with db() as c:
        rows=c.execute('SELECT * FROM jobs '+('WHERE owner=? ' if owner else '')+'ORDER BY created DESC LIMIT 100',(owner,) if owner else ()).fetchall()
    result=[]
    for row in rows:
        item=dict(row);item['request']=json.loads(item['request']);result.append(item)
    return result

def save(path, value):
    tmp=path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8'); tmp.replace(path)
    from . import storage
    storage.publish_path(path)

def read(path): return json.loads(path.read_text(encoding='utf-8'))

def script_history(owner,exclude):
    with db() as c: rows=c.execute('SELECT concept,script,fingerprint FROM script_history WHERE owner=? AND job_id<>? ORDER BY created DESC LIMIT 30',(owner,exclude)).fetchall()
    return [dict(r) for r in rows]

def remember_script(ident,concept,script,fingerprint):
    job=get(ident)
    if not job:return
    with db() as c:c.execute('INSERT INTO script_history VALUES (?,?,?,?,?,?) ON CONFLICT(job_id) DO UPDATE SET concept=excluded.concept,script=excluded.script,fingerprint=excluded.fingerprint',(ident,job['owner'],json.dumps(concept,ensure_ascii=False),script,fingerprint,time.time()))

