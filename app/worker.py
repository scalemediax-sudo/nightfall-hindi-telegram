"""Durable PostgreSQL queue. Scale worker replicas independently from the web app."""
import os,time,uuid,threading,logging,shutil
from concurrent.futures import ThreadPoolExecutor
from . import store,auth,database,storage

def claim():
    import psycopg
    from psycopg.rows import dict_row
    connection=psycopg.connect(os.environ['DATABASE_URL'],autocommit=True,row_factory=dict_row)
    try:
        with connection.transaction():
            rows=connection.execute("SELECT id,owner FROM jobs q WHERE status='queued' AND (retry_at IS NULL OR retry_at<=%s) AND NOT EXISTS (SELECT 1 FROM jobs busy WHERE busy.owner=q.owner AND busy.status='running') ORDER BY enqueued NULLS FIRST,created LIMIT 50 FOR UPDATE SKIP LOCKED",(time.time(),)).fetchall()
            for row in rows:
                # One production per tenant at a time; parallel tenants use different keys.
                locked=connection.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS locked',('tenant:'+row['owner'],)).fetchone()['locked']
                if not locked:continue
                token=uuid.uuid4().hex
                connection.execute("UPDATE jobs SET status='running',stage='Worker assigned',worker=%s,heartbeat=%s,retry_at=NULL WHERE id=%s",(token,time.time(),row['id']))
                return row['id'],token,connection
        connection.close();return None
    except BaseException:connection.close();raise

def recover():
    # Never replay automatically after a worker disappears during a paid request.
    with store.db() as c:c.execute("UPDATE jobs SET status='paused',stage='Worker interrupted. Resume to reconcile saved operations.',worker=NULL WHERE status='running' AND heartbeat<?",(time.time()-180,))

def execute(claimed):
    from . import pipeline
    ident,token,connection=claimed;stop=threading.Event();lost=threading.Event()
    def heartbeat():
        while not stop.wait(10):
            try:
                changed=connection.execute("UPDATE jobs SET heartbeat=%s WHERE id=%s AND worker=%s AND status='running'",(time.time(),ident,token)).rowcount
                if not changed:break
            except Exception:lost.set();break
    thread=threading.Thread(target=heartbeat,daemon=True);thread.start()
    try:
        storage.hydrate(ident)
        scene=store.get(ident)['regenerate_scene']
        if scene:
            pipeline.archive_scene(ident,scene)
            store.update(ident,regenerate_scene=None)
        pipeline.work(ident,lease=(token,lost))
        if not lost.is_set() and store.get(ident)['status']=='complete':
            # Private object uploads completed before the job was marked complete.
            root=store.folder(ident).resolve()
            if root.parent!=store.DATA.resolve() or root.name!=ident:raise ValueError('Invalid worker cache path')
            shutil.rmtree(root)
    except Exception:
        logging.exception('Worker failed before production: %s',ident)
        store.update(ident,status='failed',stage='Worker recovery required',error='Could not load saved assets. Resume after checking storage availability.')
    finally:
        stop.set();thread.join(timeout=15);connection.close()

def main():
    if not database.postgres() or not storage.enabled():raise RuntimeError('Workers require DATABASE_URL and private S3 storage')
    store.init();auth.init()
    capacity=int(os.getenv('WORKER_CONCURRENCY','2'))
    with ThreadPoolExecutor(max_workers=capacity) as pool:
        futures=set();last_recovery=0
        while True:
            futures={f for f in futures if not f.done()}
            try:
                if time.monotonic()-last_recovery>60:recover();last_recovery=time.monotonic()
                if len(futures)<capacity:
                    item=claim()
                    if item:futures.add(pool.submit(execute,item));continue
            except Exception:logging.exception('Queue unavailable; will retry')
            time.sleep(2)

if __name__=='__main__':main()
