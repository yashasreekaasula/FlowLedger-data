"""Independent process-level proof: batch, replay, stream, restart, redelivery.
Run from the project root. Uses an isolated temporary root; no existing data is deleted.
Writes evidence/user_verification.json only after every assertion succeeds.
"""
import json,os,shutil,sqlite3,subprocess,sys,tempfile
from pathlib import Path
from datetime import datetime,timezone

def run(root,*args):
    result=subprocess.run([sys.executable,'-m','flowledger','--root',str(root),*args],text=True,capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stdout+'\n'+result.stderr)
    return result.stdout

def counts(root):
    db=sqlite3.connect(root/'warehouse.sqlite')
    try:
        return {t:db.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in ['fact_order','fact_payment','quarantine','pipeline_run']}
    finally:db.close()

def main():
    with tempfile.TemporaryDirectory(prefix='flowledger-proof-') as tmp:
        root=Path(tmp);result={}
        print('Testing isolated batch...',flush=True)
        run(root,'generate','--orders','100')
        run(root,'batch');result['batch']=counts(root)
        assert result['batch']==dict(fact_order=100,fact_payment=90,quarantine=4,pipeline_run=1)
        print('Testing batch replay...',flush=True)
        run(root,'batch');assert counts(root)==result['batch']
        print('Testing stream and restart...',flush=True)
        run(root,'emit','--count','2','--interval','0')
        (root/'stream_inbox'/'unfinished.tmp').write_text('{incomplete')
        run(root,'stream','--once')
        result['stream']=counts(root)
        assert result['stream']['fact_payment']==92 and result['stream']['pipeline_run']==3
        run(root,'stream','--once');assert counts(root)==result['stream']
        print('Testing duplicate across stream files...',flush=True)
        source=next((root/'stream_inbox').glob('*.json'))
        shutil.copy2(source,root/'stream_inbox'/'redelivery.json')
        run(root,'stream','--once');result['redelivery']=counts(root)
        assert result['redelivery']['fact_payment']==92 and result['redelivery']['pipeline_run']==4
        run(root,'check')
        result['verified_at']=datetime.now(timezone.utc).isoformat()
        result['status']='PASS'
        Path('evidence').mkdir(exist_ok=True)
        Path('evidence/user_verification.json').write_text(json.dumps(result,indent=2))
        print(json.dumps(result,indent=2))
if __name__=='__main__':main()
