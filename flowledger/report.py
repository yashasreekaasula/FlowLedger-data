# Generate a dependency-free offline HTML dashboard from real warehouse results.
import json,html
from pathlib import Path
from .warehouse import connect,now

def report(root):
    root=Path(root)
    with connect(root) as db:
        summary={k:db.execute(q).fetchone()[0] for k,q in {
          'orders':'SELECT COUNT(*) FROM fact_order','payments':'SELECT COUNT(*) FROM fact_payment',
          'quarantined':'SELECT COUNT(*) FROM quarantine',
          'exceptions':"SELECT COUNT(*) FROM order_reconciliation WHERE reconciliation_status!='MATCHED'",
          'revenue_inr':"SELECT COALESCE(SUM(amount_cents),0)/100.0 FROM fact_payment WHERE status='SUCCESS'"}.items()}
        queries={'Reconciliation exceptions':"SELECT * FROM order_reconciliation WHERE reconciliation_status!='MATCHED' ORDER BY ABS(difference_cents) DESC LIMIT 100",
        'Revenue by event date and region':'SELECT * FROM daily_revenue ORDER BY event_date,region',
        'Committed pipeline runs':'SELECT * FROM pipeline_run ORDER BY completed_at DESC LIMIT 30',
        'Quarantine reasons':'SELECT reason,COUNT(*) AS records FROM quarantine GROUP BY reason'}
        tables={name:[dict(r) for r in db.execute(q)] for name,q in queries.items()}
    esc=lambda x:html.escape(str(x))
    sections=[]
    for name,rows in tables.items():
        if rows:
            header=''.join('<th>'+esc(k)+'</th>' for k in rows[0])
            body=''.join('<tr>'+''.join('<td>'+esc(v)+'</td>' for v in r.values())+'</tr>' for r in rows)
            content=f'<div class="scroll"><table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table></div>'
        else:content='<p>No records.</p>'
        sections.append('<section><h2>'+esc(name)+'</h2>'+content+'</section>')
    cards=''.join(f'<article><small>{esc(k.replace("_"," "))}</small><strong>{v:,.2f}</strong></article>' for k,v in summary.items())
    page='''<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>FlowLedger | Pipeline evidence</title>
<style>body{font:15px system-ui;background:#0b1220;color:#e3eaf4;margin:0;padding:40px;max-width:1500px;margin:auto}h1{font-size:42px;margin-bottom:8px}h2{font-size:20px}p,small{color:#aabbd0}.tag{color:#72e9bf;letter-spacing:3px}header{padding:25px 0}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:16px}article,section{background:#142035;border:1px solid #293c55;border-radius:14px;padding:22px}strong{display:block;font-size:28px;padding-top:12px}section{margin-top:24px}.scroll{overflow:auto}table{border-collapse:collapse;width:100%;font-size:13px}td,th{padding:12px;text-align:left;border-bottom:1px solid #293c55;white-space:nowrap}th{color:#72e9bf}input{background:#142035;color:white;padding:12px;border:1px solid #465d79;border-radius:8px;margin-top:24px;width:280px}footer{padding:25px;color:#aabbd0}</style>
<header><div class="tag">FLOWLEDGER / DATA ENGINEERING LAB</div><h1>Follow the money. Trust the pipeline.</h1><p>Synthetic payment reconciliation · UTC event time · Actual local run results</p></header>'''+f'<div class="cards">{cards}</div><input id="search" placeholder="Filter rows: merchant, status, date…" aria-label="Filter table rows">'+''.join(sections)+f'<footer>Generated {esc(now())}. Static snapshot; rerun the report command to refresh. Exception table capped at 100 rows. Monetary *_cents columns are paise.</footer>'+'''<script>document.getElementById('search').addEventListener('input',e=>{let q=e.target.value.toLowerCase();document.querySelectorAll('tbody tr').forEach(r=>r.hidden=!r.textContent.toLowerCase().includes(q))})</script></html>'''
    out=root/'reports';out.mkdir(exist_ok=True)
    (out/'dashboard.html').write_text(page)
    (out/'metrics.json').write_text(json.dumps({'generated_at':now(),'summary':summary,'tables':tables},indent=2))
    print(out/'dashboard.html');return summary

def check(root):
    with connect(root) as db:
        assert not db.execute('PRAGMA foreign_key_check').fetchall(),'Broken foreign key'
        assert db.execute('SELECT COUNT(*) FROM fact_order').fetchone()[0]>0,'Empty orders'
        assert db.execute('SELECT COUNT(*) FROM pipeline_run').fetchone()[0]>0,'No successful run'
        for r in db.execute('SELECT * FROM pipeline_run'):
            assert r['input_rows']==r['inserted_rows']+r['rejected_rows']+r['duplicate_rows'],'Accounting mismatch'
    print('Warehouse invariants passed')
