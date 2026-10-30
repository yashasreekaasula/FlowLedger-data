"""Execute the supplied read-only analytics examples, using Python's built-in SQLite."""
import argparse,json,sqlite3
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--root',type=Path,default=Path('data'))
p.add_argument('--file',type=Path,default=Path('sql/analytics.sql'))
a=p.parse_args()
db=sqlite3.connect((a.root/'warehouse.sqlite').resolve().as_uri()+'?mode=ro',uri=True)
db.row_factory=sqlite3.Row
try:
    output=[]
    query=''
    for line in a.file.read_text().splitlines(keepends=True):
        query+=line
        if sqlite3.complete_statement(query):
            output.append({'sql':query.strip(),'rows':[dict(r) for r in db.execute(query)]})
            query=''
    print(json.dumps(output,indent=2))
finally:db.close()
