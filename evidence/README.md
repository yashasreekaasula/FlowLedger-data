# Recorded evidence

Open `dashboard.html` for a ready-to-view snapshot. Open `../README.md` to run it yourself.
These are real synthetic-data test results produced during preparation, not screenshots or
claims about the recipient's professional experience. See `../docs/VERIFICATION.md` for scope.

`warehouse_snapshot.sqlite` is a consistent SQLite backup, not a raw copy of an open WAL database.
Do not use this snapshot as a live stream root without its matching checkpoint; use it only for
read-only SQL exploration. For example:

```python
import sqlite3
with sqlite3.connect('evidence/warehouse_snapshot.sqlite') as db:
    print(db.execute('SELECT COUNT(*) FROM fact_payment').fetchone())
```

`user_verification.json` is the result of the bundled independent verification script run during
preparation. Running the script again replaces it with your own result.
