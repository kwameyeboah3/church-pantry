import re
from pathlib import Path

p = Path("pantry_app.py")
txt = p.read_text(encoding="utf-8")

m = re.search(r'(?ms)^def\s+manager_stock\(\):.*?(?=^@APP\.route|^def\s|\Z)', txt)
if not m:
    raise SystemExit("❌ Could not find def manager_stock() in pantry_app.py")

block = m.group(0)
orig = block

# fix SQL comma typos: ", FROM" ", WHERE" etc.
block = re.sub(r',\s*(FROM|WHERE|GROUP BY|ORDER BY)\b', r' \1', block)
block = re.sub(r'FROM\s+items\s*,\s*WHERE\b', r'FROM items WHERE', block)

# fix wrong column name: name -> item_name (common patterns)
repls = [
    ("items.name", "items.item_name"),
    (" i.name", " i.item_name"),
    ("WHERE name", "WHERE item_name"),
    ("ORDER BY name", "ORDER BY item_name"),
    (" name =", " item_name ="),
    (" name,", " item_name,"),
    ("(name,", "(item_name,"),
    (" INTO items(name", " INTO items(item_name"),
]
for a,b in repls:
    block = block.replace(a,b)

# keep changes
txt2 = txt[:m.start()] + block + txt[m.end():]
p.write_text(txt2, encoding="utf-8")

if block != orig:
    print("✅ Patched manager_stock() (SQL commas + name→item_name).")
else:
    print("ℹ️ No changes made (manager_stock already looks patched).")
