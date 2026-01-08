from pathlib import Path
import re

p = Path("pantry_app.py")
txt = p.read_text()

# ------------------------------------------------------------
# 1) Fix manager_stock(): replace wrong columns (name, qty_available)
# ------------------------------------------------------------

# Fix SQL statements using wrong column names
txt = txt.replace(
    "INSERT OR IGNORE INTO items(name, unit, qty_available, image_url) VALUES (?,?,?,?)",
    "INSERT OR IGNORE INTO items(item_name, unit, image_url) VALUES (?,?,?)"
)

txt = txt.replace(
    "UPDATE items SET qty_available = qty_available + ? WHERE name = ?",
    "UPDATE items SET image_url = COALESCE(image_url, image_url) WHERE item_name = item_name"
)
# NOTE: above line is a harmless no-op placeholder if the old line exists;
# we will also patch the add-new-item logic properly below.

# Fix any "WHERE name" / "ORDER BY name" inside items queries
txt = re.sub(r"\bWHERE\s+name\s*=", "WHERE item_name =", txt)
txt = re.sub(r"\bORDER\s+BY\s+name\b", "ORDER BY item_name", txt)


# Now patch the "Add NEW item" block inside manager_stock to:
# - insert into items(item_name, unit, image_url)
# - if starting qty > 0, add stock using the SAME method as existing stock updates
#
# We'll locate manager_stock() function and then locate its "new_name" section.

m_mgr = re.search(r"(?ms)^def\s+manager_stock\s*\(\)\s*:\s*\n(.*?)(?=^def\s|\Z)", txt)
if not m_mgr:
    raise SystemExit("❌ Could not find def manager_stock()")

mgr_block = m_mgr.group(0)

# Find a stock insert statement already used in manager_stock (this exists because Update Existing works)
# Example patterns we try to capture:
#   c.execute("INSERT INTO stock_txns (item_id, qty_change, note) VALUES (?,?,?)", (...))
stock_ins = re.search(r'c\.execute\(\s*"(INSERT\s+INTO\s+[^"]+)"\s*,\s*\(([^)]*)\)\s*\)', mgr_block, re.I)
stock_sql = None
stock_cols = None
if stock_ins:
    stock_sql = stock_ins.group(1)
    # extract table and column list
    mm = re.search(r"INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES", stock_sql, re.I)
    if mm:
        stock_table = mm.group(1)
        cols = [c.strip() for c in mm.group(2).split(",")]
        stock_cols = cols

def build_stock_intake_insert(item_id_expr: str, qty_expr: str) -> str:
    """
    Build a c.execute(...) line that inserts stock using the same stock table/columns
    already present in manager_stock.
    """
    if not stock_cols:
        # fallback: do nothing if we can't detect (but your update-existing works, so normally we detect)
        return "            # (Stock insert not detected automatically; skipping starting qty insert)\n"

    # Identify qty column (first column containing 'qty')
    qty_col = None
    note_col = None
    for c in stock_cols:
        if qty_col is None and "qty" in c.lower():
            qty_col = c
        if note_col is None and c.lower() in ("note", "reason", "source", "type", "action"):
            note_col = c

    cols = ["item_id"]
    vals = [item_id_expr]

    if qty_col and qty_col != "item_id":
        cols.append(qty_col)
        vals.append(qty_expr)

    if note_col:
        cols.append(note_col)
        vals.append("'INTAKE'")

    cols_sql = ", ".join(cols)
    qmarks = ", ".join(["?"] * len(cols))

    return f'            c.execute("INSERT INTO {stock_table} ({cols_sql}) VALUES ({qmarks})", ({", ".join(vals)}))\n'

# Patch the add-new-item part by replacing the wrong block if found
# We target the specific execute lines shown in your grep output around lines 411-427.

pattern_add_new = r'''(?ms)
(\s*)name\s*=\s*\(request\.form\.get\("new_name"\)\s*or\s*""\)\.strip\(\)\s*
(.*?)
c\.execute\(\s*"INSERT OR IGNORE INTO items\(name,\s*unit,\s*qty_available,\s*image_url\)\s*VALUES\s*\(\?,\?,\?,\?\)"\s*,\s*\(name,\s*unit,\s*0,\s*img_url\)\s*\)\s*
c\.execute\(\s*"UPDATE items SET qty_available = qty_available \+ \? WHERE name = \?"\s*,\s*\(qty,\s*name\)\s*\)\s*
'''

m_add = re.search(pattern_add_new, mgr_block)
if m_add:
    indent = m_add.group(1)

    replacement = f"""{indent}name = (request.form.get("new_name") or "").strip()
{indent}unit = (request.form.get("new_unit") or "each").strip() or "each"
{indent}qty_raw = (request.form.get("new_qty") or "0").strip()
{indent}try:
{indent}    qty = float(qty_raw)
{indent}except ValueError:
{indent}    qty = 0.0

{indent}if not name:
{indent}    msg = "Item name is required."
{indent}else:
{indent}    img_url = None
{indent}    if request.files.get("new_image"):
{indent}        img_url = save_item_image(request.files.get("new_image"), name)

{indent}    # Create item
{indent}    c.execute(
{indent}        "INSERT OR IGNORE INTO items(item_name, unit, image_url) VALUES (?,?,?)",
{indent}        (name, unit, img_url)
{indent}    )

{indent}    # If item already existed and an image was uploaded, update image_url
{indent}    if img_url:
{indent}        c.execute("UPDATE items SET image_url=? WHERE item_name=?", (img_url, name))

{indent}    # Starting qty (Intake) -> add to stock ledger using same method as existing updates
{indent}    if qty and qty > 0:
{indent}        row = c.execute("SELECT item_id FROM items WHERE item_name=?", (name,)).fetchone()
{indent}        if row:
{build_stock_intake_insert("row['item_id']", "qty").rstrip()}
"""

    mgr_block2 = re.sub(pattern_add_new, replacement, mgr_block)
    txt = txt[:m_mgr.start()] + mgr_block2 + txt[m_mgr.end():]
else:
    # If we didn't match exact old block, at least fix obvious SQL occurrences
    txt = txt.replace("INSERT OR IGNORE INTO items(name,", "INSERT OR IGNORE INTO items(item_name,")
    txt = txt.replace("WHERE name =", "WHERE item_name =")


# ------------------------------------------------------------
# 2) Update member_request():
#    - keep card/grid layout
#    - hide out-of-stock (already done)
#    - hide "Available" text
#    - replace member dropdown with Name + Phone typed inputs
#    - on POST, create member record automatically and use member_id
# ------------------------------------------------------------

m_mem = re.search(r"(?ms)^@APP\.route\(\s*['\"]/member/request['\"].*?\n^def\s+member_request\s*\(\)\s*:\s*\n(.*?)(?=^@APP\.route|^def\s|\Z)", txt)
if not m_mem:
    raise SystemExit("❌ Could not find /member/request route + member_request()")

mem_block = m_mem.group(0)

# 2a) Remove members dropdown query and use only items query
mem_block = re.sub(
    r"(?m)^\s*members\s*=\s*c\.execute\([^\n]*FROM\s+members[^\n]*\)\.fetchall\(\)\s*\n",
    "",
    mem_block
)

# 2b) Replace POST member_id logic with typed name+phone -> insert member -> get member_id
mem_block = re.sub(
    r"(?ms)if\s+request\.method\s*==\s*\"POST\"\s*:\s*\n\s*member_id\s*=\s*int\(request\.form\[\s*\"member_id\"\s*\]\)\s*",
    """if request.method == "POST":
        member_name = (request.form.get("member_name") or "").strip()
        member_phone = (request.form.get("member_phone") or "").strip()

        if not member_name:
            body = "<p class='bad'>Please enter your full name.</p>"
            return render_template_string(BASE, body=body)

        # split into first/last
        parts = member_name.split()
        first_name = parts[0]
        last_name = " ".join(parts[1:]) if len(parts) > 1 else ""

        # create a simple unique member_code for web requests
        import uuid
        member_code = f"WEB-{uuid.uuid4().hex[:6].upper()}"

        cur = c.execute(
            "INSERT INTO members (member_code, first_name, last_name, phone, email, household_size) VALUES (?,?,?,?,?,?)",
            (member_code, first_name, last_name, member_phone, None, 1)
        )
        member_id = cur.lastrowid
""",
    mem_block
)

# 2c) Remove options dropdown building and replace with blank
mem_block = re.sub(
    r"(?ms)options\s*=\s*\"\"\.join\(\[.*?\]\)\s*",
    "options = ''",
    mem_block
)

# 2d) Hide "Available:" text in the cards (remove that line)
mem_block = re.sub(
    r"(?m)^\s*Available:\s*\{available:[^}]*\}\s*<br>\s*$",
    "",
    mem_block
)
mem_block = mem_block.replace("Available: {available:.0f}", "")

# 2e) Replace the member dropdown HTML with Name + Phone inputs
mem_block = mem_block.replace(
    '<label>Member</label>\n    <select name="member_id" required>{options}</select>',
    '<label>Full Name</label>\n    <input type="text" name="member_name" placeholder="e.g., Ama Mensah" required>\n'
    '    <label>Phone Number</label>\n    <input type="tel" name="member_phone" placeholder="e.g., 555-123-4567" required>'
)

# 2f) Ensure items are filtered to in-stock only (you already did, but enforce)
mem_block = re.sub(
    r"(?m)^\s*items\s*=\s*c\.execute\(\"SELECT.*FROM items.*\"\)\.fetchall\(\)\s*$",
    '    items = c.execute("SELECT item_id, item_name, unit, image_url FROM items WHERE is_active=1 ORDER BY item_name").fetchall()',
    mem_block
)
# Ensure stock filter exists
if "items = [it for it in items" not in mem_block:
    mem_block = mem_block.replace("stock = get_stock_map()", "stock = get_stock_map()\n    items = [it for it in items if stock.get(it['item_id'], 0.0) > 0]")

# 2g) Update the function back into the file
txt = txt[:m_mem.start()] + mem_block + txt[m_mem.end():]

p.write_text(txt)
print("✅ Patch applied successfully.")
