from pathlib import Path
import re

p = Path("pantry_app.py")
txt = p.read_text()

# ---------- helpers ----------
def replace_items_query(fn_block: str) -> str:
    """
    Replace any SELECT ... FROM items ORDER BY ... into:
    SELECT ... FROM items WHERE qty_available > 0 ORDER BY ...
    """
    # common patterns: "... FROM items ORDER BY name"
    fn_block2 = re.sub(
        r'FROM\s+items\s+ORDER\s+BY',
        'FROM items WHERE qty_available > 0 ORDER BY',
        fn_block,
        flags=re.I
    )
    # if they already have WHERE, leave it
    return fn_block2

CARD_TEMPLATE = """template = \"\"\"\
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Church Pantry</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; }
    a { text-decoration: none; }
    .nav { margin-bottom: 18px; }
    .nav a { margin-right: 12px; }

    .wrap { max-width: 1100px; }
    .title { font-size: 28px; font-weight: 700; margin-bottom: 8px; }
    .card { border: 1px solid #ddd; border-radius: 10px; padding: 18px; }
    .subtitle { font-size: 20px; font-weight: 700; margin: 8px 0 14px; }

    label { font-weight: 700; display: block; margin: 10px 0 6px; }
    select { width: 100%; padding: 8px; font-size: 14px; }

    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
      gap: 16px;
      margin-top: 12px;
    }
    .item {
      border: 1px solid #e3e3e3;
      border-radius: 12px;
      padding: 12px;
      background: #fff;
    }
    .imgbox {
      height: 150px;
      border-radius: 10px;
      background: #f3f3f3;
      display: flex;
      align-items: center;
      justify-content: center;
      overflow: hidden;
      margin-bottom: 10px;
    }
    .imgbox img {
      width: 100%;
      height: 100%;
      object-fit: contain;
      background: #fff;
    }
    .iname { font-weight: 700; margin-bottom: 4px; }
    .meta { color: #444; font-size: 13px; margin-bottom: 8px; }
    .qty { width: 100%; padding: 8px; font-size: 14px; }
    .btn { margin-top: 14px; padding: 10px 14px; font-size: 14px; cursor: pointer; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="title">Church Food Pantry</div>

    <div class="nav">
      <a href="/member/request">Member Request Form</a> |
      <a href="/manager/stock">Manager: Add Stock</a> |
      <a href="/manager/requests">Manager: Approvals</a> |
      <a href="/reports/stock">Report: Stock</a>
    </div>

    {% if msg %}
      <div class="card" style="background:#f0fff0; border-color:#bde5bd; margin-bottom:14px;">
        {{ msg }}
      </div>
    {% endif %}

    <div class="card">
      <div class="subtitle">Member Request Form</div>

      <form method="POST">
        <label>Member</label>
        <select name="member_id" required>
          {% for m in members %}
            <option value="{{ m['member_id'] }}">
              {{ m['member_code'] }} - {{ m['first_name'] }} {{ m['last_name'] }}
            </option>
          {% endfor %}
        </select>

        <label style="margin-top:14px;">Select Item Requested</label>

        <div class="grid">
          {% for it in items %}
            <div class="item">
              <div class="imgbox">
                {% if it['image_url'] %}
                  <img src="{{ it['image_url'] }}" alt="{{ it['name'] }}">
                {% else %}
                  <div style="color:#777; font-size:13px;">No Image</div>
                {% endif %}
              </div>

              <div class="iname">{{ it['name'] }}</div>
              <div class="meta">
                Unit: {{ it['unit'] }}<br>
                Available: {{ '%.0f'|format(it['qty_available']) }}
              </div>

              <input class="qty"
                     type="number"
                     name="qty_{{ it['item_id'] }}"
                     min="0"
                     max="{{ it['qty_available'] }}"
                     value="0"
                     placeholder="Qty requested">
            </div>
          {% endfor %}
        </div>

        <button class="btn" type="submit">Submit Request</button>
      </form>
    </div>
  </div>
</body>
</html>
\"\"\""""

def replace_template(fn_block: str) -> str:
    """
    Replace member request template with card grid template.
    Supports:
      1) template = """ ... """
      2) render_template_string(""" ... """, ...)
    """
    # Case 1: template = """..."""
    m = re.search(r'(?s)template\s*=\s*"""(.*?)"""', fn_block)
    if m:
        return fn_block[:m.start()] + CARD_TEMPLATE + fn_block[m.end():]

    # Case 2: render_template_string("""...""", ...)
    m2 = re.search(r'(?s)render_template_string\(\s*"""(.*?)"""\s*,', fn_block)
    if m2:
        # Replace just the """...""" part, keep args after comma
        before = fn_block[:m2.start()]
        after = fn_block[m2.end()-1:]  # keep the comma onward
        new_call = 'render_template_string(' + CARD_TEMPLATE.split("=",1)[1].strip() + ','
        return before + new_call + after

    raise SystemExit("❌ Could not find the member request HTML template block to replace.")

# ---------- locate /member/request function ----------
route_pat = r'@APP\.route\(\s*[\'"]/member/request[\'"]'
rm = re.search(route_pat, txt)
if not rm:
    raise SystemExit("❌ Could not find route decorator for /member/request")

fm = re.search(r'\ndef\s+([a-zA-Z_]\w*)\s*\(', txt[rm.end():])
if not fm:
    raise SystemExit("❌ Could not find function after /member/request")
func_name = fm.group(1)

start = re.search(rf'(?m)^def\s+{re.escape(func_name)}\s*\(', txt)
if not start:
    raise SystemExit("❌ Could not locate member_request function start")

next_route = re.search(r'(?m)^@APP\.route\(', txt[start.start()+1:])
end = (start.start()+1 + next_route.start()) if next_route else len(txt)

fn_block = txt[start.start():end]

# Apply A (card grid) + B (hide out-of-stock)
fn_block = replace_items_query(fn_block)
fn_block = replace_template(fn_block)

txt_new = txt[:start.start()] + fn_block + txt[end:]
p.write_text(txt_new)

print("✅ Applied A+B to /member/request (card layout + hide out-of-stock).")
