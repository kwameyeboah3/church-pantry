import os
import sqlite3
from datetime import datetime
from flask import Flask, request, redirect, url_for, render_template_string, Response, abort, session
from email.message import EmailMessage
import smtplib
from flask import send_from_directory
from werkzeug.utils import secure_filename

# ============================================================
# Config
# ============================================================
APP = Flask(__name__)
APP.secret_key = os.environ.get("PANTRY_SECRET_KEY", "dev-secret-change-me")


# === LOCAL_UPLOAD_EMAIL_HELPERS_BEGIN ===

# Where uploaded item images are stored (local dev)
UPLOAD_FOLDER = os.environ.get("PANTRY_UPLOAD_FOLDER", os.path.join(os.path.dirname(__file__), "uploads"))
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

ALLOWED_IMAGE_EXT = {"png", "jpg", "jpeg", "webp", "gif"}

def allowed_image(filename: str) -> bool:
    if not filename or "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in ALLOWED_IMAGE_EXT

def save_uploaded_image(file_storage):
    """
    Save uploaded image file and return a URL path like /uploads/<filename>.
    Returns None if no file provided.
    """
    if not file_storage or not getattr(file_storage, "filename", ""):
        return None
    if not allowed_image(file_storage.filename):
        raise ValueError("Unsupported image type. Use png/jpg/jpeg/webp/gif.")
    fname = secure_filename(file_storage.filename)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"{ts}_{fname}"
    out_path = os.path.join(UPLOAD_FOLDER, fname)
    file_storage.save(out_path)
    return f"/uploads/{fname}"

@APP.route("/uploads/<path:filename>")
def uploaded_file(filename):
    # Serves uploaded images in local dev
    return send_from_directory(UPLOAD_FOLDER, filename)

def send_email(to_email: str, subject: str, body: str):
    """
    Optional email notifications (works if SMTP env vars are set).
    Safe: if not configured, it prints a warning and continues.
    """
    host = os.environ.get("SMTP_HOST")
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER")
    password = os.environ.get("SMTP_PASSWORD")
    use_tls = os.environ.get("SMTP_TLS", "1") == "1"
    from_email = os.environ.get("SMTP_FROM", user or "no-reply@example.com")

    if not host or not user or not password:
        print("⚠️ Email not sent (SMTP not configured). Set SMTP_HOST/SMTP_USER/SMTP_PASSWORD.")
        return

    msg = EmailMessage()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(host, port, timeout=20) as s:
        if use_tls:
            s.starttls()
        s.login(user, password)
        s.send_message(msg)

def notify_manager_new_request(req_id: int, member_name: str, phone: str, email: str):
    manager_email = os.environ.get("MANAGER_EMAIL")
    if not manager_email:
        print("⚠️ MANAGER_EMAIL not set; manager notification skipped.")
        return
    subject = f"New Pantry Request #{req_id}"
    body = (
        f"A new pantry request was submitted.\n\n"
        f"Request ID: {req_id}\n"
        f"Member: {member_name}\n"
        f"Phone: {phone}\n"
        f"Email: {email}\n\n"
        f"Open approvals:\n"
        f"{os.environ.get('PUBLIC_BASE_URL','http://127.0.0.1:5000')}/manager/requests\n"
    )
    send_email(manager_email, subject, body)

def acknowledge_requester(req_id: int, requester_email: str, member_name: str):
    subject = f"Pantry Request Received (#{req_id})"
    body = (
        f"Hello {member_name},\n\n"
        f"We received your pantry request (Request #{req_id}).\n"
        f"Our pantry manager will review and approve/reject it.\n\n"
        f"Thank you.\n"
    )
    send_email(requester_email, subject, body)

# === LOCAL_UPLOAD_EMAIL_HELPERS_END ===

# Render containers allow writing to /tmp. Locally you can set PANTRY_DB_PATH.
DB = os.environ.get("PANTRY_DB_PATH", os.path.join("/tmp", "church_pantry.db"))

MANAGER_PASSWORD = os.environ.get("PANTRY_MANAGER_PASSWORD", "ChangeMe123!")

_DB_READY = False


# ============================================================
# DB helpers
# ============================================================
def conn():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = ON;")
    return c


def init_db():
    c = conn()
    try:
        # Base tables
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS members (
                member_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL,
                phone       TEXT NOT NULL,
                email       TEXT NOT NULL,
                created_at  TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS items (
                item_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                sku           TEXT,
                item_name     TEXT NOT NULL UNIQUE,
                unit          TEXT NOT NULL,
                qty_available REAL NOT NULL DEFAULT 0,
                is_active     INTEGER NOT NULL DEFAULT 1,
                image_url     TEXT,
                expiry_date   TEXT,  -- optional, 'YYYY-MM-DD'
                created_at    TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS stock_movements (
                movement_id  INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id      INTEGER NOT NULL,
                movement_type TEXT NOT NULL, -- IN / OUT
                qty          REAL NOT NULL,
                note         TEXT,
                created_by   TEXT NOT NULL,
                created_at   TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY(item_id) REFERENCES items(item_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS requests (
                request_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                member_id    INTEGER NOT NULL,
                status       TEXT NOT NULL DEFAULT 'PENDING', -- PENDING/APPROVED/REJECTED
                note         TEXT,
                created_at   TEXT NOT NULL DEFAULT (datetime('now')),
                decided_at   TEXT,
                decided_by   TEXT,
                FOREIGN KEY(member_id) REFERENCES members(member_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS request_items (
                request_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id      INTEGER NOT NULL,
                item_id         INTEGER NOT NULL,
                qty_requested   REAL NOT NULL,
                FOREIGN KEY(request_id) REFERENCES requests(request_id) ON DELETE CASCADE,
                FOREIGN KEY(item_id) REFERENCES items(item_id) ON DELETE CASCADE
            );
            """
        )

        # Lightweight "migration": add columns if old DB exists without them
        cols = {r["name"] for r in c.execute("PRAGMA table_info(items)").fetchall()}
        if "image_url" not in cols:
            c.execute("ALTER TABLE items ADD COLUMN image_url TEXT")
        if "expiry_date" not in cols:
            c.execute("ALTER TABLE items ADD COLUMN expiry_date TEXT")
        if "is_active" not in cols:
            c.execute("ALTER TABLE items ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
        if "qty_available" not in cols:
            c.execute("ALTER TABLE items ADD COLUMN qty_available REAL NOT NULL DEFAULT 0")

        c.commit()
    finally:
        c.close()


def ensure_db():
    global _DB_READY
    if _DB_READY:
        return
    init_db()
    _DB_READY = True


@APP.before_request
def _ensure_db_before_request():
    ensure_db()


# ============================================================
# Auth
# ============================================================
def is_manager_logged_in() -> bool:
    if session.get("is_manager") is True:
        return True
    auth = request.authorization
    if auth and auth.username == "manager" and auth.password == MANAGER_PASSWORD:
        return True
    return False


def requires_manager_auth(func):
    def wrapper(*args, **kwargs):
        if not is_manager_logged_in():
            return redirect(url_for("manager_login", next=request.path))
        return func(*args, **kwargs)

    wrapper.__name__ = func.__name__
    return wrapper


@APP.context_processor
def inject_manager_auth():
    return {"is_manager": is_manager_logged_in()}


# ============================================================
# UI Templates (simple / no graphics)
# ============================================================
BASE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Church Pantry</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; max-width: 980px; }
    a { text-decoration: none; }
    .nav { margin-bottom: 14px; }
    .card { border: 1px solid #ddd; border-radius: 8px; padding: 14px; margin: 14px 0; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; }
    .row > div { flex: 1; min-width: 240px; }
    label { display: block; font-weight: bold; margin-top: 10px; }
    input, select, textarea { width: 100%; padding: 8px; margin-top: 6px; }
    table { border-collapse: collapse; width: 100%; margin-top: 12px; }
    th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; }
    th { background: #f7f7f7; text-align: left; }
    .muted { color: #666; font-size: 0.92em; }
    .btn { display: inline-block; padding: 10px 14px; border: 1px solid #333; border-radius: 8px; background: #fff; cursor: pointer; }
    .btn-primary { background: #222; color: #fff; border-color: #222; }
    .danger { color: #b00020; }
    .ok { color: #0b6; }
  </style>
</head>
<body>
  <h2>Church Pantry</h2>
  <div class="nav">
    <a href="{{ url_for('home') }}">Home</a> |
    <a href="{{ url_for('member_request') }}">Member Request Form</a> |
    {% if is_manager %}
      <a href="{{ url_for('manager_stock') }}">Manager: Add / Update Stock</a> |
      <a href="{{ url_for('manager_requests') }}">Manager: Approvals</a> |
      <a href="/manager/stock_view">Manager: Stock Viewer</a> |
      <a href="/manager/reports">Manager: Reports</a> |
      <a href="/manager/logout">Manager Logout</a>
    {% else %}
      <a href="/manager/login">Manager Login</a>
    {% endif %}
  </div>
  {{ body|safe }}
</body>
</html>
"""


# ============================================================
# Routes
# ============================================================
@APP.get("/")
def home():
    body = """
    <div class="card">
      <p><b>Member link:</b> <a href="/member/request">/member/request</a></p>
      <p><b>Manager link:</b> <a href="/manager/login">/manager/login</a></p>
      <p class="muted">Tip: Set PANTRY_MANAGER_PASSWORD on Render.</p>
    </div>
    """
    return render_template_string(BASE, body=body)


@APP.route("/manager/login", methods=["GET", "POST"])
def manager_login():
    if is_manager_logged_in():
        return redirect(url_for("manager_stock"))

    error = ""
    next_url = request.args.get("next") or url_for("manager_stock")
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()
        next_url = request.form.get("next") or next_url
        if username == "manager" and password == MANAGER_PASSWORD:
            session["is_manager"] = True
            return redirect(next_url)
        error = "Invalid username or password."

    body = render_template_string(
        """
        <div class="card" style="max-width:420px;">
          <h3>Manager Login</h3>
          {% if error %}
            <p class="danger">{{ error }}</p>
          {% endif %}
          <form method="POST">
            <input type="hidden" name="next" value="{{ next_url }}" />
            <label>Username</label>
            <input name="username" required />
            <label>Password</label>
            <input name="password" type="password" required />
            <p style="margin-top:12px;">
              <button class="btn btn-primary" type="submit">Sign In</button>
            </p>
          </form>
        </div>
        """,
        error=error,
        next_url=next_url,
    )
    return render_template_string(BASE, body=body)


@APP.get("/manager/logout")
def manager_logout():
    session.pop("is_manager", None)
    return redirect(url_for("home"))


@APP.get("/member/request")
def member_request():
    c = conn()
    try:
        items = c.execute(
            """
            SELECT item_id, item_name, unit, qty_available, image_url
            FROM items
            WHERE is_active=1 AND COALESCE(qty_available, 0) > 0
            ORDER BY item_name
            """
        ).fetchall()
    finally:
        c.close()

    body = render_template_string(
        """
        <div class="card">
          <h3>Member Request Form</h3>
          <form method="POST" action="{{ url_for('member_request_submit') }}">
            <div class="row">
              <div>
                <label>Your Name *</label>
                <input name="name" required />
              </div>
              <div>
                <label>Phone Number *</label>
                <input name="phone" required />
              </div>
              <div>
                <label>Email *</label>
                <input name="email" type="email" required />
              </div>
            </div>

            <label>Items Requested *</label>
            {% if items|length == 0 %}
              <p class="danger">No items available right now. Please check later.</p>
            {% else %}
              <table>
                <tr><th>Item</th> <th>Qty you want</th></tr>
                {% for it in items %}
                  <tr>
                    <td>
                      {% if it["image_url"] %}
                        <img src="{{ it['image_url'] }}" alt="{{ it['item_name'] }}" style="max-width:240px; max-height:240px; display:block; margin-bottom:10px;" />
                      {% endif %}
                      <b>{{ it["item_name"] }}</b><div class="muted">Unit: {{ it["unit"] }}</div>
                    </td>
                    <td>
                      <input type="number" step="1" min="0" name="qty_{{ it['item_id'] }}" value="0" />
                    </td>
                  </tr>
                {% endfor %}
              </table>
            {% endif %}

            <label>Notes (optional)</label>
            <textarea name="note" rows="3"></textarea>

            <p style="margin-top:12px;">
              <button class="btn btn-primary" type="submit">Submit Request</button>
            </p>
          </form>
        </div>
        """,
        items=items,
    )
    return render_template_string(BASE, body=body)


@APP.post("/member/request")
def member_request_submit():
    name = (request.form.get("name") or "").strip()
    phone = (request.form.get("phone") or "").strip()
    email = (request.form.get("email") or "").strip()
    note = (request.form.get("note") or "").strip()

    if not name or not phone or not email:
        abort(400, "Name, phone, and email are required.")

    c = conn()
    try:
        # Create member
        c.execute("INSERT INTO members (name, phone, email) VALUES (?, ?, ?)", (name, phone, email))
        member_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Create request
        c.execute("INSERT INTO requests (member_id, status, note) VALUES (?, 'PENDING', ?)", (member_id, note))
        request_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Pull available items
        items = c.execute("SELECT item_id FROM items WHERE is_active=1").fetchall()

        added = 0
        for it in items:
            item_id = it["item_id"]
            qty = float(request.form.get(f"qty_{item_id}") or 0)
            if qty > 0:
                c.execute(
                    "INSERT INTO request_items (request_id, item_id, qty_requested) VALUES (?, ?, ?)",
                    (request_id, item_id, qty),
                )
                added += 1

        if added == 0:
            # remove empty request
            c.execute("DELETE FROM requests WHERE request_id=?", (request_id,))
            c.execute("DELETE FROM members WHERE member_id=?", (member_id,))
            c.commit()
            body = '<div class="card danger"><b>No quantities selected.</b> Please go back and choose at least one item.</div>'
            return render_template_string(BASE, body=body), 400

        c.commit()
    finally:
        c.close()

    body = """
    <div class="card">
      <p class="ok"><b>Request submitted!</b></p>
      <p>Please wait for approval from the pantry manager.</p>
      <p><a href="/member/request">Submit another request</a></p>
    </div>
    """
    return render_template_string(BASE, body=body)


@APP.get("/manager/stock")
@requires_manager_auth
def manager_stock():
    c = conn()
    try:
        items = c.execute(
            "SELECT item_id, item_name, unit, qty_available, expiry_date, is_active FROM items ORDER BY item_name"
        ).fetchall()
    finally:
        c.close()

    body = render_template_string(
        """
        <div class="card">
          <h3>Manager: Add / Update Stock (Intake)</h3>

          <div class="row">
            <div class="card" style="flex:1;">
              <h4>Add NEW item</h4>
              <form method="POST" action="{{ url_for('manager_add_item') }}" enctype="multipart/form-data">
                <label>Item Name *</label>
                <input name="item_name" required />

                <label>Unit *</label>
                <input name="unit" required placeholder="e.g., bag, bottle, bar" />

                <label>Expiry Date (optional)</label>
                <input type="date" name="expiry_date" />

                <label>Image Upload (optional)</label>
                <input name="image_file" type="file" placeholder="https://..." />

                <label>Initial Quantity (optional)</label>
                <input type="number" step="1" min="0" name="initial_qty" value="0" />

                <p style="margin-top:12px;">
                  <button class="btn btn-primary" type="submit">Add Item</button>
                </p>
              </form>
            </div>

            <div class="card" style="flex:1;">
              <h4>Update EXISTING item</h4>
              <form method="POST" action="{{ url_for('manager_update_item') }}">
                <label>Select Item *</label>
                <select name="item_id" required>
                  {% for it in items %}
                    <option value="{{ it['item_id'] }}">{{ it['item_name'] }}</option>
                  {% endfor %}
                </select>

                <label>Add Quantity (Intake)</label>
                <input type="number" step="1" min="0" name="add_qty" value="0" />

                <label>Set Expiry Date (optional)</label>
                <input type="date" name="expiry_date_update" />

                <label>Set Active?</label>
                <select name="is_active">
                  <option value="1">Active</option>
                  <option value="0">Inactive</option>
                </select>

                <p style="margin-top:12px;">
                  <button class="btn btn-primary" type="submit">Update Item</button>
                </p>
              </form>
            </div>
          </div>
        </div>

        <div class="card">
          <h4>Current Items (Members will see these)</h4>
          <table>
            <tr>
              <th>Item</th><th>Unit</th><th>Qty</th><th>Expiry</th><th>Status</th>
            </tr>
            {% for it in items %}
              <tr>
                <td>{{ it["item_name"] }}</td>
                <td>{{ it["unit"] }}</td>
                <td>{{ '%.2f'|format(it["qty_available"]) }}</td>
                <td>{% if it["expiry_date"] %}{{ it["expiry_date"] }}{% else %}<span class="muted">—</span>{% endif %}</td>
                <td>{% if it["is_active"] == 1 %}<span class="ok">Active</span>{% else %}<span class="danger">Inactive</span>{% endif %}</td>
              </tr>
            {% endfor %}
          </table>
        </div>
        """,
        items=items,
    )
    return render_template_string(BASE, body=body)


@APP.post("/manager/add-item")
@requires_manager_auth
def manager_add_item():
    item_name = (request.form.get("item_name") or "").strip()
    unit = (request.form.get("unit") or "").strip()
    expiry_date = (request.form.get("expiry_date") or "").strip() or None
    image_url = None
    image_file = request.files.get("image_file")
    if image_file and image_file.filename:
        try:
            image_url = save_uploaded_image(image_file)
        except ValueError as exc:
            abort(400, str(exc))
    initial_qty = float(request.form.get("initial_qty") or 0)

    if not item_name or not unit:
        abort(400, "item_name and unit are required")

    c = conn()
    try:
        c.execute(
            "INSERT INTO items (item_name, unit, expiry_date, image_url, qty_available, is_active) VALUES (?, ?, ?, ?, ?, 1)",
            (item_name, unit, expiry_date, image_url, max(0, initial_qty)),
        )
        item_id = c.execute("SELECT item_id FROM items WHERE item_name=?", (item_name,)).fetchone()["item_id"]

        if initial_qty > 0:
            c.execute(
                "INSERT INTO stock_movements (item_id, movement_type, qty, note, created_by) VALUES (?, 'IN', ?, 'Initial stock', 'manager')",
                (item_id, initial_qty),
            )
        c.commit()
    finally:
        c.close()

    return redirect(url_for("manager_stock"))


@APP.post("/manager/update-item")
@requires_manager_auth
def manager_update_item():
    item_id = int(request.form.get("item_id"))
    add_qty = float(request.form.get("add_qty") or 0)
    expiry_update = (request.form.get("expiry_date_update") or "").strip() or None
    is_active = int(request.form.get("is_active") or 1)

    c = conn()
    try:
        if add_qty > 0:
            c.execute(
                "INSERT INTO stock_movements (item_id, movement_type, qty, note, created_by) VALUES (?, 'IN', ?, 'Intake', 'manager')",
                (item_id, add_qty),
            )
            c.execute(
                "UPDATE items SET qty_available = qty_available + ? WHERE item_id=?",
                (add_qty, item_id),
            )

        if expiry_update:
            c.execute("UPDATE items SET expiry_date=? WHERE item_id=?", (expiry_update, item_id))

        c.execute("UPDATE items SET is_active=? WHERE item_id=?", (is_active, item_id))

        c.commit()
    finally:
        c.close()

    return redirect(url_for("manager_stock"))


@APP.get("/manager/requests")
@requires_manager_auth
def manager_requests():
    c = conn()
    try:
        reqs = c.execute(
            """
            SELECT r.request_id, r.status, r.note, r.created_at,
                   m.name, m.phone, m.email
            FROM requests r
            JOIN members m ON m.member_id = r.member_id
            ORDER BY r.request_id DESC
            """
        ).fetchall()

        items_by_req = {}
        for r in reqs:
            rows = c.execute(
                """
                SELECT i.item_name, i.unit, ri.qty_requested
                FROM request_items ri
                JOIN items i ON i.item_id = ri.item_id
                WHERE ri.request_id = ?
                """,
                (r["request_id"],),
            ).fetchall()
            items_by_req[r["request_id"]] = rows
    finally:
        c.close()

    body = render_template_string(
        """
        <div class="card">
          <h3>Manager: Approvals | <a href="/manager/stock_view">Manager: Stock Viewer</a> | <a href="/manager/reports">Manager: Reports</a></h3>
          {% if reqs|length == 0 %}
            <p class="muted">No requests yet.</p>
          {% endif %}

          {% for r in reqs %}
            <div class="card">
              <div><b>Request #{{ r["request_id"] }}</b> — <b>{{ r["status"] }}</b></div>
              <div class="muted">Created: {{ r["created_at"] }}</div>
              <div style="margin-top:8px;">
                <b>Member:</b> {{ r["name"] }} |
                <b>Phone:</b> {{ r["phone"] }} |
                <b>Email:</b> {{ r["email"] }}
              </div>
              {% if r["note"] %}
                <div class="muted" style="margin-top:8px;"><b>Note:</b> {{ r["note"] }}</div>
              {% endif %}

              <table>
                <tr><th>Item</th><th>Qty</th></tr>
                {% for it in items_by_req[r["request_id"]] %}
                  <tr>
                    <td>{{ it["item_name"] }} <span class="muted">({{ it["unit"] }})</span></td>
                    <td>{{ '%.2f'|format(it["qty_requested"]) }}</td>
                  </tr>
                {% endfor %}
              </table>

              {% if r["status"] == "PENDING" %}
                <form method="POST" action="{{ url_for('manager_decide_request') }}" style="margin-top:10px;">
                  <input type="hidden" name="request_id" value="{{ r['request_id'] }}" />
                  <button class="btn btn-primary" name="decision" value="APPROVE" type="submit">Approve</button>
                  <button class="btn" name="decision" value="REJECT" type="submit">Reject</button>
                </form>
              {% endif %}
            </div>
          {% endfor %}
        </div>
        """,
        reqs=reqs,
        items_by_req=items_by_req,
    )
    return render_template_string(BASE, body=body)


@APP.get("/manager/reports")
@requires_manager_auth
def manager_reports():
    try:
        low_threshold = int(request.args.get("low", "5"))
    except ValueError:
        low_threshold = 5

    try:
        exp_days = int(request.args.get("exp", "30"))
    except ValueError:
        exp_days = 30

    c = conn()
    try:
        total_items = c.execute("SELECT COUNT(*) AS cnt FROM items").fetchone()["cnt"]
        active_items = c.execute("SELECT COUNT(*) AS cnt FROM items WHERE is_active=1").fetchone()["cnt"]
        inactive_items = c.execute("SELECT COUNT(*) AS cnt FROM items WHERE is_active!=1").fetchone()["cnt"]
        in_stock_items = c.execute(
            "SELECT COUNT(*) AS cnt FROM items WHERE COALESCE(qty_available, 0) > 0"
        ).fetchone()["cnt"]
        out_stock_items = c.execute(
            "SELECT COUNT(*) AS cnt FROM items WHERE COALESCE(qty_available, 0) <= 0"
        ).fetchone()["cnt"]
        total_qty = c.execute(
            "SELECT COALESCE(SUM(qty_available), 0) AS total_qty FROM items"
        ).fetchone()["total_qty"]

        low_stock = c.execute(
            """
            SELECT item_name, unit, qty_available
            FROM items
            WHERE is_active=1 AND COALESCE(qty_available, 0) > 0 AND qty_available <= ?
            ORDER BY qty_available ASC, item_name
            """,
            (low_threshold,),
        ).fetchall()

        expiring = c.execute(
            """
            SELECT item_name, unit, qty_available, expiry_date
            FROM items
            WHERE expiry_date IS NOT NULL
              AND date(expiry_date) <= date('now', ?)
            ORDER BY date(expiry_date), item_name
            """,
            (f"+{exp_days} day",),
        ).fetchall()

        status_rows = c.execute(
            "SELECT status, COUNT(*) AS cnt FROM requests GROUP BY status"
        ).fetchall()
        status_counts = {r["status"]: r["cnt"] for r in status_rows}
        total_requests = sum(status_counts.values())
        recent_requests = c.execute(
            "SELECT COUNT(*) AS cnt FROM requests WHERE date(created_at) >= date('now', '-30 day')"
        ).fetchone()["cnt"]

        gaps = c.execute(
            """
            SELECT r.request_id, i.item_name, i.unit, ri.qty_requested, i.qty_available, i.is_active
            FROM requests r
            JOIN request_items ri ON ri.request_id = r.request_id
            JOIN items i ON i.item_id = ri.item_id
            WHERE r.status='PENDING'
              AND (i.is_active != 1 OR ri.qty_requested > COALESCE(i.qty_available, 0))
            ORDER BY r.request_id DESC, i.item_name
            """
        ).fetchall()

        top_items = c.execute(
            """
            SELECT i.item_name, i.unit, SUM(ri.qty_requested) AS total_requested
            FROM request_items ri
            JOIN items i ON i.item_id = ri.item_id
            GROUP BY i.item_id
            ORDER BY total_requested DESC
            LIMIT 10
            """
        ).fetchall()

        movement_rows = c.execute(
            """
            SELECT movement_type, COALESCE(SUM(qty), 0) AS total_qty
            FROM stock_movements
            WHERE date(created_at) >= date('now', '-30 day')
            GROUP BY movement_type
            """
        ).fetchall()
        movement_totals = {r["movement_type"]: r["total_qty"] for r in movement_rows}
    finally:
        c.close()

    body = render_template_string(
        """
        <div class="card">
          <h3>Manager: Reports</h3>
          <p class="muted">Defaults: low-stock <= {{ low_threshold }}, expiring in {{ exp_days }} days.</p>
        </div>

        <div class="card">
          <h4>Inventory Summary</h4>
          <table>
            <tr><th>Total Items</th><td>{{ total_items }}</td></tr>
            <tr><th>Active Items</th><td>{{ active_items }}</td></tr>
            <tr><th>Inactive Items</th><td>{{ inactive_items }}</td></tr>
            <tr><th>In Stock Items</th><td>{{ in_stock_items }}</td></tr>
            <tr><th>Out of Stock Items</th><td>{{ out_stock_items }}</td></tr>
            <tr><th>Total Quantity (all items)</th><td>{{ '%.2f'|format(total_qty) }}</td></tr>
          </table>
        </div>

        <div class="card">
          <h4>Low Stock (<= {{ low_threshold }})</h4>
          <table>
            <tr><th>Item</th><th>Unit</th><th>Qty</th></tr>
            {% if low_stock|length == 0 %}
              <tr><td colspan="3" class="muted">No low-stock items.</td></tr>
            {% else %}
              {% for it in low_stock %}
                <tr>
                  <td>{{ it["item_name"] }}</td>
                  <td>{{ it["unit"] }}</td>
                  <td>{{ '%.2f'|format(it["qty_available"]) }}</td>
                </tr>
              {% endfor %}
            {% endif %}
          </table>
        </div>

        <div class="card">
          <h4>Expiring Soon (next {{ exp_days }} days)</h4>
          <table>
            <tr><th>Item</th><th>Unit</th><th>Qty</th><th>Expiry</th></tr>
            {% if expiring|length == 0 %}
              <tr><td colspan="4" class="muted">No items expiring soon.</td></tr>
            {% else %}
              {% for it in expiring %}
                <tr>
                  <td>{{ it["item_name"] }}</td>
                  <td>{{ it["unit"] }}</td>
                  <td>{{ '%.2f'|format(it["qty_available"]) }}</td>
                  <td>{{ it["expiry_date"] }}</td>
                </tr>
              {% endfor %}
            {% endif %}
          </table>
        </div>

        <div class="card">
          <h4>Request Activity</h4>
          <table>
            <tr><th>Status</th><th>Count</th></tr>
            <tr><td>Pending</td><td>{{ status_counts.get("PENDING", 0) }}</td></tr>
            <tr><td>Approved</td><td>{{ status_counts.get("APPROVED", 0) }}</td></tr>
            <tr><td>Rejected</td><td>{{ status_counts.get("REJECTED", 0) }}</td></tr>
            <tr><th>Total</th><th>{{ total_requests }}</th></tr>
            <tr><td>Last 30 days</td><td>{{ recent_requests }}</td></tr>
          </table>
        </div>

        <div class="card">
          <h4>Fulfillment Gaps (Pending Requests)</h4>
          <table>
            <tr><th>Request</th><th>Item</th><th>Unit</th><th>Requested</th><th>Available</th><th>Status</th></tr>
            {% if gaps|length == 0 %}
              <tr><td colspan="6" class="muted">No gaps found.</td></tr>
            {% else %}
              {% for g in gaps %}
                <tr>
                  <td>#{{ g["request_id"] }}</td>
                  <td>{{ g["item_name"] }}</td>
                  <td>{{ g["unit"] }}</td>
                  <td>{{ '%.2f'|format(g["qty_requested"]) }}</td>
                  <td>{{ '%.2f'|format(g["qty_available"] or 0) }}</td>
                  <td>{% if g["is_active"] != 1 %}Inactive{% else %}Insufficient{% endif %}</td>
                </tr>
              {% endfor %}
            {% endif %}
          </table>
        </div>

        <div class="card">
          <h4>Top Requested Items (All Time)</h4>
          <table>
            <tr><th>Item</th><th>Unit</th><th>Total Requested</th></tr>
            {% if top_items|length == 0 %}
              <tr><td colspan="3" class="muted">No requests yet.</td></tr>
            {% else %}
              {% for it in top_items %}
                <tr>
                  <td>{{ it["item_name"] }}</td>
                  <td>{{ it["unit"] }}</td>
                  <td>{{ '%.2f'|format(it["total_requested"]) }}</td>
                </tr>
              {% endfor %}
            {% endif %}
          </table>
        </div>

        <div class="card">
          <h4>Stock Movements (Last 30 Days)</h4>
          <table>
            <tr><th>Type</th><th>Total Qty</th></tr>
            <tr><td>IN</td><td>{{ '%.2f'|format(movement_totals.get("IN", 0) or 0) }}</td></tr>
            <tr><td>OUT</td><td>{{ '%.2f'|format(movement_totals.get("OUT", 0) or 0) }}</td></tr>
          </table>
        </div>
        """,
        low_threshold=low_threshold,
        exp_days=exp_days,
        total_items=total_items,
        active_items=active_items,
        inactive_items=inactive_items,
        in_stock_items=in_stock_items,
        out_stock_items=out_stock_items,
        total_qty=total_qty,
        low_stock=low_stock,
        expiring=expiring,
        status_counts=status_counts,
        total_requests=total_requests,
        recent_requests=recent_requests,
        gaps=gaps,
        top_items=top_items,
        movement_totals=movement_totals,
    )
    return render_template_string(BASE, body=body)


@APP.post("/manager/decide")
@requires_manager_auth
def manager_decide_request():
    req_id = int(request.form.get("request_id"))
    decision = request.form.get("decision")

    if decision not in ("APPROVE", "REJECT"):
        abort(400, "Invalid decision")

    c = conn()
    try:
        r = c.execute("SELECT status FROM requests WHERE request_id=?", (req_id,)).fetchone()
        if not r:
            abort(404, "Request not found")
        if r["status"] != "PENDING":
            return redirect(url_for("manager_requests"))

        if decision == "REJECT":
            c.execute(
                "UPDATE requests SET status='REJECTED', decided_at=?, decided_by='manager' WHERE request_id=?",
                (datetime.utcnow().isoformat(), req_id),
            )
            c.commit()
            return redirect(url_for("manager_requests"))

        # APPROVE: check stock and deduct
        rows = c.execute(
            """
            SELECT ri.item_id, ri.qty_requested, i.qty_available, i.item_name
            FROM request_items ri
            JOIN items i ON i.item_id = ri.item_id
            WHERE ri.request_id = ?
            """,
            (req_id,),
        ).fetchall()

        # ensure enough stock
        for row in rows:
            if row["qty_requested"] > row["qty_available"]:
                body = f"""
                <div class="card danger">
                  <b>Not enough stock to approve.</b><br/>
                  Item: {row['item_name']}<br/>
                  Requested: {row['qty_requested']}<br/>
                  Available: {row['qty_available']}
                </div>
                """
                return render_template_string(BASE, body=body), 400

        # deduct stock
        for row in rows:
            c.execute(
                "UPDATE items SET qty_available = qty_available - ? WHERE item_id=?",
                (row["qty_requested"], row["item_id"]),
            )
            c.execute(
                "INSERT INTO stock_movements (item_id, movement_type, qty, note, created_by) VALUES (?, 'OUT', ?, ?, 'manager')",
                (row["item_id"], row["qty_requested"], f"Approved request #{req_id}"),
            )

        c.execute(
            "UPDATE requests SET status='APPROVED', decided_at=?, decided_by='manager' WHERE request_id=?",
            (datetime.utcnow().isoformat(), req_id),
        )

        c.commit()
    finally:
        c.close()

    return redirect(url_for("manager_requests"))


@APP.route("/manager/stock_view")
@requires_manager_auth
def manager_stock_view():
    c = conn()
    try:
        items = c.execute("""
            SELECT item_id, item_name, unit, qty_available, expiry_date, is_active
            FROM items
            ORDER BY item_name
        """).fetchall()
    finally:
        c.close()

    rows = []
    for it in items:
        status = "Active" if (it["is_active"] == 1) else "Inactive"
        rows.append(f"""
        <tr>
          <td>{it["item_name"]}</td>
          <td>{it["unit"]}</td>
          <td>{it["qty_available"]:.2f}</td>
          <td>{it["expiry_date"] or ""}</td>
          <td>{status}</td>
        </tr>
        """)

    body = f"""
    <h2>Current Stock (Manager View)</h2>
    <p><a href="/manager/stock">Back to Intake</a></p>
    <table border="1" cellpadding="8" cellspacing="0">
      <tr><th>Item</th><th>Unit</th><th>Qty</th><th>Expiry</th><th>Status</th></tr>
      {''.join(rows) if rows else '<tr><td colspan="5">No items found</td></tr>'}
    </table>
    """
    return render_template_string(BASE, body=body)


# ============================================================
# Local run
# ============================================================
if __name__ == "__main__":
    APP.run(host="0.0.0.0", port=5000, debug=True)


@APP.route("/manager/review/<int:req_id>")
@requires_manager_auth
def manager_review_request(req_id: int):

    c = conn()

    req = c.execute("""
        SELECT request_id, member_name, phone, email, notes, status, created_at
        FROM requests
        WHERE request_id=?
    """, (req_id,)).fetchone()

    if not req:
        body = f"<h2>Request not found</h2><p>No request with ID {req_id}.</p>"
        return render_template_string(BASE, body=body), 404

    lines = c.execute("""
        SELECT rl.request_line_id, rl.item_id, rl.qty, i.item_name, i.unit, i.qty AS stock_qty, i.is_active
        FROM request_lines rl
        JOIN items i ON i.item_id = rl.item_id
        WHERE rl.request_id=?
        ORDER BY i.item_name
    """, (req_id,)).fetchall()

    rows = []
    has_issue = False
    for ln in lines:
        available = float(ln["stock_qty"] or 0.0)
        want = float(ln["qty"] or 0.0)
        status = "OK"
        if ln["is_active"] != 1:
            status = "INACTIVE ITEM"
            has_issue = True
        elif want > available:
            status = "INSUFFICIENT STOCK"
            has_issue = True

        rows.append(f"""
        <tr>
          <td>{ln["item_name"]}</td>
          <td>{ln["unit"]}</td>
          <td>{want:.2f}</td>
          <td>{available:.2f}</td>
          <td><b>{status}</b></td>
        </tr>
        """)

    warn = ""
    if has_issue and req["status"] == "PENDING":
        warn = "<p style='color:#b00;'><b>Warning:</b> Some items are inactive or have insufficient stock. Approval will be blocked until fixed.</p>"

    body = f"""
    <h2>Review Request #{req["request_id"]} — {req["status"]}</h2>
    <p><a href="/manager/requests">← Back to Approvals</a></p>

    <p><b>Member:</b> {req["member_name"]} | <b>Phone:</b> {req["phone"]} | <b>Email:</b> {req["email"]}</p>
    <p><b>Created:</b> {req["created_at"] or ""}</p>
    <p><b>Notes:</b> {req["notes"] or ""}</p>

    {warn}

    <table border="1" cellpadding="8" cellspacing="0" style="width:100%; max-width:900px;">
      <tr><th>Item</th><th>Unit</th><th>Qty Requested</th><th>Stock Available</th><th>Check</th></tr>
      {''.join(rows) if rows else '<tr><td colspan="5">No lines found</td></tr>'}
    </table>
    """
    return render_template_string(BASE, body=body)
