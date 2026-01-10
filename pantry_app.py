import csv
import io
import os
import sqlite3
import zipfile
import base64
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from flask import Flask, request, redirect, url_for, render_template_string, Response, abort, session
from email.message import EmailMessage
import smtplib
from flask import send_from_directory
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash

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
    host = get_setting_value("smtp_host", os.environ.get("SMTP_HOST", ""))
    port_text = get_setting_value("smtp_port", os.environ.get("SMTP_PORT", "587"))
    user = get_setting_value("smtp_user", os.environ.get("SMTP_USER", ""))
    password = get_setting_value("smtp_password", os.environ.get("SMTP_PASSWORD", ""))
    use_tls = get_setting_value("smtp_tls", os.environ.get("SMTP_TLS", "1")) == "1"
    from_email = get_setting_value("smtp_from", os.environ.get("SMTP_FROM", "")) or (user or "no-reply@example.com")
    try:
        port = int(port_text)
    except ValueError:
        port = 587

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


def get_manager_emails() -> list[str]:
    c = conn()
    try:
        rows = c.execute(
            "SELECT email FROM managers WHERE is_active=1 AND email IS NOT NULL AND email != ''"
        ).fetchall()
    finally:
        c.close()
    emails = [r["email"] for r in rows]
    if not emails:
        fallback = get_setting_value("manager_email", os.environ.get("MANAGER_EMAIL", ""))
        if fallback:
            emails = [fallback]
    return emails


def csv_response(filename: str, rows: list[list[str]]) -> Response:
    output = io.StringIO()
    writer = csv.writer(output)
    for row in rows:
        writer.writerow(row)
    resp = Response(output.getvalue(), mimetype="text/csv")
    resp.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return resp


def parse_bool_status(value: str) -> int:
    if not value:
        return 1
    val = value.strip().lower()
    if val in ("inactive", "0", "false", "no"):
        return 0
    return 1


def get_setting_value(key: str, default: str = "") -> str:
    try:
        c = conn()
        try:
            row = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        finally:
            c.close()
    except sqlite3.Error:
        return default
    return row["value"] if row and row["value"] is not None else default


def set_setting_value(key: str, value: str) -> None:
    c = conn()
    try:
        c.execute(
            """
            INSERT INTO settings (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, value),
        )
        c.commit()
    finally:
        c.close()


def parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    try:
        return float(text)
    except ValueError:
        return None


def safe_extract_zip(zf: zipfile.ZipFile, target_dir: str, allow_prefixes: tuple[str, ...]):
    for member in zf.infolist():
        if member.is_dir():
            continue
        name = member.filename.replace("\\", "/")
        if name.startswith("/") or ".." in name:
            continue
        if not name.startswith(allow_prefixes):
            continue
        out_path = os.path.join(target_dir, name)
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with zf.open(member) as src, open(out_path, "wb") as dst:
            dst.write(src.read())


def to_csv_bytes(rows: list[list[str]]) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    for row in rows:
        writer.writerow(row)
    return output.getvalue().encode("utf-8")


def build_multipart(fields: dict[str, str], files: list[tuple[str, str, bytes, str]]):
    boundary = f"----pantryboundary{datetime.utcnow().timestamp()}".replace(".", "")
    body = io.BytesIO()

    def write_part(data: bytes):
        body.write(data)

    for key, value in fields.items():
        write_part(f"--{boundary}\r\n".encode("utf-8"))
        write_part(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"))
        write_part(f"{value}\r\n".encode("utf-8"))

    for name, filename, content, mime in files:
        write_part(f"--{boundary}\r\n".encode("utf-8"))
        write_part(
            f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'.encode("utf-8")
        )
        write_part(f"Content-Type: {mime}\r\n\r\n".encode("utf-8"))
        write_part(content)
        write_part(b"\r\n")

    write_part(f"--{boundary}--\r\n".encode("utf-8"))
    return body.getvalue(), f"multipart/form-data; boundary={boundary}"

def notify_manager_new_request(req_id: int, member_name: str, phone: str, email: str):
    manager_emails = get_manager_emails()
    if not manager_emails:
        print("⚠️ Manager email not set; manager notification skipped.")
        return
    public_base = get_setting_value(
        "public_base_url",
        os.environ.get("PUBLIC_BASE_URL", "http://127.0.0.1:5000"),
    )
    subject = f"New Pantry Request #{req_id}"
    body = (
        f"A new pantry request was submitted.\n\n"
        f"Request ID: {req_id}\n"
        f"Member: {member_name}\n"
        f"Phone: {phone}\n"
        f"Email: {email}\n\n"
        f"Open approvals:\n"
        f"{public_base}/manager/requests\n"
    )
    for manager_email in manager_emails:
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


def notify_request_rejected(req_id: int, requester_email: str, member_name: str, reason: str):
    subject = f"Pantry Request Update (#{req_id})"
    body = (
        f"Hello {member_name},\n\n"
        f"Your pantry request (Request #{req_id}) was not approved.\n"
        f"Reason: {reason or 'Not provided'}\n\n"
        f"Please contact the pantry manager if you have questions.\n"
    )
    send_email(requester_email, subject, body)

# === LOCAL_UPLOAD_EMAIL_HELPERS_END ===

# Render containers allow writing to /tmp. Locally you can set PANTRY_DB_PATH.
DB = os.environ.get("PANTRY_DB_PATH", os.path.join("/tmp", "church_pantry.db"))

MANAGER_PASSWORD = os.environ.get("PANTRY_MANAGER_PASSWORD", "ChangeMe123!")
CHURCH_NAME = os.environ.get("PANTRY_CHURCH_NAME", "The Church of Pentecost - Kansas District")
CHURCH_TAGLINE = os.environ.get("PANTRY_CHURCH_TAGLINE", "Serving families with dignity and care")
LOGO_URL = os.environ.get("PANTRY_LOGO_URL", "/static/church_logo.jpeg")
RENDER_BASE_URL = os.environ.get("PANTRY_RENDER_BASE_URL", "").rstrip("/")
RENDER_MANAGER_USER = os.environ.get("PANTRY_RENDER_MANAGER_USER", "")
RENDER_MANAGER_PASSWORD = os.environ.get("PANTRY_RENDER_MANAGER_PASSWORD", "")
PANTRY_SYNC_TOKEN = os.environ.get("PANTRY_SYNC_TOKEN", "")

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
                unit_cost     REAL,
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
                reject_reason TEXT,
                created_at   TEXT NOT NULL DEFAULT (datetime('now')),
                decided_at   TEXT,
                decided_by   TEXT,
                FOREIGN KEY(member_id) REFERENCES members(member_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS managers (
                manager_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                username      TEXT NOT NULL UNIQUE,
                email         TEXT,
                password_hash TEXT NOT NULL,
                is_active     INTEGER NOT NULL DEFAULT 1,
                created_at    TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS request_items (
                request_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id      INTEGER NOT NULL,
                item_id         INTEGER NOT NULL,
                qty_requested   REAL NOT NULL,
                FOREIGN KEY(request_id) REFERENCES requests(request_id) ON DELETE CASCADE,
                FOREIGN KEY(item_id) REFERENCES items(item_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
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
        if "unit_cost" not in cols:
            c.execute("ALTER TABLE items ADD COLUMN unit_cost REAL")

        req_cols = {r["name"] for r in c.execute("PRAGMA table_info(requests)").fetchall()}
        if "reject_reason" not in req_cols:
            c.execute("ALTER TABLE requests ADD COLUMN reject_reason TEXT")

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
def get_current_manager():
    manager_id = session.get("manager_id")
    if not manager_id:
        return None
    c = conn()
    try:
        row = c.execute(
            "SELECT manager_id, username, email, is_active FROM managers WHERE manager_id=?",
            (manager_id,),
        ).fetchone()
    finally:
        c.close()
    if not row or row["is_active"] != 1:
        session.pop("manager_id", None)
        session.pop("manager_username", None)
        return None
    return row


def has_managers() -> bool:
    c = conn()
    try:
        row = c.execute("SELECT COUNT(*) AS cnt FROM managers").fetchone()
    finally:
        c.close()
    return (row["cnt"] or 0) > 0


def ensure_default_manager():
    if has_managers():
        return
    c = conn()
    try:
        password_hash = generate_password_hash(MANAGER_PASSWORD)
        c.execute(
            "INSERT INTO managers (username, email, password_hash, is_active) VALUES (?, ?, ?, 1)",
            ("manager", os.environ.get("MANAGER_EMAIL", ""), password_hash),
        )
        c.commit()
    finally:
        c.close()


def check_manager_credentials(username: str, password: str) -> bool:
    if not has_managers():
        ensure_default_manager()
    c = conn()
    try:
        row = c.execute(
            "SELECT manager_id, username, password_hash, is_active FROM managers WHERE username=?",
            (username,),
        ).fetchone()
    finally:
        c.close()
    if not row or row["is_active"] != 1:
        return False
    return check_password_hash(row["password_hash"], password)


def is_manager_logged_in() -> bool:
    if get_current_manager():
        return True
    auth = request.authorization
    if auth and check_manager_credentials(auth.username, auth.password):
        return True
    return False


def requires_manager_auth(func):
    def wrapper(*args, **kwargs):
        if not is_manager_logged_in():
            return redirect(url_for("manager_login", next=request.path))
        return func(*args, **kwargs)

    wrapper.__name__ = func.__name__
    return wrapper


def current_manager_name() -> str:
    if session.get("manager_username"):
        return session.get("manager_username")
    auth = request.authorization
    if auth and check_manager_credentials(auth.username, auth.password):
        return auth.username
    return "manager"


def is_sync_token_valid() -> bool:
    token = PANTRY_SYNC_TOKEN or get_setting_value("sync_token") or session.get("sync_token") or ""
    if not token:
        return False
    header_token = request.headers.get("X-PANTRY-SYNC-TOKEN", "")
    form_token = request.form.get("sync_token") or ""
    return header_token == token or form_token == token


def requires_import_auth(func):
    def wrapper(*args, **kwargs):
        if is_manager_logged_in() or is_sync_token_valid():
            return func(*args, **kwargs)
        return redirect(url_for("manager_login", next=request.path))

    wrapper.__name__ = func.__name__
    return wrapper


def requires_sync_or_manager(func):
    def wrapper(*args, **kwargs):
        if is_manager_logged_in() or is_sync_token_valid():
            return func(*args, **kwargs)
        return redirect(url_for("manager_login", next=request.path))

    wrapper.__name__ = func.__name__
    return wrapper


@APP.context_processor
def inject_manager_auth():
    return {
        "is_manager": is_manager_logged_in(),
        "current_manager": get_current_manager(),
        "church_name": get_setting_value("church_name", CHURCH_NAME),
        "church_tagline": get_setting_value("church_tagline", CHURCH_TAGLINE),
        "logo_url": get_setting_value("logo_url", LOGO_URL),
    }


# ============================================================
# UI Templates
# ============================================================
BASE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>{{ church_name }}</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600&family=Source+Serif+4:opsz,wght@8..60,500;8..60,700&display=swap');
    :root {
      --ink: #0e1320;
      --muted: #5e6573;
      --brand: #0b2c5f;
      --accent: #d4a017;
      --surface: #ffffff;
      --soft: #f2f4f8;
      --line: #dde3ef;
      --shadow: 0 12px 28px rgba(10, 22, 52, 0.18);
    }
    * { box-sizing: border-box; }
    body {
      font-family: "Space Grotesk", "Helvetica Neue", Arial, sans-serif;
      color: var(--ink);
      margin: 0;
      background:
        radial-gradient(1200px 600px at 10% -10%, #efe4ff 0%, rgba(239,228,255,0) 58%),
        radial-gradient(900px 500px at 90% 0%, #e4f0ff 0%, rgba(228,240,255,0) 55%),
        linear-gradient(180deg, #fbfcff 0%, #f3f6fb 100%);
    }
    a { text-decoration: none; color: inherit; }
    .page { max-width: 1100px; margin: 0 auto; padding: 28px 20px 48px; }
    .site-header {
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
      align-items: center;
      justify-content: space-between;
      padding: 18px 20px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: linear-gradient(120deg, rgba(255,255,255,0.96) 0%, rgba(246,248,253,0.96) 100%);
      box-shadow: var(--shadow);
      margin-bottom: 22px;
      position: sticky;
      top: 16px;
      backdrop-filter: blur(6px);
      z-index: 5;
    }
    .brand-title {
      font-family: "Source Serif 4", "Times New Roman", serif;
      font-size: 26px;
      font-weight: 700;
      letter-spacing: 0.4px;
    }
    .brand-subtitle { color: var(--muted); font-size: 14px; }
    .brand {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .brand-logo {
      width: 140px;
      height: 140px;
      border-radius: 50%;
      border: 2px solid var(--accent);
      background: #fff;
      padding: 4px;
      object-fit: cover;
      box-shadow: 0 8px 18px rgba(10, 22, 52, 0.2);
    }
    .nav { display: flex; flex-wrap: wrap; gap: 10px; }
    .nav a {
      padding: 8px 12px;
      border-radius: 999px;
      background: var(--soft);
      border: 1px solid transparent;
      transition: transform 0.2s ease, background 0.2s ease, border-color 0.2s ease;
      font-size: 14px;
    }
    .nav a:hover { transform: translateY(-1px); border-color: var(--line); background: #ffffff; }
    .content { display: block; }
    .card {
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 18px;
      margin: 16px 0;
      background: var(--surface);
      box-shadow: var(--shadow);
      animation: rise 0.45s ease both;
    }
    .row { display: flex; gap: 12px; flex-wrap: wrap; }
    .row > div { flex: 1; min-width: 240px; }
    label { display: block; font-weight: 600; margin-top: 10px; }
    input, select, textarea {
      width: 100%;
      padding: 10px 12px;
      margin-top: 6px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: #fff;
      font-family: inherit;
    }
    table { border-collapse: collapse; width: 100%; margin-top: 12px; }
    th, td { border: 1px solid var(--line); padding: 10px; vertical-align: top; }
    th { background: #f0f3ec; text-align: left; font-weight: 600; }
    table tr:nth-child(even) td { background: #fafaf7; }
    .muted { color: var(--muted); font-size: 0.92em; }
    .btn {
      display: inline-block;
      padding: 10px 14px;
      border: 1px solid var(--ink);
      border-radius: 999px;
      background: #fff;
      cursor: pointer;
      font-weight: 600;
    }
    .btn-primary {
      background: var(--brand);
      color: #fff;
      border-color: var(--brand);
    }
    .danger { color: #b00020; font-weight: 600; }
    .ok { color: #0b6; font-weight: 600; }
    .badge {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 600;
      margin-right: 6px;
      background: #eef2ea;
      border: 1px solid var(--line);
    }
    .badge-warn { background: #fff4dd; border-color: #f0d59b; }
    .badge-alert { background: #ffe7e7; border-color: #f2b4b4; }
    .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; margin-top: 10px; }
    .stat-card {
      padding: 12px;
      border-radius: 14px;
      background: linear-gradient(140deg, #ffffff 0%, #f6f8f2 100%);
      border: 1px solid var(--line);
    }
    .stat-label { color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.6px; }
    .stat-value { font-size: 20px; font-weight: 700; margin-top: 6px; }
    .bar-row { display: flex; align-items: center; gap: 10px; margin: 8px 0; }
    .bar-label { width: 120px; font-size: 13px; color: var(--muted); }
    .bar-track { flex: 1; height: 10px; background: #e6ebf5; border-radius: 999px; overflow: hidden; }
    .bar { height: 10px; background: linear-gradient(90deg, #0b2c5f, #d4a017); border-radius: 999px; }
    .hero {
      display: grid;
      grid-template-columns: minmax(280px, 1.1fr) minmax(240px, 0.9fr);
      gap: 16px;
      align-items: center;
      padding: 18px;
      background: linear-gradient(120deg, rgba(11,44,95,0.08), rgba(212,160,23,0.12));
      border: 1px solid rgba(11,44,95,0.15);
    }
    .hero h3 { margin: 0 0 8px; font-size: 26px; }
    .hero p { margin: 0 0 10px; }
    .hero-card {
      padding: 14px;
      border-radius: 14px;
      background: rgba(255,255,255,0.9);
      border: 1px solid var(--line);
      text-align: center;
    }
    .hero-badges { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }
    .hero-badge {
      padding: 6px 10px;
      border-radius: 999px;
      background: #0b2c5f;
      color: #fff;
      font-size: 12px;
      font-weight: 600;
      letter-spacing: 0.4px;
      text-transform: uppercase;
    }
    .hero-image {
      width: 100%;
      height: 360px;
      border-radius: 18px;
      object-fit: cover;
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
      margin-top: 16px;
    }
    @keyframes rise { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
    @media (max-width: 720px) {
      .site-header { position: static; }
      .brand-title { font-size: 20px; }
      .hero { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="page">
    <header class="site-header">
      <div class="brand">
        <img class="brand-logo" src="{{ logo_url }}" alt="{{ church_name }} logo" />
        <div>
          <div class="brand-title">{{ church_name }}</div>
          <div class="brand-subtitle">{{ church_tagline }}</div>
        </div>
      </div>
      <nav class="nav">
        <a href="{{ url_for('home') }}">Home</a>
        <a href="{{ url_for('member_request') }}">Member Request Form</a>
        {% if is_manager %}
          <a href="{{ url_for('manager_stock') }}">Stock Intake</a>
          <a href="{{ url_for('manager_requests') }}">Approvals</a>
          <a href="/manager/stock_view">Stock View</a>
          <a href="/manager/reports">Reports</a>
          <a href="/manager/backup">Backup</a>
          <a href="/manager/import">Import</a>
          <a href="/manager/members">Members</a>
          <a href="/manager/managers">Users</a>
          <a href="/manager/settings">Settings</a>
          <a href="/manager/logout">Logout</a>
        {% else %}
          <a href="/manager/login">Login</a>
        {% endif %}
      </nav>
    </header>
    <main class="content">
      {{ body|safe }}
    </main>
  </div>
</body>
</html>
"""


# ============================================================
# Routes
# ============================================================
@APP.get("/")
def home():
    body = """
    <div class="card hero">
      <div>
        <h3>Welcome to the Pantry Portal</h3>
        <p class="muted">We serve our community with compassion and organization. Members can request items online.</p>
        <div class="hero-badges">
          <span class="hero-badge">Community</span>
          <span class="hero-badge">Care</span>
          <span class="hero-badge">Stewardship</span>
        </div>
        <img class="hero-image" src="/static/hero_pantry.webp" alt="Sharing food and pantry support" />
      </div>
      <div class="hero-card">
        <svg width="220" height="140" viewBox="0 0 220 140" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Community care">
          <rect x="4" y="8" width="212" height="124" rx="18" fill="#f2f4ff" stroke="#0b2c5f" stroke-width="2"/>
          <circle cx="70" cy="60" r="18" fill="#0b2c5f"/>
          <circle cx="150" cy="60" r="18" fill="#d4a017"/>
          <path d="M38 104 C58 84, 86 84, 106 104" fill="none" stroke="#0b2c5f" stroke-width="4"/>
          <path d="M114 104 C134 84, 162 84, 182 104" fill="none" stroke="#d4a017" stroke-width="4"/>
          <path d="M108 58 L112 58 L112 44 L116 44 L116 58 L120 58 L120 62 L116 62 L116 76 L112 76 L112 62 L108 62 Z" fill="#0b2c5f"/>
        </svg>
        <p class="muted" style="margin-top:10px;">A place of support, nourishment, and shared hope.</p>
      </div>
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
        if check_manager_credentials(username, password):
            c = conn()
            try:
                row = c.execute(
                    "SELECT manager_id, username FROM managers WHERE username=?",
                    (username,),
                ).fetchone()
            finally:
                c.close()
            if row:
                session["manager_id"] = row["manager_id"]
                session["manager_username"] = row["username"]
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
    session.pop("manager_id", None)
    session.pop("manager_username", None)
    return redirect(url_for("home"))


@APP.route("/manager/profile", methods=["GET", "POST"])
@requires_manager_auth
def manager_profile():
    manager = get_current_manager()
    if not manager:
        return redirect(url_for("manager_login"))

    message = ""
    error = ""
    if request.method == "POST":
        email = (request.form.get("email") or "").strip()
        current_password = (request.form.get("current_password") or "").strip()
        new_password = (request.form.get("new_password") or "").strip()
        confirm_password = (request.form.get("confirm_password") or "").strip()

        c = conn()
        try:
            row = c.execute(
                "SELECT password_hash FROM managers WHERE manager_id=?",
                (manager["manager_id"],),
            ).fetchone()
            if not row:
                error = "Manager not found."
            else:
                c.execute(
                    "UPDATE managers SET email=? WHERE manager_id=?",
                    (email, manager["manager_id"]),
                )
                if new_password:
                    if not current_password or not check_password_hash(row["password_hash"], current_password):
                        error = "Current password is incorrect."
                    elif new_password != confirm_password:
                        error = "New passwords do not match."
                    else:
                        c.execute(
                            "UPDATE managers SET password_hash=? WHERE manager_id=?",
                            (generate_password_hash(new_password), manager["manager_id"]),
                        )
                if not error:
                    message = "Profile updated."
                c.commit()
        finally:
            c.close()

        manager = get_current_manager()

    body = render_template_string(
        """
        <div class="card">
          <h3>Manager Profile</h3>
          {% if message %}<p class="ok">{{ message }}</p>{% endif %}
          {% if error %}<p class="danger">{{ error }}</p>{% endif %}
          <form method="POST">
            <label>Username</label>
            <input value="{{ manager['username'] }}" disabled />
            <label>Email</label>
            <input name="email" value="{{ manager['email'] or '' }}" />
            <hr style="margin:16px 0; border:0; border-top:1px solid var(--line);" />
            <label>Current Password</label>
            <input name="current_password" type="password" />
            <label>New Password</label>
            <input name="new_password" type="password" />
            <label>Confirm New Password</label>
            <input name="confirm_password" type="password" />
            <p style="margin-top:12px;">
              <button class="btn btn-primary" type="submit">Save Changes</button>
            </p>
          </form>
        </div>
        """,
        manager=manager,
        message=message,
        error=error,
    )
    return render_template_string(BASE, body=body)


@APP.route("/manager/managers", methods=["GET", "POST"])
@requires_manager_auth
def manager_users():
    message = ""
    error = ""
    if request.method == "POST":
        action = request.form.get("action")
        if action == "add":
            username = (request.form.get("username") or "").strip()
            email = (request.form.get("email") or "").strip()
            password = (request.form.get("password") or "").strip()
            if not username or not password:
                error = "Username and password are required."
            else:
                c = conn()
                try:
                    existing = c.execute(
                        "SELECT manager_id FROM managers WHERE username=?",
                        (username,),
                    ).fetchone()
                    if existing:
                        error = "Username already exists."
                    else:
                        c.execute(
                            "INSERT INTO managers (username, email, password_hash, is_active) VALUES (?, ?, ?, 1)",
                            (username, email, generate_password_hash(password)),
                        )
                        c.commit()
                        message = "Manager added."
                finally:
                    c.close()
        elif action == "toggle":
            manager_id = int(request.form.get("manager_id") or 0)
            c = conn()
            try:
                current = get_current_manager()
                if current and current["manager_id"] == manager_id:
                    error = "You cannot deactivate your own account."
                else:
                    active_count = c.execute(
                        "SELECT COUNT(*) AS cnt FROM managers WHERE is_active=1"
                    ).fetchone()["cnt"]
                    target = c.execute(
                        "SELECT is_active FROM managers WHERE manager_id=?",
                        (manager_id,),
                    ).fetchone()
                    if not target:
                        error = "Manager not found."
                    else:
                        new_state = 0 if target["is_active"] == 1 else 1
                        if new_state == 0 and active_count <= 1:
                            error = "At least one active manager is required."
                        else:
                            c.execute(
                                "UPDATE managers SET is_active=? WHERE manager_id=?",
                                (new_state, manager_id),
                            )
                            c.commit()
                            message = "Manager updated."
            finally:
                c.close()
        elif action == "edit":
            manager_id = int(request.form.get("manager_id") or 0)
            username = (request.form.get("username") or "").strip()
            email = (request.form.get("email") or "").strip()
            password = (request.form.get("password") or "").strip()
            is_active = int(request.form.get("is_active") or 1)
            if not username:
                error = "Username is required."
            else:
                c = conn()
                try:
                    current = get_current_manager()
                    target = c.execute(
                        "SELECT manager_id, is_active FROM managers WHERE manager_id=?",
                        (manager_id,),
                    ).fetchone()
                    if not target:
                        error = "Manager not found."
                    elif current and current["manager_id"] == manager_id and is_active == 0:
                        error = "You cannot deactivate your own account."
                    else:
                        active_count = c.execute(
                            "SELECT COUNT(*) AS cnt FROM managers WHERE is_active=1"
                        ).fetchone()["cnt"]
                        if target["is_active"] == 1 and is_active == 0 and active_count <= 1:
                            error = "At least one active manager is required."
                        else:
                            existing = c.execute(
                                "SELECT manager_id FROM managers WHERE username=?",
                                (username,),
                            ).fetchone()
                            if existing and existing["manager_id"] != manager_id:
                                error = "Username already exists."
                            else:
                                if password:
                                    c.execute(
                                        """
                                        UPDATE managers
                                        SET username=?, email=?, password_hash=?, is_active=?
                                        WHERE manager_id=?
                                        """,
                                        (
                                            username,
                                            email,
                                            generate_password_hash(password),
                                            is_active,
                                            manager_id,
                                        ),
                                    )
                                else:
                                    c.execute(
                                        """
                                        UPDATE managers
                                        SET username=?, email=?, is_active=?
                                        WHERE manager_id=?
                                        """,
                                        (username, email, is_active, manager_id),
                                    )
                                c.commit()
                                message = "Manager updated."
                finally:
                    c.close()
        elif action == "delete":
            manager_id = int(request.form.get("manager_id") or 0)
            confirm = request.form.get("confirm") == "yes"
            if not confirm:
                error = "Please confirm delete."
            else:
                c = conn()
                try:
                    current = get_current_manager()
                    target = c.execute(
                        "SELECT manager_id, is_active FROM managers WHERE manager_id=?",
                        (manager_id,),
                    ).fetchone()
                    if not target:
                        error = "Manager not found."
                    elif current and current["manager_id"] == manager_id:
                        error = "You cannot delete your own account."
                    else:
                        active_count = c.execute(
                            "SELECT COUNT(*) AS cnt FROM managers WHERE is_active=1"
                        ).fetchone()["cnt"]
                        if target["is_active"] == 1 and active_count <= 1:
                            error = "At least one active manager is required."
                        else:
                            c.execute("DELETE FROM managers WHERE manager_id=?", (manager_id,))
                            c.commit()
                            message = "Manager deleted."
                finally:
                    c.close()

    c = conn()
    try:
        managers = c.execute(
            "SELECT manager_id, username, email, is_active, created_at FROM managers ORDER BY username"
        ).fetchall()
    finally:
        c.close()

    body = render_template_string(
        """
        <div class="card">
          <h3>Manager Users</h3>
          {% if message %}<p class="ok">{{ message }}</p>{% endif %}
          {% if error %}<p class="danger">{{ error }}</p>{% endif %}

          <h4>Add Manager</h4>
          <form method="POST">
            <input type="hidden" name="action" value="add" />
            <div class="row">
              <div>
                <label>Username</label>
                <input name="username" required />
              </div>
              <div>
                <label>Email</label>
                <input name="email" type="email" />
              </div>
              <div>
                <label>Password</label>
                <input name="password" type="password" required />
              </div>
            </div>
            <p style="margin-top:12px;">
              <button class="btn btn-primary" type="submit">Add Manager</button>
            </p>
          </form>
        </div>

        <div class="card">
          <h4>Existing Managers</h4>
          <table>
            <tr><th>Username</th><th>Email</th><th>Status</th><th>Created</th><th>Actions</th></tr>
            {% if managers|length == 0 %}
              <tr><td colspan="5" class="muted">No managers found.</td></tr>
            {% else %}
              {% for m in managers %}
                <tr>
                  <td>
                    <form method="POST">
                      <input type="hidden" name="action" value="edit" />
                      <input type="hidden" name="manager_id" value="{{ m['manager_id'] }}" />
                      <input name="username" value="{{ m['username'] }}" required />
                  </td>
                  <td>
                      <input name="email" value="{{ m['email'] or '' }}" />
                  </td>
                  <td>
                      <select name="is_active">
                        <option value="1" {% if m["is_active"] == 1 %}selected{% endif %}>Active</option>
                        <option value="0" {% if m["is_active"] != 1 %}selected{% endif %}>Inactive</option>
                      </select>
                      <div class="muted" style="margin-top:6px;">New password (optional)</div>
                      <input name="password" type="password" />
                  </td>
                  <td>{{ m["created_at"] }}</td>
                  <td>
                      <button class="btn" type="submit">Save</button>
                    </form>
                    <form method="POST" style="margin-top:8px;">
                      <input type="hidden" name="action" value="delete" />
                      <input type="hidden" name="manager_id" value="{{ m['manager_id'] }}" />
                      <label class="muted" style="display:block;">
                        <input type="checkbox" name="confirm" value="yes" />
                        Confirm delete
                      </label>
                      <button class="btn" type="submit">Delete</button>
                    </form>
                  </td>
                </tr>
              {% endfor %}
            {% endif %}
          </table>
        </div>
        """,
        managers=managers,
        message=message,
        error=error,
    )
    return render_template_string(BASE, body=body)


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
          <form method="POST" action="{{ url_for('member_request_preview') }}">
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
                <label>Email (optional)</label>
                <input name="email" type="email" />
              </div>
            </div>

            <label>Items Requested *</label>
            {% if items|length == 0 %}
              <p class="danger">No items available right now. Please check later.</p>
            {% else %}
              <table>
                <tr><th>Item</th><th>Item</th></tr>
                {% for it in items %}
                  {% if loop.index0 % 2 == 0 %}
                    <tr>
                  {% endif %}
                  <td>
                    {% if it["image_url"] %}
                      <img src="{{ it['image_url'] }}" alt="{{ it['item_name'] }}" style="max-width:240px; max-height:240px; display:block; margin-bottom:10px;" />
                    {% endif %}
                    <b>{{ it["item_name"] }}</b><div class="muted">Unit: {{ it["unit"] }}</div>
                    <div style="margin-top:10px;">
                      <label class="muted">Qty you want</label>
                      <input type="number" step="1" min="0" name="qty_{{ it['item_id'] }}" value="0" />
                    </div>
                  </td>
                  {% if loop.index0 % 2 == 1 %}
                    </tr>
                  {% endif %}
                {% endfor %}
                {% if items|length % 2 == 1 %}
                  <td></td></tr>
                {% endif %}
              </table>
            {% endif %}

            <label>Recommendations for items you would like us to have (optional)</label>
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


@APP.post("/member/request/preview")
def member_request_preview():
    name = (request.form.get("name") or "").strip()
    phone = (request.form.get("phone") or "").strip()
    email = (request.form.get("email") or "").strip()
    note = (request.form.get("note") or "").strip()

    if not name or not phone:
        abort(400, "Name and phone are required.")

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

    selected = []
    for it in items:
        qty = float(request.form.get(f"qty_{it['item_id']}") or 0)
        if qty > 0:
            selected.append(
                {
                    "item_id": it["item_id"],
                    "item_name": it["item_name"],
                    "unit": it["unit"],
                    "qty": qty,
                    "image_url": it["image_url"],
                }
            )

    if not selected:
        body = '<div class="card danger"><b>No quantities selected.</b> Please go back and choose at least one item.</div>'
        return render_template_string(BASE, body=body), 400

    body = render_template_string(
        """
        <div class="card">
          <h3>Review Your Request</h3>
          <p class="muted">Please confirm the items and quantities before submitting.</p>
          <div class="row">
            <div>
          <p><b>Name:</b> {{ name }}</p>
          <p><b>Phone:</b> {{ phone }}</p>
          {% if email %}<p><b>Email:</b> {{ email }}</p>{% endif %}
              {% if note %}<p><b>Notes:</b> {{ note }}</p>{% endif %}
            </div>
          </div>
          <table>
            <tr><th>Item</th><th>Unit</th><th>Qty</th></tr>
            {% for it in selected %}
              <tr>
                <td>
                  {% if it["image_url"] %}
                    <img src="{{ it['image_url'] }}" alt="{{ it['item_name'] }}" style="max-width:120px; max-height:120px; display:block; margin-bottom:8px;" />
                  {% endif %}
                  {{ it["item_name"] }}
                </td>
                <td>{{ it["unit"] }}</td>
                <td>{{ '%.2f'|format(it["qty"]) }}</td>
              </tr>
            {% endfor %}
          </table>
          <form method="POST" action="{{ url_for('member_request_submit') }}">
            <input type="hidden" name="name" value="{{ name }}" />
            <input type="hidden" name="phone" value="{{ phone }}" />
            <input type="hidden" name="email" value="{{ email }}" />
            <input type="hidden" name="note" value="{{ note }}" />
            {% for it in selected %}
              <input type="hidden" name="qty_{{ it['item_id'] }}" value="{{ it['qty'] }}" />
            {% endfor %}
            <p style="margin-top:12px;">
              <button class="btn btn-primary" type="submit">Confirm and Submit</button>
              <a class="btn" href="/member/request">Edit</a>
            </p>
          </form>
        </div>
        """,
        name=name,
        phone=phone,
        email=email,
        note=note,
        selected=selected,
    )
    return render_template_string(BASE, body=body)


@APP.post("/member/request/submit")
def member_request_submit():
    name = (request.form.get("name") or "").strip()
    phone = (request.form.get("phone") or "").strip()
    email = (request.form.get("email") or "").strip()
    note = (request.form.get("note") or "").strip()

    if not name or not phone:
        abort(400, "Name and phone are required.")

    c = conn()
    try:
        # Reuse member if email or phone already exists
        member_row = c.execute(
            "SELECT member_id FROM members WHERE email=? OR phone=? ORDER BY created_at DESC LIMIT 1",
            (email, phone),
        ).fetchone()
        if member_row:
            member_id = member_row["member_id"]
            c.execute(
                "UPDATE members SET name=?, phone=?, email=? WHERE member_id=?",
                (name, phone, email, member_id),
            )
        else:
            c.execute("INSERT INTO members (name, phone, email) VALUES (?, ?, ?)", (name, phone, email))
            member_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Create request
        c.execute("INSERT INTO requests (member_id, status, note) VALUES (?, 'PENDING', ?)", (member_id, note))
        request_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Pull available items
        items = c.execute(
            "SELECT item_id, item_name, unit FROM items WHERE is_active=1 AND COALESCE(qty_available, 0) > 0"
        ).fetchall()

        added = 0
        selected_items = []
        for it in items:
            item_id = it["item_id"]
            qty = float(request.form.get(f"qty_{item_id}") or 0)
            if qty > 0:
                c.execute(
                    "INSERT INTO request_items (request_id, item_id, qty_requested) VALUES (?, ?, ?)",
                    (request_id, item_id, qty),
                )
                selected_items.append({"item_name": it["item_name"], "unit": it["unit"], "qty": qty})
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

    try:
        notify_manager_new_request(request_id, name, phone, email)
        if email:
            acknowledge_requester(request_id, email, name)
    except Exception as exc:
        print(f"⚠️ Email notification failed: {exc}")

    body = render_template_string(
        """
        <div class="card">
          <h3>Request Submitted</h3>
          <p class="ok"><b>Thank you! Your request has been received.</b></p>
          <p><b>Request ID:</b> {{ request_id }}</p>
          <p>Please wait for approval from the pantry manager.</p>
          <table>
            <tr><th>Item</th><th>Unit</th><th>Qty</th></tr>
            {% for it in selected_items %}
              <tr>
                <td>{{ it["item_name"] }}</td>
                <td>{{ it["unit"] }}</td>
                <td>{{ '%.2f'|format(it["qty"]) }}</td>
              </tr>
            {% endfor %}
          </table>
          <p style="margin-top:12px;">
            <button class="btn" onclick="window.print()">Print Confirmation</button>
            <a class="btn btn-primary" href="/member/request">Submit another request</a>
          </p>
        </div>
        """,
        request_id=request_id,
        selected_items=selected_items,
    )
    return render_template_string(BASE, body=body)


@APP.get("/manager/stock")
@requires_manager_auth
def manager_stock():
    q = (request.args.get("q") or "").strip()
    sort = (request.args.get("sort") or "name").strip()
    direction = (request.args.get("dir") or "asc").strip().lower()
    message = (request.args.get("msg") or "").strip()
    error = (request.args.get("err") or "").strip()
    sort_map = {
        "name": "item_name",
        "qty": "qty_available",
        "expiry": "expiry_date",
        "status": "is_active",
    }
    order_col = sort_map.get(sort, "item_name")
    order_dir = "DESC" if direction == "desc" else "ASC"

    c = conn()
    try:
        items_all = c.execute(
            "SELECT item_id, item_name FROM items ORDER BY item_name"
        ).fetchall()

        params = []
        where_clause = ""
        if q:
            where_clause = "WHERE item_name LIKE ? OR unit LIKE ?"
            like = f"%{q}%"
            params.extend([like, like])

        items_table = c.execute(
            f"""
            SELECT item_id, item_name, unit, qty_available, expiry_date, is_active, unit_cost
            FROM items
            {where_clause}
            ORDER BY {order_col} {order_dir}, item_name
            """,
            params,
        ).fetchall()
    finally:
        c.close()

    body = render_template_string(
        """
        <div class="card">
          <h3>Stock Intake</h3>
          {% if message %}
            <p class="ok">{{ message }}</p>
          {% endif %}
          {% if error %}
            <p class="danger">{{ error }}</p>
          {% endif %}

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

                <label>Unit Cost (optional)</label>
                <input type="number" step="0.01" min="0" name="unit_cost" value="" placeholder="e.g., 2.50" />

                <p style="margin-top:12px;">
                  <button class="btn btn-primary" type="submit">Add Item</button>
                </p>
              </form>
            </div>

            <div class="card" style="flex:1;">
              <h4>Update EXISTING item</h4>
              <form method="POST" action="{{ url_for('manager_update_item') }}" enctype="multipart/form-data">
                <label>Select Item *</label>
                <select name="item_id" required>
                  {% for it in items_all %}
                    <option value="{{ it['item_id'] }}">{{ it['item_name'] }}</option>
                  {% endfor %}
                </select>

                <label>Add Quantity (Intake)</label>
                <input type="number" step="1" min="0" name="add_qty" value="0" />

                <label>Set Expiry Date (optional)</label>
                <input type="date" name="expiry_date_update" />

                <label>Set Unit Cost (optional)</label>
                <input type="number" step="0.01" min="0" name="unit_cost_update" value="" />

                <label>Update Image (optional)</label>
                <input name="image_file_update" type="file" />

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
          <form method="GET" style="margin-bottom:10px;">
            <div class="row">
              <div>
                <label>Search</label>
                <input name="q" value="{{ q }}" placeholder="Search by name or unit" />
              </div>
              <div>
                <label>Sort by</label>
                <select name="sort">
                  <option value="name" {% if sort == "name" %}selected{% endif %}>Name</option>
                  <option value="qty" {% if sort == "qty" %}selected{% endif %}>Qty</option>
                  <option value="expiry" {% if sort == "expiry" %}selected{% endif %}>Expiry</option>
                  <option value="status" {% if sort == "status" %}selected{% endif %}>Status</option>
                </select>
              </div>
              <div>
                <label>Order</label>
                <select name="dir">
                  <option value="asc" {% if direction == "asc" %}selected{% endif %}>Ascending</option>
                  <option value="desc" {% if direction == "desc" %}selected{% endif %}>Descending</option>
                </select>
              </div>
              <div style="align-self:flex-end;">
                <button class="btn btn-primary" type="submit">Apply</button>
              </div>
            </div>
          </form>
          <table>
            <tr>
              <th>Item</th><th>Unit</th><th>Qty</th><th>Expiry</th><th>Status</th><th>Actions</th>
            </tr>
            {% if items_table|length == 0 %}
              <tr><td colspan="6" class="muted">No items found.</td></tr>
            {% else %}
              {% for it in items_table %}
              <tr>
                <td>{{ it["item_name"] }}</td>
                <td>{{ it["unit"] }}</td>
                <td>{{ '%.2f'|format(it["qty_available"]) }}</td>
                <td>{% if it["expiry_date"] %}{{ it["expiry_date"] }}{% else %}<span class="muted">—</span>{% endif %}</td>
                <td>{% if it["is_active"] == 1 %}<span class="ok">Active</span>{% else %}<span class="danger">Inactive</span>{% endif %}</td>
                <td>
                  <form method="POST" action="{{ url_for('manager_edit_item') }}" style="margin-bottom:8px;">
                    <input type="hidden" name="item_id" value="{{ it['item_id'] }}" />
                    <input name="item_name" value="{{ it['item_name'] }}" required style="margin-bottom:6px;" />
                    <input name="unit" value="{{ it['unit'] }}" required style="margin-bottom:6px;" />
                    <div class="muted">Set qty</div>
                    <input type="number" step="0.01" min="0" name="qty_set" value="{{ it['qty_available'] }}" style="margin-bottom:6px;" />
                    <input type="number" step="0.01" min="0" name="unit_cost" value="{{ it['unit_cost'] or '' }}" placeholder="Unit cost" style="margin-bottom:6px;" />
                    <input type="date" name="expiry_date" value="{{ it['expiry_date'] or '' }}" style="margin-bottom:6px;" />
                    <select name="is_active" style="margin-bottom:6px;">
                      <option value="1" {% if it["is_active"] == 1 %}selected{% endif %}>Active</option>
                      <option value="0" {% if it["is_active"] != 1 %}selected{% endif %}>Inactive</option>
                    </select>
                    <button class="btn" type="submit">Save</button>
                  </form>
                  <form method="POST" action="{{ url_for('manager_delete_item') }}" style="margin:0;">
                    <input type="hidden" name="item_id" value="{{ it['item_id'] }}" />
                    <label class="muted" style="display:block;">
                      <input type="checkbox" name="confirm" value="yes" />
                      Confirm delete
                    </label>
                    <button class="btn" type="submit">Delete</button>
                  </form>
                </td>
              </tr>
              {% endfor %}
            {% endif %}
          </table>
        </div>
        """,
        items_all=items_all,
        items_table=items_table,
        q=q,
        sort=sort,
        direction=direction,
        message=message,
        error=error,
    )
    return render_template_string(BASE, body=body)


@APP.post("/manager/add-item")
@requires_manager_auth
def manager_add_item():
    item_name = (request.form.get("item_name") or "").strip()
    unit = (request.form.get("unit") or "").strip()
    expiry_date = (request.form.get("expiry_date") or "").strip() or None
    unit_cost = request.form.get("unit_cost")
    unit_cost_val = float(unit_cost) if unit_cost not in (None, "") else None
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
            "INSERT INTO items (item_name, unit, expiry_date, image_url, qty_available, unit_cost, is_active) VALUES (?, ?, ?, ?, ?, ?, 1)",
            (item_name, unit, expiry_date, image_url, max(0, initial_qty), unit_cost_val),
        )
        item_id = c.execute("SELECT item_id FROM items WHERE item_name=?", (item_name,)).fetchone()["item_id"]

        if initial_qty > 0:
            c.execute(
                "INSERT INTO stock_movements (item_id, movement_type, qty, note, created_by) VALUES (?, 'IN', ?, 'Initial stock', ?)",
                (item_id, initial_qty, current_manager_name()),
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
    unit_cost_update = request.form.get("unit_cost_update")
    unit_cost_val = float(unit_cost_update) if unit_cost_update not in (None, "") else None
    is_active = int(request.form.get("is_active") or 1)
    image_url = None
    image_file = request.files.get("image_file_update")
    if image_file and image_file.filename:
        try:
            image_url = save_uploaded_image(image_file)
        except ValueError as exc:
            abort(400, str(exc))

    c = conn()
    try:
        if add_qty > 0:
            c.execute(
                "INSERT INTO stock_movements (item_id, movement_type, qty, note, created_by) VALUES (?, 'IN', ?, 'Intake', ?)",
                (item_id, add_qty, current_manager_name()),
            )
            c.execute(
                "UPDATE items SET qty_available = qty_available + ? WHERE item_id=?",
                (add_qty, item_id),
            )

        if expiry_update:
            c.execute("UPDATE items SET expiry_date=? WHERE item_id=?", (expiry_update, item_id))

        if unit_cost_val is not None:
            c.execute("UPDATE items SET unit_cost=? WHERE item_id=?", (unit_cost_val, item_id))

        if image_url:
            c.execute("UPDATE items SET image_url=? WHERE item_id=?", (image_url, item_id))

        c.execute("UPDATE items SET is_active=? WHERE item_id=?", (is_active, item_id))

        c.commit()
    finally:
        c.close()

    return redirect(url_for("manager_stock"))


@APP.post("/manager/edit-item")
@requires_manager_auth
def manager_edit_item():
    item_id_text = (request.form.get("item_id") or "").strip()
    if not item_id_text.isdigit():
        abort(400, "Invalid item_id")
    item_id = int(item_id_text)
    item_name = (request.form.get("item_name") or "").strip()
    unit = (request.form.get("unit") or "").strip()
    qty_set_raw = (request.form.get("qty_set") or "").strip()
    qty_set = parse_float(qty_set_raw) if qty_set_raw != "" else None
    unit_cost_val = parse_float(request.form.get("unit_cost"))
    expiry_date = (request.form.get("expiry_date") or "").strip() or None
    is_active = int(request.form.get("is_active") or 1)
    if not item_name or not unit:
        return redirect(url_for("manager_stock", err="Item name and unit are required."))
    if qty_set_raw != "" and qty_set is None:
        return redirect(url_for("manager_stock", err="Quantity must be a number."))
    if qty_set is not None and qty_set < 0:
        return redirect(url_for("manager_stock", err="Quantity cannot be negative."))

    c = conn()
    try:
        current_qty_row = c.execute(
            "SELECT qty_available FROM items WHERE item_id=?",
            (item_id,),
        ).fetchone()
        if not current_qty_row:
            return redirect(url_for("manager_stock", err="Item not found."))
        current_qty = float(current_qty_row["qty_available"] or 0.0)
        c.execute(
            """
            UPDATE items
            SET item_name=?, unit=?, unit_cost=?, expiry_date=?, is_active=?
            WHERE item_id=?
            """,
            (item_name, unit, unit_cost_val, expiry_date, is_active, item_id),
        )
        if qty_set is not None and qty_set != current_qty:
            delta = qty_set - current_qty
            c.execute("UPDATE items SET qty_available=? WHERE item_id=?", (qty_set, item_id))
            c.execute(
                "INSERT INTO stock_movements (item_id, movement_type, qty, note, created_by) VALUES (?, ?, ?, 'Manual adjustment', ?)",
                (item_id, "IN" if delta > 0 else "OUT", abs(delta), current_manager_name()),
            )
        c.commit()
    except sqlite3.IntegrityError:
        return redirect(url_for("manager_stock", err="Item name must be unique."))
    finally:
        c.close()

    return redirect(url_for("manager_stock", msg="Item updated."))


@APP.post("/manager/delete-item")
@requires_manager_auth
def manager_delete_item():
    item_id_text = (request.form.get("item_id") or "").strip()
    confirm = request.form.get("confirm") == "yes"
    if not item_id_text.isdigit():
        abort(400, "Invalid item_id")
    if not confirm:
        return redirect(url_for("manager_stock", err="Please confirm delete."))
    item_id = int(item_id_text)
    c = conn()
    try:
        c.execute("DELETE FROM items WHERE item_id=?", (item_id,))
        c.commit()
    finally:
        c.close()
    return redirect(url_for("manager_stock", msg="Item deleted."))


@APP.get("/manager/requests")
@requires_manager_auth
def manager_requests():
    q = (request.args.get("q") or "").strip()
    sort = (request.args.get("sort") or "id").strip()
    direction = (request.args.get("dir") or "desc").strip().lower()
    status_filter = (request.args.get("status") or "all").strip().upper()
    urgent_only = (request.args.get("urgent") or "0").strip() == "1"
    try:
        low_threshold = int(request.args.get("low", "5"))
    except ValueError:
        low_threshold = 5
    try:
        exp_days = int(request.args.get("exp", "30"))
    except ValueError:
        exp_days = 30
    sort_map = {
        "id": "r.request_id",
        "status": "r.status",
        "created": "r.created_at",
    }
    order_col = sort_map.get(sort, "r.request_id")
    order_dir = "DESC" if direction == "desc" else "ASC"

    c = conn()
    try:
        params = []
        where_clause = ""
        if q:
            where_clause = (
                "WHERE m.name LIKE ? OR m.phone LIKE ? OR m.email LIKE ? "
                "OR CAST(r.request_id AS TEXT) LIKE ?"
            )
            like = f"%{q}%"
            params.extend([like, like, like, like])
        if status_filter in ("PENDING", "APPROVED", "REJECTED"):
            where_clause = f"{where_clause} {'AND' if where_clause else 'WHERE'} r.status=?"
            params.append(status_filter)

        reqs = c.execute(
            f"""
            SELECT r.request_id, r.status, r.note, r.reject_reason, r.created_at,
                   m.name, m.phone, m.email
            FROM requests r
            JOIN members m ON m.member_id = r.member_id
            {where_clause}
            ORDER BY {order_col} {order_dir}
            """,
            params,
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

        urgent_rows = c.execute(
            """
            SELECT DISTINCT r.request_id
            FROM requests r
            JOIN request_items ri ON ri.request_id = r.request_id
            JOIN items i ON i.item_id = ri.item_id
            WHERE r.status='PENDING'
              AND (
                i.is_active != 1
                OR ri.qty_requested > COALESCE(i.qty_available, 0)
                OR (i.expiry_date IS NOT NULL AND date(i.expiry_date) <= date('now', ?))
                OR (COALESCE(i.qty_available, 0) > 0 AND COALESCE(i.qty_available, 0) <= ?)
              )
            """,
            (f"+{exp_days} day", low_threshold),
        ).fetchall()
        urgent_ids = {r["request_id"] for r in urgent_rows}
        if urgent_only:
            reqs = [r for r in reqs if r["request_id"] in urgent_ids]
    finally:
        c.close()

    body = render_template_string(
        """
        <div class="card">
          <h3>Approvals | <a href="/manager/stock_view">Stock View</a> | <a href="/manager/reports">Reports</a></h3>
          <form method="GET" style="margin-top:10px;">
            <div class="row">
              <div>
                <label>Search</label>
                <input name="q" value="{{ q }}" placeholder="Search by name, email, phone, or request id" />
              </div>
              <div>
                <label>Sort by</label>
                <select name="sort">
                  <option value="id" {% if sort == "id" %}selected{% endif %}>Request ID</option>
                  <option value="status" {% if sort == "status" %}selected{% endif %}>Status</option>
                  <option value="created" {% if sort == "created" %}selected{% endif %}>Created</option>
                </select>
              </div>
              <div>
                <label>Order</label>
                <select name="dir">
                  <option value="desc" {% if direction == "desc" %}selected{% endif %}>Descending</option>
                  <option value="asc" {% if direction == "asc" %}selected{% endif %}>Ascending</option>
                </select>
              </div>
              <div>
                <label>Status</label>
                <select name="status">
                  <option value="all" {% if status_filter == "ALL" %}selected{% endif %}>All</option>
                  <option value="PENDING" {% if status_filter == "PENDING" %}selected{% endif %}>Pending</option>
                  <option value="APPROVED" {% if status_filter == "APPROVED" %}selected{% endif %}>Approved</option>
                  <option value="REJECTED" {% if status_filter == "REJECTED" %}selected{% endif %}>Rejected</option>
                </select>
              </div>
              <div>
                <label>Urgent only</label>
                <select name="urgent">
                  <option value="0" {% if not urgent_only %}selected{% endif %}>No</option>
                  <option value="1" {% if urgent_only %}selected{% endif %}>Yes</option>
                </select>
              </div>
              <div style="align-self:flex-end;">
                <button class="btn btn-primary" type="submit">Apply</button>
              </div>
              <div style="align-self:flex-end;">
                <a class="btn" href="/manager/requests.csv?q={{ q }}&sort={{ sort }}&dir={{ direction }}&status={{ status_filter }}&urgent={{ 1 if urgent_only else 0 }}">Export CSV</a>
              </div>
            </div>
          </form>
          <form id="bulk-form" method="POST" action="{{ url_for('manager_requests_bulk') }}">
            <input type="hidden" name="q" value="{{ q }}" />
            <input type="hidden" name="sort" value="{{ sort }}" />
            <input type="hidden" name="dir" value="{{ direction }}" />
            <input type="hidden" name="status" value="{{ status_filter }}" />
            <input type="hidden" name="urgent" value="{{ 1 if urgent_only else 0 }}" />
            <div class="row" style="margin-top:10px;">
              <div>
                <label>Bulk action</label>
                <select name="bulk_action">
                  <option value="APPROVE">Approve selected</option>
                  <option value="REJECT">Reject selected</option>
                </select>
              </div>
              <div style="flex:2;">
                <label>Reject reason (if rejecting)</label>
                <input name="reject_reason" placeholder="Optional reason for rejection" />
              </div>
              <div style="align-self:flex-end;">
                <button class="btn btn-primary" type="submit">Apply to Selected</button>
              </div>
            </div>
          {% if reqs|length == 0 %}
            <p class="muted">No requests yet.</p>
          {% endif %}
          </form>

          {% for r in reqs %}
            <div class="card">
              <div>
                <input type="checkbox" name="request_id" value="{{ r['request_id'] }}" form="bulk-form" />
                <b>Request #{{ r["request_id"] }}</b> — <b>{{ r["status"] }}</b>
                {% if r["request_id"] in urgent_ids %}
                  <span class="badge badge-alert">Urgent</span>
                {% endif %}
              </div>
              <div class="muted">Created: {{ r["created_at"] }}</div>
              <div style="margin-top:8px;">
                <b>Member:</b> {{ r["name"] }} |
                <b>Phone:</b> {{ r["phone"] }} |
                <b>Email:</b> {{ r["email"] }}
              </div>
              {% if r["note"] %}
                <div class="muted" style="margin-top:8px;"><b>Note:</b> {{ r["note"] }}</div>
              {% endif %}
              {% if r["reject_reason"] %}
                <div class="danger" style="margin-top:8px;"><b>Rejection Reason:</b> {{ r["reject_reason"] }}</div>
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
                  <input type="text" name="reject_reason" placeholder="Optional rejection reason" />
                  <button class="btn btn-primary" name="decision" value="APPROVE" type="submit">Approve</button>
                  <button class="btn" name="decision" value="REJECT" type="submit">Reject</button>
                </form>
              {% endif %}
              <div class="row" style="margin-top:12px;">
                <div>
                  <a class="btn" href="/manager/request_edit/{{ r['request_id'] }}">Edit details</a>
                </div>
                <div>
                  <form method="POST" action="{{ url_for('manager_delete_request') }}">
                    <input type="hidden" name="request_id" value="{{ r['request_id'] }}" />
                    <label class="muted" style="display:block;">
                      <input type="checkbox" name="confirm" value="yes" />
                      Confirm delete (does not adjust stock)
                    </label>
                    <button class="btn" type="submit">Delete</button>
                  </form>
                </div>
              </div>
            </div>
          {% endfor %}
        </div>
        """,
        reqs=reqs,
        items_by_req=items_by_req,
        q=q,
        sort=sort,
        direction=direction,
        status_filter=status_filter,
        urgent_only=urgent_only,
        urgent_ids=urgent_ids,
    )
    return render_template_string(BASE, body=body)


@APP.get("/manager/requests.csv")
@requires_sync_or_manager
def manager_requests_csv():
    q = (request.args.get("q") or "").strip()
    sort = (request.args.get("sort") or "id").strip()
    direction = (request.args.get("dir") or "desc").strip().lower()
    status_filter = (request.args.get("status") or "all").strip().upper()
    urgent_only = (request.args.get("urgent") or "0").strip() == "1"
    try:
        low_threshold = int(request.args.get("low", "5"))
    except ValueError:
        low_threshold = 5
    try:
        exp_days = int(request.args.get("exp", "30"))
    except ValueError:
        exp_days = 30
    sort_map = {
        "id": "r.request_id",
        "status": "r.status",
        "created": "r.created_at",
    }
    order_col = sort_map.get(sort, "r.request_id")
    order_dir = "DESC" if direction == "desc" else "ASC"

    c = conn()
    try:
        params = []
        where_clause = ""
        if q:
            where_clause = (
                "WHERE m.name LIKE ? OR m.phone LIKE ? OR m.email LIKE ? "
                "OR CAST(r.request_id AS TEXT) LIKE ?"
            )
            like = f"%{q}%"
            params.extend([like, like, like, like])
        if status_filter in ("PENDING", "APPROVED", "REJECTED"):
            where_clause = f"{where_clause} {'AND' if where_clause else 'WHERE'} r.status=?"
            params.append(status_filter)

        reqs = c.execute(
            f"""
            SELECT r.request_id, r.status, r.note, r.reject_reason, r.created_at,
                   m.name, m.phone, m.email
            FROM requests r
            JOIN members m ON m.member_id = r.member_id
            {where_clause}
            ORDER BY {order_col} {order_dir}
            """,
            params,
        ).fetchall()

        if urgent_only:
            urgent_rows = c.execute(
                """
                SELECT DISTINCT r.request_id
                FROM requests r
                JOIN request_items ri ON ri.request_id = r.request_id
                JOIN items i ON i.item_id = ri.item_id
                WHERE r.status='PENDING'
                  AND (
                    i.is_active != 1
                    OR ri.qty_requested > COALESCE(i.qty_available, 0)
                    OR (i.expiry_date IS NOT NULL AND date(i.expiry_date) <= date('now', ?))
                    OR (COALESCE(i.qty_available, 0) > 0 AND COALESCE(i.qty_available, 0) <= ?)
                  )
                """,
                (f"+{exp_days} day", low_threshold),
            ).fetchall()
            urgent_ids = {r["request_id"] for r in urgent_rows}
            reqs = [r for r in reqs if r["request_id"] in urgent_ids]

        rows = [
            [
                "request_id",
                "status",
                "created_at",
                "member_name",
                "phone",
                "email",
                "note",
                "reject_reason",
                "items",
            ]
        ]
        for r in reqs:
            items = c.execute(
                """
                SELECT i.item_name, i.unit, ri.qty_requested
                FROM request_items ri
                JOIN items i ON i.item_id = ri.item_id
                WHERE ri.request_id = ?
                """,
                (r["request_id"],),
            ).fetchall()
            item_text = "; ".join(
                [f"{it['item_name']} ({it['unit']}) x {it['qty_requested']}" for it in items]
            )
            rows.append(
                [
                    r["request_id"],
                    r["status"],
                    r["created_at"],
                    r["name"],
                    r["phone"],
                    r["email"],
                    r["note"] or "",
                    r["reject_reason"] or "",
                    item_text,
                ]
            )
    finally:
        c.close()

    return csv_response("requests.csv", rows)


@APP.post("/manager/delete-request")
@requires_manager_auth
def manager_delete_request():
    req_id_text = (request.form.get("request_id") or "").strip()
    confirm = request.form.get("confirm") == "yes"
    if not req_id_text.isdigit():
        abort(400, "Invalid request_id")
    if not confirm:
        return redirect(url_for("manager_requests"))
    req_id = int(req_id_text)
    c = conn()
    try:
        c.execute("DELETE FROM request_items WHERE request_id=?", (req_id,))
        c.execute("DELETE FROM requests WHERE request_id=?", (req_id,))
        c.commit()
    finally:
        c.close()
    return redirect(url_for("manager_requests"))


@APP.route("/manager/request_edit/<int:req_id>", methods=["GET", "POST"])
@requires_manager_auth
def manager_request_edit(req_id: int):
    c = conn()
    try:
        req = c.execute(
            """
            SELECT r.request_id, r.status, r.note, r.reject_reason, r.created_at, r.decided_at, r.decided_by,
                   m.member_id, m.name, m.phone, m.email
            FROM requests r
            JOIN members m ON m.member_id = r.member_id
            WHERE r.request_id=?
            """,
            (req_id,),
        ).fetchone()
        if not req:
            return render_template_string(BASE, body="<h3>Request not found.</h3>"), 404

        items_all = c.execute(
            """
            SELECT item_id, item_name, unit, is_active
            FROM items
            ORDER BY item_name
            """
        ).fetchall()

        items = c.execute(
            """
            SELECT i.item_name, i.unit, ri.qty_requested
            FROM request_items ri
            JOIN items i ON i.item_id = ri.item_id
            WHERE ri.request_id=?
            """,
            (req_id,),
        ).fetchall()
        existing_qty = {row["item_id"]: row["qty_requested"] for row in c.execute(
            "SELECT item_id, qty_requested FROM request_items WHERE request_id=?",
            (req_id,),
        ).fetchall()}

        if request.method == "POST":
            name = (request.form.get("name") or "").strip()
            phone = (request.form.get("phone") or "").strip()
            email = (request.form.get("email") or "").strip()
            note = (request.form.get("note") or "").strip()
            reject_reason = (request.form.get("reject_reason") or "").strip()
            status = (request.form.get("status") or "PENDING").strip().upper()
            if not name or not phone:
                return render_template_string(
                    BASE,
                    body="<div class='card danger'><b>Name and phone are required.</b></div>",
                ), 400
            if status not in ("PENDING", "APPROVED", "REJECTED"):
                return render_template_string(
                    BASE,
                    body="<div class='card danger'><b>Invalid status.</b></div>",
                ), 400
            c.execute(
                "UPDATE members SET name=?, phone=?, email=? WHERE member_id=?",
                (name, phone, email, req["member_id"]),
            )
            decided_at = req["decided_at"]
            decided_by = req["decided_by"]
            if status != req["status"]:
                if status in ("APPROVED", "REJECTED"):
                    decided_at = datetime.utcnow().isoformat()
                    decided_by = current_manager_name()
                else:
                    decided_at = None
                    decided_by = None
            c.execute(
                """
                UPDATE requests
                SET status=?, note=?, reject_reason=?, decided_at=?, decided_by=?
                WHERE request_id=?
                """,
                (status, note, reject_reason, decided_at, decided_by, req_id),
            )
            c.execute("DELETE FROM request_items WHERE request_id=?", (req_id,))
            for it in items_all:
                qty_val = parse_float(request.form.get(f"qty_{it['item_id']}")) or 0.0
                if qty_val > 0:
                    c.execute(
                        "INSERT INTO request_items (request_id, item_id, qty_requested) VALUES (?, ?, ?)",
                        (req_id, it["item_id"], qty_val),
                    )
            c.commit()
            return redirect(url_for("manager_requests"))
    finally:
        c.close()

    body = render_template_string(
        """
        <div class="card">
          <h3>Edit Request #{{ req.request_id }}</h3>
          <p class="muted">Created: {{ req.created_at }}</p>
          <table>
            <tr><th>Item</th><th>Qty</th></tr>
            {% for it in items %}
              <tr>
                <td>{{ it["item_name"] }} <span class="muted">({{ it["unit"] }})</span></td>
                <td>{{ '%.2f'|format(it["qty_requested"]) }}</td>
              </tr>
            {% endfor %}
          </table>
          <p class="muted" style="margin-top:10px;">Editing status here does not adjust stock automatically.</p>
          <form method="POST" style="margin-top:12px;">
            <label>Status</label>
            <select name="status">
              <option value="PENDING" {% if req.status == "PENDING" %}selected{% endif %}>Pending</option>
              <option value="APPROVED" {% if req.status == "APPROVED" %}selected{% endif %}>Approved</option>
              <option value="REJECTED" {% if req.status == "REJECTED" %}selected{% endif %}>Rejected</option>
            </select>
            <label>Name</label>
            <input name="name" value="{{ req.name }}" required />
            <label>Phone</label>
            <input name="phone" value="{{ req.phone }}" required />
            <label>Email (optional)</label>
            <input name="email" value="{{ req.email }}" />
            <label>Notes / recommendations</label>
            <textarea name="note" rows="3">{{ req.note or "" }}</textarea>
            <label>Reject reason (optional)</label>
            <input name="reject_reason" value="{{ req.reject_reason or "" }}" />
            <h4 style="margin-top:12px;">Requested Items</h4>
            <table>
              <tr><th>Item</th><th>Unit</th><th>Qty Requested</th></tr>
              {% for it in items_all %}
                <tr>
                  <td>{{ it["item_name"] }}{% if it["is_active"] != 1 %} <span class="muted">(inactive)</span>{% endif %}</td>
                  <td>{{ it["unit"] }}</td>
                  <td>
                    <input type="number" step="1" min="0" name="qty_{{ it['item_id'] }}" value="{{ existing_qty.get(it['item_id'], 0) }}" />
                  </td>
                </tr>
              {% endfor %}
            </table>
            <p style="margin-top:12px;">
              <button class="btn btn-primary" type="submit">Save Changes</button>
              <a class="btn" href="/manager/requests">Cancel</a>
            </p>
          </form>
        </div>
        """,
        req=req,
        items=items,
        items_all=items_all,
        existing_qty=existing_qty,
    )
    return render_template_string(BASE, body=body)


@APP.get("/manager/members")
@requires_manager_auth
def manager_members():
    q = (request.args.get("q") or "").strip()
    message = (request.args.get("msg") or "").strip()
    error = (request.args.get("err") or "").strip()

    c = conn()
    try:
        params = []
        where_clause = ""
        if q:
            where_clause = "WHERE m.name LIKE ? OR m.phone LIKE ? OR m.email LIKE ?"
            like = f"%{q}%"
            params.extend([like, like, like])

        members = c.execute(
            f"""
            SELECT m.member_id, m.name, m.phone, m.email, m.created_at,
                   COUNT(r.request_id) AS request_count
            FROM members m
            LEFT JOIN requests r ON r.member_id = m.member_id
            {where_clause}
            GROUP BY m.member_id
            ORDER BY m.created_at DESC
            """,
            params,
        ).fetchall()
    finally:
        c.close()

    body = render_template_string(
        """
        <div class="card">
          <h3>Members</h3>
          {% if message %}<p class="ok">{{ message }}</p>{% endif %}
          {% if error %}<p class="danger">{{ error }}</p>{% endif %}
          <form method="GET" style="margin-top:10px;">
            <div class="row">
              <div>
                <label>Search</label>
                <input name="q" value="{{ q }}" placeholder="Search by name, phone, or email" />
              </div>
              <div style="align-self:flex-end;">
                <button class="btn btn-primary" type="submit">Apply</button>
              </div>
            </div>
          </form>
          <table>
            <tr><th>Name</th><th>Phone</th><th>Email</th><th>Requests</th><th>Actions</th></tr>
            {% if members|length == 0 %}
              <tr><td colspan="5" class="muted">No members found.</td></tr>
            {% else %}
              {% for m in members %}
              <tr>
                <td>
                  <form method="POST" action="{{ url_for('manager_edit_member') }}">
                    <input type="hidden" name="member_id" value="{{ m['member_id'] }}" />
                    <input name="name" value="{{ m['name'] }}" required />
                </td>
                <td>
                    <input name="phone" value="{{ m['phone'] }}" required />
                </td>
                <td>
                    <input name="email" value="{{ m['email'] }}" />
                </td>
                <td>{{ m["request_count"] }}</td>
                <td>
                    <button class="btn" type="submit">Save</button>
                  </form>
                  <form method="POST" action="{{ url_for('manager_delete_member') }}" style="margin-top:8px;">
                    <input type="hidden" name="member_id" value="{{ m['member_id'] }}" />
                    <label class="muted" style="display:block;">
                      <input type="checkbox" name="confirm" value="yes" />
                      Confirm delete (removes their requests)
                    </label>
                    <button class="btn" type="submit">Delete</button>
                  </form>
                </td>
              </tr>
              {% endfor %}
            {% endif %}
          </table>
        </div>
        """,
        members=members,
        q=q,
        message=message,
        error=error,
    )
    return render_template_string(BASE, body=body)


@APP.post("/manager/edit-member")
@requires_manager_auth
def manager_edit_member():
    member_id_text = (request.form.get("member_id") or "").strip()
    name = (request.form.get("name") or "").strip()
    phone = (request.form.get("phone") or "").strip()
    email = (request.form.get("email") or "").strip()
    if not member_id_text.isdigit():
        abort(400, "Invalid member_id")
    if not name or not phone:
        return redirect(url_for("manager_members", err="Name and phone are required."))

    c = conn()
    try:
        c.execute(
            "UPDATE members SET name=?, phone=?, email=? WHERE member_id=?",
            (name, phone, email, int(member_id_text)),
        )
        c.commit()
    finally:
        c.close()

    return redirect(url_for("manager_members", msg="Member updated."))


@APP.post("/manager/delete-member")
@requires_manager_auth
def manager_delete_member():
    member_id_text = (request.form.get("member_id") or "").strip()
    confirm = request.form.get("confirm") == "yes"
    if not member_id_text.isdigit():
        abort(400, "Invalid member_id")
    if not confirm:
        return redirect(url_for("manager_members", err="Please confirm delete."))
    c = conn()
    try:
        c.execute("DELETE FROM members WHERE member_id=?", (int(member_id_text),))
        c.commit()
    finally:
        c.close()
    return redirect(url_for("manager_members", msg="Member deleted."))


@APP.route("/manager/settings", methods=["GET", "POST"])
@requires_manager_auth
def manager_settings():
    message = ""
    error = ""

    if request.method == "POST":
        try:
            church_name = (request.form.get("church_name") or "").strip()
            church_tagline = (request.form.get("church_tagline") or "").strip()
            logo_url = (request.form.get("logo_url") or "").strip()
            public_base_url = (request.form.get("public_base_url") or "").strip()
            manager_email = (request.form.get("manager_email") or "").strip()

            smtp_host = (request.form.get("smtp_host") or "").strip()
            smtp_port = (request.form.get("smtp_port") or "").strip()
            smtp_user = (request.form.get("smtp_user") or "").strip()
            smtp_password = (request.form.get("smtp_password") or "").strip()
            smtp_from = (request.form.get("smtp_from") or "").strip()
            smtp_tls = "1" if request.form.get("smtp_tls") == "1" else "0"

            render_base_url = (request.form.get("render_base_url") or "").strip().rstrip("/")
            sync_token = (request.form.get("sync_token") or "").strip()

            logo_file = request.files.get("logo_file")
            if logo_file and logo_file.filename:
                try:
                    logo_url = save_uploaded_image(logo_file)
                except ValueError as exc:
                    return render_template_string(
                        BASE, body=f"<div class='card danger'><b>{exc}</b></div>"
                    ), 400

            if church_name:
                set_setting_value("church_name", church_name)
            if church_tagline:
                set_setting_value("church_tagline", church_tagline)
            if logo_url != "":
                set_setting_value("logo_url", logo_url)
            if public_base_url != "":
                set_setting_value("public_base_url", public_base_url)
            if manager_email != "":
                set_setting_value("manager_email", manager_email)

            if smtp_host != "":
                set_setting_value("smtp_host", smtp_host)
            if smtp_port != "":
                set_setting_value("smtp_port", smtp_port)
            if smtp_user != "":
                set_setting_value("smtp_user", smtp_user)
            if smtp_password != "":
                set_setting_value("smtp_password", smtp_password)
            if smtp_from != "":
                set_setting_value("smtp_from", smtp_from)
            set_setting_value("smtp_tls", smtp_tls)

            if render_base_url != "":
                set_setting_value("render_base_url", render_base_url)
            if sync_token != "":
                set_setting_value("sync_token", sync_token)

            message = "Settings saved."
        except sqlite3.Error as exc:
            error = f"Settings update failed: {exc}"

    settings = {
        "church_name": get_setting_value("church_name", CHURCH_NAME),
        "church_tagline": get_setting_value("church_tagline", CHURCH_TAGLINE),
        "logo_url": get_setting_value("logo_url", LOGO_URL),
        "public_base_url": get_setting_value("public_base_url", os.environ.get("PUBLIC_BASE_URL", "")),
        "manager_email": get_setting_value("manager_email", os.environ.get("MANAGER_EMAIL", "")),
        "smtp_host": get_setting_value("smtp_host", os.environ.get("SMTP_HOST", "")),
        "smtp_port": get_setting_value("smtp_port", os.environ.get("SMTP_PORT", "587")),
        "smtp_user": get_setting_value("smtp_user", os.environ.get("SMTP_USER", "")),
        "smtp_from": get_setting_value("smtp_from", os.environ.get("SMTP_FROM", "")),
        "smtp_tls": get_setting_value("smtp_tls", os.environ.get("SMTP_TLS", "1")),
        "render_base_url": get_setting_value("render_base_url", RENDER_BASE_URL),
        "sync_token_set": bool(get_setting_value("sync_token", PANTRY_SYNC_TOKEN)),
    }

    body = render_template_string(
        """
        <div class="card">
          <h3>Settings</h3>
          {% if message %}<p class="ok">{{ message }}</p>{% endif %}
          {% if error %}<p class="danger">{{ error }}</p>{% endif %}
          <form method="POST" enctype="multipart/form-data">
            <h4>Branding</h4>
            <label>Church Name</label>
            <input name="church_name" value="{{ settings.church_name }}" />
            <label>Tagline</label>
            <input name="church_tagline" value="{{ settings.church_tagline }}" />
            <label>Logo URL</label>
            <input name="logo_url" value="{{ settings.logo_url }}" />
            <label>Upload Logo (optional)</label>
            <input name="logo_file" type="file" />

            <h4 style="margin-top:16px;">Email</h4>
            <label>Public Base URL</label>
            <input name="public_base_url" value="{{ settings.public_base_url }}" placeholder="https://church-pantry.onrender.com" />
            <label>Manager Notification Email</label>
            <input name="manager_email" value="{{ settings.manager_email }}" />
            <label>SMTP Host</label>
            <input name="smtp_host" value="{{ settings.smtp_host }}" />
            <label>SMTP Port</label>
            <input name="smtp_port" value="{{ settings.smtp_port }}" />
            <label>SMTP User</label>
            <input name="smtp_user" value="{{ settings.smtp_user }}" />
            <label>SMTP Password (leave blank to keep current)</label>
            <input name="smtp_password" type="password" />
            <label>SMTP From</label>
            <input name="smtp_from" value="{{ settings.smtp_from }}" />
            <label>
              <input type="checkbox" name="smtp_tls" value="1" {% if settings.smtp_tls == "1" %}checked{% endif %} />
              Use TLS
            </label>

            <h4 style="margin-top:16px;">Sync</h4>
            <label>Render Base URL</label>
            <input name="render_base_url" value="{{ settings.render_base_url }}" placeholder="https://church-pantry.onrender.com" />
            <label>Sync Token (leave blank to keep current)</label>
            <input name="sync_token" type="password" placeholder="{% if settings.sync_token_set %}set{% else %}not set{% endif %}" />

            <p style="margin-top:12px;">
              <button class="btn btn-primary" type="submit">Save Settings</button>
            </p>
          </form>
        </div>
        """,
        message=message,
        error=error,
        settings=settings,
    )
    return render_template_string(BASE, body=body)


@APP.post("/manager/requests/bulk")
@requires_manager_auth
def manager_requests_bulk():
    request_ids = [int(rid) for rid in request.form.getlist("request_id") if rid.isdigit()]
    action = (request.form.get("bulk_action") or "").strip().upper()
    reject_reason = (request.form.get("reject_reason") or "").strip()

    if not request_ids:
        body = '<div class="card"><p class="muted">No requests selected.</p><p><a href="/manager/requests">Back to requests</a></p></div>'
        return render_template_string(BASE, body=body)

    if action not in ("APPROVE", "REJECT"):
        abort(400, "Invalid bulk action")

    results = {"approved": [], "rejected": [], "skipped": [], "failed": []}
    c = conn()
    try:
        for req_id in request_ids:
            r = c.execute(
                """
                SELECT r.status, r.member_id, m.name, m.email
                FROM requests r
                JOIN members m ON m.member_id = r.member_id
                WHERE r.request_id=?
                """,
                (req_id,),
            ).fetchone()
            if not r:
                results["skipped"].append((req_id, "Not found"))
                continue
            if r["status"] != "PENDING":
                results["skipped"].append((req_id, f"Already {r['status']}"))
                continue

            if action == "REJECT":
                c.execute(
                    "UPDATE requests SET status='REJECTED', reject_reason=?, decided_at=?, decided_by=? WHERE request_id=?",
                    (reject_reason, datetime.utcnow().isoformat(), current_manager_name(), req_id),
                )
                results["rejected"].append(req_id)
                if r["email"]:
                    try:
                        notify_request_rejected(req_id, r["email"], r["name"], reject_reason)
                    except Exception as exc:
                        print(f"⚠️ Reject email failed: {exc}")
                continue

            # APPROVE
            rows = c.execute(
                """
                SELECT ri.item_id, ri.qty_requested, i.qty_available, i.item_name, i.is_active
                FROM request_items ri
                JOIN items i ON i.item_id = ri.item_id
                WHERE ri.request_id = ?
                """,
                (req_id,),
            ).fetchall()

            blocked = []
            for row in rows:
                if row["is_active"] != 1:
                    blocked.append(f"{row['item_name']} inactive")
                elif row["qty_requested"] > (row["qty_available"] or 0):
                    blocked.append(f"{row['item_name']} insufficient stock")
            if blocked:
                results["failed"].append((req_id, "; ".join(blocked)))
                continue

            for row in rows:
                c.execute(
                    "UPDATE items SET qty_available = qty_available - ? WHERE item_id=?",
                    (row["qty_requested"], row["item_id"]),
                )
                c.execute(
                    "INSERT INTO stock_movements (item_id, movement_type, qty, note, created_by) VALUES (?, 'OUT', ?, ?, ?)",
                    (row["item_id"], row["qty_requested"], f"Approved request #{req_id}", current_manager_name()),
                )

            c.execute(
                "UPDATE requests SET status='APPROVED', decided_at=?, decided_by=? WHERE request_id=?",
                (datetime.utcnow().isoformat(), current_manager_name(), req_id),
            )
            results["approved"].append(req_id)

        c.commit()
    finally:
        c.close()

    body = render_template_string(
        """
        <div class="card">
          <h3>Bulk Action Results</h3>
          <p><b>Approved:</b> {{ results["approved"]|length }}</p>
          <p><b>Rejected:</b> {{ results["rejected"]|length }}</p>
          <p><b>Skipped:</b> {{ results["skipped"]|length }}</p>
          <p><b>Failed:</b> {{ results["failed"]|length }}</p>
          {% if results["failed"] %}
            <div class="card">
              <h4>Failures</h4>
              <ul>
                {% for rid, reason in results["failed"] %}
                  <li>Request #{{ rid }}: {{ reason }}</li>
                {% endfor %}
              </ul>
            </div>
          {% endif %}
          {% if results["skipped"] %}
            <div class="card">
              <h4>Skipped</h4>
              <ul>
                {% for rid, reason in results["skipped"] %}
                  <li>Request #{{ rid }}: {{ reason }}</li>
                {% endfor %}
              </ul>
            </div>
          {% endif %}
          <p><a href="/manager/requests">Back to requests</a></p>
        </div>
        """,
        results=results,
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
        inventory_value = c.execute(
            "SELECT COALESCE(SUM(COALESCE(qty_available, 0) * COALESCE(unit_cost, 0)), 0) AS total_value FROM items"
        ).fetchone()["total_value"]

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

        top_items_by_members = c.execute(
            """
            SELECT i.item_name, i.unit, COUNT(DISTINCT r.member_id) AS member_count
            FROM request_items ri
            JOIN requests r ON r.request_id = ri.request_id
            JOIN items i ON i.item_id = ri.item_id
            GROUP BY i.item_id
            ORDER BY member_count DESC
            LIMIT 10
            """
        ).fetchall()

        rejected_summary = c.execute(
            """
            SELECT i.item_name, i.unit, COUNT(*) AS rejected_count
            FROM request_items ri
            JOIN requests r ON r.request_id = ri.request_id
            JOIN items i ON i.item_id = ri.item_id
            WHERE r.status='REJECTED'
            GROUP BY i.item_id
            ORDER BY rejected_count DESC
            LIMIT 10
            """
        ).fetchall()

        idle_items = c.execute(
            """
            SELECT i.item_name, i.unit, i.qty_available
            FROM items i
            WHERE NOT EXISTS (
                SELECT 1
                FROM request_items ri
                JOIN requests r ON r.request_id = ri.request_id
                WHERE ri.item_id = i.item_id
                  AND date(r.created_at) >= date('now', '-90 day')
            )
            ORDER BY i.item_name
            """
        ).fetchall()

        movement_by_month = c.execute(
            """
            SELECT strftime('%Y-%m', created_at) AS ym, movement_type, COALESCE(SUM(qty), 0) AS total_qty
            FROM stock_movements
            WHERE date(created_at) >= date('now', '-180 day')
            GROUP BY ym, movement_type
            """
        ).fetchall()
        movement_map = {}
        for row in movement_by_month:
            movement_map.setdefault(row["ym"], {})[row["movement_type"]] = row["total_qty"]

        month_labels = []
        today = datetime.utcnow().date()
        month = today.month
        year = today.year
        for _ in range(6):
            month_labels.append(f"{year:04d}-{month:02d}")
            month -= 1
            if month == 0:
                month = 12
                year -= 1
        month_labels = list(reversed(month_labels))
        monthly_trend = []
        for ym in month_labels:
            data = movement_map.get(ym, {})
            monthly_trend.append(
                {"label": ym, "in_qty": data.get("IN", 0), "out_qty": data.get("OUT", 0)}
            )

        weekly_rows = c.execute(
            """
            SELECT strftime('%Y-%W', created_at) AS yw, COUNT(*) AS cnt
            FROM requests
            WHERE date(created_at) >= date('now', '-56 day')
            GROUP BY yw
            """
        ).fetchall()
        weekly_map = {row["yw"]: row["cnt"] for row in weekly_rows}
        weekly_trend = []
        week_start = today - timedelta(days=today.weekday())
        for i in range(7, -1, -1):
            start = week_start - timedelta(weeks=i)
            key = start.strftime("%Y-%W")
            label = start.strftime("%b %d")
            weekly_trend.append({"label": label, "count": weekly_map.get(key, 0)})
        max_week_count = max([w["count"] for w in weekly_trend], default=0)

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
          <h3>Reports</h3>
          <p class="muted">Defaults: low-stock <= {{ low_threshold }}, expiring in {{ exp_days }} days.</p>
          <form method="get" class="row">
            <div>
              <label>Low stock threshold</label>
              <input name="low" type="number" min="0" step="1" value="{{ low_threshold }}" />
            </div>
            <div>
              <label>Expiring within (days)</label>
              <input name="exp" type="number" min="1" step="1" value="{{ exp_days }}" />
            </div>
            <div style="align-self:flex-end;">
              <button class="btn btn-primary" type="submit">Apply Filters</button>
            </div>
          </form>
          <div class="stats">
            <div class="stat-card">
              <div class="stat-label">Total Items</div>
              <div class="stat-value">{{ total_items }}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">In Stock Items</div>
              <div class="stat-value">{{ in_stock_items }}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Out of Stock Items</div>
              <div class="stat-value">{{ out_stock_items }}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Active Items</div>
              <div class="stat-value">{{ active_items }}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Total Requests</div>
              <div class="stat-value">{{ total_requests }}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Requests (30 days)</div>
              <div class="stat-value">{{ recent_requests }}</div>
            </div>
            <div class="stat-card">
              <div class="stat-label">Inventory Value</div>
              <div class="stat-value">${{ '%.2f'|format(inventory_value) }}</div>
            </div>
          </div>
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
            <tr><th>Estimated Inventory Value</th><td>${{ '%.2f'|format(inventory_value) }}</td></tr>
          </table>
        </div>

        <div class="card">
          <h4>Monthly Intake vs Distribution (Last 6 Months)</h4>
          {% for m in monthly_trend %}
            {% set max_val = [m.in_qty, m.out_qty]|max %}
            <div class="bar-row">
              <div class="bar-label">{{ m.label }}</div>
              <div class="bar-track">
                <div class="bar" style="width: {{ (m.in_qty / (max_val if max_val else 1)) * 100 }}%;"></div>
              </div>
              <div class="muted">IN {{ '%.2f'|format(m.in_qty) }} / OUT {{ '%.2f'|format(m.out_qty) }}</div>
            </div>
          {% endfor %}
        </div>

        <div class="card">
          <h4>Weekly Requests (Last 8 Weeks)</h4>
          {% for w in weekly_trend %}
            <div class="bar-row">
              <div class="bar-label">{{ w.label }}</div>
              <div class="bar-track">
                <div class="bar" style="width: {{ (w.count / (max_week_count if max_week_count else 1)) * 100 }}%;"></div>
              </div>
              <div class="muted">{{ w.count }} requests</div>
            </div>
          {% endfor %}
        </div>

        <div class="card">
          <div style="display:flex; flex-wrap:wrap; align-items:center; justify-content:space-between; gap:10px;">
            <h4>Low Stock (<= {{ low_threshold }})</h4>
            <a class="btn" href="/manager/reports/export/low_stock?low={{ low_threshold }}">Export CSV</a>
          </div>
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
          <div style="display:flex; flex-wrap:wrap; align-items:center; justify-content:space-between; gap:10px;">
            <h4>Expiring Soon (next {{ exp_days }} days)</h4>
            <a class="btn" href="/manager/reports/export/expiring?exp={{ exp_days }}">Export CSV</a>
          </div>
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
          <h4>Top Requested Items (By Member Count)</h4>
          <table>
            <tr><th>Item</th><th>Unit</th><th>Members</th></tr>
            {% if top_items_by_members|length == 0 %}
              <tr><td colspan="3" class="muted">No requests yet.</td></tr>
            {% else %}
              {% for it in top_items_by_members %}
                <tr>
                  <td>{{ it["item_name"] }}</td>
                  <td>{{ it["unit"] }}</td>
                  <td>{{ it["member_count"] }}</td>
                </tr>
              {% endfor %}
            {% endif %}
          </table>
        </div>

        <div class="card">
          <h4>Rejected Requests Summary (Top Items)</h4>
          <table>
            <tr><th>Item</th><th>Unit</th><th>Rejected Count</th></tr>
            {% if rejected_summary|length == 0 %}
              <tr><td colspan="3" class="muted">No rejected requests.</td></tr>
            {% else %}
              {% for it in rejected_summary %}
                <tr>
                  <td>{{ it["item_name"] }}</td>
                  <td>{{ it["unit"] }}</td>
                  <td>{{ it["rejected_count"] }}</td>
                </tr>
              {% endfor %}
            {% endif %}
          </table>
        </div>

        <div class="card">
          <h4>Items with No Requests in 90 Days</h4>
          <table>
            <tr><th>Item</th><th>Unit</th><th>Qty</th></tr>
            {% if idle_items|length == 0 %}
              <tr><td colspan="3" class="muted">No idle items found.</td></tr>
            {% else %}
              {% for it in idle_items %}
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
          <h4>Exports</h4>
          <p>
            <a class="btn" href="/manager/items.csv">Export Items CSV</a>
            <a class="btn" href="/manager/requests.csv">Export Requests CSV</a>
          </p>
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
        top_items_by_members=top_items_by_members,
        rejected_summary=rejected_summary,
        idle_items=idle_items,
        monthly_trend=monthly_trend,
        weekly_trend=weekly_trend,
        max_week_count=max_week_count,
        inventory_value=inventory_value,
        movement_totals=movement_totals,
    )
    return render_template_string(BASE, body=body)


@APP.get("/manager/reports/export/<string:kind>")
@requires_manager_auth
def manager_reports_export(kind: str):
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
        if kind == "low_stock":
            rows = c.execute(
                """
                SELECT item_name, unit, qty_available
                FROM items
                WHERE is_active=1 AND COALESCE(qty_available, 0) > 0 AND qty_available <= ?
                ORDER BY qty_available ASC, item_name
                """,
                (low_threshold,),
            ).fetchall()
            csv_rows = [["item_name", "unit", "qty_available"]]
            for it in rows:
                csv_rows.append([it["item_name"], it["unit"], it["qty_available"]])
            return csv_response("low_stock.csv", csv_rows)

        if kind == "expiring":
            rows = c.execute(
                """
                SELECT item_name, unit, qty_available, expiry_date
                FROM items
                WHERE expiry_date IS NOT NULL
                  AND date(expiry_date) <= date('now', ?)
                ORDER BY date(expiry_date), item_name
                """,
                (f"+{exp_days} day",),
            ).fetchall()
            csv_rows = [["item_name", "unit", "qty_available", "expiry_date"]]
            for it in rows:
                csv_rows.append([it["item_name"], it["unit"], it["qty_available"], it["expiry_date"]])
            return csv_response("expiring_soon.csv", csv_rows)
    finally:
        c.close()

    abort(404, "Unknown export type")


def build_stock_flags(expiry_date: str | None, qty_available: float, is_active: int, low_threshold: int, exp_days: int, today_date):
    labels = []
    if is_active != 1:
        labels.append("Inactive")
    if qty_available > 0 and qty_available <= low_threshold:
        labels.append("Low stock")
    if expiry_date:
        try:
            exp_date = datetime.strptime(expiry_date, "%Y-%m-%d").date()
            days_left = (exp_date - today_date).days
            if days_left < 0:
                labels.append("Expired")
            elif days_left <= exp_days:
                labels.append("Expiring soon")
        except ValueError:
            pass
    return labels


@APP.post("/manager/decide")
@requires_manager_auth
def manager_decide_request():
    req_id = int(request.form.get("request_id"))
    decision = request.form.get("decision")
    reject_reason = (request.form.get("reject_reason") or "").strip()

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
                    "UPDATE requests SET status='REJECTED', reject_reason=?, decided_at=?, decided_by=? WHERE request_id=?",
                    (reject_reason, datetime.utcnow().isoformat(), current_manager_name(), req_id),
                )
                c.commit()
            try:
                member = c.execute(
                    """
                    SELECT m.email, m.name
                    FROM requests r
                    JOIN members m ON m.member_id = r.member_id
                    WHERE r.request_id=?
                    """,
                    (req_id,),
                ).fetchone()
                if member and member["email"]:
                    notify_request_rejected(req_id, member["email"], member["name"], reject_reason)
            except Exception as exc:
                print(f"⚠️ Reject email failed: {exc}")
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
                "INSERT INTO stock_movements (item_id, movement_type, qty, note, created_by) VALUES (?, 'OUT', ?, ?, ?)",
                (row["item_id"], row["qty_requested"], f"Approved request #{req_id}", current_manager_name()),
            )

        c.execute(
            "UPDATE requests SET status='APPROVED', decided_at=?, decided_by=? WHERE request_id=?",
            (datetime.utcnow().isoformat(), current_manager_name(), req_id),
        )

        c.commit()
    finally:
        c.close()

    return redirect(url_for("manager_requests"))


@APP.route("/manager/stock_view")
@requires_manager_auth
def manager_stock_view():
    q = (request.args.get("q") or "").strip()
    sort = (request.args.get("sort") or "name").strip()
    direction = (request.args.get("dir") or "asc").strip().lower()
    sort_map = {
        "name": "item_name",
        "qty": "qty_available",
        "expiry": "expiry_date",
        "status": "is_active",
    }
    order_col = sort_map.get(sort, "item_name")
    order_dir = "DESC" if direction == "desc" else "ASC"

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
        params = []
        where_clause = ""
        if q:
            where_clause = "WHERE item_name LIKE ? OR unit LIKE ?"
            like = f"%{q}%"
            params.extend([like, like])

        items = c.execute(
            f"""
            SELECT item_id, item_name, unit, qty_available, expiry_date, is_active, image_url
            FROM items
            {where_clause}
            ORDER BY {order_col} {order_dir}, item_name
            """,
            params,
        ).fetchall()
    finally:
        c.close()

    rows = []
    today = datetime.utcnow().date()
    for it in items:
        status = "Active" if (it["is_active"] == 1) else "Inactive"
        qty_available = float(it["qty_available"] or 0.0)
        flags = build_stock_flags(it["expiry_date"], qty_available, it["is_active"], low_threshold, exp_days, today)
        badge_parts = []
        for label in flags:
            badge_class = "badge-alert" if ("Expiring" in label or "Expired" in label or label == "Inactive") else "badge-warn"
            badge_parts.append(f'<span class="badge {badge_class}">{label}</span>')
        badges_html = " ".join(badge_parts) if badge_parts else '<span class="muted">-</span>'
        img_html = ""
        if it["image_url"]:
            img_html = (
                f'<img src="{it["image_url"]}" alt="{it["item_name"]}" '
                'style="max-width:70px; max-height:70px; display:block;" />'
            )
        rows.append(f"""
        <tr>
          <td>{img_html}</td>
          <td>{it["item_name"]}</td>
          <td>{it["unit"]}</td>
          <td>{qty_available:.2f}</td>
          <td>{it["expiry_date"] or ""}</td>
          <td>{status}</td>
          <td>{badges_html}</td>
        </tr>
        """)

    body = f"""
    <h2>Current Stock</h2>
    <p><a href="/manager/stock">Back to Intake</a></p>
    <div class="card">
      <form method="GET">
        <div class="row">
          <div>
            <label>Search</label>
            <input name="q" value="{q}" placeholder="Search by name or unit" />
          </div>
          <div>
            <label>Sort by</label>
            <select name="sort">
              <option value="name" {"selected" if sort == "name" else ""}>Name</option>
              <option value="qty" {"selected" if sort == "qty" else ""}>Qty</option>
              <option value="expiry" {"selected" if sort == "expiry" else ""}>Expiry</option>
              <option value="status" {"selected" if sort == "status" else ""}>Status</option>
            </select>
          </div>
          <div>
            <label>Order</label>
            <select name="dir">
              <option value="asc" {"selected" if direction == "asc" else ""}>Ascending</option>
              <option value="desc" {"selected" if direction == "desc" else ""}>Descending</option>
            </select>
          </div>
          <div>
            <label>Low stock <=</label>
            <input name="low" type="number" min="0" step="1" value="{low_threshold}" />
          </div>
          <div>
            <label>Expiring within days</label>
            <input name="exp" type="number" min="1" step="1" value="{exp_days}" />
          </div>
          <div style="align-self:flex-end;">
            <button class="btn btn-primary" type="submit">Apply</button>
          </div>
          <div style="align-self:flex-end;">
            <a class="btn" href="/manager/stock_view.csv?q={q}&sort={sort}&dir={direction}&low={low_threshold}&exp={exp_days}">Export CSV</a>
          </div>
        </div>
      </form>
    </div>
    <table border="1" cellpadding="8" cellspacing="0">
      <tr><th>Image</th><th>Item</th><th>Unit</th><th>Qty</th><th>Expiry</th><th>Status</th><th>Flags</th></tr>
      {''.join(rows) if rows else '<tr><td colspan="7">No items found</td></tr>'}
    </table>
    """
    return render_template_string(BASE, body=body)


@APP.get("/manager/stock_view.csv")
@requires_manager_auth
def manager_stock_view_csv():
    q = (request.args.get("q") or "").strip()
    sort = (request.args.get("sort") or "name").strip()
    direction = (request.args.get("dir") or "asc").strip().lower()
    sort_map = {
        "name": "item_name",
        "qty": "qty_available",
        "expiry": "expiry_date",
        "status": "is_active",
    }
    order_col = sort_map.get(sort, "item_name")
    order_dir = "DESC" if direction == "desc" else "ASC"

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
        params = []
        where_clause = ""
        if q:
            where_clause = "WHERE item_name LIKE ? OR unit LIKE ?"
            like = f"%{q}%"
            params.extend([like, like])

        items = c.execute(
            f"""
            SELECT item_id, item_name, unit, qty_available, expiry_date, is_active
            FROM items
            {where_clause}
            ORDER BY {order_col} {order_dir}, item_name
            """,
            params,
        ).fetchall()
    finally:
        c.close()

    today = datetime.utcnow().date()
    rows = [["item_name", "unit", "qty_available", "expiry_date", "status", "flags"]]
    for it in items:
        qty_available = float(it["qty_available"] or 0.0)
        status = "Active" if (it["is_active"] == 1) else "Inactive"
        flags = build_stock_flags(it["expiry_date"], qty_available, it["is_active"], low_threshold, exp_days, today)
        rows.append(
            [
                it["item_name"],
                it["unit"],
                qty_available,
                it["expiry_date"] or "",
                status,
                ", ".join(flags),
            ]
        )

    return csv_response("stock_view.csv", rows)


@APP.get("/manager/items.csv")
@requires_sync_or_manager
def manager_items_csv():
    c = conn()
    try:
        items = c.execute(
            """
            SELECT item_id, item_name, unit, qty_available, unit_cost, expiry_date, is_active
            FROM items
            ORDER BY item_name
            """
        ).fetchall()
    finally:
        c.close()

    rows = [["item_id", "item_name", "unit", "qty_available", "unit_cost", "expiry_date", "status"]]
    for it in items:
        rows.append(
            [
                it["item_id"],
                it["item_name"],
                it["unit"],
                it["qty_available"],
                it["unit_cost"] if it["unit_cost"] is not None else "",
                it["expiry_date"] or "",
                "Active" if it["is_active"] == 1 else "Inactive",
            ]
        )

    return csv_response("items.csv", rows)


@APP.get("/manager/managers.csv")
@requires_sync_or_manager
def manager_managers_csv():
    c = conn()
    try:
        rows = c.execute(
            """
            SELECT manager_id, username, email, password_hash, is_active, created_at
            FROM managers
            ORDER BY username
            """
        ).fetchall()
    finally:
        c.close()

    csv_rows = [
        ["manager_id", "username", "email", "password_hash", "is_active", "created_at"]
    ]
    for r in rows:
        csv_rows.append(
            [
                r["manager_id"],
                r["username"],
                r["email"] or "",
                r["password_hash"],
                r["is_active"],
                r["created_at"],
            ]
        )
    return csv_response("managers.csv", csv_rows)


@APP.get("/manager/stock_movements.csv")
@requires_sync_or_manager
def manager_stock_movements_csv():
    c = conn()
    try:
        rows = c.execute(
            """
            SELECT movement_id, item_id, movement_type, qty, note, created_by, created_at
            FROM stock_movements
            ORDER BY movement_id
            """
        ).fetchall()
    finally:
        c.close()

    csv_rows = [
        ["movement_id", "item_id", "movement_type", "qty", "note", "created_by", "created_at"]
    ]
    for r in rows:
        csv_rows.append(
            [
                r["movement_id"],
                r["item_id"],
                r["movement_type"],
                r["qty"],
                r["note"] or "",
                r["created_by"],
                r["created_at"],
            ]
        )
    return csv_response("stock_movements.csv", csv_rows)


@APP.get("/manager/uploads.zip")
@requires_sync_or_manager
def manager_uploads_zip():
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if os.path.isdir(UPLOAD_FOLDER):
            for root, _, files in os.walk(UPLOAD_FOLDER):
                for fname in files:
                    full_path = os.path.join(root, fname)
                    rel_path = os.path.relpath(full_path, UPLOAD_FOLDER)
                    zf.write(full_path, os.path.join("uploads", rel_path))
        static_files = ["church_logo.jpeg", "hero_pantry.jpg", "hero_pantry.webp"]
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        for fname in static_files:
            full_path = os.path.join(static_dir, fname)
            if os.path.exists(full_path):
                zf.write(full_path, os.path.join("static", fname))
    mem.seek(0)
    resp = Response(mem.read(), mimetype="application/zip")
    resp.headers["Content-Disposition"] = "attachment; filename=uploads.zip"
    return resp


@APP.get("/manager/backup.zip")
@requires_manager_auth
def manager_backup_zip():
    items_bytes = to_csv_bytes(export_items_rows())
    requests_bytes = to_csv_bytes(export_requests_rows())
    movements_bytes = to_csv_bytes(export_stock_movements_rows())
    managers_bytes = to_csv_bytes(export_managers_rows())
    uploads_bytes = build_uploads_zip_bytes()

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("items.csv", items_bytes)
        zf.writestr("requests.csv", requests_bytes)
        zf.writestr("stock_movements.csv", movements_bytes)
        zf.writestr("managers.csv", managers_bytes)
        zf.writestr("uploads.zip", uploads_bytes)
    mem.seek(0)
    resp = Response(mem.read(), mimetype="application/zip")
    resp.headers["Content-Disposition"] = "attachment; filename=pantry_backup.zip"
    return resp


@APP.get("/manager/backup")
@requires_manager_auth
def manager_backup():
    body = render_template_string(
        """
        <div class="card">
          <h3>Full Backup</h3>
          <p class="muted">Download all CSVs and the uploads zip to restore later.</p>
          <p>
            <a class="btn btn-primary" href="/manager/backup.zip">Download Full Backup</a>
          </p>
          <div class="row">
            <div>
              <a class="btn" href="/manager/items.csv">Items CSV</a>
            </div>
            <div>
              <a class="btn" href="/manager/requests.csv">Requests CSV</a>
            </div>
            <div>
              <a class="btn" href="/manager/managers.csv">Managers CSV</a>
            </div>
            <div>
              <a class="btn" href="/manager/stock_movements.csv">Stock Movements CSV</a>
            </div>
            <div>
              <a class="btn" href="/manager/uploads.zip">Uploads ZIP</a>
            </div>
          </div>
          <p style="margin-top:12px;">
            <a class="btn btn-primary" href="/manager/sync_render">Sync to Render</a>
          </p>
          <div class="card">
            <h4>Restore Order</h4>
            <p class="muted">Import items, then requests, then stock movements and managers. Upload uploads.zip last.</p>
          </div>
        </div>
        """
    )
    return render_template_string(BASE, body=body)


def export_items_rows():
    c = conn()
    try:
        items = c.execute(
            """
            SELECT item_id, item_name, unit, qty_available, unit_cost, expiry_date, is_active
            FROM items
            ORDER BY item_name
            """
        ).fetchall()
    finally:
        c.close()

    rows = [["item_id", "item_name", "unit", "qty_available", "unit_cost", "expiry_date", "status"]]
    for it in items:
        rows.append(
            [
                it["item_id"],
                it["item_name"],
                it["unit"],
                it["qty_available"],
                it["unit_cost"] if it["unit_cost"] is not None else "",
                it["expiry_date"] or "",
                "Active" if it["is_active"] == 1 else "Inactive",
            ]
        )
    return rows


def export_requests_rows():
    c = conn()
    try:
        reqs = c.execute(
            """
            SELECT r.request_id, r.status, r.note, r.reject_reason, r.created_at,
                   m.name, m.phone, m.email
            FROM requests r
            JOIN members m ON m.member_id = r.member_id
            ORDER BY r.request_id
            """
        ).fetchall()
        rows = [
            [
                "request_id",
                "status",
                "created_at",
                "member_name",
                "phone",
                "email",
                "note",
                "reject_reason",
                "items",
            ]
        ]
        for r in reqs:
            items = c.execute(
                """
                SELECT i.item_name, i.unit, ri.qty_requested
                FROM request_items ri
                JOIN items i ON i.item_id = ri.item_id
                WHERE ri.request_id = ?
                """,
                (r["request_id"],),
            ).fetchall()
            item_text = "; ".join(
                [f"{it['item_name']} ({it['unit']}) x {it['qty_requested']}" for it in items]
            )
            rows.append(
                [
                    r["request_id"],
                    r["status"],
                    r["created_at"],
                    r["name"],
                    r["phone"],
                    r["email"],
                    r["note"] or "",
                    r["reject_reason"] or "",
                    item_text,
                ]
            )
    finally:
        c.close()
    return rows


def export_managers_rows():
    c = conn()
    try:
        rows_db = c.execute(
            """
            SELECT manager_id, username, email, password_hash, is_active, created_at
            FROM managers
            ORDER BY username
            """
        ).fetchall()
    finally:
        c.close()

    rows = [["manager_id", "username", "email", "password_hash", "is_active", "created_at"]]
    for r in rows_db:
        rows.append(
            [
                r["manager_id"],
                r["username"],
                r["email"] or "",
                r["password_hash"],
                r["is_active"],
                r["created_at"],
            ]
        )
    return rows


def export_stock_movements_rows():
    c = conn()
    try:
        rows_db = c.execute(
            """
            SELECT movement_id, item_id, movement_type, qty, note, created_by, created_at
            FROM stock_movements
            ORDER BY movement_id
            """
        ).fetchall()
    finally:
        c.close()

    rows = [["movement_id", "item_id", "movement_type", "qty", "note", "created_by", "created_at"]]
    for r in rows_db:
        rows.append(
            [
                r["movement_id"],
                r["item_id"],
                r["movement_type"],
                r["qty"],
                r["note"] or "",
                r["created_by"],
                r["created_at"],
            ]
        )
    return rows


def build_uploads_zip_bytes():
    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if os.path.isdir(UPLOAD_FOLDER):
            for root, _, files in os.walk(UPLOAD_FOLDER):
                for fname in files:
                    full_path = os.path.join(root, fname)
                    rel_path = os.path.relpath(full_path, UPLOAD_FOLDER)
                    zf.write(full_path, os.path.join("uploads", rel_path))
        static_files = ["church_logo.jpeg", "hero_pantry.jpg", "hero_pantry.webp"]
        static_dir = os.path.join(os.path.dirname(__file__), "static")
        for fname in static_files:
            full_path = os.path.join(static_dir, fname)
            if os.path.exists(full_path):
                zf.write(full_path, os.path.join("static", fname))
    mem.seek(0)
    return mem.read()


def post_render_import(import_type: str, filename: str, content: bytes, mime: str):
    render_base = RENDER_BASE_URL or get_setting_value("render_base_url") or session.get("render_base") or ""
    sync_token = PANTRY_SYNC_TOKEN or get_setting_value("sync_token") or session.get("sync_token") or ""
    if not (render_base and sync_token):
        raise ValueError("Render sync settings are missing.")
    url = f"{render_base}/manager/import"
    fields = {"import_type": import_type}
    body, content_type = build_multipart(fields, [("csv_file", filename, content, mime)])
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", content_type)
    req.add_header("X-PANTRY-SYNC-TOKEN", sync_token)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")


def fetch_render_file(path: str) -> bytes:
    render_base = RENDER_BASE_URL or get_setting_value("render_base_url") or session.get("render_base") or ""
    sync_token = PANTRY_SYNC_TOKEN or get_setting_value("sync_token") or session.get("sync_token") or ""
    if not (render_base and sync_token):
        raise ValueError("Render sync settings are missing.")
    url = f"{render_base}{path}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("X-PANTRY-SYNC-TOKEN", sync_token)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.read()


def apply_backup_import(
    items_bytes: bytes,
    requests_bytes: bytes,
    movements_bytes: bytes,
    managers_bytes: bytes,
    uploads_bytes: bytes,
    mirror_local: bool = False,
) -> None:
    items_stream = io.TextIOWrapper(io.BytesIO(items_bytes), encoding="utf-8", errors="replace")
    reader = csv.DictReader(items_stream)
    c = conn()
    try:
        if mirror_local:
            c.execute("DELETE FROM request_items")
            c.execute("DELETE FROM requests")
            c.execute("DELETE FROM members")
            c.execute("DELETE FROM stock_movements")
            c.execute("DELETE FROM items")
            c.execute("DELETE FROM managers")
            if os.path.isdir(UPLOAD_FOLDER):
                for root, _, files in os.walk(UPLOAD_FOLDER):
                    for fname in files:
                        os.remove(os.path.join(root, fname))

        for row in reader:
            item_id_text = (row.get("item_id") or "").strip()
            name = (row.get("item_name") or "").strip()
            unit = (row.get("unit") or "").strip()
            qty = parse_float(row.get("qty_available")) or 0.0
            unit_cost = parse_float(row.get("unit_cost"))
            expiry_date = (row.get("expiry_date") or "").strip() or None
            is_active = parse_bool_status(row.get("status") or "")
            if not name or not unit:
                continue
            if item_id_text.isdigit():
                existing = c.execute(
                    "SELECT item_id FROM items WHERE item_id=?",
                    (int(item_id_text),),
                ).fetchone()
                if existing:
                    c.execute(
                        """
                        UPDATE items
                        SET item_name=?, unit=?, qty_available=?, unit_cost=?, expiry_date=?, is_active=?
                        WHERE item_id=?
                        """,
                        (name, unit, qty, unit_cost, expiry_date, is_active, int(item_id_text)),
                    )
                else:
                    c.execute(
                        """
                        INSERT INTO items (item_id, item_name, unit, qty_available, unit_cost, expiry_date, is_active)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (int(item_id_text), name, unit, qty, unit_cost, expiry_date, is_active),
                    )
            else:
                existing = c.execute(
                    "SELECT item_id FROM items WHERE item_name=?",
                    (name,),
                ).fetchone()
                if existing:
                    c.execute(
                        """
                        UPDATE items
                        SET unit=?, qty_available=?, unit_cost=?, expiry_date=?, is_active=?
                        WHERE item_id=?
                        """,
                        (unit, qty, unit_cost, expiry_date, is_active, existing["item_id"]),
                    )
                else:
                    c.execute(
                        """
                        INSERT INTO items (item_name, unit, qty_available, unit_cost, expiry_date, is_active)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (name, unit, qty, unit_cost, expiry_date, is_active),
                    )

        req_stream = io.TextIOWrapper(io.BytesIO(requests_bytes), encoding="utf-8", errors="replace")
        req_reader = csv.DictReader(req_stream)
        for row in req_reader:
            req_id_text = (row.get("request_id") or "").strip()
            status = (row.get("status") or "PENDING").strip().upper() or "PENDING"
            created_at = (row.get("created_at") or "").strip()
            member_name = (row.get("member_name") or row.get("name") or "").strip()
            phone = (row.get("phone") or "").strip() or "unknown"
            email = (row.get("email") or "").strip()
            note = (row.get("note") or "").strip()
            reject_reason = (row.get("reject_reason") or "").strip()
            items_text = (row.get("items") or "").strip()
            if not member_name:
                continue

            if req_id_text.isdigit():
                existing_req = c.execute(
                    "SELECT request_id FROM requests WHERE request_id=?",
                    (int(req_id_text),),
                ).fetchone()
                if existing_req:
                    request_id = int(req_id_text)
                else:
                    request_id = None
            else:
                request_id = None

            member_row = c.execute(
                "SELECT member_id FROM members WHERE email=? OR phone=? ORDER BY created_at DESC LIMIT 1",
                (email, phone),
            ).fetchone()
            if member_row:
                member_id = member_row["member_id"]
                c.execute(
                    "UPDATE members SET name=?, phone=?, email=? WHERE member_id=?",
                    (member_name, phone, email, member_id),
                )
            else:
                c.execute(
                    "INSERT INTO members (name, phone, email, created_at) VALUES (?, ?, ?, ?)",
                    (member_name, phone, email, created_at or datetime.utcnow().isoformat()),
                )
                member_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

            if req_id_text.isdigit():
                if request_id is None:
                    c.execute(
                        """
                        INSERT INTO requests (request_id, member_id, status, note, reject_reason, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            int(req_id_text),
                            member_id,
                            status,
                            note,
                            reject_reason,
                            created_at or datetime.utcnow().isoformat(),
                        ),
                    )
                    request_id = int(req_id_text)
                else:
                    c.execute(
                        """
                        UPDATE requests
                        SET member_id=?, status=?, note=?, reject_reason=?, created_at=?
                        WHERE request_id=?
                        """,
                        (
                            member_id,
                            status,
                            note,
                            reject_reason,
                            created_at or datetime.utcnow().isoformat(),
                            request_id,
                        ),
                    )
                    c.execute(
                        "DELETE FROM request_items WHERE request_id=?",
                        (request_id,),
                    )
            else:
                c.execute(
                    """
                    INSERT INTO requests (member_id, status, note, reject_reason, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        member_id,
                        status,
                        note,
                        reject_reason,
                        created_at or datetime.utcnow().isoformat(),
                    ),
                )
                request_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

            if items_text:
                for part in items_text.split(";"):
                    part = part.strip()
                    if not part:
                        continue
                    qty_val = 0.0
                    name_unit = part
                    if " x " in part:
                        name_unit, qty_text = part.rsplit(" x ", 1)
                        qty_val = parse_float(qty_text) or 0.0
                    name_unit = name_unit.strip()
                    unit = ""
                    name = name_unit
                    if name_unit.endswith(")") and " (" in name_unit:
                        name, unit = name_unit.rsplit(" (", 1)
                        unit = unit[:-1]
                    name = name.strip()
                    unit = unit.strip()
                    if not name:
                        continue
                    item_row = c.execute(
                        "SELECT item_id FROM items WHERE item_name=?",
                        (name,),
                    ).fetchone()
                    if not item_row:
                        c.execute(
                            "INSERT INTO items (item_name, unit, qty_available, is_active) VALUES (?, ?, 0, 1)",
                            (name, unit or "unit"),
                        )
                        item_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
                    else:
                        item_id = item_row["item_id"]
                    if qty_val > 0:
                        c.execute(
                            "INSERT INTO request_items (request_id, item_id, qty_requested) VALUES (?, ?, ?)",
                            (request_id, item_id, qty_val),
                        )

        mgr_stream = io.TextIOWrapper(io.BytesIO(managers_bytes), encoding="utf-8", errors="replace")
        mgr_reader = csv.DictReader(mgr_stream)
        for row in mgr_reader:
            username = (row.get("username") or "").strip()
            if not username:
                continue
            email = (row.get("email") or "").strip()
            password_hash = (row.get("password_hash") or "").strip()
            is_active = 1 if str(row.get("is_active") or "1").strip() != "0" else 0
            created_at = (row.get("created_at") or "").strip() or datetime.utcnow().isoformat()
            manager_id_text = (row.get("manager_id") or "").strip()
            existing = c.execute(
                "SELECT manager_id FROM managers WHERE username=?",
                (username,),
            ).fetchone()
            if existing:
                if password_hash:
                    c.execute(
                        """
                        UPDATE managers
                        SET email=?, password_hash=?, is_active=?
                        WHERE manager_id=?
                        """,
                        (email, password_hash, is_active, existing["manager_id"]),
                    )
                else:
                    c.execute(
                        "UPDATE managers SET email=?, is_active=? WHERE manager_id=?",
                        (email, is_active, existing["manager_id"]),
                    )
            else:
                if manager_id_text.isdigit():
                    c.execute(
                        """
                        INSERT INTO managers (manager_id, username, email, password_hash, is_active, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            int(manager_id_text),
                            username,
                            email,
                            password_hash or generate_password_hash("ChangeMe123!"),
                            is_active,
                            created_at,
                        ),
                    )
                else:
                    c.execute(
                        """
                        INSERT INTO managers (username, email, password_hash, is_active, created_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            username,
                            email,
                            password_hash or generate_password_hash("ChangeMe123!"),
                            is_active,
                            created_at,
                        ),
                    )

        mov_stream = io.TextIOWrapper(io.BytesIO(movements_bytes), encoding="utf-8", errors="replace")
        mov_reader = csv.DictReader(mov_stream)
        for row in mov_reader:
            movement_id_text = (row.get("movement_id") or "").strip()
            item_id_text = (row.get("item_id") or "").strip()
            movement_type = (row.get("movement_type") or "").strip().upper()
            qty_val = parse_float(row.get("qty"))
            note = (row.get("note") or "").strip()
            created_by = (row.get("created_by") or "").strip() or "manager"
            created_at = (row.get("created_at") or "").strip() or datetime.utcnow().isoformat()
            if not item_id_text.isdigit() or not movement_type or qty_val is None:
                continue
            if movement_id_text.isdigit():
                existing = c.execute(
                    "SELECT movement_id FROM stock_movements WHERE movement_id=?",
                    (int(movement_id_text),),
                ).fetchone()
                if existing:
                    continue
                c.execute(
                    """
                    INSERT INTO stock_movements (movement_id, item_id, movement_type, qty, note, created_by, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(movement_id_text),
                        int(item_id_text),
                        movement_type,
                        qty_val,
                        note,
                        created_by,
                        created_at,
                    ),
                )
            else:
                c.execute(
                    """
                    INSERT INTO stock_movements (item_id, movement_type, qty, note, created_by, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (int(item_id_text), movement_type, qty_val, note, created_by, created_at),
                )

        c.commit()
    finally:
        c.close()

    if uploads_bytes:
        try:
            with zipfile.ZipFile(io.BytesIO(uploads_bytes)) as zf:
                safe_extract_zip(zf, os.path.dirname(__file__), ("uploads/", "static/"))
        except zipfile.BadZipFile:
            pass


@APP.route("/manager/sync_render", methods=["GET", "POST"])
@requires_manager_auth
def manager_sync_render():
    message = ""
    error = ""
    details = []

    if request.method == "POST":
        confirm = request.form.get("confirm") == "yes"
        direction = request.form.get("direction") or "push"
        mirror_local = request.form.get("mirror_local") == "yes"
        if request.form.get("save_settings") == "yes":
            session["render_base"] = (request.form.get("render_base") or "").strip().rstrip("/")
            session["sync_token"] = (request.form.get("sync_token") or "").strip()
        if not confirm:
            error = "Please confirm before syncing."
        elif not (
            (RENDER_BASE_URL or get_setting_value("render_base_url") or session.get("render_base"))
            and (PANTRY_SYNC_TOKEN or get_setting_value("sync_token") or session.get("sync_token"))
        ):
            error = "Render sync settings are missing. Set PANTRY_RENDER_BASE_URL and PANTRY_SYNC_TOKEN."
        else:
            current_base = request.host_url.rstrip("/")
            target_base = (RENDER_BASE_URL or get_setting_value("render_base_url") or session.get("render_base") or "").rstrip("/")
            if target_base and current_base == target_base:
                error = "Sync must be run from your local app, not from Render."
            else:
                try:
                    if direction == "push":
                        items_bytes = to_csv_bytes(export_items_rows())
                        requests_bytes = to_csv_bytes(export_requests_rows())
                        movements_bytes = to_csv_bytes(export_stock_movements_rows())
                        managers_bytes = to_csv_bytes(export_managers_rows())
                        uploads_bytes = build_uploads_zip_bytes()

                        steps = [
                            ("items", "items.csv", items_bytes, "text/csv"),
                            ("requests", "requests.csv", requests_bytes, "text/csv"),
                            ("stock_movements", "stock_movements.csv", movements_bytes, "text/csv"),
                            ("managers", "managers.csv", managers_bytes, "text/csv"),
                            ("uploads", "uploads.zip", uploads_bytes, "application/zip"),
                        ]
                        for import_type, filename, content, mime in steps:
                            status, _ = post_render_import(import_type, filename, content, mime)
                            details.append(f"{import_type}: HTTP {status}")
                        message = "Sync to Render completed."
                    else:
                        items_bytes = fetch_render_file("/manager/items.csv")
                        requests_bytes = fetch_render_file("/manager/requests.csv")
                        movements_bytes = fetch_render_file("/manager/stock_movements.csv")
                        managers_bytes = fetch_render_file("/manager/managers.csv")
                        uploads_bytes = fetch_render_file("/manager/uploads.zip")

                        apply_backup_import(
                            items_bytes,
                            requests_bytes,
                            movements_bytes,
                            managers_bytes,
                            uploads_bytes,
                            mirror_local=mirror_local,
                        )
                    message = "Sync from Render completed."
                except urllib.error.HTTPError as exc:
                    error = f"Sync failed: HTTP {exc.code}"
                    try:
                        details.append(exc.read().decode("utf-8", errors="replace"))
                    except Exception:
                        pass
                except Exception as exc:
                    error = f"Sync failed: {exc}"

    body = render_template_string(
        """
        <div class="card">
          <h3>Sync to Render</h3>
          <p class="muted">This will overwrite Render data with your current local data.</p>
          {% if message %}<p class="ok">{{ message }}</p>{% endif %}
          {% if error %}<p class="danger">{{ error }}</p>{% endif %}
          {% if details %}
            <div class="card">
              <h4>Details</h4>
              <ul>
                {% for line in details %}
                  <li>{{ line }}</li>
                {% endfor %}
              </ul>
            </div>
          {% endif %}
          <form method="POST">
            <label>Render base URL</label>
            <input name="render_base" value="{{ render_base or '' }}" placeholder="https://church-pantry.onrender.com" />
            <label>Sync token</label>
            <input name="sync_token" type="password" value="{{ sync_token or '' }}" />
            <label>
              <input type="checkbox" name="save_settings" value="yes" />
              Save these settings for this session
            </label>
            <label>
              <input type="checkbox" name="confirm" value="yes" />
              I understand this will overwrite data on Render.
            </label>
            <div class="card" style="margin-top:12px;">
              <h4>Mirror Mode (optional)</h4>
              <p class="muted">When checked, "Sync from Render" replaces local data to match Render exactly.</p>
              <label>
                <input type="checkbox" name="mirror_local" value="yes" />
                Replace local data with Render (wipe local first)
              </label>
            </div>
            <p style="margin-top:12px;">
              <button class="btn btn-primary" type="submit" name="direction" value="push">Sync to Render</button>
              <button class="btn" type="submit" name="direction" value="pull">Sync from Render</button>
            </p>
          </form>
          <div class="card">
            <h4>Render Settings</h4>
            <p class="muted">PANTRY_RENDER_BASE_URL: {{ render_base or 'not set' }}</p>
            <p class="muted">PANTRY_SYNC_TOKEN: {{ 'set' if sync_token else 'not set' }}</p>
          </div>
        </div>
        """,
        message=message,
        error=error,
        details=details,
        render_base=RENDER_BASE_URL or get_setting_value("render_base_url") or session.get("render_base"),
        sync_token=PANTRY_SYNC_TOKEN or get_setting_value("sync_token") or session.get("sync_token"),
    )
    return render_template_string(BASE, body=body)


@APP.route("/manager/import", methods=["GET", "POST"])
@requires_import_auth
def manager_import():
    message = ""
    error = ""

    if request.method == "POST":
        import_type = (request.form.get("import_type") or "").strip()
        csv_file = request.files.get("csv_file")
        if not import_type:
            error = "Please choose an import type."
        elif not csv_file or not csv_file.filename:
            error = "Please upload a CSV file."
        elif import_type == "backup":
            try:
                data = csv_file.read()
                with zipfile.ZipFile(io.BytesIO(data)) as zf:
                    items_bytes = zf.read("items.csv")
                    requests_bytes = zf.read("requests.csv")
                    movements_bytes = zf.read("stock_movements.csv")
                    managers_bytes = zf.read("managers.csv")
                    uploads_bytes = zf.read("uploads.zip") if "uploads.zip" in zf.namelist() else b""
                apply_backup_import(
                    items_bytes,
                    requests_bytes,
                    movements_bytes,
                    managers_bytes,
                    uploads_bytes,
                )
                message = "Backup restored successfully."
            except KeyError:
                error = "Backup zip is missing one or more required files."
            except zipfile.BadZipFile:
                error = "Backup zip is invalid."
            except Exception as exc:
                error = f"Backup restore failed: {exc}"
        else:
            stream = io.TextIOWrapper(csv_file.stream, encoding="utf-8", errors="replace")
            reader = csv.DictReader(stream)
            if import_type == "items":
                created = 0
                updated = 0
                c = conn()
                try:
                    for row in reader:
                        item_id_text = (row.get("item_id") or "").strip()
                        name = (row.get("item_name") or "").strip()
                        unit = (row.get("unit") or "").strip()
                        qty = parse_float(row.get("qty_available")) or 0.0
                        unit_cost = parse_float(row.get("unit_cost"))
                        expiry_date = (row.get("expiry_date") or "").strip() or None
                        is_active = parse_bool_status(row.get("status") or "")
                        if not name or not unit:
                            continue
                        if item_id_text.isdigit():
                            existing = c.execute(
                                "SELECT item_id FROM items WHERE item_id=?",
                                (int(item_id_text),),
                            ).fetchone()
                            if existing:
                                c.execute(
                                    """
                                    UPDATE items
                                    SET item_name=?, unit=?, qty_available=?, unit_cost=?, expiry_date=?, is_active=?
                                    WHERE item_id=?
                                    """,
                                    (name, unit, qty, unit_cost, expiry_date, is_active, int(item_id_text)),
                                )
                                updated += 1
                            else:
                                c.execute(
                                    """
                                    INSERT INTO items (item_id, item_name, unit, qty_available, unit_cost, expiry_date, is_active)
                                    VALUES (?, ?, ?, ?, ?, ?, ?)
                                    """,
                                    (int(item_id_text), name, unit, qty, unit_cost, expiry_date, is_active),
                                )
                                created += 1
                        else:
                            existing = c.execute(
                                "SELECT item_id FROM items WHERE item_name=?",
                                (name,),
                            ).fetchone()
                            if existing:
                                c.execute(
                                    """
                                    UPDATE items
                                    SET unit=?, qty_available=?, unit_cost=?, expiry_date=?, is_active=?
                                    WHERE item_id=?
                                    """,
                                    (unit, qty, unit_cost, expiry_date, is_active, existing["item_id"]),
                                )
                                updated += 1
                            else:
                                c.execute(
                                    """
                                    INSERT INTO items (item_name, unit, qty_available, unit_cost, expiry_date, is_active)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                    """,
                                    (name, unit, qty, unit_cost, expiry_date, is_active),
                                )
                                created += 1
                    c.commit()
                    message = f"Items imported. Created: {created}, Updated: {updated}."
                finally:
                    c.close()
            elif import_type == "requests":
                created = 0
                skipped = 0
                c = conn()
                try:
                    for row in reader:
                        req_id_text = (row.get("request_id") or "").strip()
                        status = (row.get("status") or "PENDING").strip().upper() or "PENDING"
                        created_at = (row.get("created_at") or "").strip()
                        member_name = (row.get("member_name") or row.get("name") or "").strip()
                        phone = (row.get("phone") or "").strip()
                        email = (row.get("email") or "").strip()
                        note = (row.get("note") or "").strip()
                        reject_reason = (row.get("reject_reason") or "").strip()
                        items_text = (row.get("items") or "").strip()

                        if not member_name:
                            skipped += 1
                            continue

                        if not phone:
                            phone = "unknown"

                        if req_id_text.isdigit():
                            existing_req = c.execute(
                                "SELECT request_id FROM requests WHERE request_id=?",
                                (int(req_id_text),),
                            ).fetchone()
                            if existing_req:
                                skipped += 1
                                continue

                        member_row = c.execute(
                            "SELECT member_id FROM members WHERE email=? OR phone=? ORDER BY created_at DESC LIMIT 1",
                            (email, phone),
                        ).fetchone()
                        if member_row:
                            member_id = member_row["member_id"]
                            c.execute(
                                "UPDATE members SET name=?, phone=?, email=? WHERE member_id=?",
                                (member_name, phone, email, member_id),
                            )
                        else:
                            c.execute(
                                "INSERT INTO members (name, phone, email, created_at) VALUES (?, ?, ?, ?)",
                                (member_name, phone, email, created_at or datetime.utcnow().isoformat()),
                            )
                            member_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

                        if req_id_text.isdigit():
                            c.execute(
                                """
                                INSERT INTO requests (request_id, member_id, status, note, reject_reason, created_at)
                                VALUES (?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    int(req_id_text),
                                    member_id,
                                    status,
                                    note,
                                    reject_reason,
                                    created_at or datetime.utcnow().isoformat(),
                                ),
                            )
                            request_id = int(req_id_text)
                        else:
                            c.execute(
                                """
                                INSERT INTO requests (member_id, status, note, reject_reason, created_at)
                                VALUES (?, ?, ?, ?, ?)
                                """,
                                (
                                    member_id,
                                    status,
                                    note,
                                    reject_reason,
                                    created_at or datetime.utcnow().isoformat(),
                                ),
                            )
                            request_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]

                        if items_text:
                            for part in items_text.split(";"):
                                part = part.strip()
                                if not part:
                                    continue
                                qty_val = 0.0
                                name_unit = part
                                if " x " in part:
                                    name_unit, qty_text = part.rsplit(" x ", 1)
                                    qty_val = parse_float(qty_text) or 0.0
                                name_unit = name_unit.strip()
                                unit = ""
                                name = name_unit
                                if name_unit.endswith(")") and " (" in name_unit:
                                    name, unit = name_unit.rsplit(" (", 1)
                                    unit = unit[:-1]
                                name = name.strip()
                                unit = unit.strip()
                                if not name:
                                    continue
                                item_row = c.execute(
                                    "SELECT item_id FROM items WHERE item_name=?",
                                    (name,),
                                ).fetchone()
                                if not item_row:
                                    c.execute(
                                        "INSERT INTO items (item_name, unit, qty_available, is_active) VALUES (?, ?, 0, 1)",
                                        (name, unit or "unit"),
                                    )
                                    item_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
                                else:
                                    item_id = item_row["item_id"]
                                if qty_val > 0:
                                    c.execute(
                                        "INSERT INTO request_items (request_id, item_id, qty_requested) VALUES (?, ?, ?)",
                                        (request_id, item_id, qty_val),
                                    )
                        created += 1
                    c.commit()
                    message = f"Requests imported. Created: {created}, Skipped: {skipped}."
                finally:
                    c.close()
            elif import_type == "managers":
                created = 0
                updated = 0
                c = conn()
                try:
                    for row in reader:
                        username = (row.get("username") or "").strip()
                        if not username:
                            continue
                        email = (row.get("email") or "").strip()
                        password_hash = (row.get("password_hash") or "").strip()
                        is_active = 1 if str(row.get("is_active") or "1").strip() != "0" else 0
                        created_at = (row.get("created_at") or "").strip() or datetime.utcnow().isoformat()
                        manager_id_text = (row.get("manager_id") or "").strip()
                        existing = c.execute(
                            "SELECT manager_id FROM managers WHERE username=?",
                            (username,),
                        ).fetchone()
                        if existing:
                            if password_hash:
                                c.execute(
                                    """
                                    UPDATE managers
                                    SET email=?, password_hash=?, is_active=?
                                    WHERE manager_id=?
                                    """,
                                    (email, password_hash, is_active, existing["manager_id"]),
                                )
                            else:
                                c.execute(
                                    """
                                    UPDATE managers
                                    SET email=?, is_active=?
                                    WHERE manager_id=?
                                    """,
                                    (email, is_active, existing["manager_id"]),
                                )
                            updated += 1
                        else:
                            if manager_id_text.isdigit():
                                c.execute(
                                    """
                                    INSERT INTO managers (manager_id, username, email, password_hash, is_active, created_at)
                                    VALUES (?, ?, ?, ?, ?, ?)
                                    """,
                                    (
                                        int(manager_id_text),
                                        username,
                                        email,
                                        password_hash or generate_password_hash("ChangeMe123!"),
                                        is_active,
                                        created_at,
                                    ),
                                )
                            else:
                                c.execute(
                                    """
                                    INSERT INTO managers (username, email, password_hash, is_active, created_at)
                                    VALUES (?, ?, ?, ?, ?)
                                    """,
                                    (
                                        username,
                                        email,
                                        password_hash or generate_password_hash("ChangeMe123!"),
                                        is_active,
                                        created_at,
                                    ),
                                )
                            created += 1
                    c.commit()
                    message = f"Managers imported. Created: {created}, Updated: {updated}."
                finally:
                    c.close()
            elif import_type == "stock_movements":
                created = 0
                skipped = 0
                c = conn()
                try:
                    for row in reader:
                        movement_id_text = (row.get("movement_id") or "").strip()
                        item_id_text = (row.get("item_id") or "").strip()
                        movement_type = (row.get("movement_type") or "").strip().upper()
                        qty_val = parse_float(row.get("qty"))
                        note = (row.get("note") or "").strip()
                        created_by = (row.get("created_by") or "").strip() or "manager"
                        created_at = (row.get("created_at") or "").strip() or datetime.utcnow().isoformat()
                        if not item_id_text.isdigit() or not movement_type or qty_val is None:
                            skipped += 1
                            continue
                        if movement_id_text.isdigit():
                            existing = c.execute(
                                "SELECT movement_id FROM stock_movements WHERE movement_id=?",
                                (int(movement_id_text),),
                            ).fetchone()
                            if existing:
                                skipped += 1
                                continue
                            c.execute(
                                """
                                INSERT INTO stock_movements (movement_id, item_id, movement_type, qty, note, created_by, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    int(movement_id_text),
                                    int(item_id_text),
                                    movement_type,
                                    qty_val,
                                    note,
                                    created_by,
                                    created_at,
                                ),
                            )
                        else:
                            c.execute(
                                """
                                INSERT INTO stock_movements (item_id, movement_type, qty, note, created_by, created_at)
                                VALUES (?, ?, ?, ?, ?, ?)
                                """,
                                (int(item_id_text), movement_type, qty_val, note, created_by, created_at),
                            )
                        created += 1
                    c.commit()
                    message = f"Stock movements imported. Created: {created}, Skipped: {skipped}."
                finally:
                    c.close()
            elif import_type == "uploads":
                try:
                    with zipfile.ZipFile(csv_file.stream) as zf:
                        safe_extract_zip(zf, os.path.dirname(__file__), ("uploads/", "static/"))
                    message = "Uploads restored."
                except zipfile.BadZipFile:
                    error = "Invalid ZIP file."
            else:
                error = "Unknown import type."

    body = render_template_string(
        """
        <div class="card">
          <h3>Import Data</h3>
          <p class="muted">Upload CSV exports to restore data after a reset.</p>
          {% if message %}<p class="ok">{{ message }}</p>{% endif %}
          {% if error %}<p class="danger">{{ error }}</p>{% endif %}
          <div class="card">
            <h4>Restore Full Backup</h4>
            <p class="muted">Upload the full backup zip to restore everything in one step.</p>
            <form method="POST" enctype="multipart/form-data">
              <input type="hidden" name="import_type" value="backup" />
              <input type="file" name="csv_file" accept=".zip" required />
              <p style="margin-top:12px;">
                <button class="btn btn-primary" type="submit">Restore Backup</button>
              </p>
            </form>
          </div>
          <form method="POST" enctype="multipart/form-data">
            <label>Import type</label>
            <select name="import_type" required>
              <option value="">Select...</option>
              <option value="backup">Full Backup (pantry_backup.zip)</option>
              <option value="items">Items (items.csv)</option>
              <option value="requests">Requests (requests.csv)</option>
              <option value="managers">Managers (managers.csv)</option>
              <option value="stock_movements">Stock Movements (stock_movements.csv)</option>
              <option value="uploads">Uploads ZIP (uploads.zip)</option>
            </select>
            <label>File</label>
            <input type="file" name="csv_file" accept=".csv,.zip" required />
            <p style="margin-top:12px;">
              <button class="btn btn-primary" type="submit">Import CSV</button>
            </p>
          </form>
          <div class="card">
            <h4>Tips</h4>
            <p class="muted">Import items first, then requests. Stock movements and managers after. Upload uploads.zip last.</p>
          </div>
        </div>
        """,
        message=message,
        error=error,
    )
    return render_template_string(BASE, body=body)


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


# ============================================================
# Local run
# ============================================================
if __name__ == "__main__":
    APP.run(host="0.0.0.0", port=5000, debug=True)
