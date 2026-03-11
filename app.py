"""
HTV Brand Dashboard — FastAPI Backend
Queries BigQuery live data and serves per-manufacturer dashboards.

★ DEAL TERMS CONFIG — To update fee terms for any manufacturer,
  edit the MANUFACTURER_FEES dict below. No other code changes needed.
"""

import os, json, hashlib, hmac, logging, asyncio, time, secrets
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, HTTPException, Query, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from google.cloud import bigquery
from google.api_core import exceptions as gcp_exceptions
from google.oauth2 import service_account

logger = logging.getLogger("brand-dashboard")

# ============================================================
# IN-MEMORY CACHE (TTL-based)
# ============================================================
CACHE_TTL = 300  # 5 minutes
_cache: dict[str, tuple[float, any]] = {}


def cache_key(endpoint: str, **kwargs) -> str:
    """Build a deterministic cache key from endpoint + params."""
    parts = sorted(f"{k}={v}" for k, v in kwargs.items() if v)
    return f"{endpoint}|{'|'.join(parts)}"


def cache_get(key: str):
    """Return cached value if still fresh, else None."""
    entry = _cache.get(key)
    if entry and (time.time() - entry[0]) < CACHE_TTL:
        return entry[1]
    return None


def cache_set(key: str, value):
    """Store a value in cache with current timestamp."""
    _cache[key] = (time.time(), value)


class BigQueryError(Exception):
    """Raised when BigQuery returns an error we want to surface gracefully."""
    def __init__(self, message: str, status_code: int = 503):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


app = FastAPI(title="HTV Brand Dashboard")
templates = Jinja2Templates(directory="templates")

# Mount static files (logos, etc.)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ============================================================
# BRAND CONFIG — colours + logo paths per manufacturer
# ============================================================
BRAND_CONFIG = {
    "cannamedical": {"color": "#2D6A24", "logo": "https://cannamedical.com/wp-content/uploads/2025/05/Cannamedical_Pharma_Logo_4c.svg", "name": "Cannamedical", "logo_invert": True},
    "four20":       {"color": "#016269", "logo": "https://cdn.prod.website-files.com/67cb2220ad02e7e2eeec6827/67cb2220ad02e7e2eeec6ae5_420Pharma-logo-schrift-weiss.svg", "name": "Four 20 Pharma", "logo_invert": False},
    "aurora":       {"color": "#052155", "logo": "https://images.ctfassets.net/g2i7xgi5mblj/5XTALMrsJhZHlWJ97YLUxh/d2616352d029c27dff86242a0eb06c71/Aurora_Europe_Primary_Logo_Blue_4x.png", "name": "Aurora", "logo_invert": True},
    "demecan":      {"color": "#002D4E", "logo": "https://www.demecan.de/wp-content/uploads/2023/06/demecan_logo_w.svg", "name": "Demecan", "logo_invert": False},
    "enua":         {"color": "#193032", "logo": "/static/logos/enua.svg", "name": "enua", "logo_invert": False},
    "alephsana":    {"color": "#103C3A", "logo": "https://www.alephsana.com/wp-content/uploads/2023/02/AlephSana_Logo_Lockup_Stacked_alephGreen.svg", "name": "AlephSana", "logo_invert": True},
    "iuvo":         {"color": "#000000", "logo": "https://cdn.prod.website-files.com/64231bdbeac474fbfe4ff7c7/642323c0d6586d71c14b9bda_IUVO-Logo.svg", "name": "IUVO", "logo_invert": False},
    "avaay":        {"color": "#181A1B", "logo": "/static/logos/avaay.svg", "name": "avaay", "logo_invert": True},
}

# ============================================================
# SESSION & SECURITY CONFIG
# ============================================================
SESSION_SECRET = os.getenv("SESSION_SECRET", secrets.token_hex(32))
PASSWORD_EXPIRY_DAYS = int(os.getenv("PASSWORD_EXPIRY_DAYS", "90"))

app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    session_cookie="htv_session",
    max_age=8 * 3600,        # Session expires after 8 hours
    same_site="lax",
    https_only=os.getenv("RENDER", "") != "",  # HTTPS-only on Render
)

# ============================================================
# RATE LIMITING (in-memory, per-IP)
# ============================================================
_login_attempts: dict[str, list[float]] = {}
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_SECONDS = 900  # 15 minutes


def check_rate_limit(ip: str) -> bool:
    """Return True if IP is allowed to attempt login, False if locked out."""
    now = time.time()
    attempts = _login_attempts.get(ip, [])
    # Keep only attempts within the lockout window
    attempts = [t for t in attempts if now - t < LOCKOUT_SECONDS]
    _login_attempts[ip] = attempts
    return len(attempts) < MAX_LOGIN_ATTEMPTS


def record_failed_attempt(ip: str):
    """Record a failed login attempt for rate limiting."""
    _login_attempts.setdefault(ip, []).append(time.time())


@app.exception_handler(BigQueryError)
async def bigquery_error_handler(request: Request, exc: BigQueryError):
    """Return a clean JSON error instead of a 500 traceback."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.message, "status": "unavailable"},
    )

@app.exception_handler(Exception)
async def general_error_handler(request: Request, exc: Exception):
    """Catch-all: log the traceback but return a safe generic message."""
    import traceback
    tb = traceback.format_exc()
    logger.error("Unhandled error: %s\n%s", exc, tb)
    return JSONResponse(
        status_code=500,
        content={"error": "An unexpected error occurred. Please try again later."},
    )

# ============================================================
# BIGQUERY CONNECTION
# Supports three auth modes (checked in order):
#  1. GOOGLE_SA_KEY_JSON env var (for Render / external hosting)
#  2. sa-key.json file in project root (for local development)
#  3. Application Default Credentials (for Cloud Run)
# ============================================================
SA_KEY_JSON = os.getenv("GOOGLE_SA_KEY_JSON", "")
SA_KEY_FILE = os.path.join(os.path.dirname(__file__), "sa-key.json")

if SA_KEY_JSON:
    sa_info = json.loads(SA_KEY_JSON)
    credentials = service_account.Credentials.from_service_account_info(
        sa_info, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    bq_client = bigquery.Client(credentials=credentials, project=sa_info.get("project_id"))
elif os.path.exists(SA_KEY_FILE):
    credentials = service_account.Credentials.from_service_account_file(
        SA_KEY_FILE, scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
else:
    bq_client = bigquery.Client()

PROJECT_DATASET = os.getenv("BQ_DATASET", "htv-data-foundation-prod.datamarts")

# ============================================================
# ★ MANUFACTURER AUTH
# Each manufacturer has a password (stored in env vars).
# Passwords are compared using constant-time hmac to prevent
# timing attacks. Set PASSWORD_SET_<SLUG> env vars to track
# when passwords were last rotated (format: YYYY-MM-DD).
# ============================================================
MANUFACTURER_PASSWORDS = {
    "cannamedical":  os.getenv("TOKEN_CANNAMEDICAL",  "cm_demo_token_2025"),
    "four20":        os.getenv("TOKEN_FOUR20",         "f20_demo_token_2025"),
    "aurora":        os.getenv("TOKEN_AURORA",          "au_demo_token_2025"),
    "demecan":       os.getenv("TOKEN_DEMECAN",         "dm_demo_token_2025"),
    "enua":          os.getenv("TOKEN_ENUA",            "en_demo_token_2025"),
    "alephsana":     os.getenv("TOKEN_ALEPHSANA",       "al_demo_token_2025"),
    "iuvo":          os.getenv("TOKEN_IUVO",            "iv_demo_token_2025"),
    "avaay":         os.getenv("TOKEN_AVAAY",           "av_demo_token_2025"),
}

# Password set dates — used for 90-day expiry. Default = today (first deploy).
_today_str = datetime.now().strftime("%Y-%m-%d")
MANUFACTURER_PASSWORD_SET = {
    slug: os.getenv(f"PASSWORD_SET_{slug.upper()}", _today_str)
    for slug in MANUFACTURER_PASSWORDS
}

# Slug → actual BigQuery product_manufacturer_name
MANUFACTURER_BQ_NAMES = {
    "cannamedical":  "Cannamedical",
    "four20":        "Four 20 Pharma",
    "aurora":        "Aurora",
    "demecan":       "Demecan",
    "enua":          "enua",
    "alephsana":     "AlephSana",
    "iuvo":          "IUVO",
    "avaay":         "avaay Medical",
}

# ============================================================
# ★ DEAL TERMS — EASY TO EDIT ★
# To change deal terms in the future, only edit this dict.
# type options: 'per_gram', 'percentage', 'fixed', 'advance'
# ============================================================
MANUFACTURER_FEES = {
    "cannamedical": {
        "type": "percentage", "rate": 0.0625,
        "label": "6.25%", "desc": "Percentage of net revenue",
        "effective_date": "2024-06-01",
        "notes": "Renegotiated Jun 2024",
    },
    "four20": {
        "type": "per_gram", "rate": 0.40,
        "label": "€0.40 / g", "desc": "Per-gram transaction fee",
        "effective_date": "2025-01-01",
        "notes": "Review scheduled Jul 2026",
    },
    "aurora": {
        "type": "per_gram", "rate": 0.90,
        "label": "€0.90 / g", "desc": "Per-gram transaction fee",
        "effective_date": "2024-01-01",
        "notes": "",
    },
    "demecan": {
        "type": "fixed", "rate": 50000,
        "label": "€50,000 / month", "desc": "Fixed monthly fee",
        "effective_date": "2024-03-01",
        "notes": "",
    },
    "enua": {
        "type": "per_gram", "rate": 0.50,
        "label": "€0.50 / g", "desc": "Per-gram transaction fee",
        "effective_date": "2025-01-01",
        "notes": "",
    },
    "alephsana": {
        "type": "per_gram", "rate": 0.90,
        "label": "€0.90 / g", "desc": "Per-gram transaction fee",
        "effective_date": "2024-09-01",
        "notes": "",
    },
    "iuvo": {
        "type": "per_gram", "rate": 0.90,
        "label": "€0.90 / g", "desc": "Per-gram transaction fee",
        "effective_date": "2025-03-01",
        "notes": "",
    },
    "avaay": {
        "type": "advance", "rate": 0.375,
        "label": "€300k prepaid (800 kg)", "desc": "Advance — €300,000 for 800 kg",
        "effective_date": "2025-01-01",
        "notes": "Prepaid 800 kg; new contract after volume fulfilled",
        "advance_total_eur": 300000,
        "advance_total_kg": 800,
    },
}
# ============================================================

def verify_password(slug: str, password: str) -> bool:
    """Check password using constant-time comparison."""
    expected = MANUFACTURER_PASSWORDS.get(slug)
    if not expected:
        return False
    return hmac.compare_digest(password, expected)


def is_password_expired(slug: str) -> bool:
    """Check if the manufacturer's password has exceeded the rotation period."""
    set_date_str = MANUFACTURER_PASSWORD_SET.get(slug, "")
    if not set_date_str:
        return False
    try:
        set_date = datetime.strptime(set_date_str, "%Y-%m-%d")
        return (datetime.now() - set_date).days > PASSWORD_EXPIRY_DAYS
    except ValueError:
        return False


def verify_session(request: Request, slug: str) -> str:
    """Check session cookie and return BQ manufacturer name, or raise 403."""
    session_slug = request.session.get("slug")
    if session_slug != slug:
        raise HTTPException(status_code=403, detail="Not authenticated")
    return MANUFACTURER_BQ_NAMES[slug]


def calc_fee(fee_info: dict, volume_g, revenue, months: int = 1) -> float:
    """Calculate fee based on deal terms."""
    t = fee_info["type"]
    r = fee_info["rate"]
    vol = float(volume_g or 0)
    rev = float(revenue or 0)
    if t == "per_gram":
        return vol * r
    elif t == "percentage":
        return rev * r
    elif t == "fixed":
        return r * months
    elif t == "advance":
        return vol * r
    return 0


def run_query(sql: str, params: list = None):
    """Execute a parameterized BigQuery query and return rows as dicts."""
    job_config = bigquery.QueryJobConfig(query_parameters=params or [])
    try:
        result = bq_client.query(sql, job_config=job_config).result()
        return [dict(row) for row in result]
    except gcp_exceptions.Forbidden as exc:
        logger.error("BigQuery permission denied: %s", exc)
        raise BigQueryError(
            "Data temporarily unavailable — BigQuery access is being configured. "
            "Please contact your HTV administrator.",
            status_code=503,
        )
    except gcp_exceptions.NotFound as exc:
        logger.error("BigQuery resource not found: %s", exc)
        raise BigQueryError(
            "The requested data source could not be found. "
            "Please contact your HTV administrator.",
            status_code=404,
        )
    except gcp_exceptions.BadRequest as exc:
        logger.error("BigQuery bad request: %s", exc)
        raise BigQueryError(
            "There was a problem with the data query. "
            "Please adjust your filters or contact support.",
            status_code=400,
        )
    except Exception as exc:
        logger.error("Unexpected BigQuery error: %s", exc)
        raise BigQueryError(
            "An unexpected error occurred while fetching data. "
            "Please try again later or contact support.",
            status_code=500,
        )


async def run_query_async(sql: str, params: list = None):
    """Run a BigQuery query on a thread so multiple can execute in parallel."""
    return await asyncio.to_thread(run_query, sql, params)


def date_params(start_date: str, end_date: str, category: str = ""):
    """Build date + category filter SQL + params."""
    clauses, params = [], []
    if start_date:
        clauses.append("DATE(o.created_at) >= @start_date")
        params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))
    if end_date:
        clauses.append("DATE(o.created_at) <= @end_date")
        params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date))
    if category:
        clauses.append("oi.product_vertical = @category")
        params.append(bigquery.ScalarQueryParameter("category", "STRING", category))
    where = (" AND " + " AND ".join(clauses)) if clauses else ""
    return where, params


# ============================================================
# PAGE ROUTES — Login + Dashboard
# ============================================================
@app.get("/brand/{slug}", response_class=HTMLResponse)
async def brand_page(request: Request, slug: str):
    """Show dashboard if logged in, otherwise redirect to login."""
    if slug not in MANUFACTURER_BQ_NAMES:
        raise HTTPException(status_code=404, detail="Brand not found")

    # Check session
    if request.session.get("slug") != slug:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "slug": slug,
            "manufacturer_name": MANUFACTURER_BQ_NAMES[slug],
            "error": "",
        })

    fee = MANUFACTURER_FEES.get(slug, {})
    brand = BRAND_CONFIG.get(slug, {"color": "#1e293b", "logo": "", "name": slug})
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "slug": slug,
        "manufacturer_name": MANUFACTURER_BQ_NAMES[slug],
        "fee": fee,
        "brand": brand,
    })


@app.post("/brand/{slug}/login")
async def brand_login(request: Request, slug: str, password: str = Form(...)):
    """Handle login form submission."""
    if slug not in MANUFACTURER_BQ_NAMES:
        raise HTTPException(status_code=404, detail="Brand not found")

    mfg_name = MANUFACTURER_BQ_NAMES[slug]
    client_ip = request.client.host if request.client else "unknown"

    # Rate limiting
    if not check_rate_limit(client_ip):
        return templates.TemplateResponse("login.html", {
            "request": request, "slug": slug,
            "manufacturer_name": mfg_name,
            "error": "Too many login attempts. Please try again in 15 minutes.",
        }, status_code=429)

    # Check password expiry
    if is_password_expired(slug):
        return templates.TemplateResponse("login.html", {
            "request": request, "slug": slug,
            "manufacturer_name": mfg_name,
            "error": "Your password has expired. Please contact HTV for a new password.",
        }, status_code=403)

    # Verify password
    if not verify_password(slug, password):
        record_failed_attempt(client_ip)
        return templates.TemplateResponse("login.html", {
            "request": request, "slug": slug,
            "manufacturer_name": mfg_name,
            "error": "Invalid password. Please try again.",
        }, status_code=401)

    # Success — set session
    request.session["slug"] = slug
    request.session["login_time"] = datetime.now().isoformat()
    return RedirectResponse(url=f"/brand/{slug}", status_code=303)


@app.get("/brand/{slug}/logout")
async def brand_logout(request: Request, slug: str):
    """Clear session and redirect to login."""
    request.session.clear()
    return RedirectResponse(url=f"/brand/{slug}", status_code=303)


# ============================================================
# API: Summary KPIs
# ============================================================
@app.get("/api/brand/{slug}/summary")
async def api_summary(
    request: Request, slug: str, start_date: str = "", end_date: str = "",
    category: str = "", compare_start: str = "", compare_end: str = "",
):
    mfg_name = verify_session(request, slug)

    # Check cache first
    ck = cache_key("summary", slug=slug, s=start_date, e=end_date, cat=category,
                   cs=compare_start, ce=compare_end)
    cached = cache_get(ck)
    if cached:
        return cached

    date_where, date_p = date_params(start_date, end_date, category)

    # If the frontend doesn't supply explicit compare dates, fall back to
    # the "previous period" logic (same-length window before start_date).
    s = start_date or "2020-01-01"
    e = end_date or datetime.now().strftime("%Y-%m-%d")
    cs = compare_start or ""
    ce = compare_end or ""

    # Build comparison WHERE clause
    if cs and ce:
        # Frontend sent explicit comparison dates
        compare_clause = "DATE(o.created_at) >= DATE(@comp_start) AND DATE(o.created_at) <= DATE(@comp_end)"
    else:
        # Default: mirror-length window right before the current start
        compare_clause = (
            "DATE(o.created_at) >= DATE_SUB(DATE(@comp_start), "
            "INTERVAL DATE_DIFF(DATE(@comp_end), DATE(@comp_start), DAY) DAY) "
            "AND DATE(o.created_at) < DATE(@comp_start)"
        )
        cs = s
        ce = e

    sql = f"""
    WITH curr AS (
      SELECT
        COUNT(DISTINCT o.order_id) AS prescriptions,
        SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur) AS revenue_eur,
        SUM(oi.quantity_after_cancellations) AS sales_volume_g,
        (SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) - COALESCE(SUM(oi.vat_amount_after_cancellations_eur),0) - COALESCE(SUM(oi.refund_amount_including_vat_eur),0)) AS net_revenue_eur,
        SAFE_DIVIDE(SUM(oi.cancelled_quantity), SUM(oi.quantity_before_cancellations)) AS cancellation_rate,
        SAFE_DIVIDE(SUM(oi.quantity_after_cancellations), COUNT(DISTINCT o.order_id)) AS avg_g_per_prescription,
        SAFE_DIVIDE(SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur), NULLIF(SUM(oi.quantity_after_cancellations),0)) AS avg_eur_per_g,
        SAFE_DIVIDE(COUNT(oi.order_item_id), COUNT(DISTINCT o.order_id)) AS avg_products_per_order,
        COUNT(DISTINCT o.customer_id) AS total_patients,
        SAFE_DIVIDE(SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur), COUNT(DISTINCT o.order_id)) AS avg_order_value
      FROM `{PROJECT_DATASET}.order_items` oi
      JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
      WHERE oi.product_manufacturer_name = @mfg {date_where}
    ),
    repeat_stats AS (
      SELECT
        SAFE_DIVIDE(COUNTIF(order_count >= 2), COUNT(*)) AS repeat_purchase_rate
      FROM (
        SELECT o.customer_id, COUNT(DISTINCT o.order_id) AS order_count
        FROM `{PROJECT_DATASET}.order_items` oi
        JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
        WHERE oi.product_manufacturer_name = @mfg {date_where}
        GROUP BY 1
      )
    ),
    prev AS (
      SELECT
        COUNT(DISTINCT o.order_id) AS prescriptions,
        SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur) AS revenue_eur,
        SUM(oi.quantity_after_cancellations) AS sales_volume_g,
        (SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) - COALESCE(SUM(oi.vat_amount_after_cancellations_eur),0) - COALESCE(SUM(oi.refund_amount_including_vat_eur),0)) AS net_revenue_eur
      FROM `{PROJECT_DATASET}.order_items` oi
      JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
      WHERE oi.product_manufacturer_name = @mfg
        AND {compare_clause}
        {"AND oi.product_vertical = @category" if category else ""}
    )
    SELECT
      c.*, p.prescriptions AS prev_rx, p.revenue_eur AS prev_rev,
      p.sales_volume_g AS prev_vol, p.net_revenue_eur AS prev_net,
      rp.repeat_purchase_rate
    FROM curr c, prev p, repeat_stats rp
    """

    params = [
        bigquery.ScalarQueryParameter("mfg", "STRING", mfg_name),
        bigquery.ScalarQueryParameter("comp_start", "DATE", cs),
        bigquery.ScalarQueryParameter("comp_end", "DATE", ce),
    ] + date_p

    rows = run_query(sql, params)
    r = rows[0] if rows else {}
    fee = MANUFACTURER_FEES.get(slug, {})
    fee_amount = calc_fee(fee, r.get("sales_volume_g", 0), r.get("net_revenue_eur", 0))

    def safe_growth(curr, prev):
        if curr is None or prev is None or prev == 0:
            return None
        return (curr - prev) / prev

    result = {
        "current": {
            "prescriptions": r.get("prescriptions"),
            "revenue_eur": r.get("revenue_eur"),
            "sales_volume_g": r.get("sales_volume_g"),
            "net_revenue_eur": r.get("net_revenue_eur"),
            "cancellation_rate": r.get("cancellation_rate"),
            "avg_g_per_prescription": r.get("avg_g_per_prescription"),
            "avg_eur_per_g": r.get("avg_eur_per_g"),
            "avg_products_per_order": r.get("avg_products_per_order"),
            "total_patients": r.get("total_patients"),
            "avg_order_value": r.get("avg_order_value"),
            "repeat_purchase_rate": r.get("repeat_purchase_rate"),
        },
        "growth": {
            "prescriptions": safe_growth(r.get("prescriptions"), r.get("prev_rx")),
            "revenue": safe_growth(r.get("revenue_eur"), r.get("prev_rev")),
            "volume": safe_growth(r.get("sales_volume_g"), r.get("prev_vol")),
            "net_revenue": safe_growth(r.get("net_revenue_eur"), r.get("prev_net")),
        },
        "fee": {"amount": fee_amount},
    }
    cache_set(ck, result)
    return result


# ============================================================
# API: Monthly Trends
# ============================================================
@app.get("/api/brand/{slug}/trends")
async def api_trends(request: Request, slug: str, start_date: str = "", end_date: str = "", category: str = ""):
    mfg_name = verify_session(request, slug)

    ck = cache_key("trends", slug=slug, s=start_date, e=end_date, cat=category)
    cached = cache_get(ck)
    if cached:
        return cached

    date_where, date_p = date_params(start_date, end_date, category)

    sql = f"""
    SELECT
      FORMAT_DATE('%Y-%m', DATE(o.created_at)) AS period,
      COUNT(DISTINCT o.order_id) AS prescriptions,
      SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur) AS revenue_eur,
      SUM(oi.quantity_after_cancellations) AS sales_volume_g,
      (SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) - COALESCE(SUM(oi.vat_amount_after_cancellations_eur),0) - COALESCE(SUM(oi.refund_amount_including_vat_eur),0)) AS net_revenue_eur,
      SAFE_DIVIDE(SUM(oi.cancelled_quantity), SUM(oi.quantity_before_cancellations)) AS cancellation_rate,
      SUM(oi.refund_amount_including_vat_eur) AS refund_eur,
      SAFE_DIVIDE(SUM(oi.refund_amount_including_vat_eur), NULLIF(SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur),0)) AS refund_rate
    FROM `{PROJECT_DATASET}.order_items` oi
    JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
    WHERE oi.product_manufacturer_name = @mfg {date_where}
    GROUP BY period ORDER BY period
    """
    params = [bigquery.ScalarQueryParameter("mfg", "STRING", mfg_name)] + date_p
    rows = run_query(sql, params)

    fee = MANUFACTURER_FEES.get(slug, {})
    for r in rows:
        r["fee_amount"] = calc_fee(fee, r.get("sales_volume_g", 0), r.get("net_revenue_eur", 0))

    result = {"data": rows}
    cache_set(ck, result)
    return result


# ============================================================
# API: Products (with region, brand, category, origin filters)
# ============================================================
@app.get("/api/brand/{slug}/products")
async def api_products(
    request: Request, slug: str, start_date: str = "", end_date: str = "",
    region: str = "", brand: str = "", category: str = "", origin: str = "",
):
    mfg_name = verify_session(request, slug)

    ck = cache_key("products", slug=slug, s=start_date, e=end_date,
                   region=region, brand=brand, category=category, origin=origin)
    cached = cache_get(ck)
    if cached:
        return cached

    date_where, date_p = date_params(start_date, end_date)

    extra_where = ""
    extra_params = []

    if region:
        extra_where += " AND o.shipping_address.region = @region"
        extra_params.append(bigquery.ScalarQueryParameter("region", "STRING", region))
    if brand:
        extra_where += " AND oi.product_brand_name = @brand"
        extra_params.append(bigquery.ScalarQueryParameter("brand", "STRING", brand))
    if category:
        extra_where += " AND oi.product_vertical = @category"
        extra_params.append(bigquery.ScalarQueryParameter("category", "STRING", category))
    if origin:
        extra_where += " AND oi.product_country_or_origin = @origin"
        extra_params.append(bigquery.ScalarQueryParameter("origin", "STRING", origin))

    sql = f"""
    SELECT
      oi.product_name,
      oi.product_brand_name,
      oi.product_vertical AS category,
      oi.product_country_or_origin AS origin,
      COUNT(DISTINCT o.order_id) AS prescriptions,
      SUM(oi.quantity_after_cancellations) AS volume_g,
      SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur) AS revenue_eur,
      (SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) - COALESCE(SUM(oi.vat_amount_after_cancellations_eur),0) - COALESCE(SUM(oi.refund_amount_including_vat_eur),0)) AS net_revenue_eur,
      SAFE_DIVIDE(SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur), NULLIF(SUM(oi.quantity_after_cancellations),0)) AS avg_eur_per_g,
      SAFE_DIVIDE(SUM(oi.quantity_after_cancellations), COUNT(DISTINCT o.order_id)) AS avg_g_per_rx
    FROM `{PROJECT_DATASET}.order_items` oi
    JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
    WHERE oi.product_manufacturer_name = @mfg {date_where} {extra_where}
    GROUP BY 1,2,3,4
    ORDER BY revenue_eur DESC
    """
    params = [bigquery.ScalarQueryParameter("mfg", "STRING", mfg_name)] + date_p + extra_params
    result = {"data": run_query(sql, params)}
    cache_set(ck, result)
    return result


# ============================================================
# API: Breakdowns (category, origin, price tier, products/order)
# ============================================================
@app.get("/api/brand/{slug}/breakdowns")
async def api_breakdowns(request: Request, slug: str, start_date: str = "", end_date: str = "", category: str = ""):
    mfg_name = verify_session(request, slug)

    # Check cache first
    ck = cache_key("breakdowns", slug=slug, s=start_date, e=end_date, cat=category)
    cached = cache_get(ck)
    if cached:
        return cached

    date_where, date_p = date_params(start_date, end_date, category)
    base_params = [bigquery.ScalarQueryParameter("mfg", "STRING", mfg_name)] + date_p

    cat_sql = f"""
    SELECT oi.product_vertical AS category,
      (SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) - COALESCE(SUM(oi.vat_amount_after_cancellations_eur),0) - COALESCE(SUM(oi.refund_amount_including_vat_eur),0)) AS net_revenue_eur
    FROM `{PROJECT_DATASET}.order_items` oi
    JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
    WHERE oi.product_manufacturer_name = @mfg {date_where}
    GROUP BY 1 ORDER BY net_revenue_eur DESC
    """
    ori_sql = f"""
    SELECT oi.product_country_or_origin AS origin,
      (SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) - COALESCE(SUM(oi.vat_amount_after_cancellations_eur),0) - COALESCE(SUM(oi.refund_amount_including_vat_eur),0)) AS net_revenue_eur
    FROM `{PROJECT_DATASET}.order_items` oi
    JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
    WHERE oi.product_manufacturer_name = @mfg {date_where}
    GROUP BY 1 ORDER BY net_revenue_eur DESC
    """
    pt_sql = f"""
    SELECT
      CASE
        WHEN SAFE_DIVIDE(oi.total_price_after_cancellations_before_discounts_including_vat_eur, NULLIF(oi.quantity_after_cancellations,0)) < 6 THEN '< €6/g'
        WHEN SAFE_DIVIDE(oi.total_price_after_cancellations_before_discounts_including_vat_eur, NULLIF(oi.quantity_after_cancellations,0)) < 8 THEN '€6–8/g'
        WHEN SAFE_DIVIDE(oi.total_price_after_cancellations_before_discounts_including_vat_eur, NULLIF(oi.quantity_after_cancellations,0)) < 10 THEN '€8–10/g'
        WHEN SAFE_DIVIDE(oi.total_price_after_cancellations_before_discounts_including_vat_eur, NULLIF(oi.quantity_after_cancellations,0)) < 14 THEN '€10–14/g'
        ELSE '> €14/g'
      END AS price_tier,
      (SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) - COALESCE(SUM(oi.vat_amount_after_cancellations_eur),0) - COALESCE(SUM(oi.refund_amount_including_vat_eur),0)) AS net_revenue_eur
    FROM `{PROJECT_DATASET}.order_items` oi
    JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
    WHERE oi.product_manufacturer_name = @mfg {date_where}
    GROUP BY 1 ORDER BY 1
    """
    ppo_sql = f"""
    SELECT items AS products_per_order, COUNT(*) AS order_count
    FROM (
      SELECT o.order_id, LEAST(COUNT(oi.order_item_id), 4) AS items
      FROM `{PROJECT_DATASET}.order_items` oi
      JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
      WHERE oi.product_manufacturer_name = @mfg {date_where}
      GROUP BY 1
    ) GROUP BY 1 ORDER BY 1
    """
    # Run all 4 queries in parallel instead of sequentially
    cats, oris, pts, ppos = await asyncio.gather(
        run_query_async(cat_sql, list(base_params)),
        run_query_async(ori_sql, list(base_params)),
        run_query_async(pt_sql, list(base_params)),
        run_query_async(ppo_sql, list(base_params)),
    )
    result = {
        "categories": cats,
        "origins": oris,
        "price_tiers": pts,
        "products_per_order": ppos,
    }
    cache_set(ck, result)
    return result


# ============================================================
# API: Patient Insights
# ============================================================
@app.get("/api/brand/{slug}/patients")
async def api_patients(request: Request, slug: str, start_date: str = "", end_date: str = "", category: str = ""):
    mfg_name = verify_session(request, slug)

    ck = cache_key("patients", slug=slug, s=start_date, e=end_date, cat=category)
    cached = cache_get(ck)
    if cached:
        return cached

    date_where, date_p = date_params(start_date, end_date, category)
    base_params = [bigquery.ScalarQueryParameter("mfg", "STRING", mfg_name)] + date_p

    nr_sql = f"""
    WITH first_order AS (
      SELECT o.customer_id, MIN(DATE(o.created_at)) AS first_date
      FROM `{PROJECT_DATASET}.order_items` oi
      JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
      WHERE oi.product_manufacturer_name = @mfg
        {"AND oi.product_vertical = @category" if category else ""}
      GROUP BY 1
    )
    SELECT
      FORMAT_DATE('%Y-%m', DATE(o.created_at)) AS period,
      IF(DATE(o.created_at) = f.first_date, 'new', 'returning') AS patient_type,
      COUNT(DISTINCT o.customer_id) AS patient_count
    FROM `{PROJECT_DATASET}.order_items` oi
    JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
    JOIN first_order f ON o.customer_id = f.customer_id
    WHERE oi.product_manufacturer_name = @mfg {date_where}
    GROUP BY 1,2 ORDER BY 1,2
    """
    age_sql = f"""
    SELECT
      CASE
        WHEN o.customer_age_at_time_of_order BETWEEN 18 AND 25 THEN '18–25'
        WHEN o.customer_age_at_time_of_order BETWEEN 26 AND 35 THEN '26–35'
        WHEN o.customer_age_at_time_of_order BETWEEN 36 AND 45 THEN '36–45'
        WHEN o.customer_age_at_time_of_order BETWEEN 46 AND 55 THEN '46–55'
        WHEN o.customer_age_at_time_of_order BETWEEN 56 AND 65 THEN '56–65'
        WHEN o.customer_age_at_time_of_order > 65 THEN '65+'
        ELSE 'Unknown'
      END AS age_segment,
      COUNT(DISTINCT o.customer_id) AS patient_count
    FROM `{PROJECT_DATASET}.order_items` oi
    JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
    WHERE oi.product_manufacturer_name = @mfg {date_where}
    GROUP BY 1 ORDER BY 1
    """
    reg_sql = f"""
    SELECT o.shipping_address.region AS region,
      COUNT(DISTINCT o.customer_id) AS patient_count
    FROM `{PROJECT_DATASET}.order_items` oi
    JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
    WHERE oi.product_manufacturer_name = @mfg {date_where}
      AND o.shipping_address.region IS NOT NULL
    GROUP BY 1 ORDER BY patient_count DESC LIMIT 15
    """
    # Run all 3 queries in parallel instead of sequentially
    nr, ages, regs = await asyncio.gather(
        run_query_async(nr_sql, list(base_params)),
        run_query_async(age_sql, list(base_params)),
        run_query_async(reg_sql, list(base_params)),
    )
    result = {
        "new_returning": nr,
        "age_segments": ages,
        "regions": regs,
    }
    cache_set(ck, result)
    return result


# ============================================================
# API: Pricing (Avg €/g over time)
# ============================================================
@app.get("/api/brand/{slug}/pricing")
async def api_pricing(request: Request, slug: str, start_date: str = "", end_date: str = "", category: str = ""):
    mfg_name = verify_session(request, slug)

    ck = cache_key("pricing", slug=slug, s=start_date, e=end_date, cat=category)
    cached = cache_get(ck)
    if cached:
        return cached

    date_where, date_p = date_params(start_date, end_date, category)

    sql = f"""
    SELECT
      FORMAT_DATE('%Y-%m', DATE(o.created_at)) AS period,
      SAFE_DIVIDE(SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur), NULLIF(SUM(oi.quantity_after_cancellations),0)) AS avg_eur_per_g
    FROM `{PROJECT_DATASET}.order_items` oi
    JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
    WHERE oi.product_manufacturer_name = @mfg {date_where}
    GROUP BY period ORDER BY period
    """
    params = [bigquery.ScalarQueryParameter("mfg", "STRING", mfg_name)] + date_p
    result = {"data": run_query(sql, params)}
    cache_set(ck, result)
    return result


# ============================================================
# API: Categories for a manufacturer
# ============================================================
@app.get("/api/brand/{slug}/categories")
async def api_categories(request: Request, slug: str):
    mfg_name = verify_session(request, slug)
    sql = f"""
    SELECT DISTINCT oi.product_vertical AS category
    FROM `{PROJECT_DATASET}.order_items` oi
    WHERE oi.product_manufacturer_name = @mfg
      AND oi.product_vertical IS NOT NULL
    ORDER BY 1
    """
    params = [bigquery.ScalarQueryParameter("mfg", "STRING", mfg_name)]
    rows = run_query(sql, params)
    return {"categories": [r["category"] for r in rows]}


# ============================================================
# Health check
# ============================================================
@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

# TEMP: schema inspection (remove after use)
@app.get("/debug/schema/{table}")
async def debug_schema(table: str):
    allowed = {"order_items", "orders"}
    if table not in allowed:
        return {"error": f"Table must be one of {allowed}"}
    q = f"""
    SELECT column_name, data_type
    FROM `{PROJECT_DATASET}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table}'
    ORDER BY ordinal_position
    """
    rows = bq_client.query(q).result()
    return {"table": table, "columns": [{"name": r.column_name, "type": r.data_type} for r in rows]}

@app.get("/debug/payment_statuses")
async def debug_payment_statuses():
    q = f"""
    SELECT
      o.payment_status,
      o.is_cancelled,
      COUNT(*) AS cnt,
      SUM(o.total_refund_amount_including_vat_eur) AS total_refunds
    FROM `{PROJECT_DATASET}.orders` o
    JOIN `{PROJECT_DATASET}.order_items` oi ON oi.order_id = o.order_id
    WHERE oi.product_manufacturer_name = 'Cannamedical Pharma GmbH'
      AND DATE(o.created_at) >= '2026-01-01'
    GROUP BY 1, 2
    ORDER BY cnt DESC
    """
    rows = bq_client.query(q).result()
    return {"data": [dict(r) for r in rows]}

