"""
HTV Brand Dashboard — FastAPI Backend
Queries BigQuery live data and serves per-manufacturer dashboards.

★ DEAL TERMS CONFIG — To update fee terms for any manufacturer,
  edit the MANUFACTURER_FEES dict below. No other code changes needed.
"""

import os, json, hashlib, hmac, logging
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from google.cloud import bigquery
from google.api_core import exceptions as gcp_exceptions
from google.oauth2 import service_account

logger = logging.getLogger("brand-dashboard")


class BigQueryError(Exception):
    """Raised when BigQuery returns an error we want to surface gracefully."""
    def __init__(self, message: str, status_code: int = 503):
        self.message = message
        self.status_code = status_code
        super().__init__(message)


app = FastAPI(title="HTV Brand Dashboard")
templates = Jinja2Templates(directory="templates")


@app.exception_handler(BigQueryError)
async def bigquery_error_handler(request: Request, exc: BigQueryError):
    """Return a clean JSON error instead of a 500 traceback."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.message, "status": "unavailable"},
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
# ★ MANUFACTURER TOKEN AUTH
# Each manufacturer gets a unique URL: /brand/{slug}?token=xxx
# ============================================================
MANUFACTURER_TOKENS = {
    "cannamedical":  os.getenv("TOKEN_CANNAMEDICAL",  "cm_demo_token_2025"),
    "four20":        os.getenv("TOKEN_FOUR20",         "f20_demo_token_2025"),
    "aurora":        os.getenv("TOKEN_AURORA",          "au_demo_token_2025"),
    "demecan":       os.getenv("TOKEN_DEMECAN",         "dm_demo_token_2025"),
    "tilray":        os.getenv("TOKEN_TILRAY",          "tl_demo_token_2025"),
    "enua":          os.getenv("TOKEN_ENUA",            "en_demo_token_2025"),
    "alephsana":     os.getenv("TOKEN_ALEPHSANA",       "al_demo_token_2025"),
    "iuvo":          os.getenv("TOKEN_IUVO",            "iv_demo_token_2025"),
    "avaay":         os.getenv("TOKEN_AVAAY",           "av_demo_token_2025"),
}

# Slug → actual BigQuery product_manufacturer_name
MANUFACTURER_BQ_NAMES = {
    "cannamedical":  "Cannamedical",
    "four20":        "Four 20 Pharma",
    "aurora":        "Aurora",
    "demecan":       "Demecan",
    "tilray":        "Tilray",
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
    "tilray": {
        "type": "per_gram", "rate": 0.50,
        "label": "€0.50 / g", "desc": "Per-gram transaction fee",
        "effective_date": "2024-06-01",
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

def verify_access(slug: str, token: str) -> str:
    """Verify token and return the BQ manufacturer name."""
    expected = MANUFACTURER_TOKENS.get(slug)
    if not expected or not hmac.compare_digest(token, expected):
        raise HTTPException(status_code=403, detail="Invalid token")
    return MANUFACTURER_BQ_NAMES[slug]


def calc_fee(fee_info: dict, volume_g: float, revenue: float, months: int = 1) -> float:
    """Calculate fee based on deal terms."""
    t = fee_info["type"]
    r = fee_info["rate"]
    if t == "per_gram":
        return (volume_g or 0) * r
    elif t == "percentage":
        return (revenue or 0) * r
    elif t == "fixed":
        return r * months
    elif t == "advance":
        # For advance/prepaid deals, the fee is the effective per-gram rate
        # applied to actual volume sold in the period
        return (volume_g or 0) * r
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
# PAGE ROUTE — serves the HTML dashboard
# ============================================================
@app.get("/brand/{slug}", response_class=HTMLResponse)
async def brand_page(request: Request, slug: str, token: str = ""):
    mfg_name = verify_access(slug, token)
    fee = MANUFACTURER_FEES.get(slug, {})
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "slug": slug,
        "token": token,
        "manufacturer_name": mfg_name,
        "fee": fee,
    })


# ============================================================
# API: Summary KPIs
# ============================================================
@app.get("/api/brand/{slug}/summary")
async def api_summary(slug: str, token: str, start_date: str = "", end_date: str = "", category: str = ""):
    mfg_name = verify_access(slug, token)
    date_where, date_p = date_params(start_date, end_date, category)

    sql = f"""
    WITH curr AS (
      SELECT
        COUNT(DISTINCT o.order_id) AS prescriptions,
        SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur) AS revenue_eur,
        SUM(oi.quantity_after_cancellations) AS sales_volume_g,
        SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) AS net_revenue_eur,
        SAFE_DIVIDE(SUM(oi.cancelled_quantity), SUM(oi.quantity_before_cancellations)) AS cancellation_rate,
        SAFE_DIVIDE(SUM(oi.quantity_after_cancellations), COUNT(DISTINCT o.order_id)) AS avg_g_per_prescription,
        SAFE_DIVIDE(SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur), NULLIF(SUM(oi.quantity_after_cancellations),0)) AS avg_eur_per_g,
        SAFE_DIVIDE(COUNT(oi.order_item_id), COUNT(DISTINCT o.order_id)) AS avg_products_per_order,
        COUNT(DISTINCT o.customer_id) AS total_patients
      FROM `{PROJECT_DATASET}.order_items` oi
      JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
      WHERE oi.product_manufacturer_name = @mfg {date_where}
    ),
    prev AS (
      SELECT
        COUNT(DISTINCT o.order_id) AS prescriptions,
        SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur) AS revenue_eur,
        SUM(oi.quantity_after_cancellations) AS sales_volume_g,
        SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) AS net_revenue_eur
      FROM `{PROJECT_DATASET}.order_items` oi
      JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
      WHERE oi.product_manufacturer_name = @mfg
        AND DATE(o.created_at) >= DATE_SUB(
            DATE(@start_prev), INTERVAL
            DATE_DIFF(DATE(@end_prev), DATE(@start_prev), DAY) DAY
        )
        AND DATE(o.created_at) < DATE(@start_prev)
        {"AND oi.product_vertical = @category" if category else ""}
    )
    SELECT
      c.*, p.prescriptions AS prev_rx, p.revenue_eur AS prev_rev,
      p.sales_volume_g AS prev_vol, p.net_revenue_eur AS prev_net
    FROM curr c, prev p
    """

    s = start_date or "2020-01-01"
    e = end_date or datetime.now().strftime("%Y-%m-%d")
    params = [
        bigquery.ScalarQueryParameter("mfg", "STRING", mfg_name),
        bigquery.ScalarQueryParameter("start_prev", "DATE", s),
        bigquery.ScalarQueryParameter("end_prev", "DATE", e),
    ] + date_p

    rows = run_query(sql, params)
    r = rows[0] if rows else {}
    fee = MANUFACTURER_FEES.get(slug, {})
    fee_amount = calc_fee(fee, r.get("sales_volume_g", 0), r.get("net_revenue_eur", 0))

    def safe_growth(curr, prev):
        if curr is None or prev is None or prev == 0:
            return None
        return (curr - prev) / prev

    return {
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
        },
        "growth": {
            "prescriptions": safe_growth(r.get("prescriptions"), r.get("prev_rx")),
            "revenue": safe_growth(r.get("revenue_eur"), r.get("prev_rev")),
            "volume": safe_growth(r.get("sales_volume_g"), r.get("prev_vol")),
            "net_revenue": safe_growth(r.get("net_revenue_eur"), r.get("prev_net")),
        },
        "fee": {"amount": fee_amount},
    }


# ============================================================
# API: Monthly Trends
# ============================================================
@app.get("/api/brand/{slug}/trends")
async def api_trends(slug: str, token: str, start_date: str = "", end_date: str = "", category: str = ""):
    mfg_name = verify_access(slug, token)
    date_where, date_p = date_params(start_date, end_date, category)

    sql = f"""
    SELECT
      FORMAT_DATE('%Y-%m', DATE(o.created_at)) AS period,
      COUNT(DISTINCT o.order_id) AS prescriptions,
      SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur) AS revenue_eur,
      SUM(oi.quantity_after_cancellations) AS sales_volume_g,
      SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) AS net_revenue_eur,
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

    return {"data": rows}


# ============================================================
# API: Products (with region, brand, category, origin filters)
# ============================================================
@app.get("/api/brand/{slug}/products")
async def api_products(
    slug: str, token: str, start_date: str = "", end_date: str = "",
    region: str = "", brand: str = "", category: str = "", origin: str = "",
):
    mfg_name = verify_access(slug, token)
    date_where, date_p = date_params(start_date, end_date)

    extra_where = ""
    extra_join = ""
    extra_params = []

    if region:
        extra_join = f"JOIN `{PROJECT_DATASET}.orders` o2 ON oi.order_id = o2.order_id"
        extra_where += " AND o2.shipping_address.region = @region"
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
      SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) AS net_revenue_eur,
      SAFE_DIVIDE(SUM(oi.total_price_after_cancellations_before_discounts_including_vat_eur), NULLIF(SUM(oi.quantity_after_cancellations),0)) AS avg_eur_per_g,
      SAFE_DIVIDE(SUM(oi.quantity_after_cancellations), COUNT(DISTINCT o.order_id)) AS avg_g_per_rx
    FROM `{PROJECT_DATASET}.order_items` oi
    JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
    {extra_join}
    WHERE oi.product_manufacturer_name = @mfg {date_where} {extra_where}
    GROUP BY 1,2,3,4
    ORDER BY revenue_eur DESC
    """
    params = [bigquery.ScalarQueryParameter("mfg", "STRING", mfg_name)] + date_p + extra_params
    return {"data": run_query(sql, params)}


# ============================================================
# API: Breakdowns (category, origin, price tier, products/order)
# ============================================================
@app.get("/api/brand/{slug}/breakdowns")
async def api_breakdowns(slug: str, token: str, start_date: str = "", end_date: str = "", category: str = ""):
    mfg_name = verify_access(slug, token)
    date_where, date_p = date_params(start_date, end_date, category)
    base_params = [bigquery.ScalarQueryParameter("mfg", "STRING", mfg_name)] + date_p

    cat_sql = f"""
    SELECT oi.product_vertical AS category,
      SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) AS net_revenue_eur
    FROM `{PROJECT_DATASET}.order_items` oi
    JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
    WHERE oi.product_manufacturer_name = @mfg {date_where}
    GROUP BY 1 ORDER BY net_revenue_eur DESC
    """
    ori_sql = f"""
    SELECT oi.product_country_or_origin AS origin,
      SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) AS net_revenue_eur
    FROM `{PROJECT_DATASET}.order_items` oi
    JOIN `{PROJECT_DATASET}.orders` o ON oi.order_id = o.order_id
    WHERE oi.product_manufacturer_name = @mfg {date_where}
    GROUP BY 1 ORDER BY net_revenue_eur DESC
    """
    pt_sql = f"""
    SELECT
      CASE
        WHEN SAFE_DIVIDE(oi.total_price_after_cancellations_before_discounts_including_vat_eur, NULLIF(oi.quantity_after_cancellations,0)) < 8 THEN '< €8/g'
        WHEN SAFE_DIVIDE(oi.total_price_after_cancellations_before_discounts_including_vat_eur, NULLIF(oi.quantity_after_cancellations,0)) < 10 THEN '€8–10/g'
        WHEN SAFE_DIVIDE(oi.total_price_after_cancellations_before_discounts_including_vat_eur, NULLIF(oi.quantity_after_cancellations,0)) < 12 THEN '€10–12/g'
        WHEN SAFE_DIVIDE(oi.total_price_after_cancellations_before_discounts_including_vat_eur, NULLIF(oi.quantity_after_cancellations,0)) < 15 THEN '€12–15/g'
        ELSE '> €15/g'
      END AS price_tier,
      SUM(oi.total_price_after_cancellations_and_discounts_including_vat_eur) AS net_revenue_eur
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
    return {
        "categories": run_query(cat_sql, list(base_params)),
        "origins": run_query(ori_sql, list(base_params)),
        "price_tiers": run_query(pt_sql, list(base_params)),
        "products_per_order": run_query(ppo_sql, list(base_params)),
    }


# ============================================================
# API: Patient Insights
# ============================================================
@app.get("/api/brand/{slug}/patients")
async def api_patients(slug: str, token: str, start_date: str = "", end_date: str = "", category: str = ""):
    mfg_name = verify_access(slug, token)
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
    return {
        "new_returning": run_query(nr_sql, list(base_params)),
        "age_segments": run_query(age_sql, list(base_params)),
        "regions": run_query(reg_sql, list(base_params)),
    }


# ============================================================
# API: Pricing (Avg €/g over time)
# ============================================================
@app.get("/api/brand/{slug}/pricing")
async def api_pricing(slug: str, token: str, start_date: str = "", end_date: str = "", category: str = ""):
    mfg_name = verify_access(slug, token)
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
    return {"data": run_query(sql, params)}


# ============================================================
# API: Categories for a manufacturer
# ============================================================
@app.get("/api/brand/{slug}/categories")
async def api_categories(slug: str, token: str):
    mfg_name = verify_access(slug, token)
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

