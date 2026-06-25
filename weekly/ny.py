"""
NY Weekly Report — automated weekly snapshot for the New York region.

Same shape as weekly/vn.py: batch the Redash queries for the most recent COMPLETE week,
assemble one weekly column, write an .xlsx, and upload it to Slack. The layout (metric
order, section grouping, blank rows) mirrors the NY Google Sheet so the column can be pasted
straight in. Derived metrics are written as Excel FORMULAS (not pre-computed values) so they
stay transparent and recompute if a base number is edited. Cross-week metrics that depend on
the previous week (% Growth, % Resurrect, % Churn) are left blank — those formulas live in
the master sheet, which has the prior columns.

NY is USD-native, single market (no car/bike split), so the modal blocks from VN drop out.

Run:  python weekly/ny.py
Env:  REDASH_API_KEY, REDASH_BASE_URL, SLACK_TOKEN, SLACK_CHANNEL_NY (or SLACK_CHANNEL)
"""
import os
import re
import sys
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

try:                                   # stdlib on 3.9+; tzdata is in requirements
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
except Exception:                      # pragma: no cover - fallback if zoneinfo unavailable
    NY_TZ = None

# Match weekly/vn.py: make `utils` importable when run from anywhere.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.helpers import Query, Redash  # noqa: E402
from utils.slack import SlackBot          # noqa: E402

# Redash query IDs (logical name -> id).
QIDS = {
    "trips":   7548,   # NY - Completed Trip  (bookings, completed, rates, weekly riders/drivers)
    "wau":     7552,   # NY - Active Rider Weekly  (GA4 WAU)
    "driver":  7553,   # NY - Driver FT R C  (retained / first / resurrect / churn)
    "rider":   7554,   # NY - Rider FT R C
    "cancel":  7596,   # NY - Cancellation Rates
    "online":  7555,   # NY - Online Driver
    "fare":    7556,   # NY - Average Fare
    "promo":   7557,   # NY - Promotion Spending Weekly
    "fee":     7558,   # NY - Platform Fee Weekly
    "pm":      7559,   # NY - Payment Method Weekly
}

# Rate columns arrive from SQL already multiplied by 100 (e.g. 95.32). Store as fractions so
# Excel's percent format renders them correctly.
RATE_KEYS = {"complete_rate", "expire_rate", "rider_cancel_rate",
             "rider_cancel_after_matched_rate", "driver_cancel_rate",
             "retained_driver_pct", "retained_rider_pct"}

# Section fills (approximate the colour blocks in the sheet).
SECTION_FILL = {
    "Operational": "#DDEBF7",   # blue
    "Driver":      "#FCE4D6",   # orange
    "Rider":       "#E2EFDA",   # green
    "Fare / Promo": "#EDE7F6",  # purple
}

# ---------------------------------------------------------------------------
# Sheet layout — one entry per row, in the exact order of the screenshot.
#   ("val",   section, label, value_key, numfmt)  -> number (from a query OR computed in Python)
#   ("blankval", section, label, None, numfmt)    -> label present, value blank (cross-week)
#   ("blank", None, None, None, None)             -> empty separator row
# numfmt: int | dec | pct | money
#
# Derived rows are COMPUTED IN PYTHON (see compute_derived) and written as plain numbers, not
# Excel formulas. xlsxwriter caches a formula result of 0 and relies on the viewer to recalc,
# which Slack/Sheets previews don't — hence the zeros. Values are also safe to paste into the
# master sheet (formulas would shift their relative refs on paste).
# ---------------------------------------------------------------------------
SPEC = [
    ("val", "Operational", "Bookings", "bookings", "int"),
    ("val", "Operational", "Unique Bookings", "unique_bookings", "int"),
    ("val", "Operational", "Completed Trips", "completed", "int"),
    ("val", "Operational", "Complete Rate", "complete_rate", "pct"),
    ("val", "Operational", "Expire Rate", "expire_rate", "pct"),
    ("val", "Operational", "Avg Daily Completed", "daily_completed", "int"),
    ("blankval", "Operational", "% Growth", None, "pct"),
    ("val", "Operational", "Avg Completed Trip Per Rider", "avg_trip_per_rider", "dec"),
    ("val", "Operational", "Avg Completed Trip Per Driver", "avg_trip_per_driver", "dec"),
    ("blank", None, None, None, None),
    ("val", "Operational", "Rider Cancel Rate", "rider_cancel_rate", "pct"),
    ("val", "Operational", "Rider Cancel After Match Rate", "rider_cancel_after_matched_rate", "pct"),
    ("val", "Operational", "Driver Cancel Rate", "driver_cancel_rate", "pct"),

    ("val", "Driver", "Retained Driver Pct", "retained_driver_pct", "pct"),
    ("val", "Driver", "Unique Completed Drivers", "driver_weekly_complete", "int"),
    ("val", "Driver", "Daily Average Online Driver", "avg_online_drivers", "int"),
    ("val", "Driver", "Daily Average Completed Driver", "daily_avg_completed_drivers", "int"),
    ("val", "Driver", "CD / OD", "cd_od", "pct"),
    ("val", "Driver", "Driver Weekly Completed", "driver_weekly_complete", "int"),
    ("blank", None, None, None, None),
    ("val", "Driver", "New Driver Activated", "driver_first", "int"),
    ("val", "Driver", "Resurrected Driver", "driver_resurrect", "int"),
    ("blankval", "Driver", "% Resurrect", None, "pct"),
    ("val", "Driver", "Churn Driver", "driver_churn", "int"),
    ("blankval", "Driver", "% Churn", None, "pct"),
    ("val", "Driver", "Net New Driver", "net_new_driver", "int"),

    ("val", "Rider", "Retained Rider Pct", "retained_rider_pct", "pct"),
    ("val", "Rider", "Rider WAU", "wau", "int"),
    ("val", "Rider", "Unique Completed Riders", "rider_weekly_complete", "int"),
    ("val", "Rider", "Completed Riders / WAU", "completed_riders_per_wau", "pct"),
    ("blank", None, None, None, None),
    ("val", "Rider", "New Rider Activated", "rider_first", "int"),
    ("val", "Rider", "Resurrected Rider", "rider_resurrect", "int"),
    ("blankval", "Rider", "% Resurrect", None, "pct"),
    ("val", "Rider", "Churn Rider", "rider_churn", "int"),
    ("blankval", "Rider", "% Churn", None, "pct"),
    ("val", "Rider", "Net New Rider", "net_new_rider", "int"),
    ("val", "Rider", "R:D Ratio", "rd_ratio", "dec4"),
    ("blank", None, None, None, None),

    ("val", "Fare / Promo", "Average Fare", "average_fare", "money"),
    ("val", "Fare / Promo", "Promo Spend", "discount", "money"),
    ("val", "Fare / Promo", "Promotion Trips", "discount_trips", "int"),
    ("val", "Fare / Promo", "Non Promotion Trips", "non_promo_trips", "int"),
    ("val", "Fare / Promo", "Promotion / Completed", "promo_over_completed", "pct"),
    ("val", "Fare / Promo", "Average Promotion Value", "average_discount", "money"),
    ("val", "Fare / Promo", "Promo per completed ride", "promo_per_ride", "money"),
    ("val", "Fare / Promo", "Promo per completed rider", "promo_per_rider", "money"),
    ("val", "Fare / Promo", "Promo per completed trip / Average Fare", "promo_over_fare", "pct"),
    ("val", "Fare / Promo", "Platform Fee", "total_system_fee", "money"),
    ("val", "Fare / Promo", "Platform Fee per Completed Ride", "platform_fee_per_ride", "money"),
]


# ---------------------------------------------------------------------------
# Pull + extract
# ---------------------------------------------------------------------------
def cell(df, col):
    """First-row value of a column, tolerant of empty results / missing columns / NaN."""
    try:
        if df is None or len(df) == 0 or col not in df.columns:
            return None
        x = df[col].iloc[0]
        return None if pd.isna(x) else x
    except Exception:
        return None


def extract_values(res):
    """Flatten the query DataFrames into the {value_key: number} dict the layout references."""
    t, c = res["trips"], res["cancel"]
    d, r = res["driver"], res["rider"]
    v = {
        "bookings": cell(t, "total_bookings"),
        "unique_bookings": cell(t, "unique_bookings"),
        "completed": cell(t, "total_completed_trip"),
        "complete_rate": cell(t, "complete_rate"),
        "expire_rate": cell(t, "expire_rate"),
        "daily_completed": cell(t, "daily_completed_trip"),
        "driver_weekly_complete": cell(t, "driver_weekly_complete"),
        "rider_weekly_complete": cell(t, "rider_weekly_complete"),
        "daily_avg_completed_drivers": cell(t, "daily_avg_completed_drivers"),

        "rider_cancel_rate": cell(c, "rider_cancel_rate"),
        "rider_cancel_after_matched_rate": cell(c, "rider_cancel_after_matched_rate"),
        "driver_cancel_rate": cell(c, "driver_cancel_rate"),

        "retained_driver_pct": cell(d, "retained_driver_pct"),
        "driver_first": cell(d, "first_timers"),
        "driver_resurrect": cell(d, "resurrect"),
        "driver_churn": cell(d, "churn"),

        "retained_rider_pct": cell(r, "retained_rider_pct"),
        "rider_first": cell(r, "first_timers"),
        "rider_resurrect": cell(r, "resurrect"),
        "rider_churn": cell(r, "churn"),

        "avg_online_drivers": cell(res["online"], "avg_online_drivers"),
        "wau": cell(res["wau"], "active_users"),
        "average_fare": cell(res["fare"], "average_fare"),

        "discount_trips": cell(res["promo"], "discount_trips"),
        "discount": cell(res["promo"], "discount"),
        "average_discount": cell(res["promo"], "average_discount"),

        "total_system_fee": cell(res["fee"], "total_system_fee"),
    }
    # Convert SQL percent values (x100) to fractions for Excel's percent format.
    for k in RATE_KEYS:
        if v.get(k) is not None:
            v[k] = float(v[k]) / 100.0
    return v


def _safe_div(a, b):
    a, b = _num(a), _num(b)
    if a is None or not b:        # b None or 0
        return None
    return a / b


def compute_derived(v):
    """Derived metrics computed in Python so the sheet shows real numbers everywhere.

    Mirrors the VN derivations. Missing/empty bases yield blanks rather than zeros, so a week
    with no data doesn't masquerade as a real 0.
    """
    z = lambda k: (_num(v.get(k)) or 0)         # count, treating missing as 0
    completed = _num(v.get("completed"))
    d = {
        "avg_trip_per_rider": _safe_div(v.get("completed"), v.get("rider_weekly_complete")),
        "avg_trip_per_driver": _safe_div(v.get("daily_completed"), v.get("daily_avg_completed_drivers")),
        "cd_od": _safe_div(v.get("daily_avg_completed_drivers"), v.get("avg_online_drivers")),
        "net_new_driver": z("driver_first") + z("driver_resurrect") - z("driver_churn"),
        "net_new_rider": z("rider_first") + z("rider_resurrect") - z("rider_churn"),
        "completed_riders_per_wau": _safe_div(v.get("rider_weekly_complete"), v.get("wau")),
        "non_promo_trips": None if completed is None else completed - z("discount_trips"),
        "promo_over_completed": _safe_div(v.get("discount_trips"), v.get("completed")),
        "promo_per_ride": _safe_div(v.get("discount"), v.get("completed")),
        "promo_per_rider": _safe_div(v.get("discount"), v.get("rider_weekly_complete")),
        "rd_ratio": _safe_div(v.get("rider_weekly_complete"), v.get("driver_weekly_complete")),
        "platform_fee_per_ride": _safe_div(v.get("total_system_fee"), v.get("completed")),
    }
    # promo-per-ride relative to average fare (ratio of two derived/base numbers)
    d["promo_over_fare"] = _safe_div(d["promo_per_ride"], v.get("average_fare"))
    return d


def extract_payment(res):
    pm = res["pm"]
    return {
        "Cash Trips": (cell(pm, "cash_trips"), "int"),
        "Cash GMV": (cell(pm, "cash_gmv"), "money"),
        "Card Trips": (cell(pm, "card_trips"), "int"),
        "Card GMV": (cell(pm, "card_gmv"), "money"),
    }


# ---------------------------------------------------------------------------
# Workbook (pure: no Redash dependency, so it can be unit-tested with stub data)
# ---------------------------------------------------------------------------
NUMFMT = {"int": "#,##0", "dec": "#,##0.00", "dec4": "#,##0.0000", "pct": "0.00%", "money": "$#,##0.00"}


def _num(x):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return float(x)
    except (ValueError, TypeError):
        return None


def build_workbook(path, start_date, values, payment):
    import xlsxwriter

    wb = xlsxwriter.Workbook(path, {"nan_inf_to_errors": True})
    ws = wb.add_worksheet("Weekly Report")
    ws.set_column(0, 0, 14)
    ws.set_column(1, 1, 32)
    ws.set_column(2, 2, 16)

    fmt_cache = {}

    def fmt(numfmt=None, bg=None, bold=False, rotation=None, align=None, valign=None, border=1):
        key = (numfmt, bg, bold, rotation, align, valign, border)
        if key not in fmt_cache:
            d = {"border": border}
            if numfmt:
                d["num_format"] = NUMFMT[numfmt]
            if bg:
                d["bg_color"] = bg
            if bold:
                d["bold"] = True
            if rotation is not None:
                d["rotation"] = rotation
            if align:
                d["align"] = align
            if valign:
                d["valign"] = valign
            fmt_cache[key] = wb.add_format(d)
        return fmt_cache[key]

    # ---- header ----
    ws.write(0, 1, "NY Weekly Report", fmt(bold=True, border=0))
    ws.write(1, 1, "Monday Start Date", fmt(bold=True, border=0))
    ws.write(1, 2, start_date, fmt(bold=True, border=0))
    HDR = 2
    ws.write(HDR, 0, "Section", fmt(bold=True, bg="#1F2937", align="center"))
    ws.write(HDR, 1, "Metric", fmt(bold=True, bg="#1F2937", align="center"))
    ws.write(HDR, 2, start_date, fmt(bold=True, bg="#1F2937", align="center"))
    # white font on the dark header
    hdr_fmt = wb.add_format({"bold": True, "bg_color": "#1F2937", "font_color": "white",
                             "align": "center", "border": 1})
    for col, txt in ((0, "Section"), (1, "Metric"), (2, start_date)):
        ws.write(HDR, col, txt, hdr_fmt)

    DATA = HDR + 1  # first metric row (0-indexed)

    # Pass 1: record per-section row ranges (for the merged vertical labels).
    sec_range = {}
    for i, (kind, section, label, _payload, _nf) in enumerate(SPEC):
        r0 = DATA + i
        if section:
            lo, hi = sec_range.get(section, (r0, r0))
            sec_range[section] = (min(lo, r0), max(hi, r0))

    # Pass 2: write rows. Every derived metric is already a number in `values`.
    for i, (kind, section, label, payload, nf) in enumerate(SPEC):
        r0 = DATA + i
        if kind == "blank":
            continue
        bg = SECTION_FILL.get(section)
        ws.write(r0, 1, label, fmt(bg=bg))                       # metric label
        if kind == "val":
            ws.write(r0, 2, _num(values.get(payload)), fmt(numfmt=nf, bg="#FFF8E1"))
        else:  # blankval — label only, value intentionally empty (cross-week, filled in master)
            ws.write_blank(r0, 2, None, fmt(numfmt=nf, bg="#FFF8E1"))

    # vertical merged section labels in column A
    for section, (lo, hi) in sec_range.items():
        cell_fmt = fmt(bg=SECTION_FILL.get(section), bold=True, rotation=90,
                       align="center", valign="vcenter")
        if hi > lo:
            ws.merge_range(lo, 0, hi, 0, section, cell_fmt)
        else:
            ws.write(lo, 0, section, cell_fmt)

    # ---- Payment Method sheet ----
    pm_ws = wb.add_worksheet("Payment Method")
    pm_ws.set_column(0, 0, 24)
    pm_ws.set_column(1, 1, 16)
    pm_ws.write(0, 0, "Metric", hdr_fmt)
    pm_ws.write(0, 1, start_date, hdr_fmt)
    for i, (label, (val, nf)) in enumerate(payment.items()):
        pm_ws.write(i + 1, 0, label, fmt())
        pm_ws.write(i + 1, 1, _num(val), fmt(numfmt=nf))

    wb.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def last_complete_monday():
    """Monday (NY local) of the most recent fully complete week."""
    now = datetime.now(NY_TZ) if NY_TZ else datetime.utcnow() - timedelta(hours=4)
    this_monday = now.date() - timedelta(days=now.weekday())
    return this_monday - timedelta(days=7)


def main():
    load_dotenv()
    redash = Redash(key=os.getenv("REDASH_API_KEY"), base_url=os.getenv("REDASH_BASE_URL"))

    start_date = last_complete_monday().strftime("%Y-%m-%d")
    output_date = datetime.strptime(start_date, "%Y-%m-%d").strftime("%d_%b_%Y")
    print(f"NY weekly for week starting {start_date}")

    # Batch-run all queries (same helper as VN), then collect results.
    queries = [Query(qid, params={"week_start_date": start_date}) for qid in QIDS.values()]
    redash.run_queries(queries)
    res = {name: redash.get_result(qid) for name, qid in QIDS.items()}

    values = extract_values(res)
    values.update(compute_derived(values))   # derived metrics as real numbers
    payment = extract_payment(res)

    output_file = f"NY_Weekly_{output_date}.xlsx"
    build_workbook(output_file, start_date, values, payment)
    print(f"Wrote {output_file}")

    try:
        SlackBot().uploadFile(output_file,
                              os.getenv("SLACK_CHANNEL_NY") or os.getenv("SLACK_CHANNEL"),
                              f"Weekly Report for NY {output_date}")
    except Exception as e:  # never let a Slack failure kill the run
        print(f"Error uploading to Slack: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
