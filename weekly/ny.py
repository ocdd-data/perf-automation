"""
NY Weekly Report — automated weekly snapshot for the New York region.

Batches the NY Redash queries for the most recent COMPLETE Mon–Sun week, assembles one
weekly column, writes an .xlsx laid out like the NY sheet, and uploads to Slack.

Product segmentation: the trips (7548), cancellation (7596) and fare (7556) queries now
return one row PER product segment — 'Overall', 'Saver', 'Others'. The Operational block and
the Average Rider Fare / Average Driver Earn rows are shown per segment. Everything else
(Driver, Rider, Promo, Platform Fee, Fees) stays at the Overall level, since those queries
are not segmented — they read the 'Overall' totals.

Backfill a past week:  python weekly/ny.py 2026-06-08   (defaults to last complete week)
Env: REDASH_API_KEY, REDASH_BASE_URL, SLACK_TOKEN, SLACK_CHANNEL_NY (or SLACK_CHANNEL)
"""
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

try:
    from zoneinfo import ZoneInfo
    NY_TZ = ZoneInfo("America/New_York")
except Exception:                      # pragma: no cover
    NY_TZ = None

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.helpers import Query, Redash  # noqa: E402
from utils.slack import SlackBot          # noqa: E402

# Logical name -> Redash query id
QIDS = {
    "trips":   7548,   # per-segment: bookings, completed, rates, weekly riders/drivers
    "wau":     7552,   # GA4 rider WAU
    "driver":  7553,   # retained / first / resurrect / churn
    "rider":   7554,
    "cancel":  7596,   # per-segment cancel counts + rates
    "online":  7555,   # avg online drivers
    "fare":    7556,   # per-segment avg/total rider pay + driver earn
    "promo":   7557,   # discount trips/spend
    "fee":     7558,   # platform (system) fee
    "pm":      7559,   # payment method (cash/card)
    "fee_breakdown": 7645,  # tips + statutory/regulatory fee totals
}

# Product segments: (data value from SQL, display label on the sheet)
SEGMENTS = [("Overall", "Overall"), ("Saver", "Saver"), ("Others", "All other products")]

# Per-segment Operational metrics: (label, value_key_suffix|None, numfmt, kind)
OP_METRICS = [
    ("Bookings", "bookings", "int", "val"),
    ("Unique Bookings", "unique_bookings", "int", "val"),
    ("Completed Trips", "completed", "int", "val"),
    ("Complete Rate", "complete_rate", "pct", "val"),
    ("Expire Rate", "expire_rate", "pct", "val"),
    ("Avg Daily Completed", "daily_completed", "int", "val"),
    ("% Growth", None, "pct", "blankval"),
    ("Avg Completed Trip Per Rider", "avg_trip_per_rider", "dec", "val"),
    ("Avg Completed Trip Per Driver", "avg_trip_per_driver", "dec", "val"),
    ("Rider Cancel Rate", "rider_cancel_rate", "pct", "val"),
    ("Rider Cancel After Match Rate", "rider_cancel_after_matched_rate", "pct", "val"),
    ("Driver Cancel Rate", "driver_cancel_rate", "pct", "val"),
]

# ---------------------------------------------------------------------------
# Sheet layout — (kind, section, segment, label, value_key, numfmt).
#   segment is the sub-label in column B (only used for the Operational block).
#   Derived rows are computed in Python (compute_derived) and written as numbers.
# ---------------------------------------------------------------------------
SPEC = []
for _seg, _disp in SEGMENTS:                       # Operational block, per segment
    for _label, _suf, _fmt, _kind in OP_METRICS:
        _key = f"{_seg}_{_suf}" if _suf else None
        SPEC.append((_kind, "Operational", _disp, _label, _key, _fmt))

SPEC += [
    ("val", "Driver", None, "Retained Driver Pct", "retained_driver_pct", "pct"),
    ("val", "Driver", None, "Unique Completed Drivers", "driver_weekly_complete", "int"),
    ("val", "Driver", None, "Daily Average Online Driver", "avg_online_drivers", "int"),
    ("val", "Driver", None, "Daily Average Completed Driver", "daily_avg_completed_drivers", "int"),
    ("val", "Driver", None, "CD / OD", "cd_od", "pct"),
    ("val", "Driver", None, "Driver Weekly Completed", "driver_weekly_complete", "int"),
    ("blank", None, None, None, None, None),
    ("val", "Driver", None, "New Driver Activated", "driver_first", "int"),
    ("val", "Driver", None, "Resurrected Driver", "driver_resurrect", "int"),
    ("blankval", "Driver", None, "% Resurrect", None, "pct"),
    ("val", "Driver", None, "Churn Driver", "driver_churn", "int"),
    ("blankval", "Driver", None, "% Churn", None, "pct"),
    ("val", "Driver", None, "Net New Driver", "net_new_driver", "int"),

    ("val", "Rider", None, "Retained Rider Pct", "retained_rider_pct", "pct"),
    ("val", "Rider", None, "Rider WAU", "wau", "int"),
    ("val", "Rider", None, "Unique Completed Riders", "rider_weekly_complete", "int"),
    ("val", "Rider", None, "Completed Riders / WAU", "completed_riders_per_wau", "pct"),
    ("blank", None, None, None, None, None),
    ("val", "Rider", None, "New Rider Activated", "rider_first", "int"),
    ("val", "Rider", None, "Resurrected Rider", "rider_resurrect", "int"),
    ("blankval", "Rider", None, "% Resurrect", None, "pct"),
    ("val", "Rider", None, "Churn Rider", "rider_churn", "int"),
    ("blankval", "Rider", None, "% Churn", None, "pct"),
    ("val", "Rider", None, "Net New Rider", "net_new_rider", "int"),
    ("val", "Rider", None, "R:D Ratio", "rd_ratio", "dec4"),
    ("blank", None, None, None, None, None),

    # Average rider fare / driver earn per segment (segment baked into the label).
    ("val", "Fare / Promo / Fees", None, "Average Rider Fare (Overall)", "Overall_avg_rider_fare", "money"),
    ("val", "Fare / Promo / Fees", None, "Average Driver Earn (Overall)", "Overall_avg_driver_earn", "money"),
    ("val", "Fare / Promo / Fees", None, "Average Rider Fare (Saver)", "Saver_avg_rider_fare", "money"),
    ("val", "Fare / Promo / Fees", None, "Average Driver Earn (Saver)", "Saver_avg_driver_earn", "money"),
    ("val", "Fare / Promo / Fees", None, "Average Rider Fare (All other products)", "Others_avg_rider_fare", "money"),
    ("val", "Fare / Promo / Fees", None, "Average Driver Earn (All other products)", "Others_avg_driver_earn", "money"),
    ("val", "Fare / Promo / Fees", None, "Promo Spend", "discount", "money"),
    ("val", "Fare / Promo / Fees", None, "Promotion Trips", "discount_trips", "int"),
    ("val", "Fare / Promo / Fees", None, "Non Promotion Trips", "non_promo_trips", "int"),
    ("val", "Fare / Promo / Fees", None, "Promotion / Completed", "promo_over_completed", "pct"),
    ("val", "Fare / Promo / Fees", None, "Average Promotion Value", "average_discount", "money"),
    ("val", "Fare / Promo / Fees", None, "Promo per completed ride", "promo_per_ride", "money"),
    ("val", "Fare / Promo / Fees", None, "Promo per completed rider", "promo_per_rider", "money"),
    ("val", "Fare / Promo / Fees", None, "Promo per completed trip / Average Fare", "promo_over_fare", "pct"),
    ("val", "Fare / Promo / Fees", None, "Platform Fee", "total_system_fee", "money"),
    ("val", "Fare / Promo / Fees", None, "Platform Fee per Completed Ride", "platform_fee_per_ride", "money"),
    ("blank", None, None, None, None, None),
    ("val", "Fare / Promo / Fees", None, "Total Tips", "total_tips", "money"),
    ("val", "Fare / Promo / Fees", None, "Total Stat Fees", "total_stat_fees", "money"),
    ("val", "Fare / Promo / Fees", None, "Total Black Car Fund", "total_black_car_fund", "money"),
    ("val", "Fare / Promo / Fees", None, "Total Congestion Fee", "total_congestion_fee", "money"),
    ("val", "Fare / Promo / Fees", None, "Total Congestion Surcharge", "total_congestion_surcharge", "money"),
    ("val", "Fare / Promo / Fees", None, "Total NY Sales Tax", "total_ny_sales_tax", "money"),
]

SECTION_FILL = {
    "Operational": "#DDEBF7",
    "Driver": "#FCE4D6",
    "Rider": "#E2EFDA",
    "Fare / Promo / Fees": "#EDE7F6",
}

# Per-segment SQL rate columns arrive x100 — store as fractions for Excel's percent format.
SEG_RATE_KEYS = ("complete_rate", "expire_rate", "rider_cancel_rate",
                 "rider_cancel_after_matched_rate", "driver_cancel_rate")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def cell(df, col):
    """First-row value of a single-row result, tolerant of empty/missing/NaN."""
    try:
        if df is None or len(df) == 0 or col not in df.columns:
            return None
        x = df[col].iloc[0]
        return None if pd.isna(x) else x
    except Exception:
        return None


def _rv(row, col):
    """Value from a segment row (a pandas Series), tolerant of missing/NaN."""
    try:
        if row is None or col not in row:
            return None
        x = row[col]
        return None if pd.isna(x) else x
    except Exception:
        return None


def _seg_rows(df, col="product_segment"):
    """Index a multi-row segmented result by its product_segment value."""
    out = {}
    if df is None or len(df) == 0 or col not in df.columns:
        return out
    for _, r in df.iterrows():
        out[r[col]] = r
    return out


def _num(x):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        return float(x)
    except (ValueError, TypeError):
        return None


def _safe_div(a, b):
    a, b = _num(a), _num(b)
    if a is None or not b:
        return None
    return a / b


# ---------------------------------------------------------------------------
# Extract + derive
# ---------------------------------------------------------------------------
def extract_values(res):
    v = {}
    tseg = _seg_rows(res["trips"])
    cseg = _seg_rows(res["cancel"])
    fseg = _seg_rows(res["fare"])

    for seg, _disp in SEGMENTS:
        t, c, f = tseg.get(seg), cseg.get(seg), fseg.get(seg)
        v[f"{seg}_bookings"] = _rv(t, "total_bookings")
        v[f"{seg}_unique_bookings"] = _rv(t, "unique_bookings")
        v[f"{seg}_completed"] = _rv(t, "total_completed_trip")
        v[f"{seg}_complete_rate"] = _rv(t, "complete_rate")
        v[f"{seg}_expire_rate"] = _rv(t, "expire_rate")
        v[f"{seg}_daily_completed"] = _rv(t, "daily_completed_trip")
        v[f"{seg}_driver_weekly_complete"] = _rv(t, "driver_weekly_complete")
        v[f"{seg}_rider_weekly_complete"] = _rv(t, "rider_weekly_complete")
        v[f"{seg}_daily_avg_completed_drivers"] = _rv(t, "daily_avg_completed_drivers")
        v[f"{seg}_rider_cancel_rate"] = _rv(c, "rider_cancel_rate")
        v[f"{seg}_rider_cancel_after_matched_rate"] = _rv(c, "rider_cancel_after_matched_rate")
        v[f"{seg}_driver_cancel_rate"] = _rv(c, "driver_cancel_rate")
        v[f"{seg}_avg_rider_fare"] = _rv(f, "avg_rider_pay")
        v[f"{seg}_avg_driver_earn"] = _rv(f, "avg_driver_earn")
        for rk in SEG_RATE_KEYS:                       # x100 -> fraction
            k = f"{seg}_{rk}"
            if v.get(k) is not None:
                v[k] = float(v[k]) / 100.0

    # Overall aliases feed the non-segmented Driver / Rider / Promo / Fee rows.
    v["completed"] = v.get("Overall_completed")
    v["daily_completed"] = v.get("Overall_daily_completed")
    v["rider_weekly_complete"] = v.get("Overall_rider_weekly_complete")
    v["driver_weekly_complete"] = v.get("Overall_driver_weekly_complete")
    v["daily_avg_completed_drivers"] = v.get("Overall_daily_avg_completed_drivers")

    d, ri = res["driver"], res["rider"]
    v["retained_driver_pct"] = cell(d, "retained_driver_pct")
    v["driver_first"] = cell(d, "first_timers")
    v["driver_resurrect"] = cell(d, "resurrect")
    v["driver_churn"] = cell(d, "churn")
    v["retained_rider_pct"] = cell(ri, "retained_rider_pct")
    v["rider_first"] = cell(ri, "first_timers")
    v["rider_resurrect"] = cell(ri, "resurrect")
    v["rider_churn"] = cell(ri, "churn")
    for rk in ("retained_driver_pct", "retained_rider_pct"):    # x100 -> fraction
        if v.get(rk) is not None:
            v[rk] = float(v[rk]) / 100.0

    v["avg_online_drivers"] = cell(res["online"], "avg_online_drivers")
    v["wau"] = cell(res["wau"], "active_users")
    v["discount_trips"] = cell(res["promo"], "discount_trips")
    v["discount"] = cell(res["promo"], "discount")
    v["average_discount"] = cell(res["promo"], "average_discount")
    v["total_system_fee"] = cell(res["fee"], "total_system_fee")

    # Fee breakdown (7645). "Total Stat Fees" = base etc_fee + additional etc fees (type 999).
    fb = res["fee_breakdown"]
    etc, add_etc = cell(fb, "total_etc_fee"), cell(fb, "total_additional_etc_fee")
    v["total_tips"] = cell(fb, "total_tip_amount")
    v["total_stat_fees"] = (
        None if etc is None and add_etc is None
        else (float(etc) if etc is not None else 0.0) + (float(add_etc) if add_etc is not None else 0.0)
    )
    v["total_black_car_fund"] = cell(fb, "total_black_car_fund_surcharge")
    v["total_congestion_fee"] = cell(fb, "total_mta_congestion_fee")
    v["total_congestion_surcharge"] = cell(fb, "total_nys_congestion_surcharge")
    v["total_ny_sales_tax"] = cell(fb, "total_ny_sales_tax")
    return v


def compute_derived(v):
    z = lambda k: (_num(v.get(k)) or 0)
    d = {}
    # per-segment operational derived
    for seg, _disp in SEGMENTS:
        d[f"{seg}_avg_trip_per_rider"] = _safe_div(v.get(f"{seg}_completed"), v.get(f"{seg}_rider_weekly_complete"))
        d[f"{seg}_avg_trip_per_driver"] = _safe_div(v.get(f"{seg}_daily_completed"), v.get(f"{seg}_daily_avg_completed_drivers"))
    # driver / rider (Overall-level)
    d["cd_od"] = _safe_div(v.get("daily_avg_completed_drivers"), v.get("avg_online_drivers"))
    d["net_new_driver"] = z("driver_first") + z("driver_resurrect") - z("driver_churn")
    d["net_new_rider"] = z("rider_first") + z("rider_resurrect") - z("rider_churn")
    d["completed_riders_per_wau"] = _safe_div(v.get("rider_weekly_complete"), v.get("wau"))
    d["rd_ratio"] = _safe_div(v.get("rider_weekly_complete"), v.get("driver_weekly_complete"))
    # promo / platform (Overall-level)
    completed = _num(v.get("completed"))
    d["non_promo_trips"] = None if completed is None else completed - z("discount_trips")
    d["promo_over_completed"] = _safe_div(v.get("discount_trips"), v.get("completed"))
    d["promo_per_ride"] = _safe_div(v.get("discount"), v.get("completed"))
    d["promo_per_rider"] = _safe_div(v.get("discount"), v.get("rider_weekly_complete"))
    d["platform_fee_per_ride"] = _safe_div(v.get("total_system_fee"), v.get("completed"))
    d["promo_over_fare"] = _safe_div(d["promo_per_ride"], v.get("Overall_avg_rider_fare"))
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
# Workbook (A=section, B=segment, C=metric, D=value)
# ---------------------------------------------------------------------------
NUMFMT = {"int": "#,##0", "dec": "#,##0.00", "dec4": "#,##0.0000", "pct": "0.00%", "money": "$#,##0.00"}


def build_workbook(path, start_date, values, payment):
    import xlsxwriter

    wb = xlsxwriter.Workbook(path, {"nan_inf_to_errors": True})
    ws = wb.add_worksheet("Weekly Report")
    ws.set_column(0, 0, 12)
    ws.set_column(1, 1, 18)
    ws.set_column(2, 2, 34)
    ws.set_column(3, 3, 14)

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

    hdr_fmt = wb.add_format({"bold": True, "bg_color": "#1F2937", "font_color": "white",
                             "align": "center", "border": 1})
    ws.write(0, 2, "NY Weekly Report", wb.add_format({"bold": True, "font_size": 14}))
    ws.write(1, 2, "Monday Start Date", wb.add_format({"bold": True}))
    ws.write(1, 3, start_date, wb.add_format({"bold": True}))
    HDR = 2
    for col, txt in ((0, "Section"), (1, "Segment"), (2, "Metric"), (3, start_date)):
        ws.write(HDR, col, txt, hdr_fmt)
    DATA = HDR + 1

    # Pass 1: row ranges for the merged section (A) and segment (B) labels.
    sec_range, seg_range = {}, {}
    for i, (kind, section, segment, label, key, nf) in enumerate(SPEC):
        r0 = DATA + i
        if section:
            lo, hi = sec_range.get(section, (r0, r0))
            sec_range[section] = (min(lo, r0), max(hi, r0))
        if segment:
            sk = (section, segment)
            lo, hi = seg_range.get(sk, (r0, r0))
            seg_range[sk] = (min(lo, r0), max(hi, r0))

    # Pass 2: metric (C) + value (D). Column B is filled per-row only where there's no
    # segment label (segment cells are written by the merge loop below).
    for i, (kind, section, segment, label, key, nf) in enumerate(SPEC):
        r0 = DATA + i
        if kind == "blank":
            continue
        bg = SECTION_FILL.get(section)
        if segment is None:
            ws.write_blank(r0, 1, None, fmt(bg=bg))
        ws.write(r0, 2, label, fmt(bg=bg))
        if kind == "val":
            ws.write(r0, 3, _num(values.get(key)), fmt(numfmt=nf, bg="#FFF8E1"))
        else:
            ws.write_blank(r0, 3, None, fmt(numfmt=nf, bg="#FFF8E1"))

    def label_fmt(section):
        return fmt(bg=SECTION_FILL.get(section), bold=True, rotation=90, align="center", valign="vcenter")

    for section, (lo, hi) in sec_range.items():
        if hi > lo:
            ws.merge_range(lo, 0, hi, 0, section, label_fmt(section))
        else:
            ws.write(lo, 0, section, label_fmt(section))
    for (section, segment), (lo, hi) in seg_range.items():
        if hi > lo:
            ws.merge_range(lo, 1, hi, 1, segment, label_fmt(section))
        else:
            ws.write(lo, 1, segment, label_fmt(section))

    # Payment Method sheet
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
    now = datetime.now(NY_TZ) if NY_TZ else datetime.utcnow() - timedelta(hours=4)
    this_monday = now.date() - timedelta(days=now.weekday())
    return this_monday - timedelta(days=7)


def main():
    load_dotenv()
    redash = Redash(key=os.getenv("REDASH_API_KEY"), base_url=os.getenv("REDASH_BASE_URL"))

    # Optional override: `python weekly/ny.py 2026-06-08` to backfill a specific week.
    # Must be a Monday (queries bucket on week-start); a non-Monday snaps back. No arg = last week.
    if len(sys.argv) > 1:
        d = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        if d.weekday() != 0:
            d = d - timedelta(days=d.weekday())
            print(f"Note: snapped to week-start Monday {d}")
        start_date = d.strftime("%Y-%m-%d")
    else:
        start_date = last_complete_monday().strftime("%Y-%m-%d")
    output_date = datetime.strptime(start_date, "%Y-%m-%d").strftime("%d_%b_%Y")
    print(f"NY weekly for week starting {start_date}")

    queries = [Query(qid, params={"week_start_date": start_date}) for qid in QIDS.values()]
    redash.run_queries(queries)
    res = {name: redash.get_result(qid) for name, qid in QIDS.items()}

    values = extract_values(res)
    values.update(compute_derived(values))
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
