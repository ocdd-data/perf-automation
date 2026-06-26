import calendar
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

# Make `utils` importable whether run as a module (python -m monthly.ny) or directly.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.helpers import Query, Redash  # noqa: E402
from utils.slack import SlackBot          # noqa: E402


# Monthly NY report — same structure/metrics as monthly/sg.py, with two differences:
#   1. No PHV/taxi split (NY is a single product), so those 8 rows are removed.
#   2. Some SG queries are consolidated on the NY side:
#        - driver online + utilisation hours (SG 2203/2208/2209) -> one query (7586)
#        - driver sign up + total approved (SG 2205/5000)        -> one query (7584)
#        - the "unique booking" count lives in Monthly rides (7579, nyc_unique_booking)
#   All queries take a single {{ date }} param (region/region_id are hard-coded in the SQL).

# Logical name -> Redash query id
QIDS = {
    "trips":          7578,  # completed, demand, matched, cancels, completed_drivers/riders
    "rides":          7579,  # nyc_booking, nyc_unique_booking, nyc_completed, ...
    "rider_signup":   7590,  # rider_signup
    "rider_ft":       7591,  # all_time, same_month (rider first trip)
    "open_search":    7622,  # open_monthly, search_monthly, open_daily, search_daily (GA4)
    "rider_bc":       7592,  # book_monthly, completed_monthly, book_daily, completed_daily
    "wait":           7624,  # avg_waiting_time_* (rider/driver before cancel)
    "driver_online":  7586,  # online_driver_daily/count, avg_online_hour, avg_utilisation_hours
    "drivers_daily":  7585,  # ride_per_driver, completed_driver_daily
    "driver_signup":  7584,  # total_approved, driver_sign_up, driver_same_month_approved, driver_approved
    "driver_ft":      7625,  # all_time, same_month (driver first trip)
    "eta":            7583,  # median_eta
    "rider_resurrect": 7632, # resurrect_2_month / 3_4 / 5_12 / all (rider)
    "driver_resurrect": 7634,# resurrect_2_month / 3_4 / 5_12 / all (driver)
    "ping":           7636,  # pinged_drivers_daily, ping_per_driver_daily
    "first_attempt":  7594,  # first_try_cater_rate, retry_initiation_rate, retry_success_rate
    "rider_arc":      7593,  # repeated, activated, resurrected, churned (rider)
    "driver_arc":     7588,  # repeated, activated, resurrected, churned (driver)
    "match_expire":   7595,  # median_time_to_match_sec, median_time_to_expire_sec
    "book_search":    7637,  # unique_search_users, unique_order_users, *_daily_avg, book_search_ratio_daily
}

# GA4 "active users - rider" query (SG 2187 equivalent) — feeds rider_mau and dependents.
RIDER_MAU_QID = 7621


def main():
    load_dotenv()
    redash = Redash(key=os.getenv("REDASH_API_KEY"), base_url=os.getenv("REDASH_BASE_URL"))

    # Optional override: `python -m monthly.ny 2026-06` (or 2026-06-01) to run a specific month.
    # With no argument, defaults to the last COMPLETED month.
    if len(sys.argv) > 1:
        try:
            d = datetime.strptime(sys.argv[1], "%Y-%m-%d")
        except ValueError:
            d = datetime.strptime(sys.argv[1], "%Y-%m")
        YEAR, MONTH = d.year, d.month
    else:
        first_day_this_month = datetime.today().replace(day=1)
        last_month_date = first_day_this_month - timedelta(days=1)
        YEAR, MONTH = last_month_date.year, last_month_date.month

    query_date = datetime(YEAR, MONTH, 1).strftime("%Y-%m-%d")
    DAYS_IN_MONTH = calendar.monthrange(YEAR, MONTH)[1]
    output_date = datetime(YEAR, MONTH, 1).strftime("%b_%Y")

    # Batch-run all queries.
    query_list = [Query(qid, params={"date": query_date}) for qid in QIDS.values()]
    if RIDER_MAU_QID:
        query_list.append(Query(RIDER_MAU_QID, params={"date": query_date}))
    redash.run_queries(query_list)

    r = {name: redash.get_result(qid) for name, qid in QIDS.items()}
    rider_mau_df = redash.get_result(RIDER_MAU_QID) if RIDER_MAU_QID else None

    trips        = r["trips"]
    rides        = r["rides"]
    rider_signup = r["rider_signup"]
    rider_ft     = r["rider_ft"]
    open_search  = r["open_search"]
    rider_bc     = r["rider_bc"]
    wait         = r["wait"]
    d_online     = r["driver_online"]
    d_daily      = r["drivers_daily"]
    d_signup     = r["driver_signup"]
    driver_ft    = r["driver_ft"]
    eta          = r["eta"]
    r_resurrect  = r["rider_resurrect"]
    d_resurrect  = r["driver_resurrect"]
    ping         = r["ping"]
    first_att    = r["first_attempt"]
    rider_arc    = r["rider_arc"]
    driver_arc   = r["driver_arc"]
    match_expire = r["match_expire"]
    book_search  = r["book_search"]

    df = pd.DataFrame()

    # ---- total summaries ----
    df['rides'] = trips.completed
    df['demand'] = trips.demand
    df['match_rate'] = trips.matched / df.demand
    df['completion_rate'] = df.rides / df.demand
    df['daily_rides'] = df.rides / DAYS_IN_MONTH
    df['uncompleted'] = df.demand - df.rides
    df['cater_rate'] = df.rides / rides.nyc_unique_booking          # SG: rides / df7.unique
    df['first_try_cater_rate'] = first_att.first_try_cater_rate
    df['retry_initiation_rate'] = first_att.retry_initiation_rate
    df['retry_success_rate'] = first_att.retry_success_rate
    df['daily_median_eta'] = eta.median_eta
    df['median_time_to_match_sec'] = match_expire.median_time_to_match_sec
    df['median_time_to_expire_sec'] = match_expire.median_time_to_expire_sec
    # (PHV/taxi split rows from SG intentionally removed — NY has no split.)

    df = df.copy()

    # ---- driver ----
    df['driver_mau'] = d_online.online_driver_count
    df['completed_driver'] = trips.completed_drivers
    df['total_approved'] = d_signup.total_approved
    df['driver_online_daily'] = d_online.online_driver_daily
    df['pinged_drivers_daily'] = ping['pinged_drivers_daily'].iloc[0]
    df['completed_driver_daily'] = d_daily.completed_driver_daily
    df['online_mau'] = df.driver_online_daily / df.driver_mau
    df['completed_online'] = df.completed_driver_daily / df.driver_online_daily
    df['online_no_complete'] = d_online.online_driver_daily - df.completed_driver_daily
    df['ride_per_driver'] = d_daily.ride_per_driver
    df['driver_downloads'] = None
    df['driver_sign_up'] = d_signup.driver_sign_up
    df['driver_sign_up_daily'] = d_signup.driver_sign_up / DAYS_IN_MONTH
    df['driver_ft_all_time'] = driver_ft.all_time
    df['driver_ft_same_month'] = driver_ft.same_month
    df['driver_sign_up_activation_rate'] = df.driver_ft_same_month / df.driver_sign_up
    df['driver_approved_activation_rate'] = df.driver_ft_same_month / d_signup.driver_same_month_approved
    df['driver_approved'] = d_signup.driver_approved
    df['driver_same_month_approved'] = d_signup.driver_same_month_approved
    df['driver_average_online_hours'] = d_online.avg_online_hour
    df['driver_average_utilisation_hours'] = d_online.avg_utilisation_hours
    df['ping_per_driver_daily'] = ping.ping_per_driver_daily
    df['driver_waiting_before_cancel'] = wait.avg_waiting_time_driver_cxl
    df['driver_cancellation_rate'] = trips.driver_cancel / trips.matched * 100
    df['drivers_ft_unique'] = df.driver_ft_all_time / df.completed_driver
    df['drivers_repeated'] = driver_arc.repeated
    df['resurrect_2_month_driver'] = d_resurrect.resurrect_2_month
    df['resurrect_3_4_month_driver'] = d_resurrect.resurrect_3_4_month
    df['resurrect_5_12_month_driver'] = d_resurrect.resurrect_5_12_month
    df['driver_resurrected_over_12mth'] = None
    df['driver_resurrected_all'] = d_resurrect.resurrect_all
    df['drivers_repeated/unique_complete'] = driver_arc.repeated / df['completed_driver']
    df['resurrect_2_month_driver/unique_complete'] = d_resurrect.resurrect_2_month / df['completed_driver']
    df['resurrect_3_4_month_driver/unique_complete'] = d_resurrect.resurrect_3_4_month / df['completed_driver']
    df['resurrect_5_12_month_driver/unique_complete'] = d_resurrect.resurrect_5_12_month / df['completed_driver']
    df['driver_resurrected_over_12mth/unique_complete'] = None
    df['driver_resurrected_rate'] = None
    df['driver_churned'] = driver_arc.churned
    df['driver_churned_rate'] = None
    df['driver_inflow'] = driver_arc.activated + driver_arc.resurrected - driver_arc.churned

    df = df.copy()

    # ---- rider ----
    if rider_mau_df is not None:
        df['rider_mau'] = rider_mau_df.active_users
    else:
        df['rider_mau'] = None                      # awaiting "active users - rider" query id
    df['rider_mau_demand'] = df.demand / df.rider_mau
    df['rider_mau_rides'] = df.rides / df.rider_mau
    df['r_d_ratio'] = df.rider_mau / df.driver_mau
    df['rider_downloads'] = None
    df['rider_signup'] = rider_signup.rider_signup
    df['rider_signup_daily'] = rider_signup.rider_signup / DAYS_IN_MONTH
    df['rider_ft_all_time'] = rider_ft.all_time
    df['rider_ft_same_month'] = rider_ft.same_month
    df['rider_same_month_activation'] = df.rider_ft_same_month / df.rider_signup
    df['rider_unique_open_monthly'] = open_search.open_monthly
    df['rider_unique_search_monthly'] = book_search.unique_search_users
    df['rider_unique_book_monthly'] = book_search.unique_order_users
    df['rider_unique_complete_monthly'] = rider_bc.completed_monthly
    df['rider_unique_open_daily'] = open_search.open_daily
    df['rider_unique_search_daily'] = book_search.rider_unique_search_daily_avg
    df['rider_unique_book_daily'] = book_search.rider_unique_book_daily_avg
    df['rider_unique_complete_daily'] = rider_bc.completed_daily
    df['book_search_ratio_daily'] = book_search.book_search_ratio_daily
    df['booking_per_user'] = df.demand / df.rider_unique_book_monthly
    df['complete_per_user'] = df.rides / df.rider_unique_complete_monthly
    df['duplicate_ratio'] = df.demand / rides.nyc_unique_booking     # SG: demand / df7.unique
    df['rider_waiting_before_cancel'] = wait.avg_waiting_time_rider_cxl
    df['rider_cancellation_rate'] = trips.rider_cancel / df.demand * 100
    df['riders_ft_unique'] = df.rider_ft_all_time / df.rider_unique_complete_monthly
    df['riders_repeated'] = rider_arc.repeated
    df['resurrect_2_month'] = r_resurrect.resurrect_2_month
    df['resurrect_3_4_month'] = r_resurrect.resurrect_3_4_month
    df['resurrect_5_12_month'] = r_resurrect.resurrect_5_12_month
    df['rider_resurrected_over_12mth'] = None
    df['rider_resurrected_all'] = r_resurrect.resurrect_all
    df['rider_repeated/unique_complete'] = rider_arc.repeated / rider_bc.completed_monthly
    df['resurrect_2_month_rider/unique_complete'] = r_resurrect.resurrect_2_month / rider_bc.completed_monthly
    df['resurrect_3_4_month_rider/unique_complete'] = r_resurrect.resurrect_3_4_month / rider_bc.completed_monthly
    df['resurrect_5_12_month_rider/unique_complete'] = r_resurrect.resurrect_5_12_month / rider_bc.completed_monthly
    df['rider_resurrected_over_12mth/unique_complete'] = None
    df['rider_resurrected_rate'] = None
    df['rider_churned'] = rider_arc.churned
    df['rider_churned_rate'] = None
    df['rider_inflow'] = rider_arc.activated + rider_arc.resurrected - rider_arc.churned

    df = df.copy()

    df = df.T
    df.columns = [f"{output_date}"]

    output_file = f"NY_{output_date}.csv"
    df.to_csv(output_file)

    slack = SlackBot()
    slack.uploadFile(output_file,
                     os.getenv("SLACK_CHANNEL_NY") or os.getenv("SLACK_CHANNEL"),
                     f"Monthly Report for NY {output_date}")


if __name__ == '__main__':
    main()
