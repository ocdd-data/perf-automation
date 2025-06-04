import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import pandas as pd
import calendar
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
from utils.helpers import Redash, Query
from utils.slack import SlackBot


def main():
  load_dotenv()

  redash = Redash(key=os.getenv("REDASH_API_KEY"), base_url=os.getenv("REDASH_BASE_URL"))

  # Get the first day of this month
  today = datetime.today()
  first_day_this_month = today.replace(day=1)

  # Go back one day → lands on last day of previous month
  last_month_date = first_day_this_month - timedelta(days=1)

  YEAR = last_month_date.year
  MONTH = last_month_date.month

  # Format strings
  dt_format = "%Y-%m-%d"
  query_date = datetime(YEAR, MONTH, 1).strftime(dt_format)

  start_date = query_date
  last_day = calendar.monthrange(YEAR, MONTH)[1]
  end_date = datetime(YEAR, MONTH, last_day).strftime(dt_format)
  DAYS_IN_MONTH = last_day
  output_date = datetime(YEAR, MONTH, 1).strftime("%b_%Y")

  queries = [[
    Query(2183, params={"date": query_date}),
    Query(2184, params={"date": query_date}),
    Query(2187, params={"date": query_date}),
    Query(2188, params={"date": query_date}),
    Query(2189, params={"date": query_date}),
    Query(2192, params={"date": query_date}),
    Query(2194, params={"date": query_date}),
    Query(2195, params={"date": query_date}),
    Query(2197, params={"date": query_date}),
    Query(2203, params={"date": query_date}),
    Query(2204, params={"date": query_date}),
    Query(2205, params={"date": query_date}),
    Query(2206, params={"date": query_date}),
    Query(2208, params={"date": query_date}),
    Query(2209, params={"date": query_date}),
    Query(2210, params={"date": query_date}),
    Query(2214, params={"date": query_date}),
    Query(2246, params={"date": query_date}),
    Query(2247, params={"date": query_date}),
    Query(2353, params={"date": query_date}),
    Query(2354, params={"date": query_date}),
    Query(2198, params={"date": query_date}),
    Query(4691, params={"date": query_date}),
    Query(4724, params={"date": query_date}),
    Query(4727, params={"date": query_date}),
    Query(4814, params={"Date Range": {"start": start_date, "end": end_date}, "region": region_id}),  
  ]]

  for query_list in queries:
    redash.run_queries(query_list)

  bq1 = redash.get_result(2187) # active users - rider
  bq2 = redash.get_result(2192) # search and open
  bq3 = redash.get_result(2203) # online driver
  bq4 = redash.get_result(2198) # ping driver daily
  bq5 = redash.get_result(2208) # driver avg online hour

  df1 = redash.get_result(2183) # completed & others
  df2 = redash.get_result(2184) # trip booking
  df3 = redash.get_result(2214) # approved
  df4 = redash.get_result(2188) # rider sign up
  df5 = redash.get_result(2189) # all-time / same mth rider
  df6 = redash.get_result(2194) # book / completed monthly
  df7 = redash.get_result(2195) # unique
  df8 = redash.get_result(2210) # median ETA
  df9 = redash.get_result(2246) # resurrect rider
  df10 = redash.get_result(2247) # churned
  df11 = redash.get_result(2197) # rider wait before cxl
  df12 = redash.get_result(2203) # online driver daily / count
  df13 = redash.get_result(2205) # driver
  df14 = redash.get_result(2204) # driver completed / ride per
  df15 = redash.get_result(2206) # driver all-time / same mth
  df16 = redash.get_result(2209) # driver avg util hrs
  df17 = redash.get_result(2353) # resurrect driver
  # df18 = redash.get_result(2354) # churned driver - old query
  df19 = redash.get_result(4691) # monthly ps - first try
  df20 = redash.get_result(4724) # rider act, resurrect, churn
  df21 = redash.get_result(4727) # rider act, resurrect, churn
  df22 = redash.get_result(4814) # median time to match, expire


  df = pd.DataFrame()

  # total summaries

  df['rides'] = df1.completed
  df['demand'] = df1.demand
  df['match_rate'] = df1.matched/df.demand
  df['completion_rate'] = df.rides/df.demand
  df['daily_rides'] = df.rides/DAYS_IN_MONTH
  df['uncompleted'] = df.demand - df.rides
  df['cater_rate'] = df.rides/df7.unique
  df['first_try_cater_rate'] = df19.first_try_cater_rate
  df['retry_initiation_rate'] = df19.retry_initiation_rate
  df['retry_success_rate'] = df19.retry_success_rate
  df['daily_median_eta'] = df8.median_eta
  df['median_time_to_match_sec'] = df22.median_time_to_match_sec
  df['median_time_to_expire_sec'] = df22.median_time_to_expire_sec

  df['rides_phv'] = df2.phv_trip_count
  df['demand_phv'] = df2.phv_trip_booking
  df['completed_riders_phv'] = df2.phv_driver_completed
  df['phv_approved_drivers'] = df3.approved_phv

  df['rides_taxi'] = df2.taxi_trip_count
  df['demand_taxi'] = df2.taxi_trip_booking
  df['completed_riders_taxi'] = df2.taxi_driver_completed
  df['taxi_approved_drivers'] = df3.approved_taxi

  df = df.copy()

  # driver starting here

  df['driver_mau'] = bq3.online_driver_count
  df['completed_driver'] = df1.completed_drivers
  df['total_approved'] = None
  df['driver_online_daily'] = bq3.online_driver_daily
  df['pinged_drivers_daily'] = bq4['pinged_drivers_daily'].iloc[0]
  df['completed_driver_daily'] = df14.completed_driver_daily
  df['online_mau'] = df.driver_online_daily/df.driver_mau
  df['completed_online'] = df.completed_driver_daily/df.driver_online_daily
  df['online_no_complete'] = bq3.online_driver_daily-df.completed_driver_daily
  df['ride_per_driver'] = df14.ride_per_driver
  df['driver_downloads'] = None
  df['driver_sign_up'] = df13.driver_sign_up
  df['driver_sign_up_daily'] = df13.driver_sign_up/DAYS_IN_MONTH
  df['driver_ft_all_time'] = df15.all_time
  df['driver_ft_same_month'] = df15.same_month
  df['driver_sign_up_activation_rate'] = df.driver_ft_same_month/df.driver_sign_up
  df['driver_approved_activation_rate'] = df.driver_ft_same_month/df13.driver_same_month_approved
  df['driver_approved'] = df13.driver_approved
  df['driver_same_month_approved'] = df13.driver_same_month_approved
  df['driver_average_online_hours'] = bq5.avg_online_hour
  df['driver_average_utilisation_hours'] = df16.avg_utilisation_hours
  df['ping_per_driver_daily'] = bq4.ping_per_driver_daily
  df['driver_waiting_before_cancel'] = df11.avg_waiting_time_driver_cxl
  df['driver_cancellation_rate'] = df1.driver_cancel/df1.matched*100
  df['drivers_ft_unique'] = df.driver_ft_all_time/df.completed_driver
  df['drivers_repeated'] = df21.repeated
  df['resurrect_2_month_driver'] = df17.resurrect_2_month
  df['resurrect_3_4_month_driver'] = df17.resurrect_3_4_month
  df['resurrect_5_12_month_driver'] = df17.resurrect_5_12_month
  df['driver_resurrected_over_12mth'] = None
  df['driver_resurrected_all'] = df17.resurrect_all
  df['drivers_repeated/unique_complete'] = df21.repeated / df['completed_driver']
  df['resurrect_2_month_driver/unique_complete'] = df17.resurrect_2_month / df['completed_driver']
  df['resurrect_3_4_month_driver/unique_complete'] = df17.resurrect_3_4_month / df['completed_driver']
  df['resurrect_5_12_month_driver/unique_complete'] = df17.resurrect_5_12_month / df['completed_driver']
  df['driver_resurrected_over_12mth/unique_complete'] = None
  df['driver_resurrected_rate'] = None
  df['driver_churned'] = df21.churned
  df['driver_churned_rate'] = None
  df['driver_inflow'] =  df21.resurrected + df['driver_ft_all_time']

  df = df.copy()

  # rider start here

  df['rider_mau'] = bq1.active_users
  df['rider_mau_demand'] = df.demand/df.rider_mau
  df['rider_mau_rides'] = df.rides/df.rider_mau
  df['r_d_ratio'] = df.rider_mau/df.driver_mau
  df['rider_downloads'] = None
  df['rider_signup'] = df4.rider_signup
  df['rider_signup_daily'] = df4.rider_signup/DAYS_IN_MONTH
  df['rider_ft_all_time'] = df5.all_time
  df['rider_ft_same_month'] = df5.same_month
  df['rider_same_month_activation'] = df.rider_ft_same_month/df.rider_signup
  df['rider_unique_open_monthly'] = bq2.open_monthly
  df['rider_unique_search_monthly'] = bq2.search_monthly
  df['rider_unique_book_monthly'] = df6.book_monthly
  df['rider_unique_complete_monthly'] = df6.completed_monthly
  df['rider_unique_open_daily'] = bq2.open_daily
  df['rider_unique_search_daily'] = bq2.search_daily
  df['rider_unique_book_daily'] = df6.book_daily
  df['rider_unique_complete_daily'] = df6.completed_daily
  df['book_search_ratio_daily'] = df.rider_unique_book_daily/df.rider_unique_search_daily
  df['booking_per_user'] = df.demand/df.rider_unique_book_monthly
  df['complete_per_user'] = df.rides/df.rider_unique_complete_monthly
  df['duplicate_ratio'] = df.demand/df7.unique
  df['rider_waiting_before_cancel'] = df11.avg_waiting_time_rider_cxl
  df['rider_cancellation_rate'] = df1.rider_cancel/df.demand*100
  df['riders_ft_unique'] = df.rider_ft_all_time/df.rider_unique_complete_monthly
  df['riders_repeated'] = df20.repeated
  df['resurrect_2_month'] = df9.resurrect_2_month
  df['resurrect_3_4_month'] = df9.resurrect_3_4_month
  df['resurrect_5_12_month'] = df9.resurrect_5_12_month
  df['rider_resurrected_over_12mth'] = None
  df['rider_resurrected_all'] = df9.resurrect_all
  df['rider_repeated/unique_complete'] = df20.repeated /  df6.completed_monthly
  df['resurrect_2_month_rider/unique_complete'] = df9.resurrect_2_month /  df6.completed_monthly
  df['resurrect_3_4_month_rider/unique_complete'] = df9.resurrect_3_4_month /  df6.completed_monthly
  df['resurrect_5_12_month_rider/unique_complete'] = df9.resurrect_5_12_month /  df6.completed_monthly
  df['rider_resurrected_over_12mth/unique_complete'] = None
  df['rider_resurrected_rate'] = None
  df['rider_churned'] = df20.churned
  df['rider_churned_rate'] = None
  df['rider_inflow'] = df20.activated + df20.resurrected - df20.churned

  df = df.copy()

  df = df.T
  df.columns = [f"{output_date}"]

  output_file = f"SG_{output_date}.csv"

  df.to_csv(output_file)

  slack = SlackBot()
  slack.uploadFile(output_file,
                   os.getenv("SLACK_CHANNEL"),
                   f"Monthly Report for SG {output_date}")


if __name__ == '__main__':
  main()
