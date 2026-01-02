import os
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from utils.helpers import Query, Redash
from utils.slack import SlackBot

import calendar
import os
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from utils.helpers import Query, Redash
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

  region_id = 3
  region = 'KH'
  timezone = 7

  queries = [[
    Query(1625, params={"date": start_date}),
    Query(2373, params={"date": start_date}),
    Query(2374, params={"date": start_date}),
    Query(2375, params={"date": start_date}),
    Query(2376, params={"date": start_date}),
    Query(2377, params={"date": start_date}),
    Query(2383, params={"date": start_date}),
    Query(5349, params={"date": start_date}),
    Query(2385, params={"date": start_date}),
    Query(2386, params={"date": start_date}),
    Query(2387, params={"date": start_date}),
    Query(2388, params={"date": start_date}),
    Query(2390, params={"date": start_date}),
    Query(2550, params={"date": start_date}),
    Query(2547, params={"date": start_date}),
    Query(2545, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(2549, params={"date": start_date}),
    Query(2664, params={"date": start_date}),
    Query(4752, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4814, params={"date_range": {"start": start_date, "end": end_date}, "region": region_id}),
    Query(4411, params={"date_range": {"start": start_date, "end": end_date}, "region": region_id}),
    Query(4819, params={"date_range": {"start": start_date, "end": end_date}, "region": region}),
    Query(3108, params={"region": region, "timezone": timezone, "date": start_date}),
    Query(2670, params={"region": region_id, "timezone": timezone, "date": start_date}),
    Query(5613, params={"date": start_date}),
    Query(5615, params={"date": start_date}),
    Query(5618, params={"date": start_date}),
    Query(4819, params={"date_range": {"start": start_date, "end": end_date}, "region": region}),
    Query(5621, params={"date": start_date}),
    Query(5604, params={"region": region, "timezone": timezone, "date": start_date}),

  ]]

  for query_list in queries:
    redash.run_queries(query_list)

  # Fetch df
  df1 = redash.get_result(1625)  # KH - All trips
  df2 = redash.get_result(2373)  # KH - Active Drivers Monthly
  df3 = redash.get_result(2374)  # KH - Daily Online Drivers
  df4 = redash.get_result(2375)  # KH - Daily Average Finished Drivers
  df5 = redash.get_result(2376)  # KH - Signed Up Drivers/Riders
  df6 = redash.get_result(2377)  # KH - First Trip Riders
  df7 = redash.get_result(2383)  # KH - Daily First Trip Drivers/Riders
  df8 = redash.get_result(5349)  # KH - Active Rider Activity
  df9 = redash.get_result(2385)  # KH - Monthly and Daily Users
  df10 = redash.get_result(2386)  # KH - ETA
  df11 = redash.get_result(2387)  # KH - Driver Utilization
  df12 = redash.get_result(2388)  # KH - Average Online Hours and Pings Per Driver
  df13 = redash.get_result(2390)  # KH - First Trip Drivers
  df14 = redash.get_result(2550)  # KH - Monthly Resurrected Riders
  df15 = redash.get_result(2547)  # KH - Monthly Resurrected Drivers
  df16 = redash.get_result(2545)  # KH - Churned Riders
  df17 = redash.get_result(2549)  # KH - Driver CAR - I changed the query to match sg perf sheet - need to rerun everything
  df18 = redash.get_result(2664)  # KH - Monthly Completed Drivers
  df19 = redash.get_result(4752)  # monthly ps - first try
  df20 = redash.get_result(4814)  # median ttm / expire
  df21 = redash.get_result(4411)  # avg median eta
  df22 = redash.get_result(4819) # new book search logic
  df23 = redash.get_result(5604) # KH - approved drivers
  df24 = redash.get_result(5613) # KH - rides cancel
  df25 = redash.get_result(4819) # new bs logic
  df26 = redash.get_result(5621) # unique booking

  bq2 = redash.get_result(5618) # open monthly
  bq4 = redash.get_result(2670) # pinged

  # Construct monthly DataFrame
  df = pd.DataFrame()

  # confirmed start here

  df['fin'] = df1.fin
  df['all_'] = df1.all_
  df['match_rate'] = df1.matched/df1.all_
  df['all_c_r'] = df.fin/df.all_
  df['daily_fin'] = df.fin/DAYS_IN_MONTH
  df['uncompleted'] = df.all_ - df.fin
  df['cater_rate'] = df9.cater_rate
  df['first_try_cater_rate'] = df19.first_try_cater_rate
  df['retry_initiation_rate'] = df19.retry_initiation_rate
  df['retry_success_rate'] = df19.retry_success_rate
  df['booked_riders'] = df1.booked_riders
  df['completed_riders'] = df1.completed_riders
  df['daily_median_eta'] = df21.avg_daily_median_eta
  df['median_time_to_match_sec'] = df20.median_time_to_match_sec
  df['median_time_to_expire_sec'] = df20.median_time_to_expire_sec

  # sh/concierge - not looking at it anym

  # app metrics

  df['revenue'] = None
  df['app_fin'] = df1.app_fin
  df['app_daily_fin'] = df.app_fin/DAYS_IN_MONTH
  df['app_all'] = df1.app_all
  df['app_daily_all'] = df.app_all/DAYS_IN_MONTH
  df['app_c_r']= df.app_fin/df.app_all

  df = df.copy()

  # by city metrics here

  df['pp_tt_fin'] = df1.pp_tt_fin
  df['pp_daily_tt_fin'] = df.pp_tt_fin/DAYS_IN_MONTH
  df['pp_tt_all'] = df1.pp_tt_all
  df['pp_tt_c_r'] = df.pp_tt_fin/df.pp_tt_all

  df['pp_t1_fin'] = df1.pp_t1_fin
  df['pp_t1_daily_fin'] = df.pp_t1_fin/DAYS_IN_MONTH
  df['pp_t1_all'] = df1.pp_t1_all
  df['pp_t1_c_r'] = df.pp_t1_fin/df.pp_t1_all

  df['pp_bike_fin'] = df1.pp_bike_fin
  df['pp_bike_daily_fin'] = df.pp_bike_fin/DAYS_IN_MONTH
  df['pp_bike_all'] = df1.pp_bike_all
  df['pp_bike_c_r'] = df.pp_bike_fin/df.pp_bike_all

  df['pp_remor_fin'] = df1.pp_remor_fin
  df['pp_remor_daily_fin'] = df.pp_remor_fin/DAYS_IN_MONTH
  df['pp_remor_all'] = df1.pp_remor_all
  df['pp_remor_c_r'] = df.pp_remor_fin/df.pp_remor_all

  df['pp_car_fin'] = df1.pp_car_fin
  df['pp_car_daily_fin'] = df.pp_car_fin/DAYS_IN_MONTH
  df['pp_car_all'] = df1.pp_car_all
  df['pp_car_c_r'] = df.pp_car_fin/df.pp_car_all

  df['sr_tt_fin'] = df1.sr_tt_fin
  df['sr_daily_tt_fin'] = df.sr_tt_fin/DAYS_IN_MONTH
  df['sr_tt_all'] = df1.sr_tt_all
  df['sr_tt_c_r'] = df.sr_tt_fin/df.sr_tt_all

  df['sr_t1_fin'] = df1.sr_t1_fin
  df['sr_t1_daily_fin'] = df.sr_t1_fin/DAYS_IN_MONTH
  df['sr_t1_all'] = df1.sr_t1_all
  df['sr_t1_c_r'] = df.sr_t1_fin/df.sr_t1_all

  df['sr_remor_fin'] = df1.sr_remor_fin
  df['sr_remor_daily_fin'] = df.sr_remor_fin/DAYS_IN_MONTH
  df['sr_remor_all'] = df1.sr_remor_all
  df['sr_remor_c_r'] = df.sr_remor_fin/df.sr_remor_all

  df['sr_car_fin'] = df1.sr_car_fin
  df['sr_car_all'] = df1.sr_car_all
  df['sr_car_c_r'] = df.sr_car_fin/df.sr_car_all

  df['shv_tt_fin'] = df1.shv_tt_fin
  df['shv_daily_tt_fin'] = df.shv_tt_fin/DAYS_IN_MONTH
  df['shv_tt_all'] = df1.shv_tt_all
  df['shv_tt_c_r'] = df.shv_tt_fin/df.shv_tt_all

  df['shv_car_fin'] = df1.shv_car_fin
  df['shv_car_all'] = df1.shv_car_all
  df['shv_car_c_r'] = df.shv_car_fin/df.shv_car_all

  df['kpk_tt_fin'] = df1.kpk_tt_fin
  df['kpk_daily_tt_fin'] = df.kpk_tt_fin/DAYS_IN_MONTH
  df['kpk_tt_all'] = df1.kpk_tt_all
  df['kpk_tt_c_r'] = df.kpk_tt_fin/df.kpk_tt_all

  df['kpk_remor_fin'] = df1.kpk_remor_fin
  df['kpk_remor_all'] = df1.kpk_remor_all
  df['kpk_remor_c_r'] = df.kpk_remor_fin/df.kpk_remor_all

  df['kpk_car_fin'] = df1.kpk_car_fin
  df['kpk_car_all'] = df1.kpk_car_all
  df['kpk_car_c_r'] = df.kpk_car_fin/df.kpk_car_all

  df = df.copy()

  # driver metrics here

  df['driver_mau'] = df2.mau_driver
  df['completed_driver'] = df1.completed_drivers
  df['total_approved_drivers'] = df23.total_approved
  df['driver_online_daily'] = df3.avg_daily_online_drivers
  df['pinged_drivers_daily'] = bq4.pinged_drivers_daily
  df['completed_driver_daily'] = df4.avg_daily_finished_driver_count
  df['ol_mau'] = df.driver_online_daily/df.driver_mau
  df['completed_online'] = df.completed_driver_daily/df.driver_online_daily
  df['online_no_complete'] = df.driver_online_daily-df.completed_driver_daily
  df['ride_per_driver'] = df4.avg_daily_ride_per_driver_total
  df['driver_downloads'] = None
  df['driver_sign_up'] = df5.su_drivers
  df['driver_sign_up_daily'] = df.driver_sign_up/DAYS_IN_MONTH
  df['driver_ft_all_time'] = df7.all_first_drivers
  df['driver_ft_same_month'] = df7.same_month_first_trip
  df['driver_sign_up_activation_rate'] = df.driver_ft_same_month/df.driver_sign_up
  df['driver_approved_activation_rate'] = df.driver_ft_same_month/df23.driver_same_month_approved
  df['driver_approved'] = df23.driver_approved
  df['driver_same_month_approved'] = df23.driver_same_month_approved
  df['driver_average_online_hours'] = df12.AvgOnlineHour
  df['driver_average_utilisation_hours'] = df11.monthly_daily_avg_hour
  df['ping_per_driver_daily'] = df12.avg_ping_per_driver
  df['driver_waiting_before_cancel'] = df24.avg_waiting_time_driver_cxl
  df['driver_cancellation_rate'] = df1.driver_cancel/df1.matched*100
  df['drivers_ft_unique'] = df.driver_ft_all_time/df.completed_driver
  df['drivers_repeated'] = df17.repeated
  df['resurrect_2_month_driver'] = df15.resurrect_2_month
  df['resurrect_3_4_month_driver'] = df15.resurrect_3_4_month
  df['resurrect_5_12_month_driver'] = df15.resurrect_5_12_month
  df['driver_resurrected_over_12mth'] = None
  df['driver_resurrected_all'] = df15.resurrect_all
  df["drivers_repeated/unique_complete"] = df17.repeated/ df['completed_driver']
  df['resurrect_2_month_driver/unique_complete'] = df15.resurrect_2_month / df['completed_driver']
  df['resurrect_3_4_month_driver/unique_complete'] = df15.resurrect_3_4_month / df['completed_driver']
  df['resurrect_5_12_month_driver/unique_complete'] = df15.resurrect_5_12_month / df['completed_driver']
  df['driver_resurrected_over_12mth/unique_complete'] = None
  df['driver_resurrected_rate'] = None
  df['driver_churned'] = df17.churned
  df['driver_churned_rate'] = None
  df['driver_inflow'] = df17.activated + df17.resurrected - df17.churned

  df = df.copy()

  # rider metrics here


  df['rider_mau'] = df8.active_users
  df['rider_mau_demand'] = df.all_/df.rider_mau
  df['rider_mau_rides'] = df.fin/df.rider_mau
  df['r_d_ratio'] = df.rider_mau/df.driver_mau
  df['rider_downloads'] = None
  df['rider_signup'] = df5.su_riders
  df['rider_signup_daily'] = df5.su_riders/DAYS_IN_MONTH
  df['rider_ft_all_time'] = df6.all_time
  df['rider_ft_same_month'] = df6.same_month
  df['rider_same_month_activation'] = df.rider_ft_same_month/df.rider_signup
  df['rider_unique_open_monthly'] = bq2.open_monthly
  df['rider_unique_search_monthly'] = df25.unique_search_users
  df['rider_unique_book_monthly'] = df25.unique_order_users
  df['rider_unique_complete_monthly'] = df9.finished_rider_count
  df['rider_unique_open_daily'] = bq2.open_daily
  df['rider_unique_search_daily'] = df25.rider_unique_search_daily_avg
  df['rider_unique_book_daily'] = df25.rider_unique_book_daily_avg
  df['rider_unique_complete_daily'] = df9.daily_avg_finished_rider_count
  df['book_search_ratio_daily'] = df25.book_search_ratio_daily
  df['booking_per_user'] = df.all_/df.rider_unique_book_monthly
  df['complete_per_user'] = df.fin/df.rider_unique_complete_monthly
  df['duplicate_ratio'] = df.all_/df26.unique
  df['rider_waiting_before_cancel'] = df24.avg_waiting_time_rider_cxl
  df['rider_cancellation_rate'] = df1.rider_cancel/df.all_*100
  df['riders_ft_unique'] = df.rider_ft_all_time/df.rider_unique_complete_monthly
  df['riders_repeated'] = df16.repeated
  df['resurrect_2_month'] = df14.resurrect_2_month
  df['resurrect_3_4_month'] = df14.resurrect_3_4_month
  df['resurrect_5_12_month'] = df14.resurrect_5_12_month
  df['rider_resurrected_over_12mth'] = None
  df['resurrect_total'] = df14.resurrect_all
  df['rider_repeated/unique_complete'] = df16.repeated /  df9.finished_rider_count
  df['resurrect_2_month_rider/unique_complete'] = df14.resurrect_2_month /  df9.finished_rider_count
  df['resurrect_3_4_month_rider/unique_complete'] = df14.resurrect_3_4_month /  df9.finished_rider_count
  df['resurrect_5_12_month_rider/unique_complete'] = df14.resurrect_5_12_month /  df9.finished_rider_count
  df['rider_resurrected_over_12mth/unique_complete'] = None
  df['rider_resurrected_rate'] = None
  df['rider_churned'] = df16.churned
  df['rider_churned_rate'] = None
  df['rider_inflow'] = df16.activated + df16.resurrected - df16.churned


  df = df.copy()

  df = df.T

  df.columns = [f"{output_date}"]

  output_file = f"KH_{output_date}.csv"

  df.to_csv(output_file)

  slack = SlackBot()
  slack.uploadFile(output_file,
                   os.getenv("SLACK_CHANNEL"),
                   f"Monthly Report for KH {output_date}")


if __name__ == '__main__':
  main()
