import os
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from utils.helpers import Query, Redash
from utils.slack import SlackBot


def main():
  load_dotenv()

  redash = Redash(key=os.getenv("REDASH_API_KEY"), base_url=os.getenv("REDASH_BASE_URL"))

  dt_format = "%Y-%m-%d"
  start_date = (datetime.today().replace(day=1) - timedelta(days=1)).replace(day=1).strftime(dt_format)
  end_date = (datetime.today().replace(day=1) - timedelta(days=1)).strftime(dt_format)

  DAYS_IN_MONTH = int(end_date.split("-")[2])

  output_date = datetime.strptime(start_date, dt_format).strftime("%b_%Y")
  region_id = 3
  region = 'KH' 

  queries = [[
    Query(1625),
    Query(2373),
    Query(2374),
    Query(2375),
    Query(2376),
    Query(2377),
    Query(2383),
    Query(2384),
    Query(2385),
    Query(2386),
    Query(2387),
    Query(2388),
    Query(2390),
    Query(2550),
    Query(2547),
    Query(2545),
    Query(2549),
    Query(2664),
    Query(4752, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4814, params={"Date Range": {"start": start_date, "end": end_date}, "region": region}),
    Query(4411, params={"Date Range": {"start": start_date, "end": end_date}, "region": region}),
    Query(4819, params={"date_range": {"start": start_date, "end": end_date}, "region": region})
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
  df8 = redash.get_result(2384)  # KH - Active Rider Activity
  df9 = redash.get_result(2385)  # KH - Monthly and Daily Users
  df10 = redash.get_result(2386)  # KH - ETA
  df11 = redash.get_result(2387)  # KH - Driver Utilization
  df12 = redash.get_result(2388)  # KH - Average Online Hours and Pings Per Driver
  df13 = redash.get_result(2390)  # KH - First Trip Drivers
  df14 = redash.get_result(2550)  # KH - Monthly Resurrected Riders
  df15 = redash.get_result(2547)  # KH - Monthly Resurrected Drivers
  df16 = redash.get_result(2545)  # KH - Churned Riders
  df17 = redash.get_result(2549)  # KH - Churned Drivers
  df18 = redash.get_result(2664)  # KH - Monthly Completed Drivers
  df19 = redash.get_result(4752)  # monthly ps - first try
  df20 = redash.get_result(4814)  # median ttm / expire
  df21 = redash.get_result(4411)  # avg median eta
  df22 = redash.get_result(4819) # new book search logic

  # Construct monthly DataFrame
  df = pd.DataFrame()

  df['fin'] = df1.fin
  df['daily_fin'] = df.fin/DAYS_IN_MONTH
  df['all_'] = df1.all_
  df['daily_all'] = df.all_/DAYS_IN_MONTH
  df['all_c_r'] = df.fin/df.all_
  df['cater_rate'] = df9.cater_rate
  df['first_try_cater_rate'] = df19.first_try_cater_rate
  df['retry_initiation_rate'] = df19.retry_initiation_rate
  df['retry_success_rate'] = df19.retry_success_rate
  df['daily_median_eta'] = df21.avg_daily_median_eta
  df['median_time_to_match_sec'] = df20.median_time_to_match_sec
  df['median_time_to_expire_sec'] = df20.median_time_to_expire_sec
  df['growth'] = None

  df['sh_fin'] = df1.sh_fin
  df['sh_all'] = df1.sh_all
  df['sh_c_r'] = df.sh_fin/df.sh_all

  df['revenue'] = None

  df['app_fin'] = df1.app_fin
  df['app_daily_fin'] = df.app_fin/DAYS_IN_MONTH
  df['app_all'] = df1.app_all
  df['app_daily_all'] = df.app_all/DAYS_IN_MONTH
  df['app_c_r']= df.app_fin/df.app_all

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

  df['rider_mau'] = df8.MAU
  df['maur_demand'] = df.all_/df.rider_mau
  df['maur_ride'] = df.app_fin/df.rider_mau

  df['mau_driver'] = df2.mau_driver

  df['rd_ratio'] = df.rider_mau/ df.mau_driver

  df['avg_daily_online_drivers'] = df3.avg_daily_online_drivers
  df['ol_mau'] = df.avg_daily_online_drivers/df.mau_driver

  df['avg_daily_finished_driver_count'] = df4.avg_daily_finished_driver_count
  df['cp_ol'] = df.avg_daily_finished_driver_count/df.avg_daily_online_drivers
  df['ol_non_cp'] = df.avg_daily_online_drivers - df.avg_daily_finished_driver_count
  df['non_cp_trips'] = df.all_ - df.fin
  df['avg_rider_per_driver'] = (df.app_fin/DAYS_IN_MONTH)/df.avg_daily_finished_driver_count

  df['rider_download'] = None

  df['su_riders'] = df5.su_riders

  df['ft'] = df6.ft

  df['ft_pnh_rep_shv'] = df6.ft_pnh_rep_shv
  df['ft_rep'] = df6.ft_rep
  df['ft_shv'] = df6.ft_shv
  df['ft_kpk'] = df6.ft_kpk

  df['ft_su'] = df6.ft_su

  df['ft_su_ratio'] = df.ft_su/df.su_riders
  df['daily_su'] = df.su_riders/DAYS_IN_MONTH

  df = df.copy()

  df['avg_daily_all_first_riders'] = df7.avg_daily_all_first_riders

  df['MAU'] = df8.MAU
  df['unique_rider_search'] = df8.unique_rider_search

  df['rider_booking_monthly'] = df9.all_rider_count

  df['book_app_demand'] = None
  df['rider_fin_monthly'] = df9.finished_rider_count
  df['user_cater_rate'] = df.rider_fin_monthly/df.rider_booking_monthly

  df['resurrect_2_month'] = df14.resurrect_2_month
  df['resurrect_3_4_month'] = df14.resurrect_3_4_month
  df['resurrect_5_12_month'] = df14.resurrect_5_12_month

  df['resurrect_total'] = df14.resurrect_all

  df['rider_churned'] = df16.churned
  df['churn_rate'] = None

  df['rider_booking_dest_monthly'] = df9.all_destination_rider_count
  df['rider_fin_dest_monthly'] = df9.all_destination_fin_rider_count

  df['dup_ratio'] = df9.duplicate_ratio
  df['avg_rc_waiting_time'] = df9.avg_rc_waiting_time
  df['rc'] = df9.rc_rate

  df['eta_fin'] = df10.eta_fin
  df['eta_rc'] = df10.eta_rc

  df = df.copy()

  df['dau'] = df8.avg_user_open
  df['daily_user_search'] = df22.rider_unique_search_daily_avg

  df['rider_booking_daily'] = df22.rider_unique_book_daily_avg
  df['rider_booking_dest_daily'] = df9.daily_avg_all_destination_rider_count
  df['book_search_ratio'] = df22.book_search_ratio_daily
  df['book_open_ratio'] = None

  df['rider_fin_daily'] = df9.daily_avg_finished_rider_count
  df['new_existing_ratio'] = df.avg_daily_all_first_riders/df.rider_fin_daily
  df['rider_fin_dest_daily'] = df9.daily_avg_all_destination_fin_rider_count

  df['driver_download'] = None

  df['su_drivers'] = df5.su_drivers

  df['driver_ft'] = df7.all_first_drivers

  df['driver_ft_su'] = df13.ft_su_drivers
  df['driver_ft_su_rate'] = df.driver_ft_su/df.su_drivers

  df['finished_drivers'] = df18.finished_drivers

  df['d_resurrect_2_month'] = df15.resurrect_2_month
  df['d_resurrect_3_4_month'] = df15.resurrect_3_4_month
  df['d_resurrect_5_12_month'] = df15.resurrect_5_12_month
  
  df['d_resurrect_total']= df15.resurrect_all

  df = df.copy()

  df['driver_churned'] = df17.churned

  df['AvgOnlineHour'] = df12.AvgOnlineHour

  df['avg_util_hour'] = df11.monthly_daily_avg_hour

  df['avg_pinged_drivers'] = df12.avg_pinged_drivers_count
  df['avg_ping_per_driver'] = df12.avg_ping_per_driver

  df['avg_dc_waiting_time'] = df9.avg_dc_waiting_time
  df['dc'] = df9.dc_rate

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
