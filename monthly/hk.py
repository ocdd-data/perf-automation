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
  region_id = 7
  region = "HK"

  queries = [[
    Query(3771, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3772, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3773, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3774, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3775, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3777, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3779, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3780, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3781, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3782, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3783, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3784, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3785, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3786, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3787, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3788, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3790, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4753, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4814, params={"date_range": {"start": start_date, "end": end_date}, "region": region_id}),
    Query(4819, params={"date_range": {"start": start_date, "end": end_date}, "region": region}),
  ]]

  for query_list in queries:
    redash.run_queries(query_list)

  bq1 = redash.get_result(3773)
  bq2 = redash.get_result(3775)
  bq3 = redash.get_result(3779)
  bq4 = redash.get_result(3782)

  df1 = redash.get_result(3771)
  df2 = redash.get_result(3772)
  df3 = redash.get_result(3774)
  df4 = redash.get_result(3777)
  df5 = redash.get_result(3780)
  df6 = redash.get_result(3781)
  df7 = redash.get_result(3783)
  df8 = redash.get_result(3784)
  df9 = redash.get_result(3785)
  df10 = redash.get_result(3786)
  df11 = redash.get_result(3787)
  df12 = redash.get_result(3788)
  df13 = redash.get_result(3790)
  df14 = redash.get_result(4753)
  df15 = redash.get_result(4814)
  df16 = redash.get_result(4819)

  df = pd.DataFrame()

  df['rides'] = df1.completed
  df['demand'] = df1.demand
  df['match_rate'] = df1.matched/df.demand
  df['completion_rate'] = df.rides/df.demand
  df['daily_rides'] = df.rides/DAYS_IN_MONTH
  df['uncompleted'] = df.demand - df.rides
  df['cater_rate'] = df.rides/df2.unique
  df['first_try_cater_rate'] = df14.first_try_cater_rate
  df['retry_initiation_rate'] = df14.retry_initiation_rate
  df['retry_success_rate'] = df14.retry_success_rate
  df['booked_riders'] = df1.booked_riders
  df['completed_riders'] = df1.completed_riders
  df['daily_median_eta'] = df3.median_eta
  df['median_time_to_match_sec'] = df15.median_time_to_match_sec
  df['median_time_to_expire_sec'] = df15.median_time_to_expire_sec


  df['rides_phv'] = df1.completed_phv
  df['demand_phv'] = df1.demand_phv
  df['match_rate_phv'] = df1.matched_phv/df.demand_phv
  df['completion_rate_phv'] = df.rides_phv/df.demand_phv
  df['daily_rides_phv'] = df.rides_phv/DAYS_IN_MONTH
  df['uncompleted_phv'] = df.demand_phv - df.rides_phv
  df['cater_rate_phv'] = df.rides_phv/df2.unique_phv
  df['booked_riders_phv'] = df1.booked_riders_phv
  df['completed_riders_phv'] = df1.completed_riders_phv
  df['daily_median_eta_phv'] = df3.median_eta_phv


  df['rides_taxi'] = df1.completed_taxi
  df['demand_taxi'] = df1.demand_taxi
  df['match_rate_taxi'] = df1.matched_taxi/df.demand_taxi
  df['completion_rate_taxi'] = df.rides_taxi/df.demand_taxi
  df['daily_rides_taxi'] = df.rides_taxi/DAYS_IN_MONTH
  df['uncompleted_taxi'] = df.demand_taxi - df.rides_taxi
  df['cater_rate_taxi'] = df.rides_taxi/df2.unique_taxi
  df['booked_riders_taxi'] = df1.booked_riders_taxi
  df['completed_riders_taxi'] = df1.completed_riders_taxi
  df['daily_median_eta_taxi'] = df3.median_eta_taxi


  df['driver_mau'] = bq2.online
  df['completed_driver'] = df1.completed_drivers
  df['total_approved'] = df4.total_approved
  df['driver_online_daily'] = bq2.online_daily
  df['pinged_drivers_daily'] = bq3.pinged_drivers_daily
  df['completed_driver_daily'] = df5.completed_driver_daily
  df['online_mau'] = df.driver_online_daily/df.driver_mau
  df['completed_online'] = df.completed_driver_daily/df.driver_online_daily
  df['online_no_complete'] = df.driver_online_daily-df.completed_driver_daily
  df['ride_per_driver'] = df5.ride_per_driver
  df['driver_downloads'] = None
  df['driver_sign_up'] = df4.sign_up
  df['driver_sign_up_daily'] = df.driver_sign_up/DAYS_IN_MONTH
  df['driver_ft_all_time'] = df6.all_time
  df['driver_ft_same_month'] = df6.same_month
  df['driver_sign_up_activation_rate'] = df.driver_ft_same_month/df.driver_sign_up
  df['driver_approved_activation_rate'] = df.driver_ft_same_month/df4.same_month_approved
  df['driver_approved'] = df4.approved
  df['driver_same_month_approved'] = df4.same_month_approved
  df['driver_average_online_hours'] = bq4.avg_online_hour
  df['driver_average_utilisation_hours'] = df7.avg_utilisation_hours
  df['ping_per_driver_daily'] = bq3.ping_per_driver_daily
  df['driver_waiting_before_cancel'] = df8.avg_waiting_time_cxl
  df['driver_cancellation_rate'] = df1.driver_cancel/df1.matched*100
  df['drivers_ft_unique'] = df.driver_ft_all_time/df.completed_driver
  df['drivers_repeated'] = df9.repeated
  df['driver_resurrected'] = df9.resurrected
  df['driver_resurrected_rate'] = None
  df['driver_churned'] = df9.churned
  df['driver_churned_rate'] = None
  df['driver_inflow'] = df9.activated + df9.resurrected - df9.churned

  df = df.copy()

  df['driver_mau_phv'] = bq2.online_phv
  df['completed_driver_phv'] = df1.completed_drivers_phv
  df['total_approved_phv'] = df4.total_approved_phv
  df['driver_online_daily_phv'] = bq2.online_daily_phv
  df['pinged_drivers_daily_phv'] = bq3.pinged_drivers_daily_phv
  df['completed_driver_daily_phv'] = df5.completed_driver_daily_phv
  df['online_mau_phv'] = df.driver_online_daily_phv/df.driver_mau_phv
  df['completed_online_phv'] = df.completed_driver_daily_phv/df.driver_online_daily_phv
  df['online_no_complete_phv'] = df.driver_online_daily_phv-df.completed_driver_daily_phv
  df['ride_per_driver_phv'] = df5.ride_per_driver_phv
  df['driver_sign_up_phv'] = df4.sign_up_phv
  df['driver_sign_up_daily_phv'] = df.driver_sign_up_phv/DAYS_IN_MONTH
  df['driver_ft_all_time_phv'] = df6.all_time_phv
  df['driver_ft_same_month_phv'] = df6.same_month_phv
  df['driver_sign_up_activation_rate_phv'] = df.driver_ft_same_month_phv/df.driver_sign_up_phv
  df['driver_approved_activation_rate_phv'] = df.driver_ft_same_month_phv/df4.same_month_approved_phv
  df['driver_approved_phv'] = df4.approved_phv
  df['driver_same_month_approved_phv'] = df4.same_month_approved_phv
  df['driver_average_online_hours_phv'] = bq4.avg_online_hour_phv
  df['driver_average_utilisation_hours_phv'] = df7.avg_utilisation_hours_phv
  df['ping_per_driver_daily_phv'] = bq3.ping_per_driver_daily_phv
  df['driver_waiting_before_cancel_phv'] = df8.avg_waiting_time_cxl_phv
  df['driver_cancellation_rate_phv'] = df1.driver_cancel_phv/df1.matched_phv*100
  df['drivers_ft_unique_phv'] = df.driver_ft_all_time_phv/df.completed_driver_phv
  df['drivers_repeated_phv'] = df9.repeated_phv
  df['driver_resurrected_phv'] = df9.resurrected_phv
  df['driver_resurrected_rate_phv'] = None
  df['driver_churned_phv'] = df9.churned_phv
  df['driver_churned_rate_phv'] = None
  df['driver_inflow_phv'] = df9.activated_phv + df9.resurrected_phv - df9.churned_phv

  df = df.copy()

  df['driver_mau_taxi'] = bq2.online_taxi
  df['completed_driver_taxi'] = df1.completed_drivers_taxi
  df['total_approved_taxi'] = df4.total_approved_taxi
  df['driver_online_daily_taxi'] = bq2.online_daily_taxi
  df['pinged_drivers_daily_taxi'] = bq3.pinged_drivers_daily_taxi
  df['completed_driver_daily_taxi'] = df5.completed_driver_daily_taxi
  df['online_mau_taxi'] = df.driver_online_daily_taxi/df.driver_mau_taxi
  df['completed_online_taxi'] = df.completed_driver_daily_taxi/df.driver_online_daily_taxi
  df['online_no_complete_taxi'] = df.driver_online_daily_taxi-df.completed_driver_daily_taxi
  df['ride_per_driver_taxi'] = df5.ride_per_driver_taxi
  df['driver_sign_up_taxi'] = df4.sign_up_taxi
  df['driver_sign_up_daily_taxi'] = df.driver_sign_up_taxi/DAYS_IN_MONTH
  df['driver_ft_all_time_taxi'] = df6.all_time_taxi
  df['driver_ft_same_month_taxi'] = df6.same_month_taxi
  df['driver_sign_up_activation_rate_taxi'] = df.driver_ft_same_month_taxi/df.driver_sign_up_taxi
  df['driver_approved_activation_rate_taxi'] = df.driver_ft_same_month_taxi/df4.same_month_approved_taxi
  df['driver_approved_taxi'] = df4.approved_taxi
  df['driver_same_month_approved_taxi'] = df4.same_month_approved_taxi
  df['driver_average_online_hours_taxi'] = bq4.avg_online_hour_taxi
  df['driver_average_utilisation_hours_taxi'] = df7.avg_utilisation_hours_taxi
  df['ping_per_driver_daily_taxi'] = bq3.ping_per_driver_daily_taxi
  df['driver_waiting_before_cancel_taxi'] = df8.avg_waiting_time_cxl_taxi
  df['driver_cancellation_rate_taxi'] = df1.driver_cancel_taxi/df1.matched_taxi*100
  df['drivers_ft_unique_taxi'] = df.driver_ft_all_time_taxi/df.completed_driver_taxi
  df['drivers_repeated_taxi'] = df9.repeated_taxi
  df['driver_resurrected_taxi'] = df9.resurrected_taxi
  df['driver_resurrected_rate_taxi'] = None
  df['driver_churned_taxi'] = df9.churned_taxi
  df['driver_churned_rate_taxi'] = None
  df['driver_inflow_taxi'] = df9.activated_taxi + df9.resurrected_taxi - df9.churned_taxi

  df = df.copy()

  df['rider_mau'] = bq1.active_users
  df['rider_mau_demand'] = df.demand/df.rider_mau
  df['rider_mau_rides'] = df.rides/df.rider_mau
  df['r_d_ratio'] = df.rider_mau/df.driver_mau
  df['rider_downloads'] = None
  df['rider_signup'] = df10.rider_signup
  df['rider_signup_daily'] = df.rider_signup/DAYS_IN_MONTH
  df['rider_ft_all_time'] = df11.all_time
  df['rider_ft_same_month'] = df11.same_month
  df['rider_same_month_activation'] = df.rider_ft_same_month/df.rider_signup
  df['rider_unique_open_monthly'] = bq1.active_users
  df['rider_unique_search_monthly'] = df16.unique_search_users
  df['rider_unique_book_monthly'] = df16.unique_order_users
  df['rider_unique_complete_monthly'] = df12.completed_monthly
  df['rider_unique_open_daily'] = bq1.open_daily
  df['rider_unique_search_daily'] = df16.rider_unique_search_daily_avg
  df['rider_unique_book_daily'] = df16.rider_unique_book_daily_avg
  df['rider_unique_complete_daily'] = df12.completed_daily
  df['book_search_ratio_daily'] = df16.book_search_ratio_daily
  df['booking_per_user'] = df.demand/df.rider_unique_book_monthly
  df['complete_per_user'] = df.rides/df.rider_unique_complete_monthly
  df['duplicate_ratio'] = df.demand/df2.unique
  df['rider_waiting_before_cancel'] = df8.avg_waiting_time_cxl_rider
  df['rider_cancellation_rate'] = df1.rider_cancel/df.demand
  df['riders_ft_unique'] = df.rider_ft_all_time/df.rider_unique_complete_monthly
  df['riders_repeated'] = df13.repeated
  df['rider_resurrected'] = df13.resurrected
  df['rider_resurrected_rate'] = None
  df['rider_churned'] = df13.churned
  df['rider_churned_rate'] = None
  df['rider_inflow'] = df13.activated + df13.resurrected - df13.churned

  df = df.T
  df.columns = [f"{output_date}"]

  output_file = f"HK_{output_date}.csv"

  df.to_csv(output_file)

  slack = SlackBot()
  slack.uploadFile(output_file, 
                   os.getenv("SLACK_CHANNEL"),
                   f"Monthly Report for HK {output_date}")


if __name__ == '__main__':
  main()
