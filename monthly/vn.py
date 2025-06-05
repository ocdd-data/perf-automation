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
  region_id = 2 

  queries = [[
    Query(4562, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4563, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4565, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4566, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4568, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4569, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4570, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4571, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4572, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4574, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4576, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4577, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4578, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4579, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4580, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4581, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4582, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4594, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4598, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4750, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4814, params={"date_range": {"start": start_date, "end": end_date}, "region": region_id}),
    Query(4819, params={"date_range": {"start": start_date, "end": end_date}, "region": region_id})
  ]]

  for query_list in queries:
            redash.run_queries(query_list)

  # Fetch results
  bq1 = redash.get_result(4565) # active riders
  bq2 = redash.get_result(4568) # active drivers
  bq3 = redash.get_result(4570) # drivers ping
  bq4 = redash.get_result(4574) # driver avg hours

  df1 = redash.get_result(4562) # rides
  df2 = redash.get_result(4563) # Rider Unique
  df3 = redash.get_result(4566) # Drivers ETA
  df4 = redash.get_result(4569) # Drivers Signup
  df5 = redash.get_result(4571) # Drivers Daily
  df6 = redash.get_result(4572) # Drivers FT
  df7 = redash.get_result(4576) # Drivers Utilisation
  df8 = redash.get_result(4577) # Rides Cancel
  df9 = redash.get_result(4578) # Drivers CAR
  df10 = redash.get_result(4579) # Rider Signup
  df11 = redash.get_result(4580) # Rider FT
  df12 = redash.get_result(4581) # Rider Daily
  df13 = redash.get_result(4582) # Riders CAR
  df14 = redash.get_result(4594) # Monthly Resurrected Riders
  df15 = redash.get_result(4598) # Monthly Resurrected Drivers
  df16 = redash.get_result(4750) # monthly ps - first try
  df17 = redash.get_result(4814) # median ttm / expire
  df18 = redash.get_result(4819) # book search book logic

  # Construct monthly DataFrame
  df = pd.DataFrame()

  # Performance Sheet

  df['rides'] = df1.completed
  df['demand'] = df1.demand
  df['match_rate'] = df1.matched/df.demand
  df['completion_rate'] = df.rides/df.demand
  df['daily_rides'] = df.rides/DAYS_IN_MONTH
  df['uncompleted'] = df.demand - df.rides
  df['cater_rate'] = df.rides/df2.unique
  df['booked_riders'] = df1.booked_riders
  df['completed_riders'] = df1.completed_riders
  df['first_try_cater_rate'] = df16.first_try_cater_rate
  df['retry_initiation_rate'] = df16.retry_initiation_rate
  df['retry_success_rate'] = df16.retry_success_rate
  df['daily_median_eta'] = df3.median_eta
  df['median_time_to_match_sec'] = df17.median_time_to_match_sec
  df['median_time_to_expire_sec'] = df17.median_time_to_expire_sec

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
  df['resurrect_2_month_driver'] = df15.resurrect_2_month
  df['resurrect_3_4_month_driver'] = df15.resurrect_3_4_month
  df['resurrect_5_12_month_driver'] = df15.resurrect_5_12_month
  df['driver_resurrected'] = df9.resurrected
  df['driver_resurrected_rate'] = None
  df['driver_churned'] = df9.churned
  df['driver_churned_rate'] = None
  df['driver_inflow'] = df9.activated + df9.resurrected - df9.churned

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
  df['rider_unique_search_monthly'] = df18.unique_search_users
  df['rider_unique_book_monthly'] = df18.unique_order_users
  df['rider_unique_complete_monthly'] = df12.completed_monthly_all
  df['rider_unique_open_daily'] = bq1.open_daily
  df['rider_unique_search_daily'] = df18.rider_unique_search_daily_avg
  df['rider_unique_book_daily'] = df18.rider_unique_book_daily_avg
  df['rider_unique_complete_daily'] = df12.completed_daily_all
  df['book_search_ratio_daily'] = df18.book_search_ratio_daily
  df['booking_per_user'] = df.demand/df.rider_unique_book_monthly
  df['complete_per_user'] = df.rides/df.rider_unique_complete_monthly
  df['duplicate_ratio'] = df.demand/df2.unique
  df['rider_waiting_before_cancel'] = df8.avg_waiting_time_cxl_rider
  df['rider_cancellation_rate'] = df1.rider_cancel/df.demand
  df['riders_ft_unique'] = df.rider_ft_all_time/df.rider_unique_complete_monthly
  df['riders_repeated'] = df13.repeated
  df['resurrect_2_month_rider'] = df14.resurrect_2_month
  df['resurrect_3_4_month_rider'] = df14.resurrect_3_4_month
  df['resurrect_5_12_month_rider'] = df14.resurrect_5_12_month
  df['rider_resurrected'] = df13.resurrected
  df['rider_resurrected_rate'] = None
  df['rider_churned'] = df13.churned
  df['rider_churned_rate'] = None
  df['rider_inflow'] = df13.activated + df13.resurrected - df13.churned

  df = df.copy()

  # Start of Car Sheet here

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
  df['resurrect_2_month_driver_phv'] = df15.resurrect_2_month_phv
  df['resurrect_3_4_month_driver_phv'] = df15.resurrect_3_4_month_phv
  df['resurrect_5_12_month_driver_phv'] = df15.resurrect_5_12_month_phv
  df['driver_resurrected_phv'] = df9.resurrected_phv
  df['driver_resurrected_rate_phv'] = None
  df['driver_churned_phv'] = df9.churned_phv
  df['driver_churned_rate_phv'] = None
  df['driver_inflow_phv'] = df9.activated_phv + df9.resurrected_phv - df9.churned_phv

  df = df.copy()

  df['rider_ft_all_time_phv'] = df11.all_time_phv
  df['rider_unique_book_monthly_phv'] = df12.book_monthly_phv
  df['rider_unique_complete_monthly_phv'] = df12.completed_monthly_phv
  df['rider_unique_book_daily_phv'] = df12.book_daily_phv
  df['rider_unique_complete_daily_phv'] = df12.completed_daily_phv
  df['booking_per_user_phv'] = df.demand_phv / df.rider_unique_book_monthly_phv
  df['complete_per_user_phv'] = df.rides_phv / df.rider_unique_complete_monthly_phv
  df['duplicate_ratio_phv'] = df.demand_phv / df2.unique_phv
  df['rider_waiting_before_cancel_phv'] = df8.avg_waiting_time_cxl_phv
  df['rider_cancellation_rate_phv'] = df1.rider_cancel_phv / df.demand_phv
  df['riders_ft_unique_phv'] = df.rider_ft_all_time_phv / df.rider_unique_complete_monthly_phv
  df['riders_repeated_phv'] = df13.repeated_phv
  df['resurrect_2_month_rider_phv'] = df14.resurrect_2_month_phv
  df['resurrect_3_4_month_rider_phv'] = df14.resurrect_3_4_month_phv
  df['resurrect_5_12_month_rider_phv'] = df14.resurrect_5_12_month_phv
  df['rider_resurrected_phv'] = df13.resurrected_phv
  df['rider_resurrected_rate_phv'] = None
  df['rider_churned_phv'] = df13.churned_phv
  df['rider_churned_rate_phv'] = None
  df['rider_inflow_phv'] = df13.activated_phv + df13.resurrected_phv - df13.churned_phv

  df = df.copy()

  # Start of bike sheet here

  df['rides_bike'] = df1.completed_bike
  df['demand_bike'] = df1.demand_bike
  df['match_rate_bike'] = df1.matched_bike/df.demand_bike
  df['completion_rate_bike'] = df.rides_bike/df.demand_bike
  df['daily_rides_bike'] = df.rides_bike/DAYS_IN_MONTH
  df['uncompleted_bike'] = df.demand_bike - df.rides_bike
  df['cater_rate_bike'] = df.rides_bike/df2.unique_bike
  df['booked_riders_bike'] = df1.booked_riders_bike
  df['completed_riders_bike'] = df1.completed_riders_bike
  df['daily_median_eta_bike'] = df3.median_eta_bike

  df['driver_mau_bike'] = bq2.online_bike
  df['completed_driver_bike'] = df1.completed_drivers_bike
  df['total_approved_bike'] = df4.total_approved_bike
  df['driver_online_daily_bike'] = bq2.online_daily_bike
  df['pinged_drivers_daily_bike'] = bq3.pinged_drivers_daily_bike
  df['completed_driver_daily_bike'] = df5.completed_driver_daily_bike
  df['online_mau_bike'] = df.driver_online_daily_bike/df.driver_mau_bike
  df['completed_online_bike'] = df.completed_driver_daily_bike/df.driver_online_daily_bike
  df['online_no_complete_bike'] = df.driver_online_daily_bike-df.completed_driver_daily_bike
  df['ride_per_driver_bike'] = df5.ride_per_driver_bike
  df['driver_sign_up_bike'] = df4.sign_up_bike
  df['driver_sign_up_daily_bike'] = df.driver_sign_up_bike/DAYS_IN_MONTH
  df['driver_ft_all_time_bike'] = df6.all_time_bike
  df['driver_ft_same_month_bike'] = df6.same_month_bike
  df['driver_sign_up_activation_rate_bike'] = df.driver_ft_same_month_bike/df.driver_sign_up_bike
  df['driver_approved_activation_rate_bike'] = df.driver_ft_same_month_bike/df4.same_month_approved_bike
  df['driver_approved_bike'] = df4.approved_bike
  df['driver_same_month_approved_bike'] = df4.same_month_approved_bike
  df['driver_average_online_hours_bike'] = bq4.avg_online_hour_bike
  df['driver_average_utilisation_hours_bike'] = df7.avg_utilisation_hours_bike
  df['ping_per_driver_daily_bike'] = bq3.ping_per_driver_daily_bike
  df['driver_waiting_before_cancel_bike'] = df8.avg_waiting_time_cxl_bike
  df['driver_cancellation_rate_bike'] = df1.driver_cancel_bike/df1.matched_bike*100
  df['drivers_ft_unique_bike'] = df.driver_ft_all_time_bike/df.completed_driver_bike
  df['drivers_repeated_bike'] = df9.repeated_bike
  df['resurrect_2_month_driver_bike'] = df15.resurrect_2_month_bike
  df['resurrect_3_4_month_driver_bike'] = df15.resurrect_3_4_month_bike
  df['resurrect_5_12_month_driver_bike'] = df15.resurrect_5_12_month_bike
  df['driver_resurrected_bike'] = df9.resurrected_bike
  df['driver_resurrected_rate_bike'] = None
  df['driver_churned_bike'] = df9.churned_bike
  df['driver_churned_rate_bike'] = None
  df['driver_inflow_bike'] = df9.activated_bike + df9.resurrected_bike - df9.churned_bike

  df = df.copy()

  df['rider_ft_all_time_bike'] = df11.all_time_bike
  df['rider_unique_book_monthly_bike'] = df12.book_monthly_bike
  df['rider_unique_complete_monthly_bike'] = df12.completed_monthly_bike
  df['rider_unique_book_daily_bike'] = df12.book_daily_bike
  df['rider_unique_complete_daily_bike'] = df12.completed_daily_bike
  df['booking_per_user_bike'] = df.demand_bike / df.rider_unique_book_monthly_bike
  df['complete_per_user_bike'] = df.rides_bike / df.rider_unique_complete_monthly_bike
  df['duplicate_ratio_bike'] = df.demand_bike / df2.unique_bike
  df['rider_waiting_before_cancel_bike'] = df8.avg_waiting_time_cxl_bike
  df['rider_cancellation_rate_bike'] = df1.rider_cancel_bike / df.demand_bike
  df['riders_ft_unique_bike'] = df.rider_ft_all_time_bike / df.rider_unique_complete_monthly_bike
  df['riders_repeated_bike'] = df13.repeated_bike
  df['resurrect_2_month_rider_bike'] = df14.resurrect_2_month_bike
  df['resurrect_3_4_month_rider_bike'] = df14.resurrect_3_4_month_bike
  df['resurrect_5_12_month_rider_bike'] = df14.resurrect_5_12_month_bike
  df['rider_resurrected_bike'] = df13.resurrected_bike
  df['rider_resurrected_rate_bike'] = None
  df['rider_churned_bike'] = df13.churned_bike
  df['rider_churned_rate_bike'] = None
  df['rider_inflow_bike'] = df13.activated_bike + df13.resurrected_bike - df13.churned_bike

  df = df.copy()

  df = df.T
  df.columns = [f"{output_date}"]

  output_file = f"VN_{output_date}.csv"

  df.to_csv(output_file)

  slack = SlackBot()
  slack.uploadFile(output_file, 
                   os.getenv("SLACK_CHANNEL"),
                   f"Monthly Report for VN {output_date}")

if __name__ == '__main__':
  main()
