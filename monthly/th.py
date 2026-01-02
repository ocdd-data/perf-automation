import os
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from utils.constants import IDS, REGIONS, TIMEZONES
from utils.helpers import Query, Redash
from utils.slack import SlackBot


def main():
  load_dotenv()

  redash = Redash(key=os.getenv("REDASH_API_KEY"), base_url=os.getenv("REDASH_BASE_URL"))

  # Get last date of previous month
  dt_format = "%Y-%m-%d"
  date = (datetime.today().replace(day=1) - timedelta(days=1)).strftime(dt_format)
  start_date = datetime.strptime(date, dt_format).replace(day=1).strftime(dt_format)
  end_date = date  # already the last day of previous month

  DAYS_IN_MONTH = int(date.split("-")[2])

  output_date = datetime.strptime(date, dt_format).strftime("%b_%Y")

  region = 'TH'

  timezone = TIMEZONES[region]
  region_str = REGIONS[region]
  region_id = IDS[region]

  queries = [
      [
          Query(2667, params={"region": "Thailand", "timezone": timezone, "date": date}),
          Query(2668, params={"region": "Thailand", "timezone": timezone, "date": date}),
          Query(2669, params={"region": region_id, "timezone": timezone, "date": date}),
          Query(2670, params={"region": region_id, "timezone": timezone, "date": date}),
          Query(2671, params={"region": region_id, "timezone": timezone, "date": date}),
          Query(3106, params={"date": date}),
          Query(3107, params={"region": region, "timezone": timezone, "date": date}),
          Query(3108, params={"region": region, "timezone": timezone, "date": date}),
          Query(3109, params={"region": region, "timezone": timezone, "date": date}),
          Query(3110, params={"region": region, "timezone": timezone, "date": date}),
          Query(3111, params={"region": region, "timezone": timezone, "date": date}),
          Query(3112, params={"region": region, "timezone": timezone, "date": date}),
          Query(3113, params={"region": region, "timezone": timezone, "date": date}),
          Query(3114, params={"region": region, "timezone": timezone, "date": date}),
          Query(3115, params={"region": region, "timezone": timezone, "date": date}),
          Query(3116, params={"region": region, "timezone": timezone, "date": date}),
          Query(3117, params={"region": region, "timezone": timezone, "date": date}),
          Query(3118, params={"region": region, "timezone": timezone, "date": date}),
          Query(3119, params={"region": region, "timezone": timezone, "date": date}),
          Query(3120, params={"region": region, "timezone": timezone, "date": date}),
          Query(3121, params={"region": region, "timezone": timezone, "date": date}),
          Query(3122, params={"region": region, "timezone": timezone, "date": date}),
          Query(3123, params={"region": region, "timezone": timezone, "date": date}),
          Query(3124, params={"region": region_id, "timezone": timezone, "date": date}),
          Query(3125, params={"region": region_id, "timezone": timezone, "date": date}),
          Query(3126, params={"region": region_id, "timezone": timezone, "date": date}),
          Query(3127, params={"region": region, "timezone": timezone, "date": date}),
          Query(3128, params={"region": region, "timezone": timezone, "date": date}),
          Query(3129, params={"region": region_id, "timezone": timezone, "date": date}),
          Query(3130, params={"region": region_id, "timezone": timezone, "date": date}),
          Query(3131, params={"region": region_id, "timezone": timezone, "date": date}),
          Query(3132, params={"region": region, "timezone": timezone, "date": date}),
          Query(3133, params={"region": region, "timezone": timezone, "date": date}),
          Query(4754, params={"date": date}),
          Query(4814, params={"date_range": {"start": start_date, "end": end_date}, "region": region_id}),
          Query(4411, params={"date_range": {"start": start_date, "end": end_date}, "region": region_id}),
          Query(4819, params={"date_range": {"start": start_date, "end": end_date}, "region": region}),
          Query(4981, params={"date": date}),
          Query(4985, params={"date_range": {"start": start_date, "end": end_date}}),
      ]
  ]


  for query_list in queries:
    redash.run_queries(query_list)

  bq1 = redash.get_result(2667)
  bq2 = redash.get_result(2668)
  bq3 = redash.get_result(2669)
  bq4 = redash.get_result(2670)
  bq5 = redash.get_result(2671)
  bq6 = redash.get_result(3124)
  bq7 = redash.get_result(3125)
  bq8 = redash.get_result(3126)
  bq9 = redash.get_result(3129)
  bq10 = redash.get_result(3130)
  bq11 = redash.get_result(3131)

  df1 = redash.get_result(3106)
  df2 = redash.get_result(3107)
  df3 = redash.get_result(3108)
  df4 = redash.get_result(3109)
  df5 = redash.get_result(3110)
  df6 = redash.get_result(3111)
  df7 = redash.get_result(3112)
  df8 = redash.get_result(3113)
  df9 = redash.get_result(3114)
  df10 = redash.get_result(3115)
  df11 = redash.get_result(3116)
  df12 = redash.get_result(3117)
  df13 = redash.get_result(3118)
  df14 = redash.get_result(3119)
  df15 = redash.get_result(3120)
  df16 = redash.get_result(3121)
  df17 = redash.get_result(3122)
  df18 = redash.get_result(3123)
  df19 = redash.get_result(3127)
  df20 = redash.get_result(3128)
  df21 = redash.get_result(3132)
  df22 = redash.get_result(3133)
  df23 = redash.get_result(4754)
  df24 = redash.get_result(4814)
  df25 = redash.get_result(4411)
  df26 = redash.get_result(4819)
  df27 = redash.get_result(4981)
  df28 = redash.get_result(4985)

  df = pd.DataFrame()

  # totals

  df['rides'] = df1.completed
  df['demand'] = df1.demand
  df['match_rate'] = df1.matched/df.demand
  df['completion_rate'] = df.rides/df.demand
  df['daily_rides'] = df.rides/DAYS_IN_MONTH
  df['uncompleted'] = df.demand - df.rides
  df['cater_rate'] = df.rides/df27.unique
  df['first_try_cater_rate'] = df23.first_try_cater_rate
  df['retry_initiation_rate'] = df23.retry_initiation_rate
  df['retry_success_rate'] = df23.retry_success_rate
  df['daily_rides'] = df.rides/DAYS_IN_MONTH
  df['booked_riders'] = df1.booked_riders
  df['completed_riders'] = df1.completed_riders
  df['daily_median_eta'] = df25.avg_daily_median_eta
  df['median_time_to_match_sec'] = df24.median_time_to_match_sec
  df['median_time_to_expire_sec'] = df24.median_time_to_expire_sec

  df = df.copy()

  # broken down by ride type - 4w vs bike

  df['rides_4w'] = df1._4w_completed
  df['demand_4w'] = df1._4w_demand
  df['match_rate_4w'] = df1._4w_matched/df.demand_4w
  df['completion_rate_4w'] = df.rides_4w/df.demand_4w
  df['daily_rides_4w'] = df.rides_4w/DAYS_IN_MONTH
  df['uncompleted_4w'] = df.demand_4w - df.rides_4w
  df['cater_rate_4w'] = df.rides_4w/df27.unique_4w
  # df['booked_riders_4w'] = df1.booked_riders_4w
  # df['completed_riders_4w'] = df1._4w_completed_riders
  df['daily_median_eta_4w'] = df28.median_eta_4w
  df['phv_completed_drivers'] = df1._4w_completed_drivers
  df['phv_approved_drivers'] = df3.approved_4w

  df['rides_bike'] = df1.bike_completed
  df['demand_bike'] = df1.bike_demand
  df['match_rate_bike'] = df1.bike_matched/df.demand_bike
  df['completion_rate_bike'] = df.rides_bike/df.demand_bike
  df['daily_rides_bike'] = df.rides_bike/DAYS_IN_MONTH
  df['uncompleted_bike'] = df1.bike_demand - df.rides_bike
  df['cater_rate_bike'] = df.rides_bike/df27.unique_bike
  # df['booked_riders_bike'] = df1.booked_riders_bike
  # df['completed_riders_bike'] = df1.bike_completed_riders
  df['daily_median_eta_bike'] = df28.median_eta_bike
  df['bike_completed_drivers'] = df1.bike_completed_drivers
  df['bike_approved_drivers'] = df3.approved_bike

  df = df.copy()

  # start of driver section here

  df['driver_mau'] = bq3.online_driver_count
  df['completed_driver'] = df1.completed_drivers
  df['total_approved_drivers'] = df3.total_approved
  df['driver_online_daily'] = bq3.online_driver_daily
  df['pinged_drivers_daily'] = bq4.pinged_drivers_daily
  df['completed_driver_daily'] = df10.completed_driver_daily
  df['online_mau'] = df.driver_online_daily/df.driver_mau
  df['completed_online'] = df.completed_driver_daily/df.driver_online_daily
  df['online_no_complete'] = df.driver_online_daily-df.completed_driver_daily
  df['ride_per_driver'] = df10.ride_per_driver
  df['driver_downloads'] = None
  df['driver_sign_up'] = df11.driver_sign_up
  df['driver_sign_up_daily'] = df.driver_sign_up/DAYS_IN_MONTH
  df['driver_ft_all_time'] = df12.all_time
  df['driver_ft_same_month'] = df12.same_month
  df['driver_sign_up_activation_rate'] = df.driver_ft_same_month/df.driver_sign_up
  df['driver_approved_activation_rate'] = df.driver_ft_same_month/df11.driver_same_month_approved

  df = df.copy()

  df['driver_approved'] = df11.driver_approved
  df['driver_same_month_approved'] = df11.driver_same_month_approved
  df['driver_average_online_hours'] = bq5.avg_online_hour
  df['driver_average_utilisation_hours'] = df13.avg_utilisation_hours
  df['ping_per_driver_daily'] = bq4.ping_per_driver_daily
  df['driver_waiting_before_cancel'] = df9.avg_waiting_time_driver_cxl
  df['driver_cancellation_rate'] = df1.driver_cancel/df1.matched*100
  df['drivers_ft_unique'] = df.driver_ft_all_time/df.completed_driver
  df['driver_resurrected'] = df16.resurrected
  df['driver_resurrected_%'] = None
  df['driver_churned'] = df16.churned
  df['driver_churned_%'] = None
  df['driver_inflow'] = df16.activated + df16.resurrected - df16.churned

  # start of driver phv and taxi

  df = df.copy()

  # ------------- 4W METRICS -------------
  df['_4w_mau'] = bq9.online_4w_count
  df['completed_4w'] = df1._4w_completed_drivers
  df['_4w_approved'] = df11._4w_approved

  df['_4w_online_daily'] = bq9.online_4w_daily
  df['pinged_4w_daily'] = bq10.pinged_4w_daily
  df['completed_4w_daily'] = df19.completed_4w_daily

  df['_4w_online_mau'] = df._4w_online_daily / df._4w_mau
  df['_4w_completed_online'] = df.completed_4w_daily / df._4w_online_daily
  df['_4w_online_no_complete'] = df._4w_online_daily - df.completed_4w_daily

  df['ride_per_4w'] = df19.ride_per_4w
  df['_4w_sign_up'] = df11._4w_sign_up
  df['_4w_sign_up_daily'] = df._4w_sign_up/DAYS_IN_MONTH
  df['_4w_ft_all_time'] = df12._4w_all_time
  df['_4w_ft_same_month'] = df12._4w_same_month
  df['_4w_sign_up_activation_rate'] = df._4w_ft_same_month / df._4w_sign_up
  df['_4w_approved_activation_rate'] = df._4w_ft_same_month / df11._4w_same_month_approved

  df['_4w_same_month_approved'] = df11._4w_same_month_approved

  df['_4w_average_online_hours'] = bq11.avg_online_hour
  df['_4w_average_utilisation_hours'] = df20.avg_utilisation_hours
  df['ping_per_4w_daily'] = bq10.ping_per_4w_daily

  df['_4w_waiting_before_cancel'] = df9.avg_waiting_time_4w_cxl
  df['_4w_cancellation_rate'] = df1._4w_cancel / df1._4w_matched * 100

  df['_4w_ft_unique'] = df._4w_ft_all_time / df.completed_4w
  df['_4w_repeated'] = df16.repeated_4w
  df['_4w_resurrected'] = df16.resurrected_4w
  df['_4w_resurrected_%'] = None  # added placeholder
  df['_4w_churned'] = df16.churned_4w
  df['_4w_churned_%'] = None  # added placeholder
  df['_4w_inflow'] = df16.activated_4w + df16.resurrected_4w - df16.churned_4w

  df = df.copy()

  # ------------- BIKE METRICS -------------
  df['bike_mau'] = bq6.online_bike_count
  df['completed_bike'] = df1.bike_completed_riders
  df['bike_approved'] = df11.bike_approved

  df['bike_online_daily'] = bq6.online_bike_daily
  df['pinged_bike_daily'] = bq7.pinged_bike_daily
  df['completed_bike_daily'] = df17.completed_bike_daily

  df['bike_online_mau'] = df.bike_online_daily / df.bike_mau
  df['bike_completed_online'] = df.completed_bike_daily / df.bike_online_daily
  df['bike_online_no_complete'] = df.bike_online_daily - df.completed_bike_daily

  df['ride_per_bike'] = df17.ride_per_bike
  df['bike_sign_up'] = df11.bike_sign_up
  df['bike_sign_up_daily'] = df.bike_sign_up/DAYS_IN_MONTH

  df['bike_ft_all_time'] = df12.bike_all_time
  df['bike_ft_same_month'] = df12.bike_same_month
  df['bike_sign_up_activation_rate'] = df.bike_ft_same_month / df.bike_sign_up
  df['bike_approved_activation_rate'] = df.bike_ft_same_month / df11.bike_same_month_approved

  df['bike_same_month_approved'] = df11.bike_same_month_approved

  df['bike_average_online_hours'] = bq8.avg_online_hour
  df['bike_average_utilisation_hours'] = df18.avg_utilisation_hours
  df['ping_per_bike_daily'] = bq7.ping_per_bike_daily

  df['bike_waiting_before_cancel'] = df9.avg_waiting_time_bike_cxl
  df['bike_cancellation_rate'] = (df1.bike_cancel / df1.bike_matched) * 100

  df['bike_ft_unique'] = df.bike_ft_all_time / df.completed_bike
  df['bike_repeated'] = df16.repeated_bike
  df['bike_resurrected'] = df16.resurrected_bike
  df['bike_resurrected_%'] = None  # added placeholder
  df['bike_churned'] = df16.churned_bike
  df['bike_churned_%'] = None  # added placeholder
  df['bike_inflow'] = df16.activated_bike + df16.resurrected_bike - df16.churned_bike

  df = df.copy()

  # ---------------- RIDER METRICS ----------------
  df['rider_mau'] = bq1.active_users
  df['rider_mau_demand'] = df.demand / df.rider_mau
  df['rider_mau_rides'] = df.rides / df.rider_mau
  df['r_d_ratio'] = df.rider_mau / df.driver_mau  # MAUR:MAUD Ratio

  df['rider_downloads'] = None

  df['rider_signup'] = df5.rider_signup
  df['rider_signup_daily'] = df.rider_signup / DAYS_IN_MONTH

  df['rider_ft_all_time'] = df6.all_time
  df['rider_ft_same_month'] = df6.same_month
  df['rider_same_month_activation'] = df.rider_ft_same_month / df.rider_signup

  df['rider_unique_open_monthly'] = bq2.open_monthly
  df['rider_unique_search_monthly'] = df26.unique_search_users
  df['rider_unique_book_monthly'] = df26.unique_order_users
  df['rider_unique_complete_monthly'] = df7.completed_monthly

  df['rider_unique_open_daily'] = bq2.open_daily
  df['rider_unique_search_daily'] = df26.rider_unique_search_daily_avg
  df['rider_unique_book_daily'] = df26.rider_unique_book_daily_avg
  df['rider_unique_complete_daily'] = df7.completed_daily

  df['book_search_ratio_daily'] = df26.book_search_ratio_daily
  df['booking_per_user'] = df.demand / df.rider_unique_book_monthly
  df['complete_per_user'] = df.rides / df.rider_unique_complete_monthly

  df['duplicate_ratio'] = df.demand / df8.unique

  df['rider_waiting_before_cancel'] = df9.avg_waiting_time_rider_cxl
  df['rider_cancellation_rate'] = df1.rider_cancel / df.demand

  df['riders_ft_unique'] = df.rider_ft_all_time / df.rider_unique_complete_monthly
  df['riders_repeated'] = df15.repeated
  df['rider_resurrected'] = df15.resurrected
  df['rider_resurrected_%'] = None  # Placeholder – needs logic if desired
  df['rider_churned'] = df15.churned
  df['rider_churned_%'] = None  # Placeholder – needs logic if desired
  df['rider_inflow'] = df15.activated + df15.resurrected - df15.churned

  # df['cater_rate'] = df.rides / df8.unique
  # df['average_eta'] = df14.daily_median_eta

  df = df.T
  df.columns = [f"{output_date}"]

  output_file = f"{region}_{output_date}.csv"

  df.to_csv(output_file)

  slack = SlackBot()
  slack.uploadFile(output_file,
                   os.getenv("SLACK_CHANNEL"),
                   f"Monthly Report for {region} {output_date}")

if __name__ == '__main__':
  main()
