import os
from datetime import datetime, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv

from utils.helpers import Query, Redash
from utils.slack import SlackBot


def main():
  load_dotenv()

  redash = Redash(key=os.getenv("REDASH_API_KEY"), base_url=os.getenv("REDASH_BASE_URL"))

  dt_format = "%Y-%m-%d"
  local_now = datetime.now(timezone.utc) + timedelta(hours=8)
  start_date = (local_now - timedelta(days=local_now.weekday() + 7)).strftime(dt_format)

  output_date = datetime.strptime(start_date, dt_format).strftime("%d_%b_%Y")

  queries = [[
    Query(4620, params={"week_start_date": start_date}),
    Query(4621, params={"week_start_date": start_date}),
    Query(4628, params={"week_start_date": start_date}),
    Query(4629, params={"week_start_date": start_date}),
    Query(4632, params={"week_start_date": start_date}),
    Query(4633, params={"week_start_date": start_date}),
    Query(4634, params={"week_start_date": start_date}),
    Query(4635, params={"week_start_date": start_date}),
  ]]

  for query_list in queries:
    redash.run_queries(query_list)

  # Fetch results
  df1 = redash.get_result(4620) # HK - Completed trips
  df2 = redash.get_result(4621) # HK - Active Rider Weekly
  df3 = redash.get_result(4628) # HK - Driver FT R C
  df4 = redash.get_result(4629) # HK - Rider FT R C
  df5 = redash.get_result(4632) # HK - Online
  df6 = redash.get_result(4633) # HK - Average Fare
  df7 = redash.get_result(4634) # HK - Promotion Spending Weekly
  df8 = redash.get_result(4635) # HK - Platform Fees Weekly


  # Construct weekly dataFrame
  df = pd.DataFrame()

  df['completed_trips'] = df1.total_completed_trip
  df['daily_completed'] = df1.daily_completed_trip
  df['%_growth'] = None

  df['weekly_active_user'] = df2.active_users
  df['unique_completed_riders'] = df1.rider_weekly_complete
  df['Completed Riders / WAU'] = df.unique_completed_riders / df.weekly_active_user

  df['daily_avg_online_drivers'] = df5.avg_online_drivers

  df['daily_avg_completed_drivers'] = df1.daily_avg_completed_drivers
  df['completed / online'] = df1.daily_avg_completed_drivers / df.daily_avg_online_drivers
  df['driver_weekly_complete'] = df1.driver_weekly_complete
  df['daily_avg / weekly_driver'] = df.daily_avg_completed_drivers / df.driver_weekly_complete

  df['avg_completed_trip_per_rider'] = df.completed_trips / df.unique_completed_riders
  df['avg_completed_trip_per_driver'] = df.daily_completed / df.daily_avg_completed_drivers

  df = df.copy()

  df['new_driver_activated'] = df3.first_timers
  df['resurrect_driver'] = df3.resurrect
  df['%_resurrect'] = None
  df['churn_driver'] = df3.churn
  df['%_churn'] = None
  df['net_new_driver'] = df.new_driver_activated + df.resurrect_driver - df.churn_driver

  df['new_rider_activated'] = df4.first_timers
  df['resurrect_rider'] = df4.resurrect
  df['%_resurrect_rider'] = None
  df['churn_rider'] = df4.churn
  df['%_churn_rider'] = None
  df['net_new_rider'] = df.new_rider_activated + df.resurrect_rider - df.churn_rider

  df['R:D Ratio'] = df.unique_completed_riders / df.driver_weekly_complete
  
  df['blank_1'] = None

  df = df.copy()

  df['average_fare_hkd'] = df6.average_fare
  df['average_fare_usd'] = None
  df['promo_spend_hkd'] = df7.discount
  df['promo_spend_usd'] = None
  df['promotion_trips'] = df7.discount_trips
  df['non_promotion_trips'] = df.completed_trips - df7.discount_trips
  df['promotion/completed'] = df.promotion_trips / df.completed_trips

  df['average_promotion_value'] = None
  df['promo_per_completed_ride'] = None
  df['promo_per_completed_rider'] = None
  df['promo / average_fare'] = None
  df['platform_fee_hkd'] = df8.total_system_fee
  df['platform_fee_usd'] = None
  df['platform_fee_per_completed_ride'] = None

  df['blank_2'] = None

  df = df.copy()

  df['completed_trips_phv'] = df1.phv_completed_trip
  df['phv_complete / total_complete'] = df.completed_trips_phv / df.completed_trips
  df['daily_trips_phv'] = df.completed_trips_phv / 7
  df['completed_users_phv'] = df1.rider_weekly_complete_phv
  df['first_trip_users_phv'] = df4.first_timers_phv
  df['resurrect_users_phv'] = df4.resurrect_phv
  df['churned_users_phv'] = df4.churn_phv
  df['average_fare_hkd_phv'] = df6.phv_average_fare
  df['average_fare_usd_phv'] = None
  df['promo_spend_hkd_phv'] = df7.phv_discount
  df['promo_spend_usd_phv'] = None
  df['promotion_trips_phv'] = df7.phv_discount_trips
  df['average_promotion_value_phv'] = None
  df['promo_per_completed_ride_phv'] = None
  df['promo / average_fare phv'] = None
  df['promo / completed_trips phv'] = df.promotion_trips_phv / df.completed_trips_phv
  df['platform_fee_hkd_phv'] = df8.phv_total_system_fee
  df['platform_fee_usd_phv'] = None
  df['platform_fee_per_completed_ride_phv'] = None

  df['blank_3'] = None

  df = df.copy()

  df['completed_trips_taxi'] = df1.taxi_completed_trip
  df['taxi_complete / total_complete'] = df.completed_trips_taxi / df.completed_trips
  df['daily_trips_taxi'] = df.completed_trips_taxi / 7
  df['completed_users_taxi'] = df1.rider_weekly_complete_taxi
  df['first_trip_users_taxi'] = df4.first_timers_taxi
  df['resurrect_users_taxi'] = df4.resurrect_taxi
  df['churned_users_taxi'] = df4.churn_taxi
  df['average_fare_hkd_taxi'] = df6.taxi_average_fare
  df['average_fare_usd_taxi'] = None
  df['promo_spend_hkd_taxi'] = df7.taxi_discount
  df['promo_spend_usd_taxi'] = None
  df['promotion_trips_taxi'] = df7.taxi_discount_trips
  df['average_promotion_value_taxi'] = None
  df['promo_per_completed_ride_taxi'] = None
  df['promo / average_fare taxi'] = None
  df['promo / completed_trips taxi'] = df.promotion_trips_taxi / df.completed_trips_taxi
  df['platform_fee_hkd_taxi'] = df8.taxi_total_system_fee
  df['platform_fee_usd_taxi'] = None
  df['platform_fee_per_completed_ride_taxi'] = None

  df = df.copy()

  df = df.T
  df.columns = [f"{output_date}"]

  output_file = f"HK_Weekly_{output_date}.csv"

  df.to_csv(output_file)

  slack = SlackBot()
  slack.uploadFile(output_file, 
                   os.getenv("SLACK_CHANNEL"),
                   f"Weekly Report for HK {output_date}")

if __name__ == '__main__':
  main()
