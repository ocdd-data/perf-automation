import os
from datetime import datetime, timedelta, timezone

import pandas as pd
from dotenv import load_dotenv

import os
import sys

print("=== DEBUG INFO ===")
print(f"Current working directory: {os.getcwd()}")
print(f"Script location: {os.path.abspath(__file__)}")

# Get the script directory
script_dir = os.path.dirname(os.path.abspath(__file__))
print(f"Script directory: {script_dir}")

# Go up one level to perf-automation
parent_dir = os.path.dirname(script_dir)
print(f"Parent directory (should be perf-automation): {parent_dir}")

# Check if utils exists
utils_path = os.path.join(parent_dir, 'utils')
print(f"Utils path: {utils_path}")
print(f"Utils exists: {os.path.exists(utils_path)}")
print(f"Utils is directory: {os.path.isdir(utils_path)}")

# List contents of utils
if os.path.exists(utils_path):
    print(f"Contents of utils: {os.listdir(utils_path)}")

print(f"\nCurrent sys.path:")
for p in sys.path:
    print(f"  {p}")

print("=== END DEBUG ===")

# Now add the correct path
sys.path.insert(0, parent_dir)

# Try to import
try:
    from utils.helpers import Query, Redash
    from utils.slack import SlackBot
    print("SUCCESS: Imported utils module!")
except ImportError as e:
    print(f"ERROR: {e}")
    print("Trying alternative import...")
    
    # Try direct import
    import importlib.util
    spec = importlib.util.spec_from_file_location("helpers", os.path.join(utils_path, "helpers.py"))
    if spec:
        helpers = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(helpers)
        Query = helpers.Query
        Redash = helpers.Redash
        print("SUCCESS: Imported helpers directly!")
    else:
        print("FAILED: Could not import helpers.py")
        sys.exit(1)

from utils.helpers import Query, Redash
from utils.slack import SlackBot


def process_city_data(redash, start_date, city):
    """Process data for a specific city"""
    
    # Run queries with city parameter
    queries = [[
        Query(4607, params={"week_start_date": start_date, "city": city}),
        Query(4611, params={"week_start_date": start_date, "city": city}),
        Query(4612, params={"week_start_date": start_date, "city": city}),
        Query(4613, params={"week_start_date": start_date, "city": city}),
        Query(4614, params={"week_start_date": start_date, "city": city}),
        Query(4615, params={"week_start_date": start_date, "city": city}),
        Query(4616, params={"week_start_date": start_date, "city": city}),
        Query(4617, params={"week_start_date": start_date, "city": city}),
        Query(5212, params={"week_start_date": start_date, "city": city}),
    ]]

    for query_list in queries:
        redash.run_queries(query_list)

    # Fetch results
    df1 = redash.get_result(4607) # VN - Completed trips
    df2 = redash.get_result(4611) # VN - Active Rider Weekly
    df3 = redash.get_result(4612) # VN - Driver FT R C
    df4 = redash.get_result(4613) # VN - Rider FT R C
    df5 = redash.get_result(4614) # VN - Online
    df6 = redash.get_result(4615) # VN - Average Fare
    df7 = redash.get_result(4616) # VN - Promotion Spending Weekly
    df8 = redash.get_result(4617) # VN - Platform Fees Weekly
    df9 = redash.get_result(5212) # VN - Payment Method Weekly

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

    df['average_fare_vnd'] = df6.average_fare
    df['average_fare_usd'] = None
    df['promo_spend_vnd'] = df7.discount
    df['promo_spend_usd'] = None
    df['promotion_trips'] = df7.discount_trips
    df['non_promotion_trips'] = df.completed_trips - df7.discount_trips
    df['promotion/completed'] = df.promotion_trips / df.completed_trips

    df['average_promotion_value'] = None
    df['promo_per_completed_ride'] = None
    df['promo_per_completed_rider'] = None
    df['promo / average_fare'] = None
    df['platform_fee_vnd'] = df8.total_system_fee
    df['platform_fee_usd'] = None
    df['platform_fee_per_completed_ride'] = None

    df['blank_2'] = None

    df = df.copy()

    df['completed_trips_car'] = df1.car_completed_trip
    df['car_complete / total_complete'] = df.completed_trips_car / df.completed_trips
    df['daily_trips_car'] = df.completed_trips_car / 7
    df['completed_users_car'] = df1.rider_weekly_complete_car
    df['first_trip_users_car'] = df4.first_timers_car
    df['resurrect_users_car'] = df4.resurrect_car
    df['churned_users_car'] = df4.churn_car
    df['average_fare_vnd_car'] = df6.car_average_fare
    df['average_fare_usd_car'] = None
    df['promo_spend_vnd_car'] = df7.car_discount
    df['promo_spend_usd_car'] = None
    df['promotion_trips_car'] = df7.car_discount_trips
    df['average_promotion_value_car'] = None
    df['promo_per_completed_ride_car'] = None
    df['promo / average_fare car'] = None
    df['promo / completed_trips car'] = df.promotion_trips_car / df.completed_trips_car
    df['platform_fee_vnd_car'] = df8.car_total_system_fee
    df['platform_fee_usd_car'] = None
    df['platform_fee_per_completed_ride_car'] = None

    df['blank_3'] = None

    df = df.copy()

    df['completed_trips_bike'] = df1.bike_completed_trip
    df['bike_complete / total_complete'] = df.completed_trips_bike / df.completed_trips
    df['daily_trips_bike'] = df.completed_trips_bike / 7
    df['completed_users_bike'] = df1.rider_weekly_complete_bike
    df['first_trip_users_bike'] = df4.first_timers_bike
    df['resurrect_users_bike'] = df4.resurrect_bike
    df['churned_users_bike'] = df4.churn_bike
    df['average_fare_vnd_bike'] = df6.bike_average_fare
    df['average_fare_usd_bike'] = None
    df['promo_spend_vnd_bike'] = df7.bike_discount
    df['promo_spend_usd_bike'] = None
    df['promotion_trips_bike'] = df7.bike_discount_trips
    df['average_promotion_value_bike'] = None
    df['promo_per_completed_ride_bike'] = None
    df['promo / average_fare bike'] = None
    df['promo / completed_trips bike'] = df.promotion_trips_bike / df.completed_trips_bike
    df['platform_fee_vnd_bike'] = df8.bike_total_system_fee
    df['platform_fee_usd_bike'] = None
    df['platform_fee_per_completed_ride_bike'] = None

    df = df.copy()

    df = df.T
    df.columns = [f"{start_date}"]
    
    # Payment Method sheet
    pm = pd.DataFrame()
    
    pm['total_cash_trips'] = df9.cash_trips
    pm['total_cash_gmv'] = df9.cash_gmv
    pm['total_momo_trips'] = df9.momo_trips
    pm['total_momo_gmv'] = df9.momo_gmv
    pm['total_card_trips'] = df9.card_trips
    pm['total_card_gmv'] = df9.card_gmv
    pm['bike_cash_trips'] = df9.bike_cash_trips
    pm['bike_cash_gmv'] = df9.bike_cash_gmv
    pm['bike_momo_trips'] = df9.bike_momo_trips
    pm['bike_momo_gmv'] = df9.bike_momo_gmv
    pm['bike_card_trips'] = df9.bike_card_trips
    pm['bike_card_gmv'] = df9.bike_card_gmv
    pm['car_cash_trips'] = df9.car_cash_trips
    pm['car_cash_gmv'] = df9.car_cash_gmv
    pm['car_momo_trips'] = df9.car_momo_trips
    pm['car_momo_gmv'] = df9.car_momo_gmv
    pm['car_card_trips'] = df9.car_card_trips
    pm['car_card_gmv'] = df9.car_card_gmv
    
    pm = pm.T
    pm.columns = [f"{start_date}"]
    pm.fillna(0, inplace=True)
    
    return df, pm

def main():
    load_dotenv()

    redash = Redash(key=os.getenv("REDASH_API_KEY"), base_url=os.getenv("REDASH_BASE_URL"))

    dt_format = "%Y-%m-%d"

    local_now = datetime.now(timezone.utc) + timedelta(hours=7)
    start_date = (local_now - timedelta(days=local_now.weekday() + 7)).strftime(dt_format)

    output_date = datetime.strptime(start_date, dt_format).strftime("%d_%b_%Y")

    # Process data for each city
    cities = ["ALL", "HCM", "HAN"]
    
    # Store all data
    weekly_reports = {}
    payment_methods = {}
    
    for city in cities:
        print(f"Processing data for {city}...")
        df, pm = process_city_data(redash, start_date, city)
        weekly_reports[city] = df
        payment_methods[city] = pm
    
    # Create combined sheets
    combined_weekly = pd.DataFrame()
    metrics = weekly_reports["ALL"].index
    combined_weekly["Metric"] = metrics
    
    for city in cities:
        city_data = weekly_reports[city][start_date]
        combined_weekly[city] = city_data.values
    
    combined_pm = pd.DataFrame()
    pm_metrics = payment_methods["ALL"].index
    combined_pm["Metric"] = pm_metrics
    
    for city in cities:
        city_pm_data = payment_methods[city][start_date]
        combined_pm[city] = city_pm_data.values
    
    # Create output filename
    output_file = f"VN_Weekly_{output_date}.xlsx"
    
    with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
        combined_weekly.to_excel(writer, sheet_name="Weekly Report", index=False)
        combined_pm.to_excel(writer, sheet_name="Payment Method", index=False)

    # Upload to Slack with error handling
    try:
        slack = SlackBot()
        slack.uploadFile(output_file, 
                       os.getenv("SLACK_CHANNEL"),
                       f"Weekly Report for VN {output_date}")
    except Exception as e:
        print(f"Error uploading to Slack: {str(e)}")

    print("All files processed and uploaded!")

if __name__ == '__main__':
    main()