import os

import pandas as pd
from dotenv import load_dotenv

from utils.dates import previous_week_start
from utils.helpers import Query, Redash
from utils.slack import SlackBot


def process_city_data(redash, start_date, city):
    """Process data for a specific city"""
    
    # Run queries with city parameter
    queries = [
        Query(4607, params={"week_start_date": start_date, "city": city}),
        Query(4611, params={"week_start_date": start_date, "city": city}),
        Query(4612, params={"week_start_date": start_date, "city": city}),
        Query(4613, params={"week_start_date": start_date, "city": city}),
        Query(4614, params={"week_start_date": start_date, "city": city}),
        Query(4615, params={"week_start_date": start_date, "city": city}),
        Query(4616, params={"week_start_date": start_date, "city": city}),
        Query(4617, params={"week_start_date": start_date, "city": city}),
        Query(5212, params={"week_start_date": start_date, "city": city}),
        Query(8647, params={"week_start_date": start_date, "city": city}),
    ]

    redash.run_queries(queries)

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
    df10 = redash.get_result(8647) # VN - Delivery Weekly (sender = creator_uuid; always one row)

    # Delivery (ride_type 100) has no rider_uuid and pays its fee as creator_system_fee, so the shared queries
    # count its TRIPS (4607, by car_type) but not its fare (4615), promo (4616), fee (4617) or payment method
    # (5212, method 9). The adjustments below use 8647 to make every block cover the same trips:
    #   Total = everything incl. delivery | Bike = bike ride-hailing only | Delivery = its own block.
    # Rider-user rows stay riders-only; senders are reported in the delivery block.
    delivery_trips = df10.delivery_bike_trips_4607_basis.fillna(0)   # delivery trips on 4607's basis
    ride_trips = df1.total_completed_trip - delivery_trips              # completed trips with a rider

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

    df['avg_completed_trip_per_rider'] = ride_trips / df.unique_completed_riders   # riders' own trips only
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

    # Totals include delivery: 4615/4616/4617 cover rider trips only, so add delivery from 8647.
    # Average fare is the trip-weighted mean of the riders' average (4615) and delivery's average.
    delivery_fare_sum = df10.delivery_average_fare.fillna(0) * df10.delivery_completed_trip
    df['average_fare_vnd'] = (df6.average_fare * ride_trips + delivery_fare_sum) / (ride_trips + df10.delivery_completed_trip)
    df['average_fare_usd'] = None
    df['promo_spend_vnd'] = df7.discount.fillna(0) + df10.delivery_discount
    df['promo_spend_usd'] = None
    df['promotion_trips'] = df7.discount_trips + df10.delivery_discount_trips
    df['non_promotion_trips'] = df.completed_trips - df.promotion_trips
    df['promotion/completed'] = df.promotion_trips / df.completed_trips

    df['average_promotion_value'] = None
    df['promo_per_completed_ride'] = None
    df['promo_per_completed_rider'] = None
    df['promo / average_fare'] = None
    df['platform_fee_vnd'] = df8.total_system_fee.fillna(0) + df10.delivery_platform_fee
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

    # Bike = bike ride-hailing only: 4607's bike count includes delivery (car_type 1001), while the bike users,
    # fare, promo and fee below already exclude it. Delivery has its own block further down.
    df['completed_trips_bike'] = df1.bike_completed_trip - delivery_trips
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

    df['blank_4'] = None

    df = df.copy()

    # Delivery (ride_type 100, live 22 Sep 2026): users are senders (creator_uuid), fee is creator_system_fee.
    # Its trips are in the Total above and taken out of Bike, so Total = Car + Bike + Delivery.
    df['completed_trips_delivery'] = df10.delivery_completed_trip
    df['delivery_complete / total_complete'] = df.completed_trips_delivery / df.completed_trips
    df['daily_trips_delivery'] = df.completed_trips_delivery / 7
    df['completed_users_delivery'] = df10.delivery_completed_users
    df['first_trip_users_delivery'] = df10.first_timers_delivery
    df['resurrect_users_delivery'] = df10.resurrect_delivery
    df['churned_users_delivery'] = df10.churn_delivery
    df['average_fare_vnd_delivery'] = df10.delivery_average_fare
    df['average_fare_usd_delivery'] = None
    df['promo_spend_vnd_delivery'] = df10.delivery_discount
    df['promo_spend_usd_delivery'] = None
    df['promotion_trips_delivery'] = df10.delivery_discount_trips
    df['average_promotion_value_delivery'] = None
    df['promo_per_completed_ride_delivery'] = None
    df['promo / average_fare delivery'] = None
    df['promo / completed_trips delivery'] = df.promotion_trips_delivery / df.completed_trips_delivery
    df['platform_fee_vnd_delivery'] = df10.delivery_platform_fee
    df['platform_fee_usd_delivery'] = None
    df['platform_fee_per_completed_ride_delivery'] = None

    df = df.copy()

    df = df.T
    df.columns = [f"{start_date}"]
    
    # Payment Method sheet
    pm = pd.DataFrame()
    
    # 5212 counts payment_method 0 / 19 / 5 only; delivery is always 9, so its split (from senderPaymentMethod,
    # via 8647) is added to the totals here and reported in its own rows below. Bike rows already exclude it.
    pm['total_cash_trips'] = df9.cash_trips.fillna(0) + df10.delivery_cash_trips
    pm['total_cash_gmv'] = df9.cash_gmv.fillna(0) + df10.delivery_cash_gmv
    pm['total_momo_trips'] = df9.momo_trips.fillna(0) + df10.delivery_momo_trips
    pm['total_momo_gmv'] = df9.momo_gmv.fillna(0) + df10.delivery_momo_gmv
    pm['total_card_trips'] = df9.card_trips.fillna(0) + df10.delivery_card_trips
    pm['total_card_gmv'] = df9.card_gmv.fillna(0) + df10.delivery_card_gmv
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
    pm['delivery_cash_trips'] = df10.delivery_cash_trips
    pm['delivery_cash_gmv'] = df10.delivery_cash_gmv
    pm['delivery_momo_trips'] = df10.delivery_momo_trips
    pm['delivery_momo_gmv'] = df10.delivery_momo_gmv
    pm['delivery_card_trips'] = df10.delivery_card_trips
    pm['delivery_card_gmv'] = df10.delivery_card_gmv
    
    pm = pm.T
    pm.columns = [f"{start_date}"]
    pm.fillna(0, inplace=True)
    
    return df, pm

def main():
    load_dotenv()

    redash = Redash(key=os.getenv("REDASH_API_KEY"), base_url=os.getenv("REDASH_BASE_URL"))

    start_date, output_date = previous_week_start(7)

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