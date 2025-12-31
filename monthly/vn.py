import os
import sys
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv

# Fix the import path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.helpers import Query, Redash
from utils.slack import SlackBot


def process_city_data(redash, start_date, end_date, DAYS_IN_MONTH, output_date, city):
    """Process data for a specific city"""
    
    # Common queries that need city parameter (except 4579)
    common_queries = [
        Query(4562, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4563, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4565, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4566, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4568, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4569, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4570, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4571, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4572, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4574, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4576, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4577, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4578, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4580, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4581, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4582, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4594, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4598, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(4750, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        # Updated queries: 4814 -> 6077, 4819 -> 6078 (no region param, just city)
        Query(6077, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
        Query(6078, params={"date_range": {"start": start_date, "end": end_date}, "city": city}),
    ]
    
    # Query 4579 doesn't have city param - only run for ALL city
    if city == "ALL":
        common_queries.append(Query(4579, params={"date_range": {"start": start_date, "end": end_date}}))
    
    queries = [common_queries]
    
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
    df10 = redash.get_result(4579) if city == "ALL" else pd.DataFrame() # Rider Signup (only for ALL)
    df11 = redash.get_result(4580) # Rider FT
    df12 = redash.get_result(4581) # Rider Daily
    df13 = redash.get_result(4582) # Riders CAR
    df14 = redash.get_result(4594) # Monthly Resurrected Riders
    df15 = redash.get_result(4598) # Monthly Resurrected Drivers
    df16 = redash.get_result(4750) # monthly ps - first try
    df17 = redash.get_result(6077) # median ttm / expire (was 4814)
    df18 = redash.get_result(6078) # book search book logic (was 4819)

    # Helper function to safely get column values
    def safe_get(df, column_name, default=0):
        if not df.empty and column_name in df.columns and len(df) > 0:
            return df[column_name].iloc[0]
        return default

    # Construct monthly DataFrame
    df = pd.DataFrame()

    # Performance Sheet
    df['rides'] = [safe_get(df1, 'completed')]
    df['demand'] = [safe_get(df1, 'demand')]
    df['match_rate'] = [safe_get(df1, 'matched', 1) / df['demand'].iloc[0] if df['demand'].iloc[0] != 0 else 0]
    df['completion_rate'] = [df['rides'].iloc[0] / df['demand'].iloc[0] if df['demand'].iloc[0] != 0 else 0]
    df['daily_rides'] = [df['rides'].iloc[0] / DAYS_IN_MONTH]
    df['uncompleted'] = [df['demand'].iloc[0] - df['rides'].iloc[0]]
    df['cater_rate'] = [df['rides'].iloc[0] / safe_get(df2, 'unique', 1) if safe_get(df2, 'unique', 1) != 0 else 0]
    df['booked_riders'] = [safe_get(df1, 'booked_riders')]
    df['completed_riders'] = [safe_get(df1, 'completed_riders')]
    df['first_try_cater_rate'] = [safe_get(df16, 'first_try_cater_rate')]
    df['retry_initiation_rate'] = [safe_get(df16, 'retry_initiation_rate')]
    df['retry_success_rate'] = [safe_get(df16, 'retry_success_rate')]
    df['daily_median_eta'] = [safe_get(df3, 'median_eta')]
    df['median_time_to_match_sec'] = [safe_get(df17, 'median_time_to_match_sec')]
    df['median_time_to_expire_sec'] = [safe_get(df17, 'median_time_to_expire_sec')]

    df['driver_mau'] = [safe_get(bq2, 'online')]
    df['completed_driver'] = [safe_get(df1, 'completed_drivers')]
    df['total_approved'] = [safe_get(df4, 'total_approved')]
    df['driver_online_daily'] = [safe_get(bq2, 'online_daily')]
    df['pinged_drivers_daily'] = [safe_get(bq3, 'pinged_drivers_daily')]
    df['completed_driver_daily'] = [safe_get(df5, 'completed_driver_daily')]
    df['online_mau'] = [df['driver_online_daily'].iloc[0] / df['driver_mau'].iloc[0] if df['driver_mau'].iloc[0] != 0 else 0]
    df['completed_online'] = [df['completed_driver_daily'].iloc[0] / df['driver_online_daily'].iloc[0] if df['driver_online_daily'].iloc[0] != 0 else 0]
    df['online_no_complete'] = [df['driver_online_daily'].iloc[0] - df['completed_driver_daily'].iloc[0]]
    df['ride_per_driver'] = [safe_get(df5, 'ride_per_driver')]
    df['driver_downloads'] = [None]
    df['driver_sign_up'] = [safe_get(df4, 'sign_up')]
    df['driver_sign_up_daily'] = [df['driver_sign_up'].iloc[0] / DAYS_IN_MONTH]
    df['driver_ft_all_time'] = [safe_get(df6, 'all_time')]
    df['driver_ft_same_month'] = [safe_get(df6, 'same_month')]
    df['driver_sign_up_activation_rate'] = [df['driver_ft_same_month'].iloc[0] / df['driver_sign_up'].iloc[0] if df['driver_sign_up'].iloc[0] != 0 else 0]
    df['driver_approved_activation_rate'] = [df['driver_ft_same_month'].iloc[0] / safe_get(df4, 'same_month_approved', 1) if safe_get(df4, 'same_month_approved', 1) != 0 else 0]
    df['driver_approved'] = [safe_get(df4, 'approved')]
    df['driver_same_month_approved'] = [safe_get(df4, 'same_month_approved')]
    df['driver_average_online_hours'] = [safe_get(bq4, 'avg_online_hour')]
    df['driver_average_utilisation_hours'] = [safe_get(df7, 'avg_utilisation_hours')]
    df['ping_per_driver_daily'] = [safe_get(bq3, 'ping_per_driver_daily')]
    df['driver_waiting_before_cancel'] = [safe_get(df8, 'avg_waiting_time_cxl')]
    df['driver_cancellation_rate'] = [safe_get(df1, 'driver_cancel', 0) / safe_get(df1, 'matched', 1) * 100 if safe_get(df1, 'matched', 1) != 0 else 0]
    df['drivers_ft_unique'] = [df['driver_ft_all_time'].iloc[0] / df['completed_driver'].iloc[0] if df['completed_driver'].iloc[0] != 0 else 0]
    df['drivers_repeated'] = [safe_get(df9, 'repeated')]
    df['resurrect_2_month_driver'] = [safe_get(df15, 'resurrect_2_month')]
    df['resurrect_3_4_month_driver'] = [safe_get(df15, 'resurrect_3_4_month')]
    df['resurrect_5_12_month_driver'] = [safe_get(df15, 'resurrect_5_12_month')]
    df['driver_resurrected'] = [safe_get(df9, 'resurrected')]
    df['driver_resurrected_rate'] = [None]
    df['driver_churned'] = [safe_get(df9, 'churned')]
    df['driver_churned_rate'] = [None]
    df['driver_inflow'] = [safe_get(df9, 'activated', 0) + safe_get(df9, 'resurrected', 0) - safe_get(df9, 'churned', 0)]

    df = df.copy()

    df['rider_mau'] = [safe_get(bq1, 'active_users')]
    df['rider_mau_demand'] = [df['demand'].iloc[0] / df['rider_mau'].iloc[0] if df['rider_mau'].iloc[0] != 0 else 0]
    df['rider_mau_rides'] = [df['rides'].iloc[0] / df['rider_mau'].iloc[0] if df['rider_mau'].iloc[0] != 0 else 0]
    df['r_d_ratio'] = [df['rider_mau'].iloc[0] / df['driver_mau'].iloc[0] if df['driver_mau'].iloc[0] != 0 else 0]
    df['rider_downloads'] = [None]
    df['rider_signup'] = [safe_get(df10, 'rider_signup')] if city == "ALL" else [0]
    df['rider_signup_daily'] = [df['rider_signup'].iloc[0] / DAYS_IN_MONTH] if city == "ALL" else [0]
    df['rider_ft_all_time'] = [safe_get(df11, 'all_time')]
    df['rider_ft_same_month'] = [safe_get(df11, 'same_month')]
    df['rider_same_month_activation'] = [df['rider_ft_same_month'].iloc[0] / df['rider_signup'].iloc[0] if city == "ALL" and df['rider_signup'].iloc[0] != 0 else 0]
    df['rider_unique_open_monthly'] = [safe_get(bq1, 'active_users')]
    df['rider_unique_search_monthly'] = [safe_get(df18, 'unique_search_users')]
    df['rider_unique_book_monthly'] = [safe_get(df18, 'unique_order_users')]
    df['rider_unique_complete_monthly'] = [safe_get(df12, 'completed_monthly_all')]
    df['rider_unique_open_daily'] = [safe_get(bq1, 'open_daily')]
    df['rider_unique_search_daily'] = [safe_get(df18, 'rider_unique_search_daily_avg')]
    df['rider_unique_book_daily'] = [safe_get(df18, 'rider_unique_book_daily_avg')]
    df['rider_unique_complete_daily'] = [safe_get(df12, 'completed_daily_all')]
    df['book_search_ratio_daily'] = [safe_get(df18, 'book_search_ratio_daily')]
    df['booking_per_user'] = [df['demand'].iloc[0] / df['rider_unique_book_monthly'].iloc[0] if df['rider_unique_book_monthly'].iloc[0] != 0 else 0]
    df['complete_per_user'] = [df['rides'].iloc[0] / df['rider_unique_complete_monthly'].iloc[0] if df['rider_unique_complete_monthly'].iloc[0] != 0 else 0]
    df['duplicate_ratio'] = [df['demand'].iloc[0] / safe_get(df2, 'unique', 1) if safe_get(df2, 'unique', 1) != 0 else 0]
    df['rider_waiting_before_cancel'] = [safe_get(df8, 'avg_waiting_time_cxl_rider')]
    df['rider_cancellation_rate'] = [safe_get(df1, 'rider_cancel', 0) / df['demand'].iloc[0] if df['demand'].iloc[0] != 0 else 0]
    df['riders_ft_unique'] = [df['rider_ft_all_time'].iloc[0] / df['rider_unique_complete_monthly'].iloc[0] if df['rider_unique_complete_monthly'].iloc[0] != 0 else 0]
    df['riders_repeated'] = [safe_get(df13, 'repeated')]
    df['resurrect_2_month_rider'] = [safe_get(df14, 'resurrect_2_month')]
    df['resurrect_3_4_month_rider'] = [safe_get(df14, 'resurrect_3_4_month')]
    df['resurrect_5_12_month_rider'] = [safe_get(df14, 'resurrect_5_12_month')]
    df['rider_resurrected'] = [safe_get(df13, 'resurrected')]
    df['rider_resurrected_rate'] = [None]
    df['rider_churned'] = [safe_get(df13, 'churned')]
    df['rider_churned_rate'] = [None]
    df['rider_inflow'] = [safe_get(df13, 'activated', 0) + safe_get(df13, 'resurrected', 0) - safe_get(df13, 'churned', 0)]

    df = df.copy()

    # Start of Car Sheet here
    df['rides_phv'] = [safe_get(df1, 'completed_phv')]
    df['demand_phv'] = [safe_get(df1, 'demand_phv')]
    df['match_rate_phv'] = [safe_get(df1, 'matched_phv', 1) / df['demand_phv'].iloc[0] if df['demand_phv'].iloc[0] != 0 else 0]
    df['completion_rate_phv'] = [df['rides_phv'].iloc[0] / df['demand_phv'].iloc[0] if df['demand_phv'].iloc[0] != 0 else 0]
    df['daily_rides_phv'] = [df['rides_phv'].iloc[0] / DAYS_IN_MONTH]
    df['uncompleted_phv'] = [df['demand_phv'].iloc[0] - df['rides_phv'].iloc[0]]
    df['cater_rate_phv'] = [df['rides_phv'].iloc[0] / safe_get(df2, 'unique_phv', 1) if safe_get(df2, 'unique_phv', 1) != 0 else 0]
    df['booked_riders_phv'] = [safe_get(df1, 'booked_riders_phv')]
    df['completed_riders_phv'] = [safe_get(df1, 'completed_riders_phv')]
    df['daily_median_eta_phv'] = [safe_get(df3, 'median_eta_phv')]

    df['driver_mau_phv'] = [safe_get(bq2, 'online_phv')]
    df['completed_driver_phv'] = [safe_get(df1, 'completed_drivers_phv')]
    df['total_approved_phv'] = [safe_get(df4, 'total_approved_phv')]
    df['driver_online_daily_phv'] = [safe_get(bq2, 'online_daily_phv')]
    df['pinged_drivers_daily_phv'] = [safe_get(bq3, 'pinged_drivers_daily_phv')]
    df['completed_driver_daily_phv'] = [safe_get(df5, 'completed_driver_daily_phv')]
    df['online_mau_phv'] = [df['driver_online_daily_phv'].iloc[0] / df['driver_mau_phv'].iloc[0] if df['driver_mau_phv'].iloc[0] != 0 else 0]
    df['completed_online_phv'] = [df['completed_driver_daily_phv'].iloc[0] / df['driver_online_daily_phv'].iloc[0] if df['driver_online_daily_phv'].iloc[0] != 0 else 0]
    df['online_no_complete_phv'] = [df['driver_online_daily_phv'].iloc[0] - df['completed_driver_daily_phv'].iloc[0]]
    df['ride_per_driver_phv'] = [safe_get(df5, 'ride_per_driver_phv')]
    df['driver_sign_up_phv'] = [safe_get(df4, 'sign_up_phv')]
    df['driver_sign_up_daily_phv'] = [df['driver_sign_up_phv'].iloc[0] / DAYS_IN_MONTH]
    df['driver_ft_all_time_phv'] = [safe_get(df6, 'all_time_phv')]
    df['driver_ft_same_month_phv'] = [safe_get(df6, 'same_month_phv')]
    df['driver_sign_up_activation_rate_phv'] = [df['driver_ft_same_month_phv'].iloc[0] / df['driver_sign_up_phv'].iloc[0] if df['driver_sign_up_phv'].iloc[0] != 0 else 0]
    df['driver_approved_activation_rate_phv'] = [df['driver_ft_same_month_phv'].iloc[0] / safe_get(df4, 'same_month_approved_phv', 1) if safe_get(df4, 'same_month_approved_phv', 1) != 0 else 0]
    df['driver_approved_phv'] = [safe_get(df4, 'approved_phv')]
    df['driver_same_month_approved_phv'] = [safe_get(df4, 'same_month_approved_phv')]
    df['driver_average_online_hours_phv'] = [safe_get(bq4, 'avg_online_hour_phv')]
    df['driver_average_utilisation_hours_phv'] = [safe_get(df7, 'avg_utilisation_hours_phv')]
    df['ping_per_driver_daily_phv'] = [safe_get(bq3, 'ping_per_driver_daily_phv')]
    df['driver_waiting_before_cancel_phv'] = [safe_get(df8, 'avg_waiting_time_cxl_phv')]
    df['driver_cancellation_rate_phv'] = [safe_get(df1, 'driver_cancel_phv', 0) / safe_get(df1, 'matched_phv', 1) * 100 if safe_get(df1, 'matched_phv', 1) != 0 else 0]
    df['drivers_ft_unique_phv'] = [df['driver_ft_all_time_phv'].iloc[0] / df['completed_driver_phv'].iloc[0] if df['completed_driver_phv'].iloc[0] != 0 else 0]
    df['drivers_repeated_phv'] = [safe_get(df9, 'repeated_phv')]
    df['resurrect_2_month_driver_phv'] = [safe_get(df15, 'resurrect_2_month_phv')]
    df['resurrect_3_4_month_driver_phv'] = [safe_get(df15, 'resurrect_3_4_month_phv')]
    df['resurrect_5_12_month_driver_phv'] = [safe_get(df15, 'resurrect_5_12_month_phv')]
    df['driver_resurrected_phv'] = [safe_get(df9, 'resurrected_phv')]
    df['driver_resurrected_rate_phv'] = [None]
    df['driver_churned_phv'] = [safe_get(df9, 'churned_phv')]
    df['driver_churned_rate_phv'] = [None]
    df['driver_inflow_phv'] = [safe_get(df9, 'activated_phv', 0) + safe_get(df9, 'resurrected_phv', 0) - safe_get(df9, 'churned_phv', 0)]

    df = df.copy()

    df['rider_ft_all_time_phv'] = [safe_get(df11, 'all_time_phv')]
    df['rider_unique_book_monthly_phv'] = [safe_get(df12, 'book_monthly_phv')]
    df['rider_unique_complete_monthly_phv'] = [safe_get(df12, 'completed_monthly_phv')]
    df['rider_unique_book_daily_phv'] = [safe_get(df12, 'book_daily_phv')]
    df['rider_unique_complete_daily_phv'] = [safe_get(df12, 'completed_daily_phv')]
    df['booking_per_user_phv'] = [df['demand_phv'].iloc[0] / df['rider_unique_book_monthly_phv'].iloc[0] if df['rider_unique_book_monthly_phv'].iloc[0] != 0 else 0]
    df['complete_per_user_phv'] = [df['rides_phv'].iloc[0] / df['rider_unique_complete_monthly_phv'].iloc[0] if df['rider_unique_complete_monthly_phv'].iloc[0] != 0 else 0]
    df['duplicate_ratio_phv'] = [df['demand_phv'].iloc[0] / safe_get(df2, 'unique_phv', 1) if safe_get(df2, 'unique_phv', 1) != 0 else 0]
    df['rider_waiting_before_cancel_phv'] = [safe_get(df8, 'avg_waiting_time_cxl_phv')]
    df['rider_cancellation_rate_phv'] = [safe_get(df1, 'rider_cancel_phv', 0) / df['demand_phv'].iloc[0] if df['demand_phv'].iloc[0] != 0 else 0]
    df['riders_ft_unique_phv'] = [df['rider_ft_all_time_phv'].iloc[0] / df['rider_unique_complete_monthly_phv'].iloc[0] if df['rider_unique_complete_monthly_phv'].iloc[0] != 0 else 0]
    df['riders_repeated_phv'] = [safe_get(df13, 'repeated_phv')]
    df['resurrect_2_month_rider_phv'] = [safe_get(df14, 'resurrect_2_month_phv')]
    df['resurrect_3_4_month_rider_phv'] = [safe_get(df14, 'resurrect_3_4_month_phv')]
    df['resurrect_5_12_month_rider_phv'] = [safe_get(df14, 'resurrect_5_12_month_phv')]
    df['rider_resurrected_phv'] = [safe_get(df13, 'resurrected_phv')]
    df['rider_resurrected_rate_phv'] = [None]
    df['rider_churned_phv'] = [safe_get(df13, 'churned_phv')]
    df['rider_churned_rate_phv'] = [None]
    df['rider_inflow_phv'] = [safe_get(df13, 'activated_phv', 0) + safe_get(df13, 'resurrected_phv', 0) - safe_get(df13, 'churned_phv', 0)]

    df = df.copy()

    # Start of bike sheet here
    df['rides_bike'] = [safe_get(df1, 'completed_bike')]
    df['demand_bike'] = [safe_get(df1, 'demand_bike')]
    df['match_rate_bike'] = [safe_get(df1, 'matched_bike', 1) / df['demand_bike'].iloc[0] if df['demand_bike'].iloc[0] != 0 else 0]
    df['completion_rate_bike'] = [df['rides_bike'].iloc[0] / df['demand_bike'].iloc[0] if df['demand_bike'].iloc[0] != 0 else 0]
    df['daily_rides_bike'] = [df['rides_bike'].iloc[0] / DAYS_IN_MONTH]
    df['uncompleted_bike'] = [df['demand_bike'].iloc[0] - df['rides_bike'].iloc[0]]
    df['cater_rate_bike'] = [df['rides_bike'].iloc[0] / safe_get(df2, 'unique_bike', 1) if safe_get(df2, 'unique_bike', 1) != 0 else 0]
    df['booked_riders_bike'] = [safe_get(df1, 'booked_riders_bike')]
    df['completed_riders_bike'] = [safe_get(df1, 'completed_riders_bike')]
    df['daily_median_eta_bike'] = [safe_get(df3, 'median_eta_bike')]

    df['driver_mau_bike'] = [safe_get(bq2, 'online_bike')]
    df['completed_driver_bike'] = [safe_get(df1, 'completed_drivers_bike')]
    df['total_approved_bike'] = [safe_get(df4, 'total_approved_bike')]
    df['driver_online_daily_bike'] = [safe_get(bq2, 'online_daily_bike')]
    df['pinged_drivers_daily_bike'] = [safe_get(bq3, 'pinged_drivers_daily_bike')]
    df['completed_driver_daily_bike'] = [safe_get(df5, 'completed_driver_daily_bike')]
    df['online_mau_bike'] = [df['driver_online_daily_bike'].iloc[0] / df['driver_mau_bike'].iloc[0] if df['driver_mau_bike'].iloc[0] != 0 else 0]
    df['completed_online_bike'] = [df['completed_driver_daily_bike'].iloc[0] / df['driver_online_daily_bike'].iloc[0] if df['driver_online_daily_bike'].iloc[0] != 0 else 0]
    df['online_no_complete_bike'] = [df['driver_online_daily_bike'].iloc[0] - df['completed_driver_daily_bike'].iloc[0]]
    df['ride_per_driver_bike'] = [safe_get(df5, 'ride_per_driver_bike')]
    df['driver_sign_up_bike'] = [safe_get(df4, 'sign_up_bike')]
    df['driver_sign_up_daily_bike'] = [df['driver_sign_up_bike'].iloc[0] / DAYS_IN_MONTH]
    df['driver_ft_all_time_bike'] = [safe_get(df6, 'all_time_bike')]
    df['driver_ft_same_month_bike'] = [safe_get(df6, 'same_month_bike')]
    df['driver_sign_up_activation_rate_bike'] = [df['driver_ft_same_month_bike'].iloc[0] / df['driver_sign_up_bike'].iloc[0] if df['driver_sign_up_bike'].iloc[0] != 0 else 0]
    df['driver_approved_activation_rate_bike'] = [df['driver_ft_same_month_bike'].iloc[0] / safe_get(df4, 'same_month_approved_bike', 1) if safe_get(df4, 'same_month_approved_bike', 1) != 0 else 0]
    df['driver_approved_bike'] = [safe_get(df4, 'approved_bike')]
    df['driver_same_month_approved_bike'] = [safe_get(df4, 'same_month_approved_bike')]
    df['driver_average_online_hours_bike'] = [safe_get(bq4, 'avg_online_hour_bike')]
    df['driver_average_utilisation_hours_bike'] = [safe_get(df7, 'avg_utilisation_hours_bike')]
    df['ping_per_driver_daily_bike'] = [safe_get(bq3, 'ping_per_driver_daily_bike')]
    df['driver_waiting_before_cancel_bike'] = [safe_get(df8, 'avg_waiting_time_cxl_bike')]
    df['driver_cancellation_rate_bike'] = [safe_get(df1, 'driver_cancel_bike', 0) / safe_get(df1, 'matched_bike', 1) * 100 if safe_get(df1, 'matched_bike', 1) != 0 else 0]
    df['drivers_ft_unique_bike'] = [df['driver_ft_all_time_bike'].iloc[0] / df['completed_driver_bike'].iloc[0] if df['completed_driver_bike'].iloc[0] != 0 else 0]
    df['drivers_repeated_bike'] = [safe_get(df9, 'repeated_bike')]
    df['resurrect_2_month_driver_bike'] = [safe_get(df15, 'resurrect_2_month_bike')]
    df['resurrect_3_4_month_driver_bike'] = [safe_get(df15, 'resurrect_3_4_month_bike')]
    df['resurrect_5_12_month_driver_bike'] = [safe_get(df15, 'resurrect_5_12_month_bike')]
    df['driver_resurrected_bike'] = [safe_get(df9, 'resurrected_bike')]
    df['driver_resurrected_rate_bike'] = [None]
    df['driver_churned_bike'] = [safe_get(df9, 'churned_bike')]
    df['driver_churned_rate_bike'] = [None]
    df['driver_inflow_bike'] = [safe_get(df9, 'activated_bike', 0) + safe_get(df9, 'resurrected_bike', 0) - safe_get(df9, 'churned_bike', 0)]

    df = df.copy()

    df['rider_ft_all_time_bike'] = [safe_get(df11, 'all_time_bike')]
    df['rider_unique_book_monthly_bike'] = [safe_get(df12, 'book_monthly_bike')]
    df['rider_unique_complete_monthly_bike'] = [safe_get(df12, 'completed_monthly_bike')]
    df['rider_unique_book_daily_bike'] = [safe_get(df12, 'book_daily_bike')]
    df['rider_unique_complete_daily_bike'] = [safe_get(df12, 'completed_daily_bike')]
    df['booking_per_user_bike'] = [df['demand_bike'].iloc[0] / df['rider_unique_book_monthly_bike'].iloc[0] if df['rider_unique_book_monthly_bike'].iloc[0] != 0 else 0]
    df['complete_per_user_bike'] = [df['rides_bike'].iloc[0] / df['rider_unique_complete_monthly_bike'].iloc[0] if df['rider_unique_complete_monthly_bike'].iloc[0] != 0 else 0]
    df['duplicate_ratio_bike'] = [df['demand_bike'].iloc[0] / safe_get(df2, 'unique_bike', 1) if safe_get(df2, 'unique_bike', 1) != 0 else 0]
    df['rider_waiting_before_cancel_bike'] = [safe_get(df8, 'avg_waiting_time_cxl_bike')]
    df['rider_cancellation_rate_bike'] = [safe_get(df1, 'rider_cancel_bike', 0) / df['demand_bike'].iloc[0] if df['demand_bike'].iloc[0] != 0 else 0]
    df['riders_ft_unique_bike'] = [df['rider_ft_all_time_bike'].iloc[0] / df['rider_unique_complete_monthly_bike'].iloc[0] if df['rider_unique_complete_monthly_bike'].iloc[0] != 0 else 0]
    df['riders_repeated_bike'] = [safe_get(df13, 'repeated_bike')]
    df['resurrect_2_month_rider_bike'] = [safe_get(df14, 'resurrect_2_month_bike')]
    df['resurrect_3_4_month_rider_bike'] = [safe_get(df14, 'resurrect_3_4_month_bike')]
    df['resurrect_5_12_month_rider_bike'] = [safe_get(df14, 'resurrect_5_12_month_bike')]
    df['rider_resurrected_bike'] = [safe_get(df13, 'resurrected_bike')]
    df['rider_resurrected_rate_bike'] = [None]
    df['rider_churned_bike'] = [safe_get(df13, 'churned_bike')]
    df['rider_churned_rate_bike'] = [None]
    df['rider_inflow_bike'] = [safe_get(df13, 'activated_bike', 0) + safe_get(df13, 'resurrected_bike', 0) - safe_get(df13, 'churned_bike', 0)]

    df = df.copy()

    df = df.T
    df.columns = [f"{output_date}"]
    
    return df


def main():
    load_dotenv()

    redash = Redash(key=os.getenv("REDASH_API_KEY"), base_url=os.getenv("REDASH_BASE_URL"))

    dt_format = "%Y-%m-%d"
    start_date = (datetime.today().replace(day=1) - timedelta(days=1)).replace(day=1).strftime(dt_format)
    end_date = (datetime.today().replace(day=1) - timedelta(days=1)).strftime(dt_format)

    DAYS_IN_MONTH = int(end_date.split("-")[2])

    output_date = datetime.strptime(start_date, dt_format).strftime("%b_%Y")
    output_file = f"VN_{output_date}.xlsx"

    # Create an Excel writer
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        # Process data for each city and save to separate sheets
        cities = ["ALL", "HCM", "HAN"]
        
        for city in cities:
            print(f"Processing data for {city}...")
            
            try:
                # Process data for this city
                df = process_city_data(redash, start_date, end_date, DAYS_IN_MONTH, output_date, city)
                
                # Save to Excel sheet with city name
                sheet_name = f"VN_{city}"
                # Limit sheet name to 31 characters (Excel limitation)
                if len(sheet_name) > 31:
                    sheet_name = sheet_name[:31]
                
                df.to_excel(writer, sheet_name=sheet_name)
                print(f"Added sheet: {sheet_name}")
                
            except Exception as e:
                print(f"Error processing {city}: {str(e)}")
                import traceback
                traceback.print_exc()
                # Create an empty DataFrame for this city if processing fails
                empty_df = pd.DataFrame({"Error": [f"Failed to process {city}: {str(e)}"]})
                empty_df.to_excel(writer, sheet_name=f"Error_{city}"[:31])
                continue
    
    print(f"Created Excel file: {output_file}")
    
    # Upload to Slack
    try:
        slack = SlackBot()
        slack.uploadFile(output_file, 
                       os.getenv("SLACK_CHANNEL"),
                       f"Monthly Report for VN {output_date} (ALL, HCM, HAN)")
        print(f"Uploaded {output_file} to Slack")
    except Exception as e:
        print(f"Error uploading to Slack: {str(e)}")

    print("Processing complete!")

if __name__ == '__main__':
    main()