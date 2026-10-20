/**
 * VN monthly perf sheet updater (Apps Script, bound to "VN - Perf Sheet").
 *
 * On the 1st of each month monthly/vn.py posts VN_<Mon>_<YYYY>.xlsx to Slack: tabs VN_ALL, VN_HCM, VN_HAN,
 * each a column of metric names (A) and the month's values (B). VN_ALL is tagged by utils/report_tag.py
 * (D1:E2): report = VN Monthly, month_start = the month's first day.
 *
 * To fill a month: File > Import > Upload > "Insert new sheet(s)". The script reads the tags first and only
 * goes ahead for a VN Monthly file; anything else is refused and nothing changes. Then, on all nine tabs
 * (Performance Sheet / Car / Bike for VN, HCM and HAN):
 *   - every mapped row's label is checked first, so a row added or removed stops the run untouched
 *   - the month's column is found by year (row 1) and month (row 3); if it doesn't exist yet and is the
 *     month after the last one (the VN tabs add a column each month), it is added: days (row 2), month
 *     name (row 3), the year label in row 1 extended, or a new year started
 *   - values are written by metric name (TABS below), so extra metrics in the file (e.g. delivery) are
 *     ignored and a reordered file can't shift rows; metrics the file leaves empty (e.g. downloads, which
 *     come in later) leave the cell as it is
 *   - formula rows (Resurrected %, Churned %) take the formula from the previous month
 *   - formatting is copied from the latest filled month
 *   - the Growth column is pointed at the latest filled month vs the month before
 * Re-importing a month overwrites it. On success the imported tabs are deleted; on an error they stay, with
 * the reason in red at D4 of VN_ALL, and the perf tabs are untouched.
 *
 * Setup: Extensions > Apps Script, paste this file, save, reload the sheet, VN Monthly > Enable auto-update.
 */

const REPORT = 'VN Monthly';
const YEAR_ROW = 1;    // year, merged across its months
const DAYS_ROW = 2;    // days in the month
const MONTH_ROW = 3;   // Jan..Dec, and "Growth"
const FIRST_ROW = 4;   // first metric row
const LABEL_COL = 3;   // row labels in column C
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

const MAIN_TAB = 'VN_ALL';                  // the tagged imported tab
const COMPANIONS = ['VN_HCM', 'VN_HAN'];    // imported with it
const REPORT_KEY = 'report';
const MONTH_KEY = 'month_start';
const TAG_SCAN_COLS = 30;
const FORMULA_LOOKBACK = 12;  // earlier month columns searched for a formula row's formula
const FORMAT_MONTHS = 6;      // recent filled months whose most common number format each row takes

// Per perf tab: the imported tab it reads, and [sheet row, metric name in the file, the row's label].
// Labels are checked before anything is written. Rows not listed (Resurrected %, Churned %) are formula rows.
const TABS = [
  { tab: " Performance Sheet VN", source: "VN_ALL", rows: [
    [4, "rides", "Rides"],
    [5, "demand", "Demand"],
    [6, "match_rate", "Match Rate"],
    [7, "completion_rate", "Completion Rate"],
    [8, "daily_rides", "Daily Average Completed Rides"],
    [9, "uncompleted", "Uncompleted"],
    [10, "cater_rate", "Cater Rate"],
    [11, "booked_riders", "Unique users making a booking"],
    [12, "completed_riders", "Unique users completing a trip"],
    [13, "first_try_cater_rate", "First Try Cater Rate"],
    [14, "retry_initiation_rate", "Retry Initiation Rate"],
    [15, "retry_success_rate", "Retry Success Rate"],
    [16, "daily_median_eta", "Daily Average Median ETA (minutes)"],
    [17, "median_time_to_match_sec", "Median Time to Match (seconds)"],
    [18, "median_time_to_expire_sec", "Median Time to Expire (seconds)"],
    [19, "driver_mau", "MAU Driver"],
    [20, "completed_driver", "Monthly Completed Driver"],
    [21, "total_approved", "Total Approved Driver"],
    [22, "driver_online_daily", "Daily Average Driver Online"],
    [23, "pinged_drivers_daily", "Daily Average drivers who receive at least 1 ping"],
    [24, "completed_driver_daily", "Daily Average Driver Complete"],
    [25, "online_mau", "Online : MAU"],
    [26, "completed_online", "Online vs completed %"],
    [27, "online_no_complete", "Driver online but did not complete"],
    [28, "ride_per_driver", "Average trips/completed driver"],
    [29, "driver_downloads", "Driver Downloads"],
    [30, "driver_sign_up", "Driver Sign-ups"],
    [31, "driver_sign_up_daily", "Daily Sign ups"],
    [32, "driver_ft_all_time", "Driver First-ride (All time)"],
    [33, "driver_ft_same_month", "First Time Driver (Same Month Sign up)"],
    [34, "driver_sign_up_activation_rate", "Same Month Activation Rate (FT/Signup)"],
    [35, "driver_approved_activation_rate", "Same Month Activation Rate (FT/Approved)"],
    [36, "driver_approved", "Driver Approved"],
    [37, "driver_same_month_approved", "Driver Same Month Approved"],
    [38, "driver_average_online_hours", "Average driver online hours"],
    [39, "driver_average_utilisation_hours", "Average driver utilisation hour"],
    [40, "ping_per_driver_daily", "Average number of ping per driver (Daily Average)"],
    [41, "driver_waiting_before_cancel", "Average waiting time before cancellation (minutes)"],
    [42, "driver_cancellation_rate", "Cancellation rate by drivers"],
    [43, "drivers_ft_unique", "First Time / Unique Completed Drivers"],
    [44, "drivers_repeated", "Repeated Drivers"],
    [45, "resurrect_2_month_driver", "Unique users resurrecting from 2 months ago"],
    [46, "resurrect_3_4_month_driver", "Unique users resurrecting from 3-4 months ago"],
    [47, "resurrect_5_12_month_driver", "Unique users resurrecting from 5-12 months ago"],
    [48, "driver_resurrected", "Resurrected"],
    [50, "driver_churned", "Churned (did not take trip this month)"],
    [52, "driver_inflow", "Inflow"],
    [53, "rider_mau", "MAU Rider"],
    [54, "rider_mau_demand", "MAUR:Demand"],
    [55, "rider_mau_rides", "MAUR:Ride"],
    [56, "r_d_ratio", "MAUR:MAUD Ratio"],
    [57, "rider_downloads", "Rider Downloads"],
    [58, "rider_signup", "Rider Sign-ups"],
    [59, "rider_signup_daily", "Daily Sign-up"],
    [60, "rider_ft_all_time", "First Time Riders (All Time)"],
    [61, "rider_ft_same_month", "First Time Riders (Same month sign up)"],
    [62, "rider_same_month_activation", "Same Month Activation Rate (FT/Signup)"],
    [63, "rider_unique_open_monthly", "Unique users opening our app"],
    [64, "rider_unique_search_monthly", "Unique users making a search"],
    [65, "rider_unique_book_monthly", "Unique users making a booking"],
    [66, "rider_unique_complete_monthly", "Unique users completing a trip"],
    [67, "rider_unique_open_daily", "Unique users opening our app(Daily Average)"],
    [68, "rider_unique_search_daily", "Unique users making a search(Daily Average)"],
    [69, "rider_unique_book_daily", "Unique users making a booking(Daily Average)"],
    [70, "rider_unique_complete_daily", "Unique users completing a trip(Daily Average)"],
    [71, "book_search_ratio_daily", "Book:Search Ratio"],
    [72, "booking_per_user", "Average number of bookings a unique user makes"],
    [73, "complete_per_user", "Average number of trips a unique user makes"],
    [74, "duplicate_ratio", "Duplicate Ratio"],
    [75, "rider_waiting_before_cancel", "Average waiting time before cancellation (minutes)"],
    [76, "rider_cancellation_rate", "Cancellation rate"],
    [77, "riders_ft_unique", "First Time / Unique Completed Riders"],
    [78, "riders_repeated", "Repeated Riders"],
    [79, "resurrect_2_month_rider", "Unique users resurrecting from 2 months ago"],
    [80, "resurrect_3_4_month_rider", "Unique users resurrecting from 3-4 months ago"],
    [81, "resurrect_5_12_month_rider", "Unique users resurrecting from 5-12 months ago"],
    [82, "rider_resurrected", "Resurrected"],
    [84, "rider_churned", "Churned (did not take trip this month)"],
    [86, "rider_inflow", "Inflow"],
  ] },
  { tab: "Car VN", source: "VN_ALL", rows: [
    [4, "rides_phv", "Rides"],
    [5, "demand_phv", "Demand"],
    [6, "match_rate_phv", "Match Rate"],
    [7, "completion_rate_phv", "Completion Rate"],
    [8, "daily_rides_phv", "Daily Average Completed Rides"],
    [9, "uncompleted_phv", "Uncompleted"],
    [10, "cater_rate_phv", "Cater Rate"],
    [11, "booked_riders_phv", "Unique users making a booking"],
    [12, "completed_riders_phv", "Unique users completing a trip"],
    [13, "daily_median_eta_phv", "Daily Average Median ETA"],
    [14, "driver_mau_phv", "MAU Driver"],
    [15, "completed_driver_phv", "Monthly Completed Driver"],
    [16, "total_approved_phv", "Total Approved Driver"],
    [17, "driver_online_daily_phv", "Daily Average Driver Online"],
    [18, "pinged_drivers_daily_phv", "Daily Average drivers who receive at least 1 ping"],
    [19, "completed_driver_daily_phv", "Daily Average Driver Complete"],
    [20, "online_mau_phv", "Online : MAU"],
    [21, "completed_online_phv", "Online vs completed %"],
    [22, "online_no_complete_phv", "Driver online but did not complete"],
    [23, "ride_per_driver_phv", "Average trips/completed driver"],
    [24, "driver_sign_up_phv", "Driver Sign-ups"],
    [25, "driver_sign_up_daily_phv", "Daily Sign ups"],
    [26, "driver_ft_all_time_phv", "Driver First-ride (All time)"],
    [27, "driver_ft_same_month_phv", "First Time Driver (Same Month Sign up)"],
    [28, "driver_sign_up_activation_rate_phv", "Same Month Activation Rate (FT/Signup)"],
    [29, "driver_approved_activation_rate_phv", "Same Month Activation Rate (FT/Approved)"],
    [30, "driver_approved_phv", "Driver Approved"],
    [31, "driver_same_month_approved_phv", "Driver Same Month Approved"],
    [32, "driver_average_online_hours_phv", "Average driver online hours"],
    [33, "driver_average_utilisation_hours_phv", "Average Driver utilisation hour"],
    [34, "ping_per_driver_daily_phv", "Average number of ping per driver (Daily Average)"],
    [35, "driver_waiting_before_cancel_phv", "Average waiting time before cancellation (minutes)"],
    [36, "driver_cancellation_rate_phv", "Cancellation rate by drivers"],
    [37, "drivers_ft_unique_phv", "First Time / Unique Completed Drivers"],
    [38, "drivers_repeated_phv", "Repeated Drivers"],
    [39, "resurrect_2_month_driver_phv", "Unique users resurrecting from 2 months ago"],
    [40, "resurrect_3_4_month_driver_phv", "Unique users resurrecting from 3-4 months ago"],
    [41, "resurrect_5_12_month_driver_phv", "Unique users resurrecting from 5-12 months ago"],
    [42, "driver_resurrected_phv", "Resurrected"],
    [44, "driver_churned_phv", "Churned (did not take trip this month)"],
    [46, "driver_inflow_phv", "Inflow"],
    [47, "rider_ft_all_time_phv", "First Time Riders (All Time)"],
    [48, "rider_unique_book_monthly_phv", "Unique users making a booking"],
    [49, "rider_unique_complete_monthly_phv", "Unique users completing a trip"],
    [50, "rider_unique_book_daily_phv", "Unique users making a booking(Daily Average)"],
    [51, "rider_unique_complete_daily_phv", "Unique users completing a trip(Daily Average)"],
    [52, "booking_per_user_phv", "Average number of bookings a unique user makes"],
    [53, "complete_per_user_phv", "Average number of trips a unique user makes"],
    [54, "duplicate_ratio_phv", "Duplicate Ratio"],
    [55, "rider_waiting_before_cancel_phv", "Average waiting time before cancellation (minutes)"],
    [56, "rider_cancellation_rate_phv", "Cancellation rate"],
    [57, "riders_ft_unique_phv", "First Time / Unique Completed Riders"],
    [58, "riders_repeated_phv", "Repeated Riders"],
    [59, "resurrect_2_month_rider_phv", "Unique users resurrecting from 2 months ago"],
    [60, "resurrect_3_4_month_rider_phv", "Unique users resurrecting from 3-4 months ago"],
    [61, "resurrect_5_12_month_rider_phv", "Unique users resurrecting from 5-12 months ago"],
    [62, "rider_resurrected_phv", "Resurrected"],
    [64, "rider_churned_phv", "Churned (did not take trip this month)"],
    [66, "rider_inflow_phv", "Inflow"],
  ] },
  { tab: " Bike VN", source: "VN_ALL", rows: [
    [4, "rides_bike", "Rides"],
    [5, "demand_bike", "Demand"],
    [6, "match_rate_bike", "Match Rate"],
    [7, "completion_rate_bike", "Completion Rate"],
    [8, "daily_rides_bike", "Daily Average Completed Rides"],
    [9, "uncompleted_bike", "Uncompleted"],
    [10, "cater_rate_bike", "Cater Rate"],
    [11, "booked_riders_bike", "Unique users making a booking"],
    [12, "completed_riders_bike", "Unique users completing a trip"],
    [13, "daily_median_eta_bike", "Daily Average Median ETA"],
    [14, "driver_mau_bike", "MAU Driver"],
    [15, "completed_driver_bike", "Monthly Completed Driver"],
    [16, "total_approved_bike", "Total Approved Driver"],
    [17, "driver_online_daily_bike", "Daily Average Driver Online"],
    [18, "pinged_drivers_daily_bike", "Daily Average drivers who receive at least 1 ping"],
    [19, "completed_driver_daily_bike", "Daily Average Driver Complete"],
    [20, "online_mau_bike", "Online : MAU"],
    [21, "completed_online_bike", "Online vs completed %"],
    [22, "online_no_complete_bike", "Driver online but did not complete"],
    [23, "ride_per_driver_bike", "Average trips/completed driver"],
    [24, "driver_sign_up_bike", "Driver Sign-ups"],
    [25, "driver_sign_up_daily_bike", "Daily Sign ups"],
    [26, "driver_ft_all_time_bike", "Driver First-ride (All time)"],
    [27, "driver_ft_same_month_bike", "First Time Driver (Same Month Sign up)"],
    [28, "driver_sign_up_activation_rate_bike", "Same Month Activation Rate (FT/Signup)"],
    [29, "driver_approved_activation_rate_bike", "Same Month Activation Rate (FT/Approved)"],
    [30, "driver_approved_bike", "Driver Approved"],
    [31, "driver_same_month_approved_bike", "Driver Same Month Approved"],
    [32, "driver_average_online_hours_bike", "Average driver online hours"],
    [33, "driver_average_utilisation_hours_bike", "Average Driver utilisation hour"],
    [34, "ping_per_driver_daily_bike", "Average number of ping per driver (Daily Average)"],
    [35, "driver_waiting_before_cancel_bike", "Average waiting time before cancellation (minutes)"],
    [36, "driver_cancellation_rate_bike", "Cancellation rate by drivers"],
    [37, "drivers_ft_unique_bike", "First Time / Unique Completed Drivers"],
    [38, "drivers_repeated_bike", "Repeated Drivers"],
    [39, "resurrect_2_month_driver_bike", "Unique users resurrecting from 2 months ago"],
    [40, "resurrect_3_4_month_driver_bike", "Unique users resurrecting from 3-4 months ago"],
    [41, "resurrect_5_12_month_driver_bike", "Unique users resurrecting from 5-12 months ago"],
    [42, "driver_resurrected_bike", "Resurrected"],
    [44, "driver_churned_bike", "Churned (did not take trip this month)"],
    [46, "driver_inflow_bike", "Inflow"],
    [47, "rider_ft_all_time_bike", "First Time Riders (All Time)"],
    [48, "rider_unique_book_monthly_bike", "Unique users making a booking"],
    [49, "rider_unique_complete_monthly_bike", "Unique users completing a trip"],
    [50, "rider_unique_book_daily_bike", "Unique users making a booking(Daily Average)"],
    [51, "rider_unique_complete_daily_bike", "Unique users completing a trip(Daily Average)"],
    [52, "booking_per_user_bike", "Average number of bookings a unique user makes"],
    [53, "complete_per_user_bike", "Average number of trips a unique user makes"],
    [54, "duplicate_ratio_bike", "Duplicate Ratio"],
    [55, "rider_waiting_before_cancel_bike", "Average waiting time before cancellation (minutes)"],
    [56, "rider_cancellation_rate_bike", "Cancellation rate"],
    [57, "riders_ft_unique_bike", "First Time / Unique Completed Riders"],
    [58, "riders_repeated_bike", "Repeated Riders"],
    [59, "resurrect_2_month_rider_bike", "Unique users resurrecting from 2 months ago"],
    [60, "resurrect_3_4_month_rider_bike", "Unique users resurrecting from 3-4 months ago"],
    [61, "resurrect_5_12_month_rider_bike", "Unique users resurrecting from 5-12 months ago"],
    [62, "rider_resurrected_bike", "Resurrected"],
    [64, "rider_churned_bike", "Churned (did not take trip this month)"],
    [66, "rider_inflow_bike", "Inflow"],
  ] },
  { tab: "Performance Sheet HCM", source: "VN_HCM", rows: [
    [4, "rides", "Rides"],
    [5, "demand", "Demand"],
    [6, "match_rate", "Match Rate"],
    [7, "completion_rate", "Completion Rate"],
    [8, "daily_rides", "Daily Average Completed Rides"],
    [9, "uncompleted", "Uncompleted"],
    [10, "cater_rate", "Cater Rate"],
    [11, "booked_riders", "Unique users making a booking"],
    [12, "completed_riders", "Unique users completing a trip"],
    [13, "first_try_cater_rate", "First Try Cater Rate"],
    [14, "retry_initiation_rate", "Retry Initiation Rate"],
    [15, "retry_success_rate", "Retry Success Rate"],
    [16, "daily_median_eta", "Daily Average Median ETA (minutes)"],
    [17, "median_time_to_match_sec", "Median Time to Match (seconds)"],
    [18, "median_time_to_expire_sec", "Median Time to Expire (seconds)"],
    [19, "driver_mau", "MAU Driver"],
    [20, "completed_driver", "Monthly Completed Driver"],
    [21, "total_approved", "Total Approved Driver"],
    [22, "driver_online_daily", "Daily Average Driver Online"],
    [23, "pinged_drivers_daily", "Daily Average drivers who receive at least 1 ping"],
    [24, "completed_driver_daily", "Daily Average Driver Complete"],
    [25, "online_mau", "Online : MAU"],
    [26, "completed_online", "Online vs completed %"],
    [27, "online_no_complete", "Driver online but did not complete"],
    [28, "ride_per_driver", "Average trips/completed driver"],
    [29, "driver_sign_up", "Driver Sign-ups"],
    [30, "driver_sign_up_daily", "Daily Sign ups"],
    [31, "driver_ft_all_time", "Driver First-ride (All time)"],
    [32, "driver_ft_same_month", "First Time Driver (Same Month Sign up)"],
    [33, "driver_sign_up_activation_rate", "Same Month Activation Rate (FT/Signup)"],
    [34, "driver_approved_activation_rate", "Same Month Activation Rate (FT/Approved)"],
    [35, "driver_approved", "Driver Approved"],
    [36, "driver_same_month_approved", "Driver Same Month Approved"],
    [37, "driver_average_online_hours", "Average driver online hours"],
    [38, "driver_average_utilisation_hours", "Average driver utilisation hour"],
    [39, "ping_per_driver_daily", "Average number of ping per driver (Daily Average)"],
    [40, "driver_waiting_before_cancel", "Average waiting time before cancellation (minutes)"],
    [41, "driver_cancellation_rate", "Cancellation rate by drivers"],
    [42, "drivers_ft_unique", "First Time / Unique Completed Drivers"],
    [43, "drivers_repeated", "Repeated Drivers"],
    [44, "resurrect_2_month_driver", "Unique users resurrecting from 2 months ago"],
    [45, "resurrect_3_4_month_driver", "Unique users resurrecting from 3-4 months ago"],
    [46, "resurrect_5_12_month_driver", "Unique users resurrecting from 5-12 months ago"],
    [47, "driver_resurrected", "Resurrected"],
    [49, "driver_churned", "Churned (did not take trip this month)"],
    [51, "driver_inflow", "Inflow"],
    [52, "rider_mau", "MAU Rider"],
    [53, "rider_mau_demand", "MAUR:Demand"],
    [54, "rider_mau_rides", "MAUR:Ride"],
    [55, "r_d_ratio", "MAUR:MAUD Ratio"],
    [56, "rider_ft_all_time", "First Time Riders (All Time)"],
    [57, "rider_ft_same_month", "First Time Riders (Same month sign up)"],
    [58, "rider_unique_open_monthly", "Unique users opening our app"],
    [59, "rider_unique_search_monthly", "Unique users making a search"],
    [60, "rider_unique_book_monthly", "Unique users making a booking"],
    [61, "rider_unique_complete_monthly", "Unique users completing a trip"],
    [62, "rider_unique_open_daily", "Unique users opening our app(Daily Average)"],
    [63, "rider_unique_search_daily", "Unique users making a search(Daily Average)"],
    [64, "rider_unique_book_daily", "Unique users making a booking(Daily Average)"],
    [65, "rider_unique_complete_daily", "Unique users completing a trip(Daily Average)"],
    [66, "book_search_ratio_daily", "Book:Search Ratio"],
    [67, "booking_per_user", "Average number of bookings a unique user makes"],
    [68, "complete_per_user", "Average number of trips a unique user makes"],
    [69, "duplicate_ratio", "Duplicate Ratio"],
    [70, "rider_waiting_before_cancel", "Average waiting time before cancellation (minutes)"],
    [71, "rider_cancellation_rate", "Cancellation rate"],
    [72, "riders_ft_unique", "First Time / Unique Completed Riders"],
    [73, "riders_repeated", "Repeated Riders"],
    [74, "resurrect_2_month_rider", "Unique users resurrecting from 2 months ago"],
    [75, "resurrect_3_4_month_rider", "Unique users resurrecting from 3-4 months ago"],
    [76, "resurrect_5_12_month_rider", "Unique users resurrecting from 5-12 months ago"],
    [77, "rider_resurrected", "Resurrected"],
    [79, "rider_churned", "Churned (did not take trip this month)"],
    [81, "rider_inflow", "Inflow"],
  ] },
  { tab: "Car HCM", source: "VN_HCM", rows: [
    [4, "rides_phv", "Rides"],
    [5, "demand_phv", "Demand"],
    [6, "match_rate_phv", "Match Rate"],
    [7, "completion_rate_phv", "Completion Rate"],
    [8, "daily_rides_phv", "Daily Average Completed Rides"],
    [9, "uncompleted_phv", "Uncompleted"],
    [10, "cater_rate_phv", "Cater Rate"],
    [11, "booked_riders_phv", "Unique users making a booking"],
    [12, "completed_riders_phv", "Unique users completing a trip"],
    [13, "daily_median_eta_phv", "Daily Average Median ETA"],
    [14, "driver_mau_phv", "MAU Driver"],
    [15, "completed_driver_phv", "Monthly Completed Driver"],
    [16, "total_approved_phv", "Total Approved Driver"],
    [17, "driver_online_daily_phv", "Daily Average Driver Online"],
    [18, "pinged_drivers_daily_phv", "Daily Average drivers who receive at least 1 ping"],
    [19, "completed_driver_daily_phv", "Daily Average Driver Complete"],
    [20, "online_mau_phv", "Online : MAU"],
    [21, "completed_online_phv", "Online vs completed %"],
    [22, "online_no_complete_phv", "Driver online but did not complete"],
    [23, "ride_per_driver_phv", "Average trips/completed driver"],
    [24, "driver_sign_up_phv", "Driver Sign-ups"],
    [25, "driver_sign_up_daily_phv", "Daily Sign ups"],
    [26, "driver_ft_all_time_phv", "Driver First-ride (All time)"],
    [27, "driver_ft_same_month_phv", "First Time Driver (Same Month Sign up)"],
    [28, "driver_sign_up_activation_rate_phv", "Same Month Activation Rate (FT/Signup)"],
    [29, "driver_approved_activation_rate_phv", "Same Month Activation Rate (FT/Approved)"],
    [30, "driver_approved_phv", "Driver Approved"],
    [31, "driver_same_month_approved_phv", "Driver Same Month Approved"],
    [32, "driver_average_online_hours_phv", "Average driver online hours"],
    [33, "driver_average_utilisation_hours_phv", "Average Driver utilisation hour"],
    [34, "ping_per_driver_daily_phv", "Average number of ping per driver (Daily Average)"],
    [35, "driver_waiting_before_cancel_phv", "Average waiting time before cancellation (minutes)"],
    [36, "driver_cancellation_rate_phv", "Cancellation rate by drivers"],
    [37, "drivers_ft_unique_phv", "First Time / Unique Completed Drivers"],
    [38, "drivers_repeated_phv", "Repeated Drivers"],
    [39, "resurrect_2_month_driver_phv", "Unique users resurrecting from 2 months ago"],
    [40, "resurrect_3_4_month_driver_phv", "Unique users resurrecting from 3-4 months ago"],
    [41, "resurrect_5_12_month_driver_phv", "Unique users resurrecting from 5-12 months ago"],
    [42, "driver_resurrected_phv", "Resurrected"],
    [44, "driver_churned_phv", "Churned (did not take trip this month)"],
    [46, "driver_inflow_phv", "Inflow"],
    [47, "rider_ft_all_time_phv", "First Time Riders (All Time)"],
    [48, "rider_unique_book_monthly_phv", "Unique users making a booking"],
    [49, "rider_unique_complete_monthly_phv", "Unique users completing a trip"],
    [50, "rider_unique_book_daily_phv", "Unique users making a booking(Daily Average)"],
    [51, "rider_unique_complete_daily_phv", "Unique users completing a trip(Daily Average)"],
    [52, "booking_per_user_phv", "Average number of bookings a unique user makes"],
    [53, "complete_per_user_phv", "Average number of trips a unique user makes"],
    [54, "duplicate_ratio_phv", "Duplicate Ratio"],
    [55, "rider_waiting_before_cancel_phv", "Average waiting time before cancellation (minutes)"],
    [56, "rider_cancellation_rate_phv", "Cancellation rate"],
    [57, "riders_ft_unique_phv", "First Time / Unique Completed Riders"],
    [58, "riders_repeated_phv", "Repeated Riders"],
    [59, "resurrect_2_month_rider_phv", "Unique users resurrecting from 2 months ago"],
    [60, "resurrect_3_4_month_rider_phv", "Unique users resurrecting from 3-4 months ago"],
    [61, "resurrect_5_12_month_rider_phv", "Unique users resurrecting from 5-12 months ago"],
    [62, "rider_resurrected_phv", "Resurrected"],
    [64, "rider_churned_phv", "Churned (did not take trip this month)"],
    [66, "rider_inflow_phv", "Inflow"],
  ] },
  { tab: "Bike HCM", source: "VN_HCM", rows: [
    [4, "rides_bike", "Rides"],
    [5, "demand_bike", "Demand"],
    [6, "match_rate_bike", "Match Rate"],
    [7, "completion_rate_bike", "Completion Rate"],
    [8, "daily_rides_bike", "Daily Average Completed Rides"],
    [9, "uncompleted_bike", "Uncompleted"],
    [10, "cater_rate_bike", "Cater Rate"],
    [11, "booked_riders_bike", "Unique users making a booking"],
    [12, "completed_riders_bike", "Unique users completing a trip"],
    [13, "daily_median_eta_bike", "Daily Average Median ETA"],
    [14, "driver_mau_bike", "MAU Driver"],
    [15, "completed_driver_bike", "Monthly Completed Driver"],
    [16, "total_approved_bike", "Total Approved Driver"],
    [17, "driver_online_daily_bike", "Daily Average Driver Online"],
    [18, "pinged_drivers_daily_bike", "Daily Average drivers who receive at least 1 ping"],
    [19, "completed_driver_daily_bike", "Daily Average Driver Complete"],
    [20, "online_mau_bike", "Online : MAU"],
    [21, "completed_online_bike", "Online vs completed %"],
    [22, "online_no_complete_bike", "Driver online but did not complete"],
    [23, "ride_per_driver_bike", "Average trips/completed driver"],
    [24, "driver_sign_up_bike", "Driver Sign-ups"],
    [25, "driver_sign_up_daily_bike", "Daily Sign ups"],
    [26, "driver_ft_all_time_bike", "Driver First-ride (All time)"],
    [27, "driver_ft_same_month_bike", "First Time Driver (Same Month Sign up)"],
    [28, "driver_sign_up_activation_rate_bike", "Same Month Activation Rate (FT/Signup)"],
    [29, "driver_approved_activation_rate_bike", "Same Month Activation Rate (FT/Approved)"],
    [30, "driver_approved_bike", "Driver Approved"],
    [31, "driver_same_month_approved_bike", "Driver Same Month Approved"],
    [32, "driver_average_online_hours_bike", "Average driver online hours"],
    [33, "driver_average_utilisation_hours_bike", "Average Driver utilisation hour"],
    [34, "ping_per_driver_daily_bike", "Average number of ping per driver (Daily Average)"],
    [35, "driver_waiting_before_cancel_bike", "Average waiting time before cancellation (minutes)"],
    [36, "driver_cancellation_rate_bike", "Cancellation rate by drivers"],
    [37, "drivers_ft_unique_bike", "First Time / Unique Completed Drivers"],
    [38, "drivers_repeated_bike", "Repeated Drivers"],
    [39, "resurrect_2_month_driver_bike", "Unique users resurrecting from 2 months ago"],
    [40, "resurrect_3_4_month_driver_bike", "Unique users resurrecting from 3-4 months ago"],
    [41, "resurrect_5_12_month_driver_bike", "Unique users resurrecting from 5-12 months ago"],
    [42, "driver_resurrected_bike", "Resurrected"],
    [44, "driver_churned_bike", "Churned (did not take trip this month)"],
    [46, "driver_inflow_bike", "Inflow"],
    [47, "rider_ft_all_time_bike", "First Time Riders (All Time)"],
    [48, "rider_unique_book_monthly_bike", "Unique users making a booking"],
    [49, "rider_unique_complete_monthly_bike", "Unique users completing a trip"],
    [50, "rider_unique_book_daily_bike", "Unique users making a booking(Daily Average)"],
    [51, "rider_unique_complete_daily_bike", "Unique users completing a trip(Daily Average)"],
    [52, "booking_per_user_bike", "Average number of bookings a unique user makes"],
    [53, "complete_per_user_bike", "Average number of trips a unique user makes"],
    [54, "duplicate_ratio_bike", "Duplicate Ratio"],
    [55, "rider_waiting_before_cancel_bike", "Average waiting time before cancellation (minutes)"],
    [56, "rider_cancellation_rate_bike", "Cancellation rate"],
    [57, "riders_ft_unique_bike", "First Time / Unique Completed Riders"],
    [58, "riders_repeated_bike", "Repeated Riders"],
    [59, "resurrect_2_month_rider_bike", "Unique users resurrecting from 2 months ago"],
    [60, "resurrect_3_4_month_rider_bike", "Unique users resurrecting from 3-4 months ago"],
    [61, "resurrect_5_12_month_rider_bike", "Unique users resurrecting from 5-12 months ago"],
    [62, "rider_resurrected_bike", "Resurrected"],
    [64, "rider_churned_bike", "Churned (did not take trip this month)"],
    [66, "rider_inflow_bike", "Inflow"],
  ] },
  { tab: "Performance Sheet HAN", source: "VN_HAN", rows: [
    [4, "rides", "Rides"],
    [5, "demand", "Demand"],
    [6, "match_rate", "Match Rate"],
    [7, "completion_rate", "Completion Rate"],
    [8, "daily_rides", "Daily Average Completed Rides"],
    [9, "uncompleted", "Uncompleted"],
    [10, "cater_rate", "Cater Rate"],
    [11, "booked_riders", "Unique users making a booking"],
    [12, "completed_riders", "Unique users completing a trip"],
    [13, "first_try_cater_rate", "First Try Cater Rate"],
    [14, "retry_initiation_rate", "Retry Initiation Rate"],
    [15, "retry_success_rate", "Retry Success Rate"],
    [16, "daily_median_eta", "Daily Average Median ETA (minutes)"],
    [17, "median_time_to_match_sec", "Median Time to Match (seconds)"],
    [18, "median_time_to_expire_sec", "Median Time to Expire (seconds)"],
    [19, "driver_mau", "MAU Driver"],
    [20, "completed_driver", "Monthly Completed Driver"],
    [21, "total_approved", "Total Approved Driver"],
    [22, "driver_online_daily", "Daily Average Driver Online"],
    [23, "pinged_drivers_daily", "Daily Average drivers who receive at least 1 ping"],
    [24, "completed_driver_daily", "Daily Average Driver Complete"],
    [25, "online_mau", "Online : MAU"],
    [26, "completed_online", "Online vs completed %"],
    [27, "online_no_complete", "Driver online but did not complete"],
    [28, "ride_per_driver", "Average trips/completed driver"],
    [29, "driver_sign_up", "Driver Sign-ups"],
    [30, "driver_sign_up_daily", "Daily Sign ups"],
    [31, "driver_ft_all_time", "Driver First-ride (All time)"],
    [32, "driver_ft_same_month", "First Time Driver (Same Month Sign up)"],
    [33, "driver_sign_up_activation_rate", "Same Month Activation Rate (FT/Signup)"],
    [34, "driver_approved_activation_rate", "Same Month Activation Rate (FT/Approved)"],
    [35, "driver_approved", "Driver Approved"],
    [36, "driver_same_month_approved", "Driver Same Month Approved"],
    [37, "driver_average_online_hours", "Average driver online hours"],
    [38, "driver_average_utilisation_hours", "Average driver utilisation hour"],
    [39, "ping_per_driver_daily", "Average number of ping per driver (Daily Average)"],
    [40, "driver_waiting_before_cancel", "Average waiting time before cancellation (minutes)"],
    [41, "driver_cancellation_rate", "Cancellation rate by drivers"],
    [42, "drivers_ft_unique", "First Time / Unique Completed Drivers"],
    [43, "drivers_repeated", "Repeated Drivers"],
    [44, "resurrect_2_month_driver", "Unique users resurrecting from 2 months ago"],
    [45, "resurrect_3_4_month_driver", "Unique users resurrecting from 3-4 months ago"],
    [46, "resurrect_5_12_month_driver", "Unique users resurrecting from 5-12 months ago"],
    [47, "driver_resurrected", "Resurrected"],
    [49, "driver_churned", "Churned (did not take trip this month)"],
    [51, "driver_inflow", "Inflow"],
    [52, "rider_mau", "MAU Rider"],
    [53, "rider_mau_demand", "MAUR:Demand"],
    [54, "rider_mau_rides", "MAUR:Ride"],
    [55, "r_d_ratio", "MAUR:MAUD Ratio"],
    [56, "rider_ft_all_time", "First Time Riders (All Time)"],
    [57, "rider_ft_same_month", "First Time Riders (Same month sign up)"],
    [58, "rider_unique_open_monthly", "Unique users opening our app"],
    [59, "rider_unique_search_monthly", "Unique users making a search"],
    [60, "rider_unique_book_monthly", "Unique users making a booking"],
    [61, "rider_unique_complete_monthly", "Unique users completing a trip"],
    [62, "rider_unique_open_daily", "Unique users opening our app(Daily Average)"],
    [63, "rider_unique_search_daily", "Unique users making a search(Daily Average)"],
    [64, "rider_unique_book_daily", "Unique users making a booking(Daily Average)"],
    [65, "rider_unique_complete_daily", "Unique users completing a trip(Daily Average)"],
    [66, "book_search_ratio_daily", "Book:Search Ratio"],
    [67, "booking_per_user", "Average number of bookings a unique user makes"],
    [68, "complete_per_user", "Average number of trips a unique user makes"],
    [69, "duplicate_ratio", "Duplicate Ratio"],
    [70, "rider_waiting_before_cancel", "Average waiting time before cancellation (minutes)"],
    [71, "rider_cancellation_rate", "Cancellation rate"],
    [72, "riders_ft_unique", "First Time / Unique Completed Riders"],
    [73, "riders_repeated", "Repeated Riders"],
    [74, "resurrect_2_month_rider", "Unique users resurrecting from 2 months ago"],
    [75, "resurrect_3_4_month_rider", "Unique users resurrecting from 3-4 months ago"],
    [76, "resurrect_5_12_month_rider", "Unique users resurrecting from 5-12 months ago"],
    [77, "rider_resurrected", "Resurrected"],
    [79, "rider_churned", "Churned (did not take trip this month)"],
    [81, "rider_inflow", "Inflow"],
  ] },
  { tab: "Car HAN", source: "VN_HAN", rows: [
    [4, "rides_phv", "Rides"],
    [5, "demand_phv", "Demand"],
    [6, "match_rate_phv", "Match Rate"],
    [7, "completion_rate_phv", "Completion Rate"],
    [8, "daily_rides_phv", "Daily Average Completed Rides"],
    [9, "uncompleted_phv", "Uncompleted"],
    [10, "cater_rate_phv", "Cater Rate"],
    [11, "booked_riders_phv", "Unique users making a booking"],
    [12, "completed_riders_phv", "Unique users completing a trip"],
    [13, "daily_median_eta_phv", "Daily Average Median ETA"],
    [14, "driver_mau_phv", "MAU Driver"],
    [15, "completed_driver_phv", "Monthly Completed Driver"],
    [16, "total_approved_phv", "Total Approved Driver"],
    [17, "driver_online_daily_phv", "Daily Average Driver Online"],
    [18, "pinged_drivers_daily_phv", "Daily Average drivers who receive at least 1 ping"],
    [19, "completed_driver_daily_phv", "Daily Average Driver Complete"],
    [20, "online_mau_phv", "Online : MAU"],
    [21, "completed_online_phv", "Online vs completed %"],
    [22, "online_no_complete_phv", "Driver online but did not complete"],
    [23, "ride_per_driver_phv", "Average trips/completed driver"],
    [24, "driver_sign_up_phv", "Driver Sign-ups"],
    [25, "driver_sign_up_daily_phv", "Daily Sign ups"],
    [26, "driver_ft_all_time_phv", "Driver First-ride (All time)"],
    [27, "driver_ft_same_month_phv", "First Time Driver (Same Month Sign up)"],
    [28, "driver_sign_up_activation_rate_phv", "Same Month Activation Rate (FT/Signup)"],
    [29, "driver_approved_activation_rate_phv", "Same Month Activation Rate (FT/Approved)"],
    [30, "driver_approved_phv", "Driver Approved"],
    [31, "driver_same_month_approved_phv", "Driver Same Month Approved"],
    [32, "driver_average_online_hours_phv", "Average driver online hours"],
    [33, "driver_average_utilisation_hours_phv", "Average Driver utilisation hour"],
    [34, "ping_per_driver_daily_phv", "Average number of ping per driver (Daily Average)"],
    [35, "driver_waiting_before_cancel_phv", "Average waiting time before cancellation (minutes)"],
    [36, "driver_cancellation_rate_phv", "Cancellation rate by drivers"],
    [37, "drivers_ft_unique_phv", "First Time / Unique Completed Drivers"],
    [38, "drivers_repeated_phv", "Repeated Drivers"],
    [39, "resurrect_2_month_driver_phv", "Unique users resurrecting from 2 months ago"],
    [40, "resurrect_3_4_month_driver_phv", "Unique users resurrecting from 3-4 months ago"],
    [41, "resurrect_5_12_month_driver_phv", "Unique users resurrecting from 5-12 months ago"],
    [42, "driver_resurrected_phv", "Resurrected"],
    [44, "driver_churned_phv", "Churned (did not take trip this month)"],
    [46, "driver_inflow_phv", "Inflow"],
    [47, "rider_ft_all_time_phv", "First Time Riders (All Time)"],
    [48, "rider_unique_book_monthly_phv", "Unique users making a booking"],
    [49, "rider_unique_complete_monthly_phv", "Unique users completing a trip"],
    [50, "rider_unique_book_daily_phv", "Unique users making a booking(Daily Average)"],
    [51, "rider_unique_complete_daily_phv", "Unique users completing a trip(Daily Average)"],
    [52, "booking_per_user_phv", "Average number of bookings a unique user makes"],
    [53, "complete_per_user_phv", "Average number of trips a unique user makes"],
    [54, "duplicate_ratio_phv", "Duplicate Ratio"],
    [55, "rider_waiting_before_cancel_phv", "Average waiting time before cancellation (minutes)"],
    [56, "rider_cancellation_rate_phv", "Cancellation rate"],
    [57, "riders_ft_unique_phv", "First Time / Unique Completed Riders"],
    [58, "riders_repeated_phv", "Repeated Riders"],
    [59, "resurrect_2_month_rider_phv", "Unique users resurrecting from 2 months ago"],
    [60, "resurrect_3_4_month_rider_phv", "Unique users resurrecting from 3-4 months ago"],
    [61, "resurrect_5_12_month_rider_phv", "Unique users resurrecting from 5-12 months ago"],
    [62, "rider_resurrected_phv", "Resurrected"],
    [64, "rider_churned_phv", "Churned (did not take trip this month)"],
    [66, "rider_inflow_phv", "Inflow"],
  ] },
  { tab: "Bike HAN", source: "VN_HAN", rows: [
    [4, "rides_bike", "Rides"],
    [5, "demand_bike", "Demand"],
    [6, "match_rate_bike", "Match Rate"],
    [7, "completion_rate_bike", "Completion Rate"],
    [8, "daily_rides_bike", "Daily Average Completed Rides"],
    [9, "uncompleted_bike", "Uncompleted"],
    [10, "cater_rate_bike", "Cater Rate"],
    [11, "booked_riders_bike", "Unique users making a booking"],
    [12, "completed_riders_bike", "Unique users completing a trip"],
    [13, "daily_median_eta_bike", "Daily Average Median ETA"],
    [14, "driver_mau_bike", "MAU Driver"],
    [15, "completed_driver_bike", "Monthly Completed Driver"],
    [16, "total_approved_bike", "Total Approved Driver"],
    [17, "driver_online_daily_bike", "Daily Average Driver Online"],
    [18, "pinged_drivers_daily_bike", "Daily Average drivers who receive at least 1 ping"],
    [19, "completed_driver_daily_bike", "Daily Average Driver Complete"],
    [20, "online_mau_bike", "Online : MAU"],
    [21, "completed_online_bike", "Online vs completed %"],
    [22, "online_no_complete_bike", "Driver online but did not complete"],
    [23, "ride_per_driver_bike", "Average trips/completed driver"],
    [24, "driver_sign_up_bike", "Driver Sign-ups"],
    [25, "driver_sign_up_daily_bike", "Daily Sign ups"],
    [26, "driver_ft_all_time_bike", "Driver First-ride (All time)"],
    [27, "driver_ft_same_month_bike", "First Time Driver (Same Month Sign up)"],
    [28, "driver_sign_up_activation_rate_bike", "Same Month Activation Rate (FT/Signup)"],
    [29, "driver_approved_activation_rate_bike", "Same Month Activation Rate (FT/Approved)"],
    [30, "driver_approved_bike", "Driver Approved"],
    [31, "driver_same_month_approved_bike", "Driver Same Month Approved"],
    [32, "driver_average_online_hours_bike", "Average driver online hours"],
    [33, "driver_average_utilisation_hours_bike", "Average Driver utilisation hour"],
    [34, "ping_per_driver_daily_bike", "Average number of ping per driver (Daily Average)"],
    [35, "driver_waiting_before_cancel_bike", "Average waiting time before cancellation (minutes)"],
    [36, "driver_cancellation_rate_bike", "Cancellation rate by drivers"],
    [37, "drivers_ft_unique_bike", "First Time / Unique Completed Drivers"],
    [38, "drivers_repeated_bike", "Repeated Drivers"],
    [39, "resurrect_2_month_driver_bike", "Unique users resurrecting from 2 months ago"],
    [40, "resurrect_3_4_month_driver_bike", "Unique users resurrecting from 3-4 months ago"],
    [41, "resurrect_5_12_month_driver_bike", "Unique users resurrecting from 5-12 months ago"],
    [42, "driver_resurrected_bike", "Resurrected"],
    [44, "driver_churned_bike", "Churned (did not take trip this month)"],
    [46, "driver_inflow_bike", "Inflow"],
    [47, "rider_ft_all_time_bike", "First Time Riders (All Time)"],
    [48, "rider_unique_book_monthly_bike", "Unique users making a booking"],
    [49, "rider_unique_complete_monthly_bike", "Unique users completing a trip"],
    [50, "rider_unique_book_daily_bike", "Unique users making a booking(Daily Average)"],
    [51, "rider_unique_complete_daily_bike", "Unique users completing a trip(Daily Average)"],
    [52, "booking_per_user_bike", "Average number of bookings a unique user makes"],
    [53, "complete_per_user_bike", "Average number of trips a unique user makes"],
    [54, "duplicate_ratio_bike", "Duplicate Ratio"],
    [55, "rider_waiting_before_cancel_bike", "Average waiting time before cancellation (minutes)"],
    [56, "rider_cancellation_rate_bike", "Cancellation rate"],
    [57, "riders_ft_unique_bike", "First Time / Unique Completed Riders"],
    [58, "riders_repeated_bike", "Repeated Riders"],
    [59, "resurrect_2_month_rider_bike", "Unique users resurrecting from 2 months ago"],
    [60, "resurrect_3_4_month_rider_bike", "Unique users resurrecting from 3-4 months ago"],
    [61, "resurrect_5_12_month_rider_bike", "Unique users resurrecting from 5-12 months ago"],
    [62, "rider_resurrected_bike", "Resurrected"],
    [64, "rider_churned_bike", "Churned (did not take trip this month)"],
    [66, "rider_inflow_bike", "Inflow"],
  ] },
];

// ---------- menu and triggers ----------

function onOpen() {
  SpreadsheetApp.getUi().createMenu(REPORT)
    .addItem('Update now', 'updateNow')
    .addItem('Fill a specific month…', 'fillSpecificMonth')
    .addItem('Remove a month…', 'removeMonth')
    .addItem('Enable auto-update', 'enableAutoUpdate')
    .addToUi();
}

function updateNow() { run_(true, null); }

function fillSpecificMonth() {
  const month = askMonth_('Fill a specific month', "Month, e.g. 2026-09. That month's column is overwritten.");
  if (month) run_(true, month);
}

// Installable onChange: runs on File > Import and on edits, and returns at once unless a tagged file has
// been imported.
function onChangeHandler(e) { run_(false, null); }

function enableAutoUpdate() {
  const ss = SpreadsheetApp.getActive();
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'onChangeHandler')
    .forEach(t => ScriptApp.deleteTrigger(t));
  ScriptApp.newTrigger('onChangeHandler').forSpreadsheet(ss).onChange().create();
  ss.toast(`Auto-update is on: import a ${REPORT} file as new sheets and the month fills itself.`, REPORT, 8);
}

function askMonth_(title, text) {
  const ui = SpreadsheetApp.getUi();
  const r = ui.prompt(title, text, ui.ButtonSet.OK_CANCEL);
  if (r.getSelectedButton() !== ui.Button.OK) return null;
  const month = r.getResponseText().trim();
  if (!/^\d{4}-\d{2}$/.test(month)) { ui.alert(`"${month}" is not a month like 2026-09.`); return null; }
  return month;
}

// ---------- run ----------

function run_(manual, monthOverride) {
  const ss = SpreadsheetApp.getActive();
  const lock = LockService.getDocumentLock();
  if (!lock.tryLock(manual ? 30000 : 1000)) return;
  let imp = null;
  try {
    const found = findImports_(ss);
    if (!found.length) {
      if (manual) throw new Error(`No tagged file found. Import the ${REPORT} file with File > Import > Upload > "Insert new sheet(s)". Files posted before the report/month_start tags were added can't be used.`);
      return;
    }
    imp = found[0];
    // A failed import stays with its error noted; rerun it from the menu, not on every later edit.
    if (!manual && String(errorCell_(imp).getValue()).indexOf('Not filled') === 0) return;
    if (found.length > 1)
      throw new Error(`${found.length} imported files are in the sheet (${found.map(f => f.sheet.getName()).join(', ')}). Delete the ones you don't need, then use ${REPORT} > Update now.`);
    if (imp.report !== REPORT)
      throw new Error(`This is a ${imp.report} file, but this spreadsheet takes ${REPORT}. Nothing was changed; delete the imported tab.`);

    const tz = ss.getSpreadsheetTimeZone() || Session.getScriptTimeZone() || 'Asia/Ho_Chi_Minh';
    const month = monthOverride || monthKey_(imp.monthValue, tz);
    const sources = readSources_(ss, imp, month);

    // Plan every tab before writing anything, so a problem stops the run with the sheet untouched.
    const plans = TABS.map(cfg => plan_(ss, cfg, sources[cfg.source], month));
    const notes = [];
    plans.forEach(p => notes.push(...apply_(p)));
    SpreadsheetApp.flush();

    imp.sheets.forEach(sh => ss.deleteSheet(sh));
    const added = plans.filter(p => p.isNew).length;
    const msg = `Filled ${month}${added ? ` (column added on ${added} tabs)` : ''}.` + (notes.length ? ' Check: ' + notes.join('; ') : '');
    if (manual) SpreadsheetApp.getUi().alert(msg);
    else ss.toast(msg, REPORT, 15);
  } catch (err) {
    const msg = 'Not filled: ' + err.message;
    if (imp) errorCell_(imp).setValue(msg).setFontColor('#c5221f').setFontWeight('bold');
    if (manual) SpreadsheetApp.getUi().alert(msg);
    else ss.toast(msg, REPORT, 30);
  } finally {
    lock.releaseLock();
  }
}

// ---------- imported file ----------

// Tabs whose row 1 holds a "report" tag: each is one imported file.
function findImports_(ss) {
  const found = [];
  ss.getSheets().forEach(sh => {
    const width = Math.min(sh.getLastColumn(), TAG_SCAN_COLS);
    if (width < 2) return;
    const rows = sh.getRange(1, 1, 2, width).getValues();
    const col = rows[0].findIndex(v => String(v).trim() === REPORT_KEY);
    if (col < 0 || col + 1 >= width) return;
    found.push({ sheet: sh, tagCol: col + 1, report: String(rows[0][col + 1]).trim(), monthValue: rows[1][col + 1] });
  });
  return found;
}

function errorCell_(imp) { return imp.sheet.getRange(4, imp.tagCol); }

function monthKey_(v, tz) {
  const s = v instanceof Date ? Utilities.formatDate(new Date(v.getTime() + 12 * 3600 * 1000), tz, 'yyyy-MM-dd') : String(v).trim();
  if (!/^\d{4}-\d{2}(-\d{2})?$/.test(s))
    throw new Error(`The file's ${MONTH_KEY} tag should be a date like 2026-09-01, got "${v}".`);
  return s.slice(0, 7);
}

// metric name -> value for VN_ALL and its companion tabs (same name suffix, e.g. "VN_HCM 2").
function readSources_(ss, imp, month) {
  const name = imp.sheet.getName();
  const suffix = name.indexOf(MAIN_TAB) === 0 ? name.slice(MAIN_TAB.length) : '';
  const tabs = { [MAIN_TAB]: imp.sheet };
  imp.sheets = [imp.sheet];
  COMPANIONS.forEach(c => {
    const sh = ss.getSheetByName(c + suffix);
    if (!sh) throw new Error(`The file's "${c}" tab wasn't imported (a city that failed comes in as an Error_ tab instead).`);
    tabs[c] = sh;
    imp.sheets.push(sh);
  });
  // The header above the values says which month the file is, e.g. Sep_2026: it must agree with the tag.
  const [y, m] = month.split('-').map(Number);
  const expect = `${MONTHS[m - 1]}_${y}`;
  const sources = {};
  Object.keys(tabs).forEach(key => {
    const sh = tabs[key];
    const head = String(sh.getRange(1, 2).getValue()).trim();
    if (head && head !== expect) throw new Error(`The "${sh.getName()}" tab is for ${head}, not ${expect}.`);
    const rows = sh.getRange(2, 1, Math.max(sh.getLastRow() - 1, 1), 2).getValues();
    const values = {};
    rows.forEach(([metric, v]) => { if (String(metric).trim()) values[String(metric).trim()] = v; });
    sources[key] = values;
  });
  return sources;
}

// ---------- month columns ----------

function nextMonth_(key) {
  let [y, m] = key.split('-').map(Number);
  if (++m > 12) { m = 1; y++; }
  return `${y}-${String(m).padStart(2, '0')}`;
}

function colLetter_(c) { let s = ''; for (; c > 0; c = Math.floor((c - 1) / 26)) s = String.fromCharCode(65 + (c - 1) % 26) + s; return s; }
function colIndex_(s) { return s.split('').reduce((n, ch) => n * 26 + ch.charCodeAt(0) - 64, 0); }

// 'YYYY-MM' -> column, from the year labels (row 1, merged across their months) and month names (row 3);
// plus the last month column, which months have numbers (first metric row not empty), and the Growth column.
function monthCols_(sh) {
  const width = sh.getLastColumn();
  const yearRange = sh.getRange(YEAR_ROW, 1, 1, width);
  const years = yearRange.getValues()[0].map(v => (typeof v === 'number' && v > 2000 ? v : null));
  yearRange.getMergedRanges().forEach(r => {
    const y = r.getCell(1, 1).getValue();
    if (typeof y === 'number') for (let c = r.getColumn(); c < r.getColumn() + r.getNumColumns(); c++) years[c - 1] = y;
  });
  const names = sh.getRange(MONTH_ROW, 1, 1, width).getValues()[0];
  const first = sh.getRange(FIRST_ROW, 1, 1, width).getValues()[0];
  const m = {}, filled = {};
  let lastKey = null, lastCol = null, growthCol = null;
  names.forEach((v, i) => {
    if (String(v).trim() === 'Growth') growthCol = i + 1;
    const mi = MONTHS.indexOf(String(v).trim());
    if (mi < 0 || !years[i]) return;
    const k = `${years[i]}-${String(mi + 1).padStart(2, '0')}`;
    m[k] = i + 1;
    if (first[i] !== '') filled[k] = true;
    if (!lastKey || k > lastKey) { lastKey = k; lastCol = i + 1; }
  });
  return { m, filled, lastKey, lastCol, growthCol };
}

function latestFilled_(mc, before) {
  const k = Object.keys(mc.filled).filter(x => !before || x < before).sort().pop();
  return k ? mc.m[k] : null;
}

// ---------- perf tabs ----------

function norm_(s) { return String(s).replace(/\s+/g, ' ').trim().toLowerCase(); }

function plan_(ss, cfg, src, month) {
  const sh = ss.getSheetByName(cfg.tab);
  if (!sh) throw new Error(`Tab "${cfg.tab}" not found.`);
  const maxRow = Math.max(...cfg.rows.map(r => r[0]));
  const labels = sh.getRange(1, LABEL_COL, maxRow, 1).getValues().map(r => r[0]);
  cfg.rows.forEach(([row, , label]) => {
    if (norm_(labels[row - 1]) !== norm_(label))
      throw new Error(`${cfg.tab.trim()} row ${row} should be "${label}" but is "${labels[row - 1]}". Has a row been added or removed?`);
  });
  const mc = monthCols_(sh);
  const col = mc.m[month] || null;
  if (!col && month !== nextMonth_(mc.lastKey))
    throw new Error(`${cfg.tab.trim()} has no ${month} column, and it isn't the month after the last one (${mc.lastKey}). Fill the months in order.`);
  const styleCol = latestFilled_(mc, month);
  if (!styleCol) throw new Error(`${cfg.tab.trim()}: no earlier filled month to copy formatting and formulas from.`);
  return { sh, cfg, src, month, col, isNew: !col, mc, styleCol, maxRow };
}

function apply_(p) {
  const col = p.isNew ? addMonth_(p) : p.col;
  const notes = fillMonth_(p, col);
  updateGrowth_(p.sh, p.maxRow, null);
  return notes;
}

// Adds the month's column after the last month (before Growth): formatting from the latest filled month,
// days, month name, and the year label.
function addMonth_(p) {
  const { sh, mc, styleCol, month, maxRow } = p;
  const last = mc.lastCol;
  const col = last + 1;
  sh.insertColumnAfter(last);
  sh.setColumnWidth(col, sh.getColumnWidth(styleCol));
  const height = maxRow - DAYS_ROW + 1;
  sh.getRange(DAYS_ROW, styleCol, height, 1)
    .copyTo(sh.getRange(DAYS_ROW, col, height, 1), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);
  const [y, m] = month.split('-').map(Number);
  sh.getRange(DAYS_ROW, col).setValue(new Date(y, m, 0).getDate());
  sh.getRange(MONTH_ROW, col).setValue(MONTHS[m - 1]);

  const yearCell = sh.getRange(YEAR_ROW, last);
  const yearBlock = yearCell.isPartOfMerge() ? yearCell.getMergedRanges()[0] : yearCell;
  if (month.slice(0, 4) === mc.lastKey.slice(0, 4)) {
    const start = yearBlock.getColumn();
    yearBlock.breakApart();
    sh.getRange(YEAR_ROW, start, 1, col - start + 1).merge();
  } else {
    // New year: a label like the last year's, set field by field (copying from a merged cell can be refused).
    const prev = sh.getRange(YEAR_ROW, yearBlock.getColumn());
    sh.getRange(YEAR_ROW, col).setValue(y)
      .setFontFamily(prev.getFontFamily()).setFontSize(prev.getFontSize())
      .setFontWeight(prev.getFontWeight()).setFontStyle(prev.getFontStyle()).setFontColor(prev.getFontColor())
      .setBackground(prev.getBackground()).setNumberFormat(prev.getNumberFormat())
      .setHorizontalAlignment(prev.getHorizontalAlignment()).setVerticalAlignment(prev.getVerticalAlignment());
  }
  return col;
}

// Writes the month: mapped rows from the file (left as they are where the file is empty), formula rows from
// the previous month, formatting from the latest filled month. Returns notes for the message.
function fillMonth_(p, col) {
  const { sh, cfg, src, styleCol, maxRow, isNew } = p;
  const height = maxRow - FIRST_ROW + 1;
  if (!isNew && col !== styleCol)
    sh.getRange(FIRST_ROW, styleCol, height, 1)
      .copyTo(sh.getRange(FIRST_ROW, col, height, 1), SpreadsheetApp.CopyPasteType.PASTE_FORMAT, false);

  const metricAt = {};
  cfg.rows.forEach(([row, metric]) => (metricAt[row] = metric));
  const history = [];
  for (let c = col - 1; c >= 1 && history.length < FORMULA_LOOKBACK; c--)
    history.push(sh.getRange(FIRST_ROW, c, height, 1).getFormulasR1C1().map(r => r[0]));

  const missing = [];
  let run = null;  // consecutive value rows, written together
  const flush = () => {
    if (!run) return;
    sh.getRange(run.start, col, run.values.length, 1).setValues(run.values.map(v => [v]));
    run = null;
  };
  for (let row = FIRST_ROW; row <= maxRow; row++) {
    const metric = metricAt[row];
    if (metric) {
      const v = src[metric];
      if (v === undefined) missing.push(metric);
      if (v !== undefined && v !== '' && v !== null) {
        if (!run) run = { start: row, values: [] };
        run.values.push(v);
        continue;
      }
      flush();
      continue;
    }
    flush();
    const f = history.map(h => h[row - FIRST_ROW]).find(x => x);
    if (f) sh.getRange(row, col).setFormulaR1C1(f);
  }
  flush();
  // Number formats: each row takes the format most of the recent filled months use, so one mis-formatted
  // cell in the latest month (e.g. a count formatted as %) isn't carried forward.
  const recent = Object.keys(p.mc.filled).filter(k => k < p.month).sort().reverse()
    .slice(0, FORMAT_MONTHS).map(k => p.mc.m[k]);  // all left of `col`, so an added column doesn't shift them
  const fmts = recent.map(c => sh.getRange(FIRST_ROW, c, height, 1).getNumberFormats().map(r => r[0]));
  sh.getRange(FIRST_ROW, col, height, 1).setNumberFormats(fmts[0].map((_, i) => [mostCommon_(fmts.map(f => f[i]))]));
  return missing.length ? [`${cfg.tab.trim()}: not in the file: ${missing.join(', ')}`] : [];
}

// The most frequent value; on a tie, the one that comes first (the most recent month).
function mostCommon_(list) {
  const n = {};
  list.forEach(x => (n[x] = (n[x] || 0) + 1));
  return list.reduce((best, x) => (n[x] > n[best] ? x : best), list[0]);
}

// Points the Growth column (if the tab has one) at the latest filled month vs the month before. `before`
// is the Growth formulas as they were before a column was deleted (their references are #REF! after).
function updateGrowth_(sh, maxRow, before) {
  const mc = monthCols_(sh);
  if (!mc.growthCol) return;
  const latest = latestFilled_(mc, null);
  if (!latest) return;
  const to = [colLetter_(latest), colLetter_(latest - 1)];
  const range = sh.getRange(FIRST_ROW, mc.growthCol, maxRow - FIRST_ROW + 1, 1);
  const formulas = before || range.getFormulas().map(r => r[0]);
  const ref = /(\$?)([A-Z]{1,3})(\$?\d+)(?![\d(])/g;
  formulas.forEach((f, i) => {
    if (!f) return;
    const letters = [...new Set([...f.matchAll(ref)].map(x => x[2]))];
    if (letters.length !== 2) return;
    const [hi, lo] = letters.sort((a, b) => colIndex_(b) - colIndex_(a));
    const map = { [hi]: to[0], [lo]: to[1] };
    const nf = f.replace(ref, (all, a, c, d) => (c in map ? a + map[c] + d : all));
    if (before || nf !== f) sh.getRange(FIRST_ROW + i, mc.growthCol).setFormula(nf);
  });
}

// ---------- undo ----------

// Takes a month back out. Where it is the last month column of a tab (the VN tabs) the column is deleted;
// where later months are already laid out (HCM, HAN) its mapped rows are cleared and formulas kept.
function removeMonth() {
  const ui = SpreadsheetApp.getUi();
  const month = askMonth_('Remove a month', 'Month, e.g. 2026-09.');
  if (!month) return;
  const ss = SpreadsheetApp.getActive();
  const hits = TABS.map(cfg => ({ cfg, sh: ss.getSheetByName(cfg.tab) }))
    .filter(o => o.sh)
    .map(o => ({ ...o, mc: monthCols_(o.sh) }))
    .filter(o => o.mc.m[month] && o.mc.filled[month]);
  if (!hits.length) { ui.alert(`Nothing to remove: no tab has numbers for ${month}.`); return; }
  const del = hits.filter(o => o.mc.m[month] === o.mc.lastCol).map(o => o.cfg.tab.trim());
  const ok = ui.alert(`Remove ${month}?`,
    `Deletes the ${month} column on ${del.join(', ') || 'no tab'}, and clears ${month}'s numbers on the other tabs. Typed-in rows such as downloads go with it.`,
    ui.ButtonSet.YES_NO);
  if (ok !== ui.Button.YES) return;
  hits.forEach(({ cfg, sh, mc }) => {
    const col = mc.m[month];
    const maxRow = Math.max(...cfg.rows.map(r => r[0]));
    const height = maxRow - FIRST_ROW + 1;
    if (col === mc.lastCol) {
      const before = mc.growthCol ? sh.getRange(FIRST_ROW, mc.growthCol, height, 1).getFormulas().map(r => r[0]) : null;
      sh.deleteColumn(col);
      updateGrowth_(sh, maxRow, before);
    } else {
      const mapped = {};
      cfg.rows.forEach(([row]) => (mapped[row] = true));
      const range = sh.getRange(FIRST_ROW, col, height, 1);
      const values = range.getValues(), formulas = range.getFormulas();
      range.setValues(values.map((v, i) => [mapped[FIRST_ROW + i] ? '' : (formulas[i][0] || v[0])]));
      updateGrowth_(sh, maxRow, null);
    }
  });
  ui.alert(`Removed ${month}.`);
}
