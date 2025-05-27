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

  dt_format = "%Y-%m-%d"
  start_date = (datetime.today().replace(day=1) - timedelta(days=1)).replace(day=1).strftime(dt_format)
  end_date = (datetime.today().replace(day=1) - timedelta(days=1)).strftime(dt_format)

  DAYS_IN_MONTH = int(end_date.split("-")[2])

  output_date = datetime.strptime(start_date, dt_format).strftime("%b_%Y")

  # SG
  sg_queries = [[
    Query(2183, params={"date": start_date}),
    Query(2187, params={"date": start_date}),
    Query(2189, params={"date": start_date}),
    Query(2195, params={"date": start_date}),
    Query(2206, params={"date": start_date}),
    Query(2210, params={"date": start_date}),
    Query(2246, params={"date": start_date}),
    Query(2247, params={"date": start_date}),
    Query(2353, params={"date": start_date}),
    Query(2354, params={"date": start_date}),
  ]]

  for query_list in sg_queries:
    redash.run_queries(query_list)

  sg = pd.DataFrame()

  df1 = redash.get_result(2183)   # SG - All trips
  df2 = redash.get_result(2210)   # SG - ETA
  df3 = redash.get_result(2195)   # SG - Unique Trips
  df4 = redash.get_result(2187)   # SG - Rider Active Users
  df5 = redash.get_result(2189)   # SG - Riders FT
  df6 = redash.get_result(2246)   # SG - Riders Resurrected
  df7 = redash.get_result(2247)   # SG - Riders Churned
  df8 = redash.get_result(2206)   # SG - Drivers FT
  df9 = redash.get_result(2353)   # SG - Drivers Resurrected
  df10 = redash.get_result(2354)  # SG - Drivers Churned

  sg['rides'] = df1.completed
  sg['daily_rides'] = sg.rides/DAYS_IN_MONTH
  sg['eta'] = df2.daily_median_eta
  sg['cater_rate'] = sg.rides/df3.unique
  sg['rider_mau'] = df4.active_users
  sg['completed_riders'] = df1.completed_riders
  sg['rider_activated'] = df5.all_time
  sg['rider_resurrected'] = df6.resurrect_all
  sg['rider_churned'] = df7.churned
  sg['rider_inflow'] = df5.all_time + df6.resurrect_all - df7.churned

  sg['completed_driver'] = df1.completed_drivers
  sg['driver_activated'] = df8.all_time
  sg['driver_resurrected'] = df9.resurrect_all
  sg['driver_churned'] = df10.churned
  sg['driver_inflow'] = df8.all_time + df9.resurrect_all - df10.churned

  sg = sg.T
  sg.columns = [f"{output_date}"]

  # KH
  kh_queries = [[
    Query(1625),
    Query(2377),
    Query(2383),
    Query(2384),
    Query(2385),
    Query(2386),
    Query(2545),
    Query(2547),
    Query(2549),
    Query(2550),
    Query(2664),
  ]]

  for query_list in kh_queries:
    redash.run_queries(query_list)

  kh = pd.DataFrame()

  df1 = redash.get_result(1625)   # KH - All trips
  df2 = redash.get_result(2386)   # KH - ETA
  df3 = redash.get_result(2385)   # KH - Monthly and Daily Users
  df4 = redash.get_result(2384)   # KH - Active Rider Activity
  df5 = redash.get_result(2377)   # KH - First Trip Riders
  df6 = redash.get_result(2550)   # KH - Monthly Resurrected Riders
  df7 = redash.get_result(2545)   # KH - Churned Riders
  df8 = redash.get_result(2664)   # KH - Monthly Completed Drivers
  df9 = redash.get_result(2383)   # KH - First Trip Drivers
  df10 = redash.get_result(2547)  # KH - Monthly Resurrected Drivers
  df11 = redash.get_result(2549)  # KH - Churned Drivers

  kh['rides'] = df1.fin
  kh['daily_rides'] = kh.rides/DAYS_IN_MONTH
  kh['eta'] = df2.eta_fin
  kh['cater_rate'] = df3.cater_rate
  kh['rider_mau'] = df4.MAU
  kh['completed_riders'] = df3.finished_rider_count
  kh['rider_activated'] = df5.ft
  kh['rider_resurrected'] = df6.resurrect_all
  kh['rider_churned'] = df7.churned
  kh['rider_inflow'] = df5.ft + df6.resurrect_all - df7.churned

  kh['completed_driver'] = df8.finished_drivers
  kh['driver_activated'] = df9.all_first_drivers
  kh['driver_resurrected'] = df10.resurrect_all
  kh['driver_churned'] = df11.churned
  kh['driver_inflow'] = df9.all_first_drivers + df10.resurrect_all - df11.churned

  kh = kh.T
  kh.columns = [f"{output_date}"]

  # VN
  vn_queries = [[
    Query(4562, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4563, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4565, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4566, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4578, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(4582, params={"date_range": {"start": start_date, "end": end_date}}),
  ]]

  for query_list in vn_queries:
    redash.run_queries(query_list)

  vn = pd.DataFrame()

  df1 = redash.get_result(4562)   # VN - All trips
  df2 = redash.get_result(4566)   # VN - ETA
  df3 = redash.get_result(4563)   # VN - Unique Trips
  df4 = redash.get_result(4565)   # VN - Rider Active Users
  df5 = redash.get_result(4582)   # VN - Riders CAR
  df6 = redash.get_result(4578)   # VN - Drivers CAR


  vn['rides'] = df1.completed
  vn['daily_rides'] = vn.rides/DAYS_IN_MONTH
  vn['eta'] = df2.median_eta
  vn['cater_rate'] = vn.rides/df3.unique
  vn['rider_mau'] = df4.active_users
  vn['completed_riders'] = df1.completed_riders
  vn['rider_activated'] = df5.activated
  vn['rider_resurrected'] = df5.resurrected
  vn['rider_churned'] = df5.churned
  vn['rider_inflow'] = df5.activated + df5.resurrected - df5.churned

  vn['completed_driver'] = df1.completed_drivers
  vn['driver_activated'] = df6.activated
  vn['driver_resurrected'] = df6.resurrected
  vn['driver_churned'] = df6.churned
  vn['driver_inflow'] = df6.activated + df6.resurrected - df6.churned

  vn = vn.T
  vn.columns = [f"{output_date}"]

  # TH
  region = 'TH'

  timezone = TIMEZONES[region]
  region_str = REGIONS[region]

  th_queries = [[
    Query(2667, params={"region": region_str, "timezone": timezone, "date": start_date}),
    Query(3106, params={"region": region, "timezone": timezone, "date": start_date}),
    Query(3113, params={"region": region, "timezone": timezone, "date": start_date}),
    Query(3119, params={"region": region, "timezone": timezone, "date": start_date}),
    Query(3120, params={"region": region, "timezone": timezone, "date": start_date}),
    Query(3121, params={"region": region, "timezone": timezone, "date": start_date}),
  ]]

  for query_list in th_queries:
    redash.run_queries(query_list)

  th = pd.DataFrame()

  df1 = redash.get_result(3106)   # TH - All trips
  df2 = redash.get_result(3119)   # TH - ETA
  df3 = redash.get_result(3113)   # TH - Unique Trips
  df4 = redash.get_result(2667)   # TH - Rider Active Users
  df5 = redash.get_result(3120)   # TH - Riders CAR
  df6 = redash.get_result(3121)   # TH - Drivers CAR

  th['rides'] = df1.completed
  th['daily_rides'] = th.rides/DAYS_IN_MONTH
  th['eta'] = df2.daily_median_eta
  th['cater_rate'] = th.rides/df3.unique
  th['rider_mau'] = df4.active_users
  th['completed_riders'] = df1.completed_riders
  th['rider_activated'] = df5.activated
  th['rider_resurrected'] = df5.resurrected
  th['rider_churned'] = df5.churned
  th['rider_inflow'] = df5.activated + df5.resurrected - df5.churned

  th['completed_driver'] = df1.completed_drivers
  th['driver_activated'] = df6.activated
  th['driver_resurrected'] = df6.resurrected
  th['driver_churned'] = df6.churned
  th['driver_inflow'] = df6.activated + df6.resurrected - df6.churned

  th = th.T
  th.columns = [f"{output_date}"]

  # HK
  hk_queries = [[
    Query(3771, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3772, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3773, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3774, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3785, params={"date_range": {"start": start_date, "end": end_date}}),
    Query(3790, params={"date_range": {"start": start_date, "end": end_date}}),
  ]]

  for query_list in hk_queries:
    redash.run_queries(query_list)

  hk = pd.DataFrame()

  df1 = redash.get_result(3771)   # HK - Completed trips
  df2 = redash.get_result(3774)   # HK - ETA
  df3 = redash.get_result(3772)   # HK - Unique Trips
  df4 = redash.get_result(3773)   # HK - Rider Active Users
  df5 = redash.get_result(3790)   # HK - User Flow
  df6 = redash.get_result(3785)   # HK - Driver Flow

  hk['rides'] = df1.completed
  hk['daily_rides'] = hk.rides/DAYS_IN_MONTH
  hk['eta'] = df2.median_eta
  hk['cater_rate'] = hk.rides/df3.unique
  hk['rider_mau'] = df4.active_users
  hk['completed_riders'] = df1.completed_riders
  hk['rider_activated'] = df5.activated
  hk['rider_resurrected'] = df5.resurrected
  hk['rider_churned'] = df5.churned
  hk['rider_inflow'] = df5.activated + df5.resurrected - df5.churned

  hk['completed_driver'] = df1.completed_drivers
  hk['driver_activated'] = df6.activated
  hk['driver_resurrected'] = df6.resurrected
  hk['driver_churned'] = df6.churned
  hk['driver_inflow'] = df6.activated + df6.resurrected - df6.churned

  hk = hk.T
  hk.columns = [f"{output_date}"]

  output_file = f"Monthly_Report_J_{output_date}.xlsx"

  with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
    sg.to_excel(writer, sheet_name="SG", index=False)
    kh.to_excel(writer, sheet_name="KH", index=False)
    vn.to_excel(writer, sheet_name="VN", index=False)
    th.to_excel(writer, sheet_name="TH", index=False)
    hk.to_excel(writer, sheet_name="HK", index=False)

  slack = SlackBot()
  slack.uploadFile(output_file,
                   os.getenv("SLACK_CHANNEL"),
                   f"Monthly Report J for {output_date}")

if __name__ == "__main__":
  main()