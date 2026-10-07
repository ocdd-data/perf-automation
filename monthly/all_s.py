import os
import calendar
from datetime import datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

from utils.helpers import Query, Redash
from utils.slack import SlackBot


def main():
  load_dotenv()

  redash = Redash(key=os.getenv('REDASH_API_KEY'), base_url=os.getenv('REDASH_BASE_URL'))

  dt_format = '%Y-%m-%d'
  start_date = (datetime.today().replace(day=1) - timedelta(days=1)).replace(day=1).strftime(dt_format)

  start_dt = datetime.strptime(start_date, dt_format)
  end_date = start_dt.replace(day=calendar.monthrange(start_dt.year, start_dt.month)[1]).strftime(dt_format)
  date_range = {'Date Range': {'start': start_date, 'end': end_date}}

  output_date = start_dt.strftime('%b_%Y')

  queries = [[
    Query(2856, params={'date': start_date}),
    Query(2857, params={'date': start_date}),
    Query(3001, params={'date': start_date}),
    Query(3004, params={'date': start_date}),
    Query(1581),
    Query(7644, params={'date': start_date}),
    Query(7680, params={'date': start_date}),
    Query(6038, params=date_range),  # SG/KH/VN/TH/HK - Trips by payment method
    Query(7699, params=date_range),  # NY - Trips by payment method
  ]]

  for query_list in queries:
    redash.run_queries(query_list)

  df1 = redash.get_result(2856)  # SG - All trips breakdown by product
  df2 = redash.get_result(2857)  # KH/TH/VN - All trips breakdown by type
  df3 = redash.get_result(3001)  # GMV
  df4 = redash.get_result(3004)  # KH - T1
  df5 = redash.get_result(1581)  # KH - T1 signup
  df6 = redash.get_result(7644)  # NY - Total trips and GMV
  df7 = redash.get_result(7680)  # NY - Trips by product
  df8 = redash.get_result(6038)  # SG/KH/VN/TH/HK - Trips by payment method
  df9 = redash.get_result(7699)  # NY - Trips by payment method

  # ---- Summary: laid out to paste straight into the reporting sheet ----
  # Trip, GMV (SGD) and the unnamed exchange-rate columns are left blank:
  # the reporting sheet fills those itself.
  gmv_summary_df = pd.concat([df3, df6], ignore_index=True)
  gmv_summary_df['trip_month'] = pd.to_datetime(gmv_summary_df['trip_month']).dt.strftime('%Y-%m-%d')
  gmv_summary_df['region'] = gmv_summary_df['region'].str.upper()

  by_region = gmv_summary_df.groupby('region')[['total_finished_rides', 'gmv']].sum()

  def region_value(region, metric):
    return by_region.at[region, metric] if region in by_region.index else pd.NA

  # (header, value) in the reporting sheet's column order
  summary_cols = [('trip_month', start_date), ('Trip', pd.NA), ('GMV (SGD)', pd.NA)]
  for region, gmv_header, has_fx in [
    ('SG', 'GMV (SG)', False),
    ('KH', 'GMV (KH)', True),
    ('VN', 'GMV (VN)', True),
    ('TH', 'GMV (TH)', True),
    ('HK', 'GMV(HK)', True),
    ('NY', 'GMV(NY)', True),
  ]:
    summary_cols.append((f'Trip ({region})', region_value(region, 'total_finished_rides')))
    summary_cols.append((gmv_header, region_value(region, 'gmv')))
    if has_fx:
      summary_cols.append(('', pd.NA))

  summary = pd.DataFrame([[v for _, v in summary_cols]], columns=[h for h, _ in summary_cols])

  # ---- Breakdown: every region / car type stacked on one sheet ----
  sg = df1.rename(columns={'product_type': 'car_type'})
  sg.insert(0, 'region', 'SG')

  sea = df2.copy()
  kh_mask = sea['region'] == 'KH'
  sea.insert(sea.columns.get_loc('gmv') + 1, 'gmv_usd', pd.NA)
  sea.loc[kh_mask, 'gmv_usd'] = sea.loc[kh_mask, 'gmv'] / 4100

  ny = df7.rename(columns={'product_type': 'car_type'})
  ny.insert(0, 'region', 'NY')
  ny.insert(2, 'trip_month', start_date)

  # Row order of the breakdown sheet
  breakdown_order = [
    ('SG', 'AnyTada'), ('SG', 'Taxi'), ('SG', 'PH'), ('SG', 'EV'),
    ('KH', '3W'), ('KH', '4W'), ('KH', 'Bike'),
    ('VN', 'Car'), ('VN', 'Bike'),
    ('TH', 'Car'), ('TH', 'Bike'),
    ('NY', 'NY Ride'), ('NY', 'NY Saver'),
  ]

  # Column order follows the KH/VN/TH query (gmv_usd sits next to gmv);
  # columns only one source has (e.g. NY fees) go at the end.
  combined = pd.concat([sea, sg, ny], ignore_index=True)
  combined['trip_month'] = pd.to_datetime(combined['trip_month'], format='mixed').dt.strftime('%Y-%m-%d')
  rows = pd.DataFrame(breakdown_order, columns=['region', 'car_type'])
  breakdown = rows.merge(combined, on=['region', 'car_type'], how='inner')

  df4['trip_month'] = pd.to_datetime(df4['trip_month'])
  df5['sign_up'] = pd.to_datetime(df5['sign_up'])
  df5_trimmed = df5[['sign_up', 'e_tt']]
  t1 = df4.merge(df5_trimmed, left_on='trip_month', right_on='sign_up', how='left')
  t1['trip_month'] = t1['trip_month'].dt.strftime('%Y-%m-%d')
  t1 = t1.drop(columns=['sign_up'])

  # ---- Payment method: trips per bucket, then each bucket's share of trips ----
  payment_regions = [
    ('SG', ['Cash', 'Card', 'Others']),
    ('KH', ['Cash', 'Card', 'Others']),
    ('VN', ['Cash', 'Card', 'Others']),
    ('TH', ['Cash', 'Card', 'Others']),
    ('HK', ['Cash', 'Card', 'Others']),
    ('NY', ['Card', 'Mobile', 'Others']),
  ]
  bucket_names = {'CASH': 'Cash', 'CARD': 'Card', 'CREDITCARD': 'Card', 'MOBILE_PAY': 'Mobile', 'OTHER': 'Others'}

  payments = pd.concat([df8, df9], ignore_index=True)
  payments['bucket'] = payments['payment_bucket'].map(bucket_names)
  payment_trips = payments.groupby(['region_name', 'bucket'])['finished_trips'].sum()

  payment_row = [start_date]
  for region, buckets in payment_regions:
    trips = [int(payment_trips.get((region, b), 0)) for b in buckets]
    total = sum(trips)
    payment_row += trips + [t / total if total else None for t in trips]

  # Save to Excel
  output_file = f'Monthly_Report_S_{output_date}.xlsx'

  with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    summary.to_excel(writer, sheet_name='Summary', index=False)
    breakdown.to_excel(writer, sheet_name='Breakdown', index=False)
    t1.to_excel(writer, sheet_name='KH T1', index=False)

    # Payment Method: three header rows (region / Trips-% / bucket), like the reporting sheet
    book = writer.book
    ws = book.add_worksheet('Payment Method')
    head = book.add_format({'bold': True, 'align': 'center'})
    num = book.add_format({'num_format': '#,##0'})
    pct = book.add_format({'num_format': '0%'})

    ws.write(2, 0, 'trip_month', head)
    ws.write(3, 0, payment_row[0])
    col = 1
    for region, buckets in payment_regions:
      ws.merge_range(0, col, 0, col + 5, region, head)
      ws.merge_range(1, col, 1, col + 2, 'Trips', head)
      ws.merge_range(1, col + 3, 1, col + 5, '%', head)
      for i, b in enumerate(buckets + buckets):
        ws.write(2, col + i, b, head)
        value = payment_row[col + i]
        if value is not None:
          ws.write(3, col + i, value, num if i < 3 else pct)
      col += 6
    ws.set_column(0, 0, 12)

  slack = SlackBot()
  slack.uploadFile(output_file, 
                   os.getenv('SLACK_CHANNEL'),
                   f'Monthly Report S for {output_date}')

if __name__ == '__main__':
  main()
