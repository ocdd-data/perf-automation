import os
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

  output_date = datetime.strptime(start_date, dt_format).strftime('%b_%Y')

  queries = [[
    Query(2856, params={'date': start_date}),
    Query(2857, params={'date': start_date}),
    Query(3001, params={'date': start_date}),
    Query(3004, params={'date': start_date}),
    Query(1581),
  ]]

  for query_list in queries:
    redash.run_queries(query_list)

  df1 = redash.get_result(2856)  # SG - All trips breakdown by product
  df2 = redash.get_result(2857)  # KH/TH/VN - All trips breakdown by type
  df3 = redash.get_result(3001)  # GMV
  df4 = redash.get_result(3004)  # KH - T1
  df5 = redash.get_result(1581)  # KH - T1 signup

  summary = df3.pivot_table(
    index='trip_month',
    columns='region',
    values=['total_finished_rides', 'gmv']
  )

  # Flatten the MultiIndex columns
  summary.columns = [f'{metric.lower()}_{region.lower()}' for metric, region in summary.columns]
  summary = summary.reset_index()

  # Reorder columns to match desired region order
  desired_order = ['sg', 'kh', 'vn', 'th', 'hk']
  ordered_cols = ['trip_month']
  for region in desired_order:
      ordered_cols.append(f'total_finished_rides_{region}')
      ordered_cols.append(f'gmv_{region}')
  summary = summary[ordered_cols]

  summary.insert(11, 'gmv_hk_sgd', pd.NA)
  summary.insert(9, 'gmv_th_sgd', pd.NA)
  summary.insert(7, 'gmv_vn_sgd', pd.NA)
  summary.insert(5, 'gmv_kh_sgd', pd.NA)

  sg_anytada = df1[df1['product_type'] == 'AnyTada'].copy()
  sg_taxi = df1[df1['product_type'] == 'Taxi'].copy()
  sg_phv = df1[df1['product_type'] == 'PH'].copy()
  sg_ev = df1[df1['product_type'] == 'EV'].copy()

  kh = df2[df2['region'] == 'KH'].copy()
  kh.insert(18, 'gmv_usd', pd.NA)
  kh['gmv_usd'] = kh['gmv'] / 4100

  kh_3w = kh[kh['car_type'] == '3W'].copy()
  kh_4w = kh[kh['car_type'] == '4W'].copy()
  kh_bike = kh[kh['car_type'] == 'BIKE'].copy()

  vn_car = df2[(df2['region'] == 'VN') & (df2['car_type'] == 'Car')].copy()
  vn_bike = df2[(df2['region'] == 'VN') & (df2['car_type'] == 'Bike')].copy()
  th_car = df2[(df2['region'] == 'TH') & (df2['car_type'] == 'Car')].copy()
  th_bike = df2[(df2['region'] == 'TH') & (df2['car_type'] == 'Bike')].copy()

  df4['trip_month'] = pd.to_datetime(df4['trip_month'])
  df5['sign_up'] = pd.to_datetime(df5['sign_up'])
  df5_trimmed = df5[['sign_up', 'e_tt']]
  t1 = df4.merge(df5_trimmed, left_on='trip_month', right_on='sign_up', how='left')
  t1 = t1.drop(columns=['sign_up'])

  # Save to Excel
  output_file = f'Monthly_Report_S_{output_date}.xlsx'

  with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    summary.to_excel(writer, sheet_name='Summary', index=False)
    sg_anytada.to_excel(writer, sheet_name='SG AnyTada', index=False)
    sg_taxi.to_excel(writer, sheet_name='SG Taxi', index=False)
    sg_phv.to_excel(writer, sheet_name='SG PHV', index=False)
    sg_ev.to_excel(writer, sheet_name='SG EV', index=False)
    kh_3w.to_excel(writer, sheet_name='KH 3W', index=False)
    kh_4w.to_excel(writer, sheet_name='KH 4W', index=False)
    kh_bike.to_excel(writer, sheet_name='KH Bike', index=False)
    vn_car.to_excel(writer, sheet_name='VN Car', index=False)
    vn_bike.to_excel(writer, sheet_name='VN Bike', index=False)
    th_car.to_excel(writer, sheet_name='TH Car', index=False)
    th_bike.to_excel(writer, sheet_name='TH Bike', index=False)
    t1.to_excel(writer, sheet_name='KH T1', index=False)

  slack = SlackBot()
  slack.uploadFile(output_file, 
                   os.getenv('SLACK_CHANNEL'),
                   f'Monthly Report S for {output_date}')

if __name__ == '__main__':
  main()