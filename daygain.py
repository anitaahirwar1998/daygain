import datetime
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
import yfinance as yf
from oauth2client.service_account import ServiceAccountCredentials
def stocknum(name, days=1):
    days = str(days) + "d"
    symbol = name + ".NS"
    try:
        data = yf.download(symbol, period=days, progress=False)
    except KeyError as e:
        print(f"Failed to download data for {symbol}: {str(e)}")
        return None

    if data.empty:
        print(f"No data available for {symbol}")
        return None

    df = data[['Open', 'High', 'Low', 'Close', 'Volume']].copy()
    df['Symbol'] = name
    return df
creds = ServiceAccountCredentials.from_json_keyfile_name('./gssep-399015-22a26cd898e7.json')
client = gspread.authorize(creds)
gsselect = client.open('News Database Sep 2023')
sheetsel_stock = gsselect.worksheet('Ups')
sel_stk_all_records = sheetsel_stock.get_all_records()
stk_raw_df=pd.DataFrame(sel_stk_all_records)
final_df=pd.DataFrame()
for i in list(stk_raw_df["Tag"]):
  df=stocknum(i)
  final_df=pd.concat([df,final_df],ignore_index=True)
final_df["RT"]=round((final_df["High"]-final_df["Open"])*100/final_df["Open"],2)
final_df_sort=final_df[['Symbol','Open', 'High', 'Low', 'Close', 'Volume',"RT"]].copy()
out_sheet=gsselect.worksheet("DayAnalysis")
out_sheet.clear()
set_with_dataframe(out_sheet, final_df_sort)
