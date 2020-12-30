import pandas as pd
import numpy as np
import os
import pickle
from dataguide_refresh import update_dataguide
from datetime import datetime, timedelta
from hist_price_dump import DumpData
from data_setting import SortStockData
from send_telegram import telegram_bot


class SeriesData:

    def __init__(self):
        
        self.check_holiday = list(pd.read_csv('2020_holiday.csv', index_col=0, engine='python').index)
        self.file_name = "series_data.xlsm"
        self.path = os.path.join(os.path.dirname( os.path.abspath(self.file_name) ) , self.file_name)


    def refresh_data(self, date = None,  refresh = True):

        ''' DataGuide - Time Series '''

        if date is None:
            date = datetime.today() - timedelta(1) # 전날
            date  = date.strftime("%Y-%m-%d")
            
        check_weekend = pd.date_range(end=date, freq="B", periods=1)[0].strftime("%Y-%m-%d")
        print(f"{date} starts and the target is {check_weekend}")

        if date not in self.check_holiday and date == check_weekend:
            
            if refresh:
                ### DataGuide 로그인 되어있어야 함
                update_dataguide(self.path, f"{self.file_name}!AllSheetRefresh", date.replace('-',''))
            
            sort_data = SortStockData(self.path)
            sort_data.sort_stock_data(sort_type='item', save_type=None)
            new_data = sort_data.new_data
            
            data_set = {}
            def organize_data(key):
                value = new_data[key]
                try:
                    value.index = list(map(lambda x: x.strftime("%Y-%m-%d"), value.index))
                except:
                    pass
                value = pd.DataFrame(value.loc[date]).T
                value.index = [date]
                data_set[key.split('(')[0].strip()]=value

            list(map(organize_data, new_data.keys()))
            date_check = list(map(lambda x: True if x==date else False, [value.index[0] for value in data_set.values()]))

            if not all(date_check):

                telegram_bot().send_msg(f'{date} 데이터 갱신 확인 필요. \n로그인 여부 확인해주세요. ')
                print(f'{date} 데이터 갱신 확인 필요 ')

            else: 
                print(date, 'will be updated')
                DumpData(data_set, [date]).dump_dataset()

        else: 
            print(date, ' is holiday')
            return



if __name__ == "__main__":

    SeriesData().refresh_data("2020-10-30", refresh=True)



