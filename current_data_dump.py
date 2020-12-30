import pandas as pd
import numpy as np
import os
import pickle
from datetime import datetime, timedelta
from dataguide_refresh import update_dataguide
from hist_price_dump import DumpData
from send_telegram import telegram_bot


class NewestData:

    def __init__(self):

        self.check_holiday = list(pd.read_csv('2020_holiday.csv', index_col=0, engine='python').index)
        self.file_name = "current_data.xlsm"
        self.path = os.path.join(os.path.dirname( os.path.abspath(self.file_name) ) , self.file_name)

    def organize_current(self, data, today):

        dataset = {}
        data_cols = set(data.columns) - set(["Name"])
        for item_name in data_cols:

            item_data = data[item_name]
            if "순매수대금(개인)" in item_name:
                item_name = "개인순매수"
            elif "순매수대금(기관계)" in item_name:
                item_name = "기관순매수"
            elif "순매수대금(등록외국인)" in item_name:
                item_name = "등록외국인순매수"
            elif "순매수대금(외국인계)" in item_name:
                item_name = "외인순매수"

            else:
                item_name = item_name.split(".")[0].split("(")[0].strip()

            item_data = pd.DataFrame(item_data).T
            item_data.index = [today]
            dataset[item_name] = item_data

        return dataset


    def refresh_data(self, date = None, refresh=True):

        ''' DataGuide Cross Sectional '''

        if refresh:
            ### DataGuide 로그인 되어있어야 함
            dataguide = "C:\Program Files\FnGuide\DataGuide\FnDGLoader.exe"
            os.popen(dataguide)
            # subprocess.call
            # import ctypes - ctypes.windll.shell32.ShellExecuteA(0, 'open', dataguide, None, None, 1)
            update_dataguide(self.path, f"{self.file_name}!SheetRefresh")

        data = pd.read_excel(self.path, thousands=",", skiprows=[1,2,3,4], index_col=0)

        if date is None:
            cur_date, cur_time = data.columns[0].split(' ')[2:]
            Y, M, D = cur_date.split('-')
            h, m, s = cur_time.split(':')
            date = datetime(int(Y), int(M), int(D), int(h), int(m), int(s), 00000)
            check_date = datetime.today()

            if '0000' < cur_time.replace(':', '')[:4] < '0730':
                ## 자정 이후 단일가 매매 시작( 07:30 AM ) 이전에는 이전 날짜로 설정함
                date = date - timedelta(1)
                check_date = datetime.today() - timedelta(1)

            date = date.strftime("%Y-%m-%d")

        else:
            date = date

        data.columns = data.iloc[0]
        data.drop('Symbol', inplace=True)
        data_set = self.organize_current(data, date)
        k_dates = [date]

        # if date != check_date.strftime("%Y-%m-%d"):
        #
        #     telegram_bot().send_msg(f'{date} 데이터 갱신 확인 필요. \n로그인 여부 확인해주세요. ')
        #     print(f'{date} 데이터 갱신 확인 필요 ')

        # else:

        print(date, 'will be updated')
        check_weekend = pd.date_range(end=date, freq="B", periods=1)[0].strftime("%Y-%m-%d")

        if date not in self.check_holiday and date == check_weekend:
            return DumpData(data_set, k_dates).dump_dataset()

        else:
            print(date, ' is holiday')
            return


if __name__ == "__main__":


    """ 가장 최근 데이터 """
    NewestData().refresh_data(refresh=True)
