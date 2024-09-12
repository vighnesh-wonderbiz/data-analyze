import pandas as pd
import numpy as np
import random
import sqlite3 as sql
from datetime import datetime, timedelta
import time
import os

class Utils:
    def __init__(self,input_d):
        self.d = datetime.strptime(input_d, '%Y-%m-%d %H:%M:%S')
        self.db_col = ["Index","EName","KPI1","KPI2","KPI3","KPI4","TimeStamp"]
        self.kpi = ["KPI1","KPI2","KPI3","KPI4"]
        try:
            self.conn = sql.connect("./el.db")
            self.cur = self.conn.cursor()
        except sql.Error as e:
            print(e)
            raise

    
    def generate_daily_data(self,days):
        e_list,k1,k2,k3,k4,t = [],[],[],[],[],[]
        daily_seconds = 24*60*60
        timestamps = pd.date_range(start='2024-09-01 00:00:00', periods=days *daily_seconds, freq='s')
        for j in range(0,daily_seconds * days):
            for i in range(1,11):
                e = 'EL0' if i < 10 else 'EL' + str(i)
                e_list.append(e)  
                t.append(timestamps[j])
                k1.append(random.uniform(48,53))
                k2.append(random.uniform(30,35))
                k3.append(random.uniform(1,2))
                k4.append(random.uniform(490,505))
        data = {
            "EName":e_list,
            "KPI1":k1,
            "KPI2":k2,
            "KPI3":k3,
            "KPI4":k4,
            "TimeStamp":t
        }
        df = pd.DataFrame(data)
        return df

    def load_data(self):
        if os.path.exists("./data.csv"):
            print("file exists")
            data = pd.read_csv("./data.csv")
        else:
            print("generating data...")
            data = self.generate_daily_data(15)
            data.to_csv("./data.csv")
            data.to_sql('Transactions',self.conn, if_exists='replace')
        return data

    def fetch_data_within_range(self,start,end):
        try:
            d = self.cur.execute(f"SELECT * from Transactions WHERE TimeStamp BETWEEN '{start}' AND '{end}';")
            selected_data = d.fetchall()
            return selected_data
        except sql.Error as e:
            print(e)
            return []
    def get_prev_week_range(self,curr_week_start):
        curr_week_start = curr_week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        prev_week_start = curr_week_start - timedelta(weeks=1)
        prev_week_end = curr_week_start
        return prev_week_start,prev_week_end
    
    def get_current_hour_range(self,current_hour):
        current_hour = current_hour.replace(minute=0, second=0, microsecond=0)
        prev_hour_end = current_hour - timedelta(hours=1)
        return current_hour,prev_hour_end
    
    def get_bar_data(self,curr_hour_df,prev_week_df,prev_2_week_df):
        res = {"x":"Week1,Week2,CurrentHour"}
        for i in self.kpi:
            l = []
            prev_week_avg = np.round(prev_week_df[i].mean(),4) if not np.isnan(prev_week_df[i].mean()) else 0.00
            prev_2_week_avg = np.round(prev_2_week_df[i].mean(),4) if not np.isnan(prev_2_week_df[i].mean()) else 0.00
            curr_hour_avg = np.round(curr_hour_df[i].mean(),4) if not np.isnan(curr_hour_df[i].mean()) else 0.00
            l.append(str(prev_week_avg))
            l.append(str(prev_2_week_avg))
            l.append(str(curr_hour_avg))
            s = ",".join(l)
            print(f"created {i} data")
            res[i]= s
        return res
    
    def generate_bar_data(self):
        
        start = time.time()
        current_hour, prev_hour_start = self.get_current_hour_range(self.d)
        current_week_start=self.d - timedelta(days=self.d.weekday())
        print("Current hour range: ", start - time.time())
        
        start = time.time()
        prev_week_start,prev_week_end = self.get_prev_week_range(current_week_start)
        prev_2_week_start,prev_2_week_end = self.get_prev_week_range(prev_week_start)
        print("week range: ", start - time.time())

        start = time.time()
        curr_hour_data = self.fetch_data_within_range(prev_hour_start,current_hour)
        prev_week_data = self.fetch_data_within_range(prev_week_start,prev_week_end)
        prev_2_week_data = self.fetch_data_within_range(prev_2_week_start,prev_2_week_end)
        print("fetched data: ", start - time.time())
        
        start = time.time()
        curr_hour_df = pd.DataFrame(curr_hour_data,columns=self.db_col)
        prev_week_df = pd.DataFrame(prev_week_data,columns=self.db_col)
        prev_2_week_df = pd.DataFrame(prev_2_week_data,columns=self.db_col)

        bar_data = self.get_bar_data(curr_hour_df,prev_week_df,prev_2_week_df)
        print("data generated: ", start - time.time())
        return bar_data

