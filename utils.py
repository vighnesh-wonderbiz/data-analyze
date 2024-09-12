import pandas as pd
import numpy as np
import random
import sqlite3 as sql
from datetime import datetime, timedelta
import os

class Utils:
    def __init__(self,input_d):
        self.d = datetime.strptime(input_d, '%Y-%m-%d %H:%M:%S')
        self.db_col = ["Index","EName","KPI1","KPI2","KPI3","KPI4","TimeStamp"]
        self.kpi = ["KPI1","KPI2","KPI3","KPI4"]
        try:
            self.conn = sql.connect("el.db")
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
    
    def get_KPI_avg(self,data):
        res = []
        for i in range(1,11):
            # el = 'EL0' if i < 10 else 'EL'
            el = "EL" + str(i)
            el_data = data[data["EName"] == el][self.kpi]
            kpi_means = {j: np.round(el_data[j].mean(), 4).item() if not np.isnan(el_data[j].mean()) else 0.00 for j in self.kpi}
            res.append({el: kpi_means})
        kpi_avg = np.round((data[self.kpi].mean()).infer_objects(copy=False).fillna(0),4).to_dict()
        res.append({"total": kpi_avg})
        return res
    
    def fetch_data_within_range(self,start,end):
        try:
            query = "SELECT * FROM Transactions WHERE TimeStamp BETWEEN ? AND ?;"
            self.cur.execute(query, (start, end))
            selected_data = self.cur.fetchall()
            return selected_data
        except sql.Error as e:
            print(e)
            return []

    def get_prev_week_range(self,curr_week_start):
        prev_week_start = curr_week_start - timedelta(weeks=1)
        prev_week_end = curr_week_start - timedelta(seconds=1)
        return prev_week_start,prev_week_end

    def get_current_week_range(self,current_date):
        current_week_start = current_date - timedelta(days=current_date.weekday())
        current_week_end = current_date + timedelta(days=6-current_date.weekday()) + timedelta(seconds=86399)
        return current_week_start,current_week_end
    
    def generate_weekly_data(self):
        current_week_start,current_week_end = self.get_current_week_range(self.d)
        prev_week_start,prev_week_end = self.get_prev_week_range(current_week_start)
        prev_2_week_start,prev_2_week_end = self.get_prev_week_range(prev_week_start)

        curr_week_data = self.fetch_data_within_range(current_week_start,self.d)
        week1_data = self.fetch_data_within_range(prev_week_start,prev_week_end)
        week2_data = self.fetch_data_within_range(prev_2_week_start,prev_2_week_end)
        
        curr_df = pd.DataFrame(curr_week_data,columns=self.db_col)
        week1_df =pd.DataFrame(week1_data,columns=self.db_col)
        week2_df =pd.DataFrame(week2_data,columns=self.db_col)
            
        curr_week = self.get_KPI_avg(curr_df)
        week1_avg = self.get_KPI_avg(week1_df)
        week2_avg = self.get_KPI_avg(week2_df)
        return [{"current week":curr_week,"previous week":week1_avg,"previous week 2":week2_avg}]

