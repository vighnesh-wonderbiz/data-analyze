import pandas as pd
import numpy as np
import sqlite3 as sql
from datetime import datetime, timedelta
import config as cfg

class Utils:
    def __init__(self,input_d):
        try:
            self.conn = sql.connect(cfg.db_path)
            self.cur = self.conn.cursor()
            self.d = input_d
            self.db_col = ["Index","EName",*cfg.kpi,"TimeStamp"]
            self.plant_data_col = ["Week","WeekStart","WeekEnd","IsUpdated",*cfg.kpi]
            print("db connected")
        except sql.Error as e:
            print(e)
            raise

    def get_date(self,date):
        currDate = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
        return currDate
    
    def fetch_data_within_range(self,start,end):
        try:
            data = self.cur.execute(f"SELECT EName, {",".join(cfg.kpi)}, TimeStamp from Transactions WHERE TimeStamp BETWEEN '{start}' AND '{end}';")
            selected_data = data.fetchall()
            df = pd.DataFrame(selected_data,columns=["EName",*cfg.kpi,"TimeStamp"])
            return df
        except sql.Error as e:
            print(e)
            return []
    
    def get_current_week_range(self,current_date):
        current_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0) 
        current_week_start = current_date - timedelta(days=current_date.weekday()) if cfg.is_monday else current_date
        current_week_end = current_week_start  + timedelta(days=6, hours=23, minutes=59, seconds=59)
        return current_week_start,current_week_end

    def get_prev_week_range(self,curr_week_start):
        curr_week_start = curr_week_start.replace(hour=0, minute=0, second=0, microsecond=0)
        prev_week_start = curr_week_start - timedelta(weeks=1)
        prev_week_end = curr_week_start - timedelta(seconds=1)
        return prev_week_start,prev_week_end

    def get_current_hour_range(self,current_hour):
        current_hour = current_hour.replace(minute=0, second=0, microsecond=0)
        prev_hour_end = current_hour - timedelta(hours=1)
        return current_hour,prev_hour_end

    def table_exists(self,table_name):
        query = "SELECT * FROM sqlite_master WHERE type='table' AND name=?;"
        self.cur.execute(query, (table_name,))
        result = self.cur.fetchone()
        return result is not None

    def check_week_data(self,):
        query = f'''SELECT IsUpdated, {", ".join(cfg.kpi)} FROM PlantData'''
        row = self.cur.execute(query)
        rows = row.fetchall()
        df = pd.DataFrame(rows,columns=["IsUpdated",*cfg.kpi])
        return True if df["IsUpdated"].sum() >= len(df) else False

    def calculate_mean(self,data):
        kpi_avg = np.round((data[cfg.kpi].mean()).infer_objects(copy=False).fillna(0),5).tolist()
        return kpi_avg

    def fetch_weekly_data(self,d):
        print("Fetching week data")
        df = pd.DataFrame(columns=self.plant_data_col)
        current_week_start,_= self.get_current_week_range(d)
        for i in cfg.weeks:
            if i != "CurrentHour":
                data_l = []
                prev_week_start,prev_week_end = self.get_prev_week_range(current_week_start)
                print(f"Fetching {i}",prev_week_start,prev_week_end)
                prev_week_data = self.fetch_data_within_range(prev_week_start,prev_week_end)
                calculated_mean = self.calculate_mean(prev_week_data)
                data_l.extend([i,prev_week_start,prev_week_end,True])
                data_l.extend(calculated_mean)
                current_week_start = prev_week_start
                df.loc[len(df)] = data_l
                print(f"{i} fetched")
        return df

    def fetch_hourly_data(self,d):
        df = pd.DataFrame(columns=self.plant_data_col)
        data_l = []
        current_hour, prev_hour_start = self.get_current_hour_range(d)
        current_week_start,current_week_end= self.get_current_week_range(d)
        curr_hour_data = self.fetch_data_within_range(prev_hour_start,current_hour)
        calculated_mean = self.calculate_mean(curr_hour_data)
        data_l.extend(['CurrentHour',current_week_start,current_week_end,True])
        data_l.extend(calculated_mean)
        df.loc[len(df)] = data_l
        return df
    
    def current_h(self):
        try:
            q = '''SELECT * FROM PlantData WHERE WEEK = "CurrentHour"'''
            data = self.cur.execute(q)
            row = data.fetchone()
            if row and len(row) > 0:
                end_date = pd.Series(row)
                return end_date[3]
            return ""
        except sql.Error as e:
            print(e)
            raise

    def is_weekend(self,d):
        current_hour, _ = self.get_current_hour_range(d)
        db_hour = self.current_h()
        curr_weekend = self.get_date(db_hour)
        return current_hour >= curr_weekend
    
    def generate_plant_data(self):
        cols = []
        for i in cfg.kpi:
            cols.append(i + " REAL Default 0")
        query1 = f'''
            CREATE TABLE IF Not Exists PlantData
            (
                Id INTEGER PRIMARY KEY,
                Week TEXT NOT NULL,
                WeekStart DATETIME DEFAULT "1999-01-01 00:00:00",
                WeekEnd DATETIME DEFAULT "1999-01-01 00:00:00",
                IsUpdated BOOLEAN DEFAULT FALSE,
                {", ".join(cols)}
            )
        '''
        
        w = []
        for i in cfg.weeks:
            w.append(f"('{i}')")

        query2 = f'''
            INSERT INTO PlantData (Week) VALUES {", ".join(w)}
        '''

        try:
            self.cur.execute(query1)
            print("Table created")
            self.cur.execute(query2)
            self.conn.commit()
            print("Data inserted")
        except Exception as e:
            print(e)
            self.conn.rollback()
            raise

    def get_data_hourly(self):
        d = self.get_date(self.d)
        if not self.table_exists("PlantData"):
            self.generate_plant_data()
        
        curr_df = self.fetch_hourly_data(d)
        print(self.is_weekend(d))
        
        if self.is_weekend(d) or not self.check_week_data():
            week_df = self.fetch_weekly_data(d)
            df = pd.concat([week_df,curr_df],ignore_index=True)
            self.dump_data(df)
        self.dump_data(df)
        
    
    def dump_data(self,df):
        try:
            if len(df) > 1:
                query_1 = 'DELETE FROM PlantData'
                self.cur.execute(query_1)
                df.to_sql("PlantData",self.conn,if_exists='replace')
            else:
                query_1 = '''
                    DELETE FROM PlantData
                    WHERE Week = 'CurrentHour';
                '''
                self.cur.execute(query_1)
                df.to_sql('PlantData',self.conn,if_exists='append')
            self.conn.commit()
        except sql.Error as e:
            print(e)
            self.conn.rollback()
            raise


    def select_bar_data(self):
        try:
            _ = '''SELECT * FROM PlantData;'''
            rows = self.cur.execute(_)
            data = rows.fetchall()
            df = pd.DataFrame(data,columns=["Index",*self.plant_data_col])
            return df
        except sql.Error as e:
            print(e)
            raise

    def convert_string(self,l):
        temp = []
        for i in l:
            temp.append(str(i))
        l_str = ", ".join(temp)
        return str(l_str)
    
    def fetch_bar_data(self):
        db_data = self.select_bar_data()
        bar_data = db_data.tail(len(cfg.weeks))
        bar_data_dict = {
            'x':", ".join(cfg.weeks)
        }
        for i in range(0,len(cfg.weeks)):
            bar_data_dict[f"{cfg.weeks[i]}"] = self.convert_string(bar_data[[*cfg.kpi]].loc[i].tolist())
        return bar_data_dict
