"""
  little program to download yearly leaderboard data from garmin
  and transform it for use in gapminder
"""
from typing import Any, Dict
import os
import datetime as dt
import math

import garth
import pandas as pd
from ratelimit import limits, sleep_and_retry

class Garmin:
    """ getting data from Garmin Connect"""

    def __init__(self, email=None, password=None, is_cn=False):

        self.username = email
        self.password = password
        self.tokenstore = os.getenv("GARMINTOKENS") or "~/.garminconnect"

        self.is_cn = is_cn

        self.garmin_connect_user_settings_url = (
            "/userprofile-service/userprofile/user-settings"
        )

        self.garmin_connect_leaderboard_activity_url = (
            "/userstats-service/leaderboard/activity/connection"
        )

        self.garmin_connect_leaderboard_wellness_url = (
            "/userstats-service/leaderboard/wellness/connection"
        )

        self.garth = garth.Client(
            domain="garmin.cn" if is_cn else "garmin.com"
        )

        self.display_name = None
        self.full_name = None
        self.unit_system = None

    @sleep_and_retry
    @limits(calls=3, period=1)  
    def connectapi(self, path, **kwargs):
        """get path data"""
        return self.garth.connectapi(path, **kwargs)

    def login(self):
        """log in using Garth"""

        try:
            print(f'connect using tokenstore:{self.tokenstore}')
            self.garth.load(self.tokenstore)
        except Exception:
            print('connect using email/password')
            self.garth.login(self.username, self.password)
            self.garth.dump(self.tokenstore)

        self.display_name = self.garth.profile["displayName"]
        self.full_name = self.garth.profile["fullName"]

        settings = self.garth.connectapi(self.garmin_connect_user_settings_url)
        self.unit_system = settings["userData"]["measurementSystem"]

    def get_leaderboard_activity( self, startdate: str, actTypeId: int, enddate=None) -> Dict[str, Any]:
        """ Return available leaderboard activity for 'startdate' """
        if enddate is None:
            enddate = startdate
        url = f"{self.garmin_connect_leaderboard_activity_url}"
        params = {"metricId": 17, "actTypeId": actTypeId,
                  "startDate": str(startdate), "endDate": str(enddate),
                  "start": 1, "limit": 999}
        return self.connectapi(url, params=params)

    def get_leaderboard_wellness( self, startdate: str, enddate=None) -> Dict[str, Any]:
        """ Return available leaderboard steps for 'startdate' """
        if enddate is None:
            enddate = startdate
        url = f"{self.garmin_connect_leaderboard_wellness_url}"
        params = {"metricId": 29, 
                  "startDate": str(startdate), "endDate": str(enddate),
                  "start": 1, "limit": 999}
        return self.connectapi(url, params=params)


class Leaderboard:
    """ manage the Steps data using pandas """

    def __init__(self, filename='leaderboard.csv', startdate=None, garmin=None):

        self.lb_file = filename
        if startdate is None:
            current_year = dt.datetime.now().year
            self.start_date = dt.datetime(current_year - 1, 1, 1)
        else:
            self.start_date = startdate

        self.next_date = None
        self.lb_df = None

        self.api = garmin

        self.activity_types = {
            'Cycling': 2,
            'Swimming': 26,
            'Walking': 9,
            'Running': 1
        }
        
    def load_data(self):
        """ load existing data and download from last spot"""
        try:
            self.lb_df = pd.read_csv(self.lb_file)
            self.lb_df['date'] = pd.to_datetime(self.lb_df['date'])
            self.next_date = self.lb_df['date'].max() + dt.timedelta(days=1)
            self.next_date = self.next_date.date()
        except FileNotFoundError:
            self.lb_df = pd.DataFrame()
            self.next_date = self.start_date

    def get_leaderboard_df(self):
        """ return the dataframe"""
        if self.lb_df is None:
            self.load_data()
        return self.lb_df

    def update_data(self):
        """ update the data by looping through it """
        if self.lb_df is None:
            self.load_data()

        yesterday = dt.date.today() - dt.timedelta(days=1)
        for date in pd.date_range(start=self.next_date, end=yesterday):
            print('get:', date) 
            distances = self.get_distances_for_date(date.date())
            steps = self.get_steps_for_date(date.date())
            distances.append(steps)
            result_df = pd.DataFrame(distances)
            self.lb_df = pd.concat([self.lb_df, result_df], ignore_index=True, sort=False)

        self.lb_df.set_index('date', inplace=True)
        self.lb_df = self.lb_df.reset_index()
        self.lb_df['date'] = pd.to_datetime(self.lb_df['date']).dt.date
        self.lb_df.to_csv(self.lb_file, index=False)

    def get_steps_for_date(self, date):
        """ return only the name and steps """
        leaderboard = self.api.get_leaderboard_wellness(date.isoformat())  
        result = {'date': date.isoformat(),
                  'metric': 'Steps' }
        for entry in leaderboard["allMetrics"]["metricsMap"]["WELLNESS_TOTAL_STEPS"]:
            value = entry["value"]
            fullname = entry["userInfo"]["fullname"]
            result[fullname] = value
        return result

    def get_distances_for_date(self, date):
        """ return only the name and steps """
        activities = []
        for name, actTypeId in self.activity_types.items():
            leaderboard = self.api.get_leaderboard_activity(date.isoformat(), actTypeId=actTypeId)
            result = {'date': date.isoformat(), 'metric': name }
            if "ACTIVITY_TOTAL_DISTANCE" in leaderboard["allMetrics"]["metricsMap"]:
                for entry in leaderboard["allMetrics"]["metricsMap"]["ACTIVITY_TOTAL_DISTANCE"]:
                    value = entry["value"]
                    fullname = entry["userInfo"]["fullname"]
                    result[fullname] = value
                activities.append(result)
        return activities

    def save_gapminder(self, year=None):
        """ transform the data to be used by gapminder """
        if self.lb_df is None:
            self.load_data()

        if year is None:
            year = dt.datetime.now().year - 1

        df = self.lb_df
        df['date'] = pd.to_datetime(df['date'])
        if year is not None:
            df = df[df['date'].dt.year == year]

        df = df.melt(id_vars=['date', 'metric'], var_name='Person', value_name='Value')
        df = df.set_index(['Person', 'date', 'metric'])['Value'].unstack().reset_index()
        df = df.fillna(0)
        df['day'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
        df.drop('date', axis=1, inplace=True)
        column_order = ['Person', 'day'] + [col for col in df.columns if col not in ['Person', 'day']]
        df = df[column_order]
        df = df.sort_values(by=['Person','day'])
        numeric_cols = df.columns.drop(['Person', 'day'])
        df = df.sort_values(by='day')
        for numeric_col in numeric_cols:
            df_subset = df[['Person', 'day', numeric_col]]
            df_subset = df_subset[df_subset[numeric_col] != 0]
            df_subset[numeric_col] = df.groupby(['Person'])[numeric_col].cumsum()
            df_subset.loc[:, 'Person'] = df_subset['Person'].str.split().str[0]
            unique_persons = df_subset['Person'].unique()
            person_to_color = {person: i+1 for i, person in enumerate(unique_persons)}
            df_subset['Color'] = df_subset['Person'].map(person_to_color)
            file_name = f"gapminder_{numeric_col}.csv"
            df_subset.to_csv(file_name, index=False)

# import getpass
# email = input("Enter email address: ")
# password = getpass("Enter password: ")

email = os.getenv("EMAIL")
password = os.getenv("PASSWORD")

api = Garmin(email, password)
api.login()

leaderboard = Leaderboard(garmin = api)
leaderboard.update_data()
# TODO fix this
leaderboard.save_gapminder()

print("fin")
