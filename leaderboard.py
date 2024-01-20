"""
  little program to download yearly leaderboard data from garmin
  and transform it for use in gapminder
"""
from typing import Any, Dict
import os
import datetime as dt

import garth
import pandas as pd
from ratelimit import limits, sleep_and_retry

class Garmin:
    """Class for fetching data from Garmin Connect."""

    def __init__(self, email=None, password=None, is_cn=False):

        self.username = email
        self.password = password
        self.tokenstore = os.getenv("GARMINTOKENS") or "~/.garminconnect"

        self.is_cn = is_cn

        self.garmin_connect_user_settings_url = (
            "/userprofile-service/userprofile/user-settings"
        )

        self.garmin_connect_leaderboards_url = (
            "/userstats-service/leaderboard/wellness/connection"
        )

        self.garth = garth.Client(
            domain="garmin.cn" if is_cn else "garmin.com"
        )

        self.display_name = None
        self.full_name = None
        self.unit_system = None

    def connectapi(self, path, **kwargs):
        """get path data"""
        return self.garth.connectapi(path, **kwargs)

    def login(self):
        """Log in using Garth."""

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

        return True

    def get_leaderboard( self, startdate: str, enddate=None) -> Dict[str, Any]:
        """ Return available leaderboard for 'startdate' """
        if enddate is None:
            enddate = startdate
        url = f"{self.garmin_connect_leaderboards_url}"
        params = {"metricId": 29,
                  "startDate": str(startdate), "endDate": str(enddate),
                  "start": 1, "limit": 999}

        return self.connectapi(url, params=params)


class Steps:
    """ Manage the Steps data using pandas """

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
            print('get:', date.isoformat())
            steps = self.get_steps_for_date(date.date())
            steps_df = pd.DataFrame([steps])
            self.lb_df = pd.concat([self.lb_df, steps_df], ignore_index=True, sort=False)

        self.lb_df.set_index('date', inplace=True)
        self.lb_df = self.lb_df.reset_index()
        self.lb_df['date'] = pd.to_datetime(self.lb_df['date']).dt.date
        self.lb_df.to_csv(self.lb_file, index=False)

    @sleep_and_retry
    @limits(calls=1, period=1)  
    def get_steps_for_date(self, date):
        """ return only the name and steps """
        steps = {'date': date.isoformat()}
        lb = self.api.get_leaderboard(date.isoformat())  
        for entry in lb["allMetrics"]["metricsMap"]["WELLNESS_TOTAL_STEPS"]:
            value = entry["value"]
            fullname = entry["userInfo"]["fullname"]
            steps[fullname] = value
        return steps

    def save_gapminder(self, year=None, file='gapminder.csv'):
        """ transform the data to be used by gapminder """
        if self.lb_df is None:
            self.load_data()

        if year is None:
            year = dt.datetime.now().year - 1

        df = self.lb_df
        df['date'] = pd.to_datetime(df['date'])
        if year is not None:
            df = df[df['date'].dt.year == year]
        df = df.melt(id_vars=['date'], var_name='Person', value_name='Steps')
        df['day'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')
        df.drop('date', axis=1, inplace=True)
        df = df[['Person', 'day', 'Steps']]
        df = df.sort_values(by=['Person', 'day'])
        df['Steps'] = df.groupby('Person')['Steps'].cumsum()
        df = df.sort_values(by='day')
        unique_persons = df['Person'].unique()
        person_to_color = {person: i+1 for i, person in enumerate(unique_persons)}
        df['Color'] = df['Person'].map(person_to_color)
        df.to_csv(file, index=False)

# import getpass
# email = input("Enter email address: ")
# password = getpass("Enter password: ")

email = os.getenv("EMAIL")
password = os.getenv("PASSWORD")

api = Garmin(email, password)
api.login()

steps = Steps(garmin = api)
steps.update_data()
steps.save_gapminder()

print("fin")