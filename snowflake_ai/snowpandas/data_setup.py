# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains DataSetup class for setting-up, initialization,
configuration, and tearing-down of data Environment
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.4.0"


import os
import glob
import sys
import importlib.util
import logging
import shutil

from pathlib import Path
from types import ModuleType
from typing import Optional, Dict, Union
from pandas import DataFrame as DF
from datetime import *
from dateutil.relativedelta import *
from pandas.tseries.offsets import BusinessDay

from snowflake_ai.common import ConfigType, ConfigKey
from snowflake_ai.common import DataConnect, AppConfig
from snowflake_ai.connect import FileConnect, \
    SnowConnect, DataFrameFactory



class DataSetup:
    """
    This class sets up, initializes, configures or tears down
    the data Environment

    To use this class, instantiate DataSetup with appropriate DataConnect
    as follows:

        >>> from snowflake_ai.common import FileConnect
        >>> from snowflake_ai.snowpandas import DataSetup
        ... 
        >>> conn: FileConnect = FileConnect()
        >>> setup: DataSetup = DataSetup(conn)
        >>> df = setup.load_data()
    """

    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))

    def __init__(
        self, 
        datasetup_key: str,
        connect: DataConnect, 
        data: Optional[Dict[str, DF]] = {},
        app_config: AppConfig = None
    ) -> None:
        self.datasetup_key = datasetup_key
        self._connect = connect
        self._data = data
        self.app_config = app_config

        # setup attributes
        self._init_setup_config()



    def get_script_name(self, app_config: Optional[AppConfig] = None) -> str:
        if not self._script_name and (app_config is not None):
            self.set_script_name(app_config)
        return self._script_name


    def set_script_name(self, app_config: AppConfig) -> str :
        p = os.path.join(app_config.root_path, app_config.script_home)
        sys.path.insert(0, os.path.expanduser(p))

        gk, k = AppConfig.split_group_key(self.datasetup_key)
        config : Dict = app_config.get_all_configs()\
                [ConfigType.DataSetups.value][gk][k]
        
        s : str = config.get(ConfigKey.SCRIPT.value)
        m : str = ''
        if s is not None and s:
            m = s.split('.')[0]
        self._logger.debug(
            f"DataSetup.set_script_name(): Script path [{p}]; "\
            f"Script file [{s}]; Module name [{m}].")
        self._script_name = m
        return m


    def load_module(self, name: Optional[str] = '') -> ModuleType:
        if not name:
            name = self._script_name
        return AppConfig.load_module(name)


    def get_connect(self) -> DataConnect:
        """
        Get the data connection for the setup.

        Returns:
            DataConnect: data connection object for the setup
        """        
        return self._connect
    

    def load_data(self, file_name: Optional[str] = None) -> DF:
        """
        Load pandas dataframe from local-csv file connection.

        Returns:
            DataFrame: Pandas dataframe
        """
        if isinstance(self._connect, FileConnect):
            if (file_name is None) or (file_name.strip() == ""):
                return DataFrameFactory.create_pdf(
                    "", self._connect
                )
            else:
                df: DF = DataFrameFactory.create_pdf(
                    file_name, self._connect
                )
                
                df.columns = df.columns.str.strip().str.lower()\
                    .str.replace(' ', '_')
                return df
        elif isinstance(self._connect, SnowConnect):
            pass
        else:
            raise ValueError(
                "DataSetup.load_data(): Error - Loading File requires "\
                "FileConnect!"
            )


    def _init_setup_config(self):
        conf_d = AppConfig.get_all_configs() if self.app_config is None \
                else self.app_config.get_all_configs()
        _, setup_d = AppConfig.get_group_item_config(
                self.datasetup_key,
                ConfigType.DataSetups.value,
                conf_d
            )
        self.setup_type = setup_d.get(ConfigKey.TYPE.value, "")
        self.type = setup_d.get(ConfigKey.TYPE.value, "")
        self.version = setup_d.get(ConfigKey.VERSION.value, "0.1.0")
        self.domain_env = setup_d.get(ConfigKey.DOMAIN_ENV.value, "dev")
        self.data_connect_ref = setup_d.get(ConfigKey.CONN_DATA.value, "")
        self.script = setup_d.get(ConfigKey.SCRIPT.value, "")
        self.class_name = setup_d.get(ConfigKey.CLASS_NAME.value, "")
        self.init_method = setup_d.get(ConfigKey.INIT.value, "init")
        self.clean_up = setup_d.get(ConfigKey.CLEAN_UP.value, "clean_up")



    def _load_file_data(self):
        pass


    def _load_snowflake_data(self):
        pass


    def get_data(self) -> Dict[str, DF]:
        """
        Get all dataframes for exploration and preparation.

        Returns:
            Dict: dictionary of dataframes
        """
        return self._data


    def clean_data(self, file_name: Optional[str] = None):
        self._data = {}
        self._connect.close_connection()


    @staticmethod
    def clean_up_directory(path:str, delete_dir=False):
        DataSetup._logger.debug(
            f"DataSetup.clean_up_directory(): Current_dir [{Path.cwd()}]")
        
        if os.path.isdir(path) and delete_dir:
                try:
                    shutil.rmtree(path)
                except OSError as e:
                    DataSetup._logger.error(
                        f"DataSetup.clean_up_directory(): Error - "\
                        f"{path} : {e.strerror}!"
                    )
        else:
            for file in glob.glob(path, recursive=True):
                if os.path.isfile(file):
                    try:
                        os.remove(file)
                        DataSetup._logger.debug(
                            f'DataSetup.clean_up_directory(): '\
                            f'[{file}] has been deleted.'
                        )
                    except OSError as e:
                        DataSetup._logger.error(
                            f'DataSetup.clean_up_directory(): Error - '\
                            f'{file} : {e.strerror}!'
                        )


    @staticmethod
    def get_calendar_date(
        month: int,  
        n_mon_days : int, 
        year: Optional[int] = 0, 
        n_years : Optional[int] = 0
    ) -> datetime.date:
        """
        Get n_th calendar date from inputted year and month. Use n_years
        for relative year.

        Parameters:
            month (int): month in the year, e.g, 1 - Jan, 2 - Feb, etc.
            n_mon_days (int): n-th calendar days of the month.
            year (int): year number. if obmitted or 0, current year is used.
            n_years (int): future or past n years based on input year value.

        Returns:
            date: date object from datetime
        """
        base_date : datetime = date.today()
        if year <= 0:
            year = base_date.year
        return datetime(
            year=year + n_years, month=month, day=n_mon_days
        ).date()


    @staticmethod
    def get_buinsess_date(
        month: int,  
        n_biz_days : int,
        year: Optional[int] = 0, 
        n_years : Optional[int] = 0
    ) -> datetime.date:
        """
        Get n_th business date from inputted year and month. For relative year,
        use n_years parameter.

        Parameters:
            month (int): month in the year, e.g, 1 - Jan, 2 - Feb, etc.
            n_biz_days (int): n-th business days of the month.
            year (int): year number. if obmitted or 0, current year is used.
            n_years (int): future or past n years based on input year value.

        Returns:
            date: date object from datetime
        """
        base_date : datetime = date.today()
        if year <= 0:
            year = base_date.year
        d = datetime(year=year+n_years, month=month, day=n_biz_days)
        mon_start = d + relativedelta(months=0, day=1)
        offset = BusinessDay(n=n_biz_days)
        return (mon_start + offset).date()


    @staticmethod
    def get_relative_months_calendar_days_date(
        n_months: int, 
        n_cal_days : Optional[int] = 0,
        base_date: Optional[Union[str, datetime, date]] = None
    ) -> datetime.date:
        """
        Get the date of the n-th calendar days (n_cal_days) of future n_months
        (if n_months is positive) or past n_months (if n_month is negative) 
        calculated based on the base input date.

        Parameters:
            n_months (int): Future n months from the current date if it is 
                positive; 0 if it is for the current month; negative if it
                is for the past months.
            n_cal_days (int): n-th's day of the month
            base_date (str, datetime, date): Optional date as the base for 
                the relative date. default is using today's date

        Returns:
            date: date object of the relative date
        """
        if base_date is None:
            base_date : datetime = date.today()
        if isinstance(base_date, date):
            base_date : datetime = datetime(
                year=base_date.year, 
                month=base_date.month,
                day=base_date.day,
            )
        elif isinstance(base_date, str):
            base_date: datetime = datetime.strptime(base_date, "%Y-%m-%d")

        if n_cal_days != 0 :
            mon_days = base_date + relativedelta(
                months=n_months, day=n_cal_days
            )
        else:
            mon_days = base_date + relativedelta(
                months=n_months
            )
        return mon_days.date()
    

    @staticmethod
    def get_relative_months_business_days_date(
        n_months: int, 
        n_biz_days : int,
        base_date: Optional[Union[str, datetime, date]] = None
    ) -> datetime.date:
        """
        Get the date of the n-th business days (n_biz_days) of future n months
        (n_months if positive) or past n months (if negative) calculated based
        on the base input date.

        Parameters:
            n_months (int): Future n months from the current date if it is 
                positive; 0 if it is for the current month; negative if it
                is for the past months.
            n_cal_days (int): n-th's business date of the month
            base_date (str, datetime, date): Optional date as the base for the 
                relative date. default is using the current today's date.

        Returns:
            date: date object of the relative date
        """
        if base_date is None:
            base_date : datetime = date.today()
        if isinstance(base_date, date):
            base_date : datetime = datetime(
                year=base_date.year, 
                month=base_date.month,
                day=base_date.day,
            )
        elif isinstance(base_date, str):
            base_date: datetime = datetime.strptime(base_date, "%Y-%m-%d")

        mon_start = base_date + relativedelta(months=n_months, day=1)
        offset = BusinessDay(n=n_biz_days)
        return (mon_start + offset).date()
    

    @staticmethod
    def get_relative_month_start_date(
        n_years: Optional[int] = 0,
        n_months: Optional[int] = 0,
        base_date: Optional[Union[str, datetime, date]] = None       
    ) -> datetime.date:
        """
        Get the 1st day of the month of future n years (n_years if positive)
        or past n years (if negative) and n months calculated based on the base
        input date's year and month.

        Parameters:
            n_years (int): Future n years from the current date if it is 
                positive; 0 if it is for the current year; negative if it
                is for the past years.
            n_months (int): Relativee n months from the current month if it is 
                positive; 0 if it is for the current month; negative if it
                is for the earlier months.
            base_date (str, datetime, date): Optional date as the base for the 
                relative date. default is using the current today's date.
        """
        if base_date is None:
            base_date : datetime = date.today()
        if isinstance(base_date, date):
            base_date : datetime = datetime(
                year=base_date.year, 
                month=base_date.month,
                day=base_date.day,
            )
        elif isinstance(base_date, str):
            base_date: datetime = datetime.strptime(base_date, "%Y-%m-%d")
        
        mon_start = base_date + relativedelta(
            years=n_years, months=n_months, day=1
        )
        return mon_start.date()
    

    @staticmethod
    def get_relative_month_end_date(
        n_years: Optional[int] = 0,
        n_months: Optional[int] = 0,
        base_date: Optional[Union[datetime, date]] = None       
    ) -> datetime.date:
        """
        Get the last day of the month of future n years (n_years if positive)
        or past n years (if negative) calculated based on the base input
        date.

        Parameters:
            n_years (int): Future n years from the current date if it is 
                positive; 0 if it is for the current year; negative if it
                is for the past years.
            base_date (str, datetime, date): Optional date as the base for the 
                relative date. default is using the current today's date.
        """
        if base_date is None:
            base_date : datetime = date.today()
        if isinstance(base_date, date):
            base_date : datetime = datetime(
                year=base_date.year, 
                month=base_date.month,
                day=base_date.day,
            )
        elif isinstance(base_date, str):
            base_date: datetime = datetime.strptime(base_date, "%Y-%m-%d")

        year_mon = base_date + relativedelta(years=n_years, months=n_months)
        nx_mon_start : date = DataSetup.get_relative_months_calendar_days_date(
            1, 1, year_mon
        )
        nx_mon_d = datetime(
            year=nx_mon_start.year, 
            month=nx_mon_start.month,
            day=nx_mon_start.day,       
        )
        mon_end : datetime = nx_mon_d + relativedelta(days=-1)
        return mon_end.date()
