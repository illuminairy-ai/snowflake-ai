# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains a default base application
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.3.0"


import os
from os.path import exists
import sys
from typing import Optional, Union, Dict
from typing import List, Tuple, Any
import logging
from types import ModuleType

from pandas._typing import (
    Dtype,
    Axes,
)
from pandas import DataFrame as DF

from snowflake.snowpark.dataframe import DataFrame as SDF
from snowflake.snowpark.types import StructType
from snowflake.snowpark import Session

from snowflake_ai.common import AppConfig, DataConnect
from snowflake_ai.common import ConfigKey, ConfigType, AppType
from snowflake_ai.connect import ConnectManager, SnowConnect
from snowflake_ai.connect import DataFrameFactory as DFF
from snowflake_ai.snowpandas import SetupManager, DataSetup
from snowflake_ai.mlops import FlowContext, Pipeline


class BaseApp(AppConfig):
    """
    This class represents a base application's configurations and 
    its corresponding application. Since it is subclass of AppConfig,
    it has the following config directory bootstrapping precedence:

    1) input custom directory 
    2) .snowflake_ai/conf subdir under current directory 
    3) .snowflake_ai/conf subdir under user_home directory
    4) current directory
    5) home directory
    6) conf subdir under snowflake_ai library installation root dir

    Assuming a base application app_1 under a default group named as
        business_group is configured, this app can be created as:

        >>> from snowflake_ai.apps import BaseApp
        ... 
        ... # initialize application config for app_1
        >>> app = BaseApp("business_group.app_1")
        """

    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))


    def __init__(
        self,
        app_key : str,
        config_dir : Optional[Union[str, None]] = None, 
        config_file : Optional[Union[str, None]] = None
    ):
        """
        Create a base app based on specific AppConfig object.

        Args:
            app_key: A string representing the application; it can have 
                the format of <app_group>.<application> ('<', '>' not 
                included)
            config_dir: directory path string for custom config load
            config_file: file name string for custom config load
        """
        super().__init__(app_key, config_dir, config_file)
        self.logger = self._logger
        if self.type not in [AppType.Default.value, AppType.Console,
                AppType.Notebook.value, AppType.Streamlit.value]:
            raise TypeError(
                "Base.init(): Application type configuration Error!"
            )
        
        # get default snowflake connection
        self.snow_connect = ConnectManager.create_default_snow_connect(self)
        self.default_setup = SetupManager.create_default_snow_setup(
            self, self.snow_connect
        )
        self.setup_module = self.default_setup.load_module()
        self.default_context = FlowContext()
        self.app_namespaces = self.get_app_namespaces()
        self.default_context.data["app_namespace"] = self.app_namespaces[0]
        self.default_context.data["app_name"] = self.app_namespaces[1]
        self.default_context.debug = False

        # overridden by children    
        session: Session = self.get_snowflake_session({})
        if session is not None:
            self.default_context.session = session
            self.set_traditional_western_week_policy(session)
            self.default_pipeline = Pipeline(self.default_context)
        else:
            print(f"BaseApp.init(): Got session [{session}]")


    def set_traditional_western_week_policy(
        self, 
        session: Optional[Session] = None
    ):
        if session is None:
            session = self.get_snowflake_session()
        session.sql("ALTER SESSION SET WEEK_OF_YEAR_POLICY=1, WEEK_START=7")\
            .collect()


    def create_df(
        self,
        data: Any, 
        columns: Optional[
            Union[StructType, Tuple, List[str], Axes, None] 
        ] = None,
        index: Optional[Axes] = None,
        dtype: Optional[Dtype] = None,
    ) -> Union[SDF, DF]:
        if self.default_context.session is None:
            return DFF.create_df(data, None, columns, index, dtype)
        else:
            return DFF.create_df(
                data, self.default_context.session, columns, index, dtype
            )


    def get_default_snow_connect(self) -> SnowConnect:
        return self.snow_connect
    

    def load_setup_module(self) -> ModuleType:
        return self.default_setup.load_module()


    def get_default_setup(self) ->DataSetup:
        return self.default_setup


    def get_snowflake_session(self, ctx: Dict = {}) -> Session:
        session = None
        if self.snow_connect is not None:
            session = self.snow_connect.get_connection()
            if session is None:
                session = self.snow_connect.create_user_session(ctx)

        return session


    def run_default_pipeline(self):
        self.default_pipeline.run()
    

