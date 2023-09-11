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
__version__ = "0.5.1"


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

from snowflake_ai.common import AppConfig
from snowflake_ai.common import ConfigManager
from snowflake_ai.common import ConfigKey, ConfigType, AppType
from snowflake_ai.connect import ConnectManager, SnowConnect
from snowflake_ai.connect import DataFrameFactory as DFF
from snowflake_ai.snowpandas import SetupManager, DataSetup, SnowSetup
from snowflake_ai.mlops import FlowContext, Pipeline


class BaseApp:
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

    # app_key : app specific config dictionary
    _base_apps : Dict [str, Dict] = {}


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
        self.logger = self._logger
        ac: AppConfig = ConfigManager.get_app_config(
                app_key, config_dir, config_file)
        self.logger.debug(
                f"BaseApp.init(): AppConfig - AppKey [{ac.app_key}]; "\
                f"Version [{ac.version}]; Root_path [{ac.root_path}];"\
                f" Script_home [{ac.script_home}].")
        self.appconf = ac
        self.all_configs = AppConfig.get_all_configs()
        self.app_key = ac.app_key
        self.root_path = ac.root_path
        self.app_configs = ac.app_config
        self.app_name = ac.app_name
        self.app_group = ac.app_group
        self.app_short_name = ac.app_short_name
        self.type = ac.type
        self.group_config = ac.group_config
        self.version = ac.version
        self.domain_env = ac.domain_env
        self.app_path = ac.app_path
        self.script_home = ac.script_home
        self.app_base_config = ac.app_base_config
        self.app_connect_refs = ac.app_connect_refs
        self.ml_ops_refs = ac.ml_ops_refs
        self.oauth_connect_configs = ac.oauth_connect_configs
        self.data_connect_configs = ac.data_connect_configs
        self.data_setup_refs = ac.data_setup_refs
        self.ml_pipeline_refs = ac.ml_pipeline_refs
        
        if self.type not in [AppType.Default.value, AppType.Console,
                AppType.Notebook.value, AppType.Streamlit.value]:
            raise TypeError(
                "Base.init(): Application type configuration Error!"
            )
        
        # get default snowflake connection
        self.snow_connect = ConnectManager.create_default_snow_connect(
                self.appconf)
        self.default_setup: SnowSetup = SetupManager.create_default_snow_setup(
                self.appconf, self.snow_connect)
        self.setup_module = self.load_module(self.default_setup.script)
        self.default_context = FlowContext()
        self.app_namespaces = self.get_app_namespaces()
        self.default_context.data["app_namespace"] = self.app_namespaces[0]
        self.default_context.data["app_name"] = self.app_namespaces[1]
        self.default_context.debug = False

        # overridden by children    
        session: Session = self.get_snowflake_session({})
        print(f"[TL] base app get session => {session}")
        if session is not None:
            self.default_context.session = session
            self.set_traditional_western_week_policy(session)
            if len(self.ml_pipeline_refs) > 0:
                pipeline_key = self.ml_pipeline_refs[0]
                self.default_pipeline = Pipeline(
                        pipeline_key, self.default_context, self.appconf)
        else:
            self.logger.debug(f"BaseApp.init(): Got session [{session}]")



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
        session: Session = None
    ) -> Union[SDF, DF]:
        if session is None:
            session = self.default_context.session
        
        if session is None:
            return DFF.create_df(data, None, columns, index, dtype)
        
        sdf: SDF
        try:
            sdf = DFF.create_df(
                data, session, columns, index, dtype
            )
        except Exception as e:
            conn: SnowConnect = self.snow_connect
            session = conn.create_service_session()
            sdf = DFF.create_df(
                data, session, columns, index, dtype
            )
            self.default_context.session = session
        return sdf


    def to_pandas_df(
            self, 
            sdf: SDF,
            conn: SnowConnect = None,
            drop_cols: List = []
        ) -> DF:
        if conn is None:
            conn = self.snow_connect
        session = self.default_context.session
        if session is None:
            self.logger.error("BaseApp.to_pandas_df(): Session is None "\
                    f"from default context. Connect [{conn.connect_key}].")
            return None
        df_rs, session = conn.to_pandas_df(sdf, session, drop_cols)
        self.default_context.session = session
        return df_rs


    def drop_table(self, full_tbl_name: str, session: Session = None) -> str:
        if session is None:
            session = self.default_context.session
        rs = SnowConnect.dcl(session, f"drop table if exists {full_tbl_name}")
        return f"{rs[0][0]}"


    def get_all_configurations(self) -> Dict:
        return self.all_configs
    

    def get_app_configurations(self) -> Dict:
        return self.app_configs
    

    def get_app_config(self) -> AppConfig:
        return self.appconf
    

    def set_app_config(self, appconf: AppConfig):
        self.appconf = appconf


    def get_default_snow_connect(self) -> SnowConnect:
        return self.snow_connect
    

    def load_setup_module(self) -> ModuleType:
        return self.default_setup.load_module()


    def get_default_setup(self) -> DataSetup:
        return self.default_setup


    def get_snowflake_session(self, ctx: Dict = {}) -> Session:
        session = None
        if self.snow_connect is not None:
            session = self.snow_connect.get_connection()
            if session is None:
                session = self.snow_connect.create_user_session(ctx)

        return session


    def get_default_pipeline_config(self, pipeline_key):
        _, d = self.get_group_item_config(
                pipeline_key, ConfigType.MLPipelines.value, self.appconf)
        return d


    def run_default_pipeline(self):
        self.default_pipeline.run()
    

    def get_app_namespaces(self) -> Tuple[str, str]:
        return self.appconf.get_app_namespaces()


    def load_configs(
        self,
        config_dir : Optional[Union[str, None]] = None, 
        config_file : Optional[Union[str, None]] = None
    ) -> Tuple[str, str, Dict]:
        return self.appconf.load_configs(config_dir, config_file)

    
    def load_default_configs(self) -> Dict:
        return self.appconf.load_default_configs()


    def get_group_item_config(
        self,
        group_item_key: str,
        root_key: str,
        configs: Optional[Union[Dict, None]] = None
    ) -> Tuple[str, Dict]:
        return self.appconf.get_group_item_config(
                group_item_key, root_key, configs)


    def load_module(self, script_name: str) -> ModuleType:
        return AppConfig.load_module(script_name)