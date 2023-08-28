# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains SetupManager class representing a factory class to 
create Setup objects.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.5.0"


import sys
from typing import Optional, Dict, List
import logging

from snowflake_ai.common import AppConfig, ConfigKey, ConfigType
from snowflake_ai.connect import AppConnect, SnowConnect
from snowflake_ai.snowpandas import DataSetup, SnowSetup



class SetupManager:
    """
    This class represents a factory class for creating DataSetup 
    objects.

    To use this class, create instance from AppConfig or extend
    it in a child class for creation of a DataSetup as following:

        >>> from snowflake_ai.snowpandas import SetupManager
        ...
        >>> ac = AppConfig("group_0.app_1")
        >>> setup = SetupManager(ac).create_default_data_setup()
    """

    _logger = logging.getLogger(__name__)

    # whether application has been initialized
    _init_d : Dict[str, bool] = {}

    # application data setup list
    _data_setups_d : Dict[str, List] = {}
    

    @classmethod
    def create_default_snow_setup(
        cls, 
        app_config: AppConfig,
        def_conn: Optional[SnowConnect] = None
    ) -> SnowSetup:
        """
        Create default snowflake data setup.

        Args:
            app_config (AppConfig):
            def_conn (SnowConnect):
            
        Returns:
            SnowSetup: default snowflake data setup
        """   
        _initialized = cls._init_d.get(app_config.app_key)
        cls._logger.debug(
            "SetupManager.create_default_snow_setup(): Initialize "\
            f"App_key [{app_config.app_key}] application setup."
        )
        if _initialized is None or not _initialized:
            cls.create_data_setups(app_config, def_conn)
        for s in cls._data_setups_d[app_config.app_key]:
            if isinstance(s, SnowSetup):
                return s
        return SnowSetup()
    

    @classmethod
    def create_data_setups(
        cls, 
        app_config: AppConfig,
        def_conn: Optional[SnowConnect] = None
    ) -> List:
        """
        Create list of application data setups instances

        Args:
            app_config (AppConfig): Application configuration object
            connect (SnowConnect): Default SNowflake Connect object
            
        Returns:
            List: application data setup instances
        """ 
        if app_config is None:
            raise ValueError(
                "SetupManager.create_data_setups(): AppConfig is required!"
            )
        
        _initialized = cls._init_d.get(app_config.app_key)
        if _initialized is None or not _initialized:
            cls._data_setups_d[app_config.app_key] = []

            if not app_config.data_setup_refs:
                cls._do_data_setup(app_config.app_key, app_config, def_conn)

            for ck in app_config.data_setup_refs:
                cls._do_data_setup(ck, app_config, def_conn)
            
            cls._init_d[app_config.app_key] = True

        return cls._data_setups_d[app_config.app_key]
    

    @classmethod
    def _do_data_setup(
        cls,
        datasetup_key: str,
        app_config: AppConfig,
        def_conn: Optional[SnowConnect] = None
    )  -> None:
        gk, k = AppConfig.split_group_key(datasetup_key)
        config: Dict = app_config.get_all_configs()\
                [ConfigType.DataSetups.value][gk][k]
                
        if (config[ConfigKey.TYPE.value] == AppConfig.T_CONN_SNFLK)\
                and (config.get(ConfigKey.CONN_DATA.value) is not None):
            dconn_ref = config.get(ConfigKey.CONN_DATA.value)
            ac = AppConnect.get_app_connects().get(dconn_ref)
            if ac is None:
                ac = SnowConnect(dconn_ref, app_config)
                AppConnect.get_app_connects()[dconn_ref] = ac

            if def_conn is None:
                def_conn = ac

            setup : SnowSetup = SnowSetup(datasetup_key, def_conn)
            cls._data_setups_d[app_config.app_key]\
                    .append(setup)
            setup.set_script_name(app_config)

            cls._logger.debug(
                "SetupManager._do_data_setups(): "\
                f"Initialize [{config[ConfigKey.TYPE.value]}] "\
                f"DataSetup with key [{datasetup_key}]."
            )
