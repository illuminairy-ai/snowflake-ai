# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains OAuthConnect class representing a generic OAuth
connection configuration.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.2.0"


import sys
from typing import Optional, Dict, List
import logging

from snowflake_ai.common import AppConfig, OAuthConnect
from snowflake_ai.common import AppConnect, DataConnect
from snowflake_ai.connect import SnowConnect, AuthCodeConnect


class ConnectManager:
    """
    This class creates and manages application connects.

    To use this class, create instance from AppConfig or extend
    it in a child class for creation of a specific OAuth connect
    instance, e.g., AuthCodeConnect as following :

        >>> from snowflake_ai.connect import AuthCodeConnect
        ...
        >>> ac = AppConfig("group_0.app_1")
        >>> conn = SecurityManager(ac).create_default_oauth_connect()
        >>> conn.grant_request()
    """

    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))

    _initialized = False
    _app_connect_list = []


    @classmethod
    def create_default_oauth_connect(cls, app_config: AppConfig) \
            -> OAuthConnect:
        """
        Create default oauth connect.

        Returns:
            OAuthConnect: default oauth connect
        """   
        if not cls._initialized:
            cls.create_app_connects(app_config)
        for c in cls._app_connect_list:
            if isinstance(c, OAuthConnect):
                return c
        return OAuthConnect()
    

    @classmethod
    def create_default_snow_connect(cls, app_config: AppConfig) \
            -> SnowConnect:
        """
        Create default snowflake connect.

        Returns:
            OAuthConnect: default snowflake connect
        """   
        if not cls._initialized:
            cls.create_app_connects(app_config)
        for c in cls._app_connect_list:
            if isinstance(c, SnowConnect):
                return c
        return SnowConnect()
    

    @classmethod
    def create_app_connects(cls, app_config: AppConfig) -> List:
        """
        Create list of application connect instances

        Returns:
            List: application connect instances
        """ 
        if app_config is None:
            raise ValueError(
                "ConnectManager.create_app_connects(): AppConfig is required!"
            )
        if not cls._initialized:
            for ck in app_config.app_connect_keys:
                gk, k = AppConfig.split_group_key(ck)
                config = app_config.get_all_configs()\
                    [AppConfig.K_APP_CONN][gk][k]
                
                if gk == OAuthConnect.K_OAUTH_CONN:
                    if config[AppConfig.K_TYPE] == "auth_code":
                        cls. _app_connect_list.append(AuthCodeConnect(ck))
                        print(f"DEBUG C1 => {config[AppConfig.K_TYPE] }")
                elif gk == DataConnect.K_DATA_CONN:
                    if config[AppConfig.K_TYPE] == "snowflake":
                        cls. _app_connect_list.append(SnowConnect(ck))
                        print(f"DEBUG C2 => {config[AppConfig.K_TYPE] }")
            
            cls._initialized = True

        return cls. _app_connect_list