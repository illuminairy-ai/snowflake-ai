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
__version__ = "0.3.0"


import sys
from typing import Optional, Dict, List
import logging
from snowflake.snowpark import Session

from snowflake_ai.common import  ConfigKey, ConfigType
from snowflake_ai.common import AppConfig, AppConnect
from snowflake_ai.common import DataConnect, OAuthConnect
from snowflake_ai.connect import SnowConnect, AuthCodeConnect
from snowflake_ai.connect import DeviceCodeConnect


class ConnectManager:
    """
    This class creates and manages application connects, including
    OAuthConnect, and DataConnect, etc.

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

    # initialization stats by app_key
    _init_d = {}

    # list of app connects by app key
    _app_conn_lst_d : Dict[str, List] = {} 
                                     

    @classmethod
    def create_default_oauth_connect(cls, app_config: AppConfig) \
            -> OAuthConnect:
        """
        Create default oauth connect.

        Returns:
            OAuthConnect: default oauth connect
        """   
        _initialized = cls._init_d.get(app_config.app_key)
        if _initialized is None or not _initialized:
            cls.create_app_connects(app_config)
        for c in cls._app_conn_lst_d[app_config.app_key]:
            if isinstance(c, OAuthConnect):
                return c
        return OAuthConnect(app_config.app_key, app_config)
    

    @classmethod
    def create_default_snow_connect(cls, app_config: AppConfig) \
            -> SnowConnect:
        """
        Create default snowflake connect.

        Returns:
            SnowConnect: default snowflake connect
        """   
        _initialized = cls._init_d.get(app_config.app_key)
        if _initialized is None or not _initialized:
            cls.create_app_connects(app_config)
        cls._logger.debug(
            f"ConnectManager.create_default_snow_connect(): App_key["\
            f"{app_config.app_key}]; List_of_connect[{cls._app_conn_lst_d}]."
        )
        for c in cls._app_conn_lst_d[app_config.app_key]:
            if isinstance(c, SnowConnect):
                return c
        return SnowConnect(app_config.app_key, app_config)
    

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
        
        _initialized = cls._init_d.get(app_config.app_key)
        if _initialized is None or not _initialized:
            cls._app_conn_lst_d[app_config.app_key] = []
            cls._logger.debug(
                "ConnectionManager.create_app_connects(): "\
                f"App_connect_refs => {app_config.app_connect_refs}"
            )
            for ck in app_config.app_connect_refs:
                gk, k = AppConfig.split_group_key(ck)
                config = app_config.get_all_configs()\
                    [ConfigType.AppConnects.value][gk][k]
                
                if gk == OAuthConnect.K_OAUTH_CONN:
                    if config[ConfigKey.TYPE.value] == \
                            AppConfig.T_OAUTH_CODE:
                        ac = AppConnect.get_app_connects().get(ck)
                        if ac is None:
                            ac = AuthCodeConnect(ck, app_config)
                            AppConnect.get_app_connects()[ck] = ac
                        cls._app_conn_lst_d[app_config.app_key].append(ac)
                        cls._logger.debug(
                            "ConnectionManager.create_app_connects(): "\
                            f"Initialize [{config[ConfigKey.TYPE.value]}] "\
                            f"OAuth Connection with Connect_Key[{ck}]."
                        )

                    elif config[ConfigKey.TYPE.value] == \
                            AppConfig.T_OAUTH_DEVICE:
                        ac = AppConnect.get_app_connects().get(ck)
                        if ac is None:
                            ac = DeviceCodeConnect(ck, app_config)
                            AppConnect.get_app_connects()[ck] = ac
                        cls._app_conn_lst_d[app_config.app_key].append(ac)
                        cls._logger.debug(
                            "ConnectionManager.create_app_connects(): "\
                            f"Initialize [{config[ConfigKey.TYPE.value]}] "\
                            f"OAuth Connection with Connect_Key[{ck}]."
                        )

                elif gk == DataConnect.K_DATA_CONN:
                    if config[ConfigKey.TYPE.value] == AppConfig.T_CONN_SNFLK:
                        ac = AppConnect.get_app_connects().get(ck)
                        if ac is None:
                            ac = SnowConnect(ck, app_config)
                            AppConnect.get_app_connects()[ck] = ac
                        cls._app_conn_lst_d[app_config.app_key].append(ac)
                        cls._logger.debug(
                            "ConnectionManager.create_app_connects(): "\
                            f"Initialize [{config[ConfigKey.TYPE.value]}] "\
                            f"Data Connection with Connect_Key[{ck}]."
                        )
            
            cls._init_d[app_config.app_key] = True

        return cls._app_conn_lst_d[app_config.app_key]
    

    @classmethod
    def get_app_connects(cls, app_config: AppConfig) -> List:
        _initialized = cls._init_d.get(app_config.app_key)
        if _initialized is None or not _initialized:
            cls.create_app_connects(app_config)
        return cls._app_conn_lst_d[app_config.app_key]
    

    @classmethod
    def get_snowflake_service_session(cls, app_config: AppConfig) \
            -> Session:
        """
        Get snowflake session for the shared service data connection.
        Those connections should already be initialized based on the
        configuration.

        Returns:
            Session: snowflake session
        """   
        _initialized = cls._init_d.get(app_config.app_key)
        if _initialized is None or not _initialized:
            cls.create_app_connects(app_config)
        for c in cls._app_conn_lst_d[app_config.app_key]:
            if isinstance(c, SnowConnect):
                cls._logger.debug(
                    "ConnectManager.get_snowflake_service_session(): "\
                    f"Connect [{c.connect_key}]; Auth_type [{c.auth_type}]"
                )
                if (c.auth_type == AppConfig.T_AUTH_KEYPAIR) or \
                        (c.auth_type == AppConfig.T_AUTH_SNFLK) or\
                        (c.oauth_flow_type == AppConfig.T_OAUTH_CRED):
                    return c.get_service_session()
        return None
    

    @classmethod
    def create_snowflake_user_session(
            cls, app_config: AppConfig, ctx: Dict
        ) -> Session:
        """
        Create user specific Snowflake session, typically through
        OAuth enabled SnowConnect.

        Returns:
            session: snowflake session
        """   
        _initialized = cls._init_d.get(app_config.app_key)
        if _initialized is None or not _initialized:
            cls.create_app_connects(app_config)
        for c in cls._app_conn_lst_d[app_config.app_key]:
            if isinstance(c, SnowConnect):
                if (c.auth_type == AppConfig.T_AUTH_OAUTH )or \
                        (c.auth_type == AppConfig.T_AUTH_EXT_BROWSER):                    
                    return c.create_user_session(ctx)
        return None