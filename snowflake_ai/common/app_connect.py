# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains AppConnect class representing a generic
application integration connection for data, security, etc.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.3.0"


import sys
from typing import Optional, Dict, Union, Tuple
import logging

from snowflake_ai.common import ConfigType, ConfigKey, AppConfig



class AppConnect:
    """
    This class represents a generic application connection.
    Currently it is extended for oauth_connect, and
    data_connect.

    To use this class, create specific instances from AppConfig
    or extend it in a child class such as DataConnect. 
    """
    
    T_SNOWFLAKE_CONN = AppConfig.T_CONN_SNFLK
    T_AUTH_SNOWFLAKE = AppConfig.T_AUTH_SNFLK
    T_AUTH_KEYPAIR = AppConfig.T_AUTH_KEYPAIR
    T_AUTH_EXT_BROWSER = AppConfig.T_AUTH_EXT_BROWSER
    T_AUTH_OAUTH = AppConfig.T_AUTH_OAUTH
    K_APP_CONN = ConfigType.AppConnects.value

    SUPPORTED_CONNECT_GROUP_TYPES = [
        ConfigType.OAuthConnects.value, 
        ConfigType.DataConnects.value
    ]

    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))

    # all configurations
    _configs = {}

    # store Connect object reference
    _connects = {}


    def __init__(
            self, 
            connect_key : Optional[str] = None,
            app_config: AppConfig = None
        ):
        """
        Creat an AppConnect object.

        Args:
            connect_key: A string representing an AppConnect object;
                it can have the format of <app_connect_group>.<app_
                connect_name> ('<', '>' not included)
        """
        self.logger = AppConnect._logger
        self.connect_type, self.connect_name, self.auth_type = '', '', ''
        if app_config is not None:
            self.app_config = app_config
            self._configs = self.app_config.get_all_configs()
        else:
            self.app_config = None
            self._configs = AppConfig.get_all_configs()

        self.connect_group, self.connect_key = \
                AppConfig.split_group_key(connect_key)
        self.connect_key, self.connect_params = \
                AppConnect.load_connect_config(self.connect_key, self._configs)
        self.logger.info(
                f"AppConnect.init(): Connect group [{self.connect_group}]; "\
                f"Connect key [{self.connect_key}]; "\
                f"Connect Config Params => {self.connect_params}"
            )

        if self.connect_key and self.connect_params:
            self.app_connects[self.connect_key] = self

        if self.connect_params:
            self.auth_type = self.connect_params.get(
                    ConfigKey.AUTH_TYPE.value, '')
            self.type = self.connect_params.get(
                    ConfigKey.TYPE.value, '')
            self.connect_type = self.type

        # setup initialization
        self.init_connects()


    @staticmethod
    def load_connect_config(
        connect_key: str, 
        configs: Optional[Union[Dict, None]] = None
    ) -> Tuple[str, Dict]:
        """
        Load app connect configuration from overall configurations. 
        Connect key consists of <app_connect_group>.<app_
        connect_name> ('<', '>' not included), e.g.,
        "oauth_connects.auth_code_def".

        Args:
            connect_key (str): app connect key in form of 
                <app_connect_group>.<app_connect_name>
            configs (Dict): overall configuration dictionary

        Returns:
            Tuple[str, dict]: tuple of the connect key string matched in
            a form of group.connect_key and the dictionary of loaded
            app connect specific configurations.
        """
        if configs is not None and \
            configs.get(AppConnect.K_APP_CONN) is None:
            s = f"AppConnect.load_connect_config(): Error - "\
                "[app_connects] is missing!"
            AppConnect._logger.error(s)
            raise ValueError(s)
        if connect_key is None:
            return ('', {})
        
        if configs is None:
            configs = AppConfig.get_all_configs()

        rd = {}
        connect_key = connect_key.strip().lower()
        gk, ck = AppConfig.split_group_key(connect_key)
        if not gk and ck:
            k, rd = AppConfig.search_key_by_group(
                    ck, AppConnect.K_APP_CONN, configs
                )
        elif not gk and not ck:
            k, rd = '', {}
        elif gk and not ck:
            k, rd = f"{gk}.", AppConfig.filter_group_key(
                    gk, AppConnect.K_APP_CONN, configs
                )
        else:
            gs = dict(configs[AppConnect.K_APP_CONN]).get(gk)
            if gs is not None:
                k = f"{gk}.{ck}" 
                rd =  configs[AppConnect.K_APP_CONN][gk][ck] if dict(
                        configs[AppConnect.K_APP_CONN][gk]
                    ).get(ck) is not None else {}
            else:
                k, rd =  f"{gk}.{ck}", {}

        AppConfig._logger.debug(
            f"AppConnect.load_connect_config(): AppConnect[{k}] => {rd}"
        )
        return (k, rd)


    @staticmethod
    def search_default_key(data_dict: Dict[str, object]) -> str:
        """
        Search a dictionary for a key containing the substring "default". 
        If found, return that key. If not found, check if "_def", return
        the first found. If not found, check if "_0", retrun first found.
        If still not found any, sort the keys and return the first key 
        in the sorted list.

        Args:
            data_dict (dict): A dictionary with string keys.

        Returns:
            str, None: A key containing "default" or "_def". If found, or 
            the first key in the sorted list if not found.
        """
        if not data_dict: return None

        default_key = None
        sorted_keys = sorted(data_dict.keys())

        for k in sorted_keys:
            if (AppConfig.T_DEFAULT in k.lower()) or \
                    (AppConfig.T_DEF in k.lower()):
                default_key = k
                break

            if "_0" in k:
                default_key = k
                break

        return default_key or sorted_keys[0]
    

    @classmethod
    def get_app_connects(cls) -> Dict:
        """
        Get a dictionary of all currently loaded application connections.

        Returns:
            dict: a dictionary of all application connections.
        """  
        return AppConnect._connects


    @property
    def app_connects(self) -> Dict:
        """
        Get/set a dictionary of all currently loaded application connections.

        Returns:
            dict: a dictionary of all application connections.
        """   
        return AppConnect._connects
    

    def init_connects(self) -> int:
        """
        Initialize default AppConnect in each group. This generally should
        be overwritten by its child class
        """
        return len(AppConnect._connects)


    def is_oauth_saml_type(self) -> bool:
        """
        Return whether this application connect has auth_type == "oauth"
        or auth_type == "externalbrowser"
        """
        auth_t = self.auth_type
        return (auth_t == AppConfig.T_OAUTH) or \
                (auth_t == AppConnect.T_AUTH_EXT_BROWSER)
