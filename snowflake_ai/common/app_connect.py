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
__version__ = "0.2.0"


import sys
from typing import Optional, Dict, Union, Tuple
import logging

from snowflake_ai.common import AppConfig



class AppConnect:
    """
    This class represents a generic application connection.
    Currently it is extended for oauth_connect, and
    data_connect.

    To use this class, create specific instances from AppConfig
    or extend it in a child class such as DataConnect. 
    """

    K_OAUTH_CONN = "oauth_connects"
    K_DATA_CONN = "data_connects"
    
    T_SNOWFLAKE_CONN = AppConfig.T_SNOWFLAKE_CONN
    T_AUTH_SNOWFLAKE = "snowflake"
    T_AUTH_KEYPAIR = "keypair"
    T_AUTH_EXT_BROWSER = "externalbrowser"
    T_AUTH_OAUTH = "oauth"

    SUPPORTED_CONNECT_GROUP_TYPES = [
        K_OAUTH_CONN, 
        K_DATA_CONN
    ]
    K_APP_CONN = AppConfig.K_APP_CONN

    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))

    _configs = AppConfig.get_all_configs()
    _connects = {}


    def __init__(self, connect_key : Optional[str] = None):
        """
        Creat an AppConnect object.

        Args:
            connect_key: A string representing an AppConnect object;
                it can have the format of <app_connect_group>.<app_
                connect> ('<', '>' not included)
        """
        self.logger = AppConnect._logger
        self.connect_type, self.connect_name, self.auth_type = '', '', ''
        
        self.connect_group, self.connect_key = \
            AppConfig.split_group_key(connect_key)
        self.connect_key, self.connect_params = \
            AppConnect.load_connect_config(self.connect_key, self.configs)
        self.logger.info(
            f"AppConnect.init(): ConnectParameters => {self.connect_params}"
        )

        if self.connect_key and self.connect_params:
            self.app_connects[self.connect_key] = self

        if self.connect_params:
            self.auth_type = self.connect_params.get(
                AppConfig.K_AUTH_TYPE, ''
            )

        # setup initialization
        self.init_connects()


    @property
    def configs(self) -> Dict:
        """
        Get all configurations loaded from AppConfig.

        Returns:
            dict: a dictionary of all configurations loaded.        
        """
        return AppConnect._configs
    

    @staticmethod
    def load_connect_config(
        connect_key: str, 
        configs: Optional[Union[Dict, None]] = None
    ) -> Tuple[str, Dict]:
        """
        Load app connect configuration from overall configurations

        Args:
            connect_key (str): app connect key.
            configs (Dict): overall configuration dictionary

        Returns:
            Tuple[str, dict]: tuple of the connect key string matched in
            a form of group.connect_key and the dictionary of loaded
            app connect specific configurations
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

        AppConfig._logger.info(
            f"AppConnect.load_connect_config(): AppConnect[{k}] => {rd}"
        )
        return (k, rd)


    @staticmethod
    def search_default_key(data_dict: Dict[str, object]) -> str:
        """
        Search a dictionary for a key containing the substring "default". 
        If found, return that key. If not found, check if "_0", return
        the first found. If still not found any, sort the keys and return
        the first key in the sorted list.

        Args:
            data_dict (dict): A dictionary with string keys.

        Returns:
            str, None: A key containing "default" or "_0". If found, or the
            first key in the sorted list if not found.
        """
        if not data_dict: return None

        default_key = None
        sorted_keys = sorted(data_dict.keys())

        for k in sorted_keys:
            if AppConfig.K_DEFAULT in k.lower():
                default_key = k
                break

            if "_0" in k:
                default_key = k
                break

        return default_key or sorted_keys[0]
    

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
