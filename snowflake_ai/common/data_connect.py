# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains DataConnect class representing a generic data
connection configuration.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.2.0"


import sys
from typing import Optional, Dict, Union
import logging

from snowflake.snowpark import Session
from snowflake_ai.common import AppConfig, AppConnect



class DataConnect(AppConnect):
    """
    This class represents a generic data connection.

    To use this class, create an instance from AppConfig or 
    extend it in a child class for creation of a specific data
    connection instance, e.g., SnowConnect as following :

        >>> from snowflake_ai.connect import SnowConnect
        ...
        ... # using default snowflake config directly
        >>> connect: SnowConnect = SnowConnect()
    """

    K_DATA_CONN = "data_connects"
    K_INIT_LIST = "init_list"

    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))

    _initialized = False

    # {<connect_key>: file-connect-string|snowflake-connection-object}
    _data_connections = {}


    def __init__(self, connect_key : Optional[str] = None):
        super().__init__(connect_key)
        self.logger = DataConnect._logger
        self._current_connection = None
        if (self.connect_key is not None) and self.connect_key:
            if ((self.data_connections.get(self.connect_key) is None) \
                    or (not self.is_current_active())) \
                    and (not self.is_oauth_saml_type()):
                self.data_connections[self.connect_key] = \
                    self.create_connection(self.connect_params)
                self.set_current_connection(
                    self.data_connections[self.connect_key]
                )

        if (connect_key is None or not connect_key or \
                self._current_connection is None) and self.data_connections:
            k = DataConnect.search_default_key(self.data_connections)
            self.connect_group, self.connect_key = \
                AppConfig.split_group_key(k)
            self.connect_key, self.connect_params = \
                AppConnect.load_connect_config(k, self.configs)
            self.app_connects[self.connect_key] = self
            self.set_current_connection(self.data_connections[k])

        if self.connect_params:
            self.connect_type = self.connect_params.get(AppConfig.K_TYPE, '')
            self.connect_name = self.connect_params.get(AppConfig.K_NAME, '')
            

    @property
    def data_connections(self) -> Dict:
        """
        Get a dictionary of all data connections.

        Returns:
            dict: a dictionary of all data connections.
        """   
        return DataConnect._data_connections


    def get_current_connection(self) -> Union[str, Session]:
        """
        Get current data connection.

        Returns:
            str, Session: specific data connection object, e.g. file
                path or Snowflake Session.
        """
        if not self.data_connections:
            self.logger.warning(
                "DataConnect.current_connection(): DataConnect."\
                "_data_connections dictionary is empty!"
            )
        elif (self._current_connection is None):
            k = DataConnect.search_default_key(self.data_connections)
            self.connect_group, self.connect_key = \
                AppConfig.split_group_key(k)
            self.connect_key, self.connect_params = \
                AppConnect.load_connect_config(k, self.configs)
            self.app_connects[self.connect_key] = self
            self.set_current_connection(self.data_connections[k])

        if ((not self.is_current_active()) or \
                (self.data_connections.get(self.connect_key) is None)) \
                and (not self.is_oauth_saml_type()):
            self.data_connections[self.connect_key] = \
                self.create_connection(self.connect_params)
            self.set_current_connection(
                self.data_connections[self.connect_key]
            )
        return self._current_connection
    

    def set_current_connection(self, conn: Union[str, Session]):
        """
        Set current data connection.

        Args:
            conn (str | Session): input connection object
        Returns:
            str, Session: specific data connection object, e.g. file
                path or Snowflake Session.
        """
        if conn is not None:
            self._current_connection = conn
        return self._current_connection


    def is_current_active(self) -> bool:
        """
        Check whether current data connection is active or not. It should
        be overwritten by specific subclass as it is implementation
        dependant.

        Returns:
            bool: True if it is active, otherwise False
        """
        if self._current_connection is None:
            return False
        elif not self._data_connections:
            return False
        elif self.is_oauth_saml_type():
            return False
        elif self.connect_type == AppConnect.T_SNOWFLAKE_CONN:
            return False
        return True
    

    def create_connection(self, params: Dict):
        """
        Create a data connection based on the specific configuration.
        Child class should implement specific connnection logic
        to override this method.

        Args:
            params (Dict): input parameters for connection creation.

        Returns:
            object: data connection object, e.g., connection session for
                snowflake connection.
        """
        conn = f"Connected to {params['host']}:{params['port']}"
        return conn


    def close_connection(self):
        """
        Close current data connection.
        
        Returns:
            int: 0 - successful; otherwise unsuccessful
        """
        rn = 0
        try:
            if self._current_connection is not None and \
                    isinstance(self._current_connection, Session):
                self._current_connection.close()
        except Exception as e:
            self.logger.error(f"DataConnect.close_connection(): Error {e}")
            rn = -1
        return rn


    def get_connection(self, connect_key: Optional[str] = None):
        """
        Get lazy created shared data connection or change existing data 
        connection to use the input connect_key to initialize connection.

        Args:
            conn_name (str): data connect name.

        Returns:
            object: data connection object, e.g., session connection for
                snowflake connection; it can return None.
        """
        if connect_key is None:
            if self._current_connection is None:
                if self.data_connections:
                    k = DataConnect.search_default_key(self.data_connections)
                    self.connect_group, self.connect_key = \
                        AppConfig.split_group_key(k)
                    self.connect_key, self.connect_params = \
                        AppConnect.load_connect_config(k, self.configs)
                    self.app_connects[self.connect_key] = self
                    self._current_connection = self.data_connections[k]
                else:
                    self.logger.warning(
                        f"DataConnect.get_connection(): connections "\
                        f"{len(self._data_connections)} initialized."
                    )
                    return None
            else:
                return self._current_connection
        else:
            qk = AppConfig.get_qualified_key(
                AppConfig.K_APP_CONN, connect_key
            )
            if self.data_connections.get(qk) is None:
                gk, k = AppConfig.split_group_key(qk)
                params = self.configs[AppConfig.K_APP_CONN]\
                    [DataConnect.K_DATA_CONN][k]
                self.connect_key = qk
                self.connect_params = params
                self.connect_group = gk
                if not self.is_oauth_saml_type():
                    self.data_connections[qk] = self.create_connection(params)
                    self._current_connection = self.data_connections[qk]

            return self.data_connections.get(qk)


    def init_connects(self) -> int:
        """
        Initialize DataConnect in [app_connects.data_connects] init_list.

        Returns:
            int: 0 - if there is no connect object has been initialized;
                otherwise, return an integer to show the number of 
                AppConnect objects have been initialized.
        """
        if not DataConnect._initialized:
            conn_group_dict = AppConfig.filter_group_key(
                DataConnect.K_DATA_CONN, 
                AppConnect.K_APP_CONN, 
                self.configs
            )
            lst = conn_group_dict.get(DataConnect.K_INIT_LIST)
            if lst is not None and len(lst) > 0 :
                for dconn in lst:
                    dc: dict = self.configs[AppConnect.K_APP_CONN]\
                        [DataConnect.K_DATA_CONN]
                    params = dc.get(dconn)
                    if params is None:
                        raise ValueError(
                            f"DataConnect.init_connets(): Error - [{dconn}]"\
                            " doesn't exist in the configuration!"
                        )
                    elif not self.is_oauth_saml_type():
                        c = self.create_connection(params)
                        s = f"{DataConnect.K_DATA_CONN}.{dconn}"
                        self.data_connections[s] = c
                DataConnect._initialized = True
        
        n = len(self.data_connections)
        self.logger.info(
            f"DataConnect.init_connects(): {n} non-oauth data connects."
        )
        return n
