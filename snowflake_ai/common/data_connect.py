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
__version__ = "0.4.0"


import sys
from typing import Optional, Dict, Union
import logging

from snowflake.snowpark import Session
from snowflake_ai.common import ConfigType, ConfigKey
from snowflake_ai.common import AppConfig, AppConnect



class DataConnect(AppConnect):
    """
    This class represents a generic data connection.

    Typically, you don't use this class to create data connection
    directly, rather create an instance from AppConfig or 
    extend it in a child class for creation of a specific data
    connection instance, e.g., SnowConnect as following :

        >>> from snowflake_ai.connect import SnowConnect
        ...
        ... # using default snowflake config directly
        >>> connect: SnowConnect = SnowConnect()
    """

    K_DATA_CONN = ConfigType.DataConnects.value
    K_INIT_LIST = ConfigKey.INIT_LIST.value

    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))

    _initialized = False

    # {<connect_key>: file-connect-string|snowflake-connection-obj}
    _data_connections = {}


    def __init__(
            self, 
            connect_key : Optional[str] = None,
            app_config: AppConfig = None
        ):
        super().__init__(connect_key, app_config)
        self.logger = DataConnect._logger
        self._current_connection = None

        # load default data service connection
        if (connect_key is None or not connect_key or \
                self._current_connection is None) and self.data_connections:
            k = DataConnect.search_default_key(self.data_connections)
            self.connect_group, self.connect_key = \
                AppConfig.split_group_key(k)
            self.connect_key, self.connect_params = \
                AppConnect.load_connect_config(k, self._configs)
            self.app_connects[self.connect_key] = self
            self.set_current_connection(self.data_connections[k])
            connect_key = self.connect_key

        # load user based connection
        if not connect_key:
            self.logger.error(f"DataConnect.init(): Error - missing key"\
                    f" [{connect_key}]; Connect_key[{self.connect_key}]")
        elif not self.connect_key:
            self.connect_key = connect_key
            self.connect_group, self.connect_name = \
                AppConfig.split_group_key(connect_key)
       
        self.logger.debug(f"DataConnect.init(): Connect_group"\
                f"[{self.connect_group}]; Connect_key[{self.connect_key}]"\
                f"; Connect_name[{self.connect_name}]")

        # setup oauth reference
        if self.connect_params:
            self.connect_type = self.connect_params.get(
                    ConfigKey.TYPE.value, '')
            self.connect_name = self.connect_params.get(
                    ConfigKey.NAME.value, '')
            self.oauth_connect_ref = self.connect_params.get(
                    ConfigKey.CONN_OAUTH.value)
            if self.oauth_connect_ref is not None and \
                    self.oauth_connect_ref:
                self.auth_type = AppConfig.T_OAUTH
            else:
                self.oauth_connect_ref = ""
            
            if self.oauth_connect_ref:
                _, self.oauth_connect_config = \
                        AppConfig.get_group_item_config(
                                self.oauth_connect_ref, 
                                ConfigType.AppConnects.value,
                                self._configs)
            else:
                self.oauth_connect_config = {}

            self.logger.debug(f"DataConnect.init(): Snowflake OAuth "\
                    f" Configuration => {self.oauth_connect_config}.")

            # match oauth connect connect_type
            if self.oauth_connect_config:
                self.oauth_flow_type = self.oauth_connect_config.get(
                        ConfigKey.TYPE.value, "")
            


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
                AppConnect.load_connect_config(k, self._configs)
            self.app_connects[self.connect_key] = self
            self.set_current_connection(self.data_connections[k])

        if ((not self.is_current_active()) or \
                (self.data_connections.get(self.connect_key) is None)) \
                and (self.is_service_connect()):
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
        Get lazy created shared data service connection or change 
        existing data connection to use the input connect_key to initialize
        connection. This should not be used for user specific data
        connection.

        Args:
            connect_key (str): data connect key in form of 
                "data_connects".<connect_name>

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
                        AppConnect.load_connect_config(k, self._configs)
                    self.app_connects[self.connect_key] = self
                    self._current_connection = self.data_connections[k]
                    self.logger.debug(
                        f"DataConnect.get_connection(): Connect_group ["\
                        f"{self.connect_group}]; Connect_key "\
                        f"[{self.connect_key}]; Current_connect_key [{k}]; "\
                        f"Current_connection [{self._current_connection}]."
                    )
                else:
                    self.logger.warning(
                        f"DataConnect.get_connection(): Warning - "\
                        f"[{len(self._data_connections)}] " \
                        "connection initialized!"
                    )
                    return None
            else:
                return self._current_connection
        else:
            qk = AppConfig.get_qualified_key(
                ConfigType.AppConnects.value, connect_key
            )
            if self.data_connections.get(qk) is None:
                gk, k = AppConfig.split_group_key(qk)
                params = self._configs[ConfigType.AppConnects.value]\
                    [DataConnect.K_DATA_CONN][k]
                self.connect_key = qk
                self.connect_params = params
                self.connect_group = gk
                if self.is_service_connect():
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
                self._configs
            )
            lst = conn_group_dict.get(DataConnect.K_INIT_LIST)
            if lst is not None and len(lst) > 0 :
                for dconn in lst:
                    dc: dict = self._configs[AppConnect.K_APP_CONN]\
                        [DataConnect.K_DATA_CONN]
                    params = dc.get(dconn)
                    if params is None:
                        raise ValueError(
                            f"DataConnect.init_connets(): Error - [{dconn}]"\
                            " doesn't exist in the configuration!"
                        )
                    elif self.is_service_connect():
                        s = f"{DataConnect.K_DATA_CONN}.{dconn}"
                        if self.data_connections.get(s) is None:
                            self.data_connections[s] = \
                                    self.create_connection(params)
                            self.set_current_connection(
                                    self.data_connections[s]
                                )
                DataConnect._initialized = True
        
        n = len(self.data_connections)
        self.logger.debug(
            f"DataConnect.init_connects(): [{n}] shared (service) "\
            f"data connections have been established."
        )
        return n


    def is_service_connect(self) -> bool:
        """
        Return whether this application connect is service type.
        This should be overridden by its childen class.
        """
        return False