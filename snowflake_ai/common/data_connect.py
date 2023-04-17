# Copyright (C) 2023 Tony Liu
#
# This software may be modified and distributed under the terms
# of the BSD 3-Clause License. See the LICENSE file for details.

"""
This module contains DataConnect class representing a generic data
connection configuration.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "BSD 3-Clause"
__version__ = "0.1.0"


import os
from os.path import exists
from pathlib import Path
from typing import Optional, Union, Dict
import logging
import toml
from importlib.resources import read_text



class DataConnect:
    """
    This class represents a generic data connection configuration.

    To use this class, extend it with a child class for specific data
    connection, for example:

        from snowflake_ai.common import SnowConnect

        connect: SnowConnect = SnowConnect()

        # optionally load custom configurations
        connect.load_config_file("./snowflake_ai/conf/")
    """

    DEFAULT_CONN = "snowflake-0"
    DEFAULT_CONF_LIB_PATH = "snowflake_ai.conf"
    DEFAULT_CONF_FILE = "app_config.toml"
    DEFAULT_CONF_DIR = "./snowflake_ai/conf/"

    _logger = logging.getLogger(__name__)


    def __init__(
        self,
        config_dir : Optional[Union[str, None]] = None, 
        config_file : Optional[Union[str, None]] = None
    ):
        self.logger = DataConnect._logger
        self.configs = {}
        self.conn_params = {}
        self.connections = {}
        self.def_conn_name = DataConnect.DEFAULT_CONN
        self.curr_conn_name = DataConnect.DEFAULT_CONN
        self.load_config_file(config_dir, config_file)


    def _load_default_config(self):
        config_file = read_text(
            DataConnect.DEFAULT_CONF_LIB_PATH, 
            DataConnect.DEFAULT_CONF_FILE
        )
        self.configs = toml.loads(config_file)
        self.logger.info(
            f"Default app configuration loaded => {self.configs}"
        )
        return self.configs


    def load_config_file(
        self, 
        config_dir : Optional[Union[str, None]] = DEFAULT_CONF_DIR, 
        config_file : Optional[Union[str, None]] = None
    ) -> Dict:
        """
        Load configuration file. the loading path of configuration file
        has the following priority:
            custom config directory and/or file, then user's home directory,
            finally try to load configuration file as part of library 
            loading path.

        Args:
            config_dir (str): Configuration file directory.
            config_file (str): Configuration file name. if none or empty
                merge all toml files

        Returns:
            dict: loaded configuration as dictionary

        Raises:
            ValueError: If config file or path doesn't exist.
        """
        if config_dir is None:
            config_dir = DataConnect.DEFAULT_CONF_DIR
        
        config_path = os.path.abspath(os.path.dirname(config_dir))
        if not exists(config_path):
            home = Path.home()
            config_path = os.path.join(
                home, os.path.dirname(config_dir)
            )
        if not exists(config_path):
            self.logger.info(
                f"DataConnect.load_config_file() Config path {config_path} "\
                f"doesn't exists, load from library config default"
            )
            return self._load_default_config() 

        if (config_file is None) or (not config_file):
            toml_files = [
                f for f in os.listdir(config_path) \
                    if f.lower().endswith(".toml")
            ]
            toml_files.sort(
                key=lambda x: os.path.getmtime(os.path.join(config_path, x))
            )
            for toml_file in toml_files:
                file_path = os.path.join(config_path, toml_file)
                with open(file_path, "r") as f:
                    try:
                        file_content = toml.load(f)
                        self.configs.update(file_content)
                    except Exception as e:
                        self.logger.exception(
                            "DataConnect.load_config_file(): Exception in "\
                            f"loading config file {file_path}: {e}"
                        )
            self.logger.info(
                f"Configurations loaded from all toml files in {config_path}"
            )
            return self.configs
        
        else:
            config_path = os.path.abspath(
                os.path.join(config_path, config_file)
            )
            if not exists(config_path):
                config_path = os.path.join(
                    config_path, 
                    DataConnect.DEFAULT_CONF_FILE
                )
            if exists(config_path): 
                with open(config_path, 'r') as f:
                    try:
                        self.configs = toml.load(f)
                        self.logger.info(
                            f"App configuration loaded from {config_path} "\
                            f"=> {self.configs}"
                        )
                    except Exception as e:
                        self.logger.exception(
                            "DataConnect.load_config_file(): Exception in "\
                            f"loading config file: {e}"
                        )
                        raise
            else:
                self.logger.info(
                    f"Configuration toml files are loaded from {config_path}"
                )
        return self.configs
    

    def connect(self):
        """
        Create all data connections specified in [data.connects] section
        of configuration file, e.g., app_config.toml

        Returns:
            Dict: data connection object dictionary with connect name as keys
        """
        for _, value in self.configs["data"]["connects"].items():
            params = dict(self.configs["data"]["connect"]).get(value)
            if params is None:
                self.logger.exception(
                    f"DataConnect.Connect configuration error: {value} "\
                    "cannot be referenced"
                )
                continue
            else:
                self.conn_params[value] = params
                try:
                    self.connections[value] = self.create_connection(params)
                except Exception as e:
                    self.logger.exception(
                        "Exception occured in connect() when creating "\
                        f"data connection: {e}"
                    )
        return self.connections


    def create_connection(self, params: Dict):
        """
        Create a data connection based on the specific configuration.
        Child class should implement specific connnection logic
        to override this method.

        Args:
            params (Dict): Configuration dictionary as input parameters.

        Returns:
            object: data connection object, e.g., connection session for
                snowflake connection
        """
        conn = f"Connected to {params['host']}:{params['port']}"
        return conn


    def close_connection(self):
        """
        Close current data connection.
        
        Returns:
            int: 0 - successful; otherwise unsuccessful
        """
        return 0
    

    def get_connection(self, conn_name: Optional[str] = None):
        """
        Get lazy created data connection.

        Args:
            conn_name (str): data connect name.

        Returns:
            object: data connection object, e.g., session connection for
                snowflake connection
        """
        conn = None
        if (conn_name is None) or (not conn_name):
            conn = self.connections.get(self.def_conn_name)
            if conn is None:
                ks = list(self.configs["data"]["connects"].keys())
                if len(ks) == 0:
                    conn_name = DataConnect.DEFAULT_CONN
                    params = self.get_connect_config()
                else: 
                    ks.sort()
                    conn_name = self.configs["data"]["connects"][ks[0]]
                    params = self.get_connect_config(conn_name)
                try:
                    self.connections[conn_name] = self.create_connection(
                        params
                    )
                    conn = self.connections[conn_name]
                    self.def_conn_name = conn_name
                    self.curr_conn_name = conn_name
                except Exception as e:
                    self.logger.exception(
                        "Exception occured in get_connection() when "\
                        f"creating default data connection: {e}"
                    )
        else:
            conn = self.connections.get(conn_name)
            if conn is None:
                params = self.get_connect_config(conn_name)
                try:
                    self.connections[conn_name] = self.create_connection(
                        params
                    )
                    conn = self.connections[conn_name]
                    self.curr_conn_name = conn_name
                except Exception as e:
                    self.logger.exception(
                        "Exception occured in get_connection() when creating"\
                        f" data connection: {e}"
                    )
        return conn


    def get_connect_config(self, conn_name: Optional[str] = None) -> Dict:
        """
        Get specific data connection configuration.

        Args:
            conn_name (str): data connect name. if none, current connection 
                name is used, if there is no current connection, default 
                connection name is used.

        Returns:
            Dict: dictionary of configruation for specific connection
        """
        params = None
        if (conn_name is None) or (not conn_name):
            if self.curr_conn_name:
                params = self.configs["data"]["connect"][self.curr_conn_name]
            else:
                params = self.configs["data"]["connect"][
                    DataConnect.DEFAULT_CONN
                ]
                conn_name = DataConnect.DEFAULT_CONN
        else:
            params = self.conn_params.get(conn_name)
            if params is None:
                params = self.configs["data"]["connect"][conn_name]
        self.conn_params[conn_name] = params
        return params


    def get_current_connection(self):
        """
        Get current data connection. if there is no current connection name
            default connection name is used.
            
        Returns:
            object: connection object, e.g., snowflake session for 
                SnowConnect or file path for FileConnect
        """
        conn = self.connections[self.curr_conn_name]
        if conn is None:
            conn = self.get_connection()
            if conn is None:
                conn_name = DataConnect.DEFAULT_CONN
                params = self.get_connect_config(conn_name)
                self.connections[conn_name] = self.create_connection(params)
                conn = self.connections[conn_name]
        return conn
    

    def set_current_connection(
            self, 
            conn_name: str,
            data_conn = None
    ) -> None:
        """
        Set current data connection.

        Args:
            conn_name (str): data connect name.
            data_conn (object): session connection object for SnowConnect or
                filepath for FileConnect
        """
        self.curr_conn_name = conn_name
        if data_conn:
            self.connections[conn_name] = data_conn
