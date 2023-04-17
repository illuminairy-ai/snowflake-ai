# Copyright (C) 2023 Tony Liu
#
# This software may be modified and distributed under the terms
# of the BSD 3-Clause license. See the LICENSE file for details.

"""
This module contains FileConnect class representing a specific file
connection.

Example:
    To use this class, just instantiate FileConnect as follows:

        from snowflake_ai.common import FileConnect

        connect: FileConnect = FileConnect()
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "BSD 3-Clause"
__version__ = "0.1.0"


import os
from os.path import exists
import logging
from typing import Dict, Optional, Union

from snowflake_ai.common import DataConnect



class FileConnect(DataConnect):
    """
    This class provides various types of file connection.

    Example:

    To use this class, just instantiate FileConnect as follows:

        from snowflake_ai.common import FileConnect

        connect: FileConnect = FileConnect()
    """

    DEFAULT_FILE_CONN = "file-0"
    DEFAULT_FILE_TYPE = "local-csv"
    _logger = logging.getLogger(__name__)


    def __init__(
        self,
        config_dir : Optional[Union[str, None]] = None, 
        config_file : Optional[Union[str, None]] = None
    ):
        super().__init__(config_dir, config_file)
        super().def_conn_name = FileConnect.DEFAULT_FILE_CONN
        self.logger = FileConnect._logger
        self.file_type = FileConnect.DEFAULT_FILE_TYPE
        params = self.get_connect_config(self.def_conn_name)
        self.connection = self.create_connection(params)
        self.curr_conn_name = self.def_conn_name


    def create_connection(self, params: Dict):
        """
        Create a file based connection based on the specific configuration.
        A set of types are supported:
            local-csv, azure-adls, azure-blob

        Args:
            params (Dict): Configuration dictionary as input parameters.

        Returns:
            object: file path or connection object
        """
        if params is None:
            raise ValueError(
                f"FileConnect.create_connection params cannot be None"
            )
        try:
            self.file_type = params["type"].lower()
            if self.file_type == "local-csv":
                self.connection = self._do_local_csv(params)
            elif self.file_type == "azure-adls":
                self.connection = self._do_azure_adls(params)
            elif self.file_type == "azure-blob":
                self.connection = self._do_azure_blob(params)
        except Exception as e:
            self.logger.exception(
                f"FileConnect.create_connection cannot create "\
                f"type={self.file_type} file connect: {e}"
            )
        return self.connection


    def get_connection(self, conn_name: Optional[str] = None):
        """
        Get lazy created file connection. If no input connection name,
        default file connection (local-csv) is created.

        Args:
            conn_name (str): data connect name.

        Returns:
            object: data connection object, e.g., local-csv file path
        """
        conn = self.connection
        if not conn_name:
            if conn is None:
                conn = self.connections.get(self.def_conn_name)
            if conn is None:
                ks = list(self.configs["data"]["connects"].keys())
                if len(ks) == 0:
                    conn_name = FileConnect.DEFAULT_FILE_CONN
                else: 
                    ks.sort()
                    for i in ks:
                        v = self.configs["data"]["connects"][ks[i]]
                        if v.lower().startwith("file"):
                            conn_name = v.lower()
                            break
                    if not conn_name:
                        raise ValueError(
                            "FileConnect.get_connection() has no default "\
                            "connection configured"
                        )
                params = self.get_connect_config(conn_name)
                self.connections[conn_name] = self.create_connection(params)
                conn = self.connections[conn_name]
        else:
            conn = self.connections.get(conn_name)
            if conn is None:
                params = self.get_connect_config(conn_name)
                self.connections[conn_name] = self.create_connection(params)
                conn = self.connections[conn_name]
            self.connection = conn
        return conn
    

    def get_current_connection(self):
        """
        Get current file connection

        Returns:
            object: connection object, e.g., file path for FileConnect
        """
        conn = self.connections.get(self.curr_conn_name)
        if conn is None:
            self.connections[self.curr_conn_name] = self.get_connection(
                self.curr_conn_name
            )
            conn = self.connections[self.curr_conn_name]
        return conn
    

    def _do_local_csv(self, params):
        path :str = params["dir_path"]
        file :str = params["file_name"]
        
        file_conn = None
        if not file:
            file_conn = os.path.dirname(os.path.abspath(path))
        else:
            file_conn = os.path.join(
                os.path.dirname(os.path.abspath(path)), file
            )

        if exists(file_conn):
            self.connection = file_conn
        else:
            raise ValueError(
                f"Cannot load from local csv path, Parameter paths "\
                f"value error: dir_path=>{path}, file_name=>{file}"
            )

        return self.connection


    def _do_azure_adls(self, params):
        pass


    def _do_azure_blob(self, params):
        pass
