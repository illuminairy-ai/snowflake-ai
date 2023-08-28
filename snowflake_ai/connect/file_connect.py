# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

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
__license__ = "Apache License 2.0"
__version__ = "0.5.0"


import os
from os.path import exists
import logging
from typing import Dict, Optional

from snowflake_ai.common import AppConfig, DataConnect



class FileConnect(DataConnect):
    """
    This class provides various types of file connection.

    Example:

    To use this class, just instantiate FileConnect as follows:

        >>> from snowflake_ai.common import FileConnect
        ... 
        >>> connect: FileConnect = FileConnect()
    """

    T_FILE_CONN = AppConfig.T_CONN_FILE
    DEF_FILE_CONN = "file_def"
    DEF_FILE_FMT = "csv"
    DEF_STRG_TYPE = "local"

    _logger = logging.getLogger(__name__)


    def __init__(
            self, 
            connect_key : Optional[str] = None,
            app_config: AppConfig = None
        ):
        super().__init__(connect_key, app_config)
        self.logger = FileConnect._logger
        if self.connect_group == DataConnect.K_DATA_CONN and \
                self.connect_params:
            try:
                self.storage_type = self.connect_params["storage_type"]
                self.format = self.connect_params["format"]
                self.dir_path = self.connect_params["dir_path"]
                self.file_name = self.connect_params["file_name"]
            except KeyError as e:
                raise KeyError(
                    f"FileConnect.init(): Configuration Error - {e}"
                )
        else:
            raise ValueError(
                "FileConnect initialization configuration error"
            )
        if (not self.connect_type) or (
            self.connect_type != AppConfig.T_CONN_FILE
        ):   
            raise ValueError(
                "FileConnect initialization data connect type error"
            )


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
            if self.storage_type == "local":
                if self.format == "csv":
                    self.set_current_connection(self._do_local_csv(params))
            elif self.storage_type == "azure_adls":
                self.set_current_connection(self._do_azure_adls(params))
            elif self.storage_type == "azure_blob":
                self.set_current_connection(self._do_azure_blob(params))
        except Exception as e:
            self.logger.exception(
                f"FileConnect.create_connection cannot create "\
                f"storage_type={self.storage_type} file connect: {e}"
            )
        return self.get_current_connection()


    def get_connection(self, connect_key: Optional[str] = None) -> str:
        """
        Get lazy created file connection. If no input connect_key,
        default file connection is created.

        Args:
            connect_key (str): data connect key.

        Returns:
            object: data connection object, e.g., local csv file path
        """
        conn = super().get_connection(connect_key)
        if self.connect_params["type"] != FileConnect.T_FILE_CONN:
            raise TypeError(
                f"FileConnect.get_connection() type error {connect_key}"
            )
        return conn


    def _do_local_csv(self, params) -> str:
        path :str = params["dir_path"]
        file :str = params["file_name"]
        
        file_conn = ''
        if not file:
            file_conn = os.path.dirname(os.path.abspath(path))
        else:
            file_conn = os.path.join(
                os.path.dirname(os.path.abspath(path)), file
            )

        if exists(file_conn):
            self.set_current_connection(file_conn)
        else:
            raise ValueError(
                f"FileConnect._do_local_csv() Cannot load file from local "\
                f"csv path, Parameter paths value Error: dir_path=>{path},"\
                f" file_name=>{file}"
            )

        return self.get_current_connection()


    def _do_azure_adls(self, params) -> str:
        # TO-DO
        return ''


    def _do_azure_blob(self, params) -> str:
        # TO-DO
        return ''
