# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains a default Notebook ML application
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.5.0"


import os
from os.path import exists
import sys
from typing import Optional, Union, Dict, Callable
import logging
from types import ModuleType

from snowflake.snowpark import Session

from snowflake_ai.common import AppType
from snowflake_ai.apps import BaseApp
from snowflake_ai.connect import DeviceCodeConnect
from snowflake_ai.snowpandas import SetupManager, DataSetup


class NotebookApp(BaseApp):
    """
    This class represents a notebook application's configurations and 
    its corresponding application. Since it is subclass of AppConfig,
    it has the following config directory bootstrapping precedence:

    1) input custom directory 
    2) .snowflake_ai/conf subdir under current directory 
    3) .snowflake_ai/conf subdir under user_home directory
    4) current directory
    5) home directory
    6) conf subdir under snowflake_ai library installation root dir

    Assuming a streamlit application app_1 under a default group named as
        business_group is configured, this app can be created as:

        >>> from snowflake_ai.apps import NotebookApp
        ... 
        ... # initialize application config for app_1
        >>> app = NotebookApp("business_group.app_1")
        """

    _logger = logging.getLogger(__name__)


    def __init__(
        self,
        app_key : str,
        config_dir : Optional[Union[str, None]] = None, 
        config_file : Optional[Union[str, None]] = None
    ):
        """
        Creat app specific AppConfig object.

        Args:
            app_key: A string representing the application; it can have 
                the format of <app_group>.<application> ('<', '>' not 
                included)
            config_dir: directory path string for custom config load
            config_file: file name string for custom config load
        """
        super().__init__(app_key, config_dir, config_file)
        self.logger = self._logger
        if  AppType.Notebook.value != self.type:
            raise TypeError(
                "NotebookApp.init(): Application type configuration Error!"
            )
        self.default_context.debug = True



    def get_snowflake_session(self, ctx: Dict = {}) -> Session:
        session = None
        if self.snow_connect is not None:
            session = self.snow_connect.get_connection()
            if session is None:
                dc: DeviceCodeConnect = self.snow_connect.get_oauth_connect()
                self.logger.debug(
                    f"NotebookApp.get_snowflake_session(): "\
                    f"Auth_type[{dc.auth_type}]"
                )
                res_d = dc.authorize_request()
                res_t = dc.process_authorize_response(res_d)
                ctx = dc.decode_token(
                    res_t, ["access_token", "refresh_token"]
                )
                self.logger.debug(
                    f"NotebookApp.get_snowflake_session(): "\
                    f"Auth_type[{dc.auth_type}]"
                )
                session = self.snow_connect.create_user_session(ctx)

        return session