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

from snowflake_ai.common import AppConfig
from snowflake_ai.snowpandas import DataSetup



class SetupManager:
    """
    This class represents a generic application connect for 
    OAuth grant flow.

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
    _init_connect_params = {}


    def __init__(self, app_conf: AppConfig):
        pass
    

    def create_data_setups(self) -> List[DataSetup]:
        """
        Create list of data setup instances

        Returns:
            List: data setups instances
        """   
        return []

