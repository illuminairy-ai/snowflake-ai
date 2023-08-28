# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains ConfigManager class for managing AppConfig objects
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.5.0"


import logging
from typing import Dict, List, Optional, Union

from snowflake_ai.common import AppConfig


class ConfigManager:
    """
    This class manages multiple AppConfig objects to ensure only
    one instance for a particular app_key. 
    """

    _logger = logging.getLogger(__name__)
    _app_confs : Dict[str, AppConfig] = {}


    def __init__(self):
        super().__init__()



    @staticmethod
    def get_app_config(
        app_key : str,
        config_dir : Optional[Union[str, None]] = None, 
        config_file : Optional[Union[str, None]] = None
    ) -> AppConfig:
        if not app_key:
            ConfigManager._logger.error(
                    f"ConfigManager.get_app_config(): Error - "\
                    f"app_key [{app_key}] is empty!")
            return None

        ac = ConfigManager._app_confs.get(app_key)
        if ac is None:
            ac = AppConfig(
                app_key=app_key, 
                config_dir=config_dir, 
                config_file=config_file
            )
            ConfigManager._app_confs[app_key] = ac
        
        return ac
