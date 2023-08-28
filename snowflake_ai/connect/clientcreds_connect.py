# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains client credentials flow OAuthConnect class.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.5.0"


import time
import os
import requests
import sys
import re
from typing import Optional, Dict, Tuple, List
import logging
import streamlit as st
from urllib.parse import urlencode

from snowflake_ai.common import AppConfig, OAuthConnect



class ClientCredsConnect(OAuthConnect):
    """
    This class represents anapplication connect for 
    OAuth Client Credentials grant flow.

    To use this class, create an instance as following :

        >>> from snowflake_ai.connect import ClientCredsConnect
        ...
        >>> connect = ClientCredsConnect()
        >>> connect.grant_request()
    """

    _logger = logging.getLogger(__name__)


    def __init__(
            self, 
            connect_key : Optional[str] = None,
            app_config: AppConfig = None
        ):
        super().__init__(connect_key, app_config)
        self.logger = ClientCredsConnect._logger



    def authorize_request(self, add_params: Dict = {}) -> Dict:
        return add_params
    

    def process_authorize_response(self, json_ctx: Dict) -> Dict:
        return json_ctx
    

    def prepare_grant_request(self, add_params = {}) -> Dict:
        return add_params


    def grant_request(self, add_params: Dict) -> Dict:
        """
        Make grant request to get access token.

        Args:
            add_params (Dict): additional parameters may required for 
                grant token request.

        Returns:
            dict: a dictionary of grant token response results.
        """
        url = re.sub(
            "\{.*?\}",f"{self.tenant_id}", self.grant_token_request_url
        )
        sec_env = add_params.get(self.K_CLIENT_SECRET)
        secret = os.environ[str(sec_env)]

        payload = {
            "client_id": self.client_id,
            "client_secret": secret,
            "scope": self.scope,
            "grant_type": self.grant_type,
        }

        headers = {"Content-Type": self.content_type}
        self.logger.debug(
            f"ClientCredsConnect.grant_request(): Payload => {payload}"
        )

        try:
            response = requests.post(url, data=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as http_err:
            self.logger.error(
                f"ClientCredsConnect.grant_request(): HTTP error occurred:"\
                f" {http_err}!"
            )
            return {}
        except Exception as err:
            self.logger.error(
                f"ClientCredsConnect.grant_request(): Other error occurred:"\
                f" {err}!"
            )
            return {}
    

    def prepare_token_refresh(self, ctx: Dict) -> Dict: 
        return {}


    def refresh_token_request(self, add_params: Dict) -> Dict:
        """
        Make grant request to get refresh token.

        Args:
            add_params (Dict): additional parameters may required for 
                grant token request.

        Returns:
            dict: a dictionary of grant token response results.
        """
        return {}


    def setup_connect(self, params: Optional[Dict] = None):
        """
        Setup OAuthConnect with proper parameters initialized; it
        should be extended by its child class.

        Args:
            params (Dict): input parameters for connection setup.

        Returns:
            OAuthConnect: self with proper parameters initialized
        """
        super().setup_connect(params)
        return self
