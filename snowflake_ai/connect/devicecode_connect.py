# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains Device code flow OAuthConnect class.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.3.0"


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



class DeviceCodeConnect(OAuthConnect):
    """
    This class represents anapplication connect for 
    OAuth Device code grant flow.

    To use this class, create an instance as following :

        >>> from snowflake_ai.connect import DeviceCodeConnect
        ...
        >>> connect = DeviceCodeConnect()
        >>> connect.authorize_request()
        >>> connect.grant_request()
    """

    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))


    K_CODE_VERIFIER = "code_verifier"
    K_CODE_CHALLENGE = "code_challenge"
    K_DEVICE_CODE = "device_code"
    T_CODE = "code"
    T_ACCESS_TOKEN = "access_token"
    T_REFRESH_TOKEN = "refresh_token"


    _initialized = False
    _init_connect_params = {}


    def __init__(
            self, 
            connect_key : Optional[str] = None,
            app_config: AppConfig = None
        ):
        super().__init__(connect_key, app_config)
        self.logger = DeviceCodeConnect._logger


    def authorize_request(self, add_params: Dict = {}) -> Dict:
        """
        Make an authorization request. This method is overridden 

        Args:
            add_params (Dict): dictionary contains code challenge for PKCE 
                (Proof Key for Code Exchange) 

        Returns:
            str: url string.
        """ 
        url = re.sub(
            "\{.*?\}",f"{self.tenant_id}", self.auth_request_url
        )

        params = {
            "client_id": self.client_id,
            "scope": self.scope,
        }

        try:
            response = requests.post(url, data=params)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as http_err:
            self.logger.error(
                f"DeviceCodeConnect.authorize_request(): HTTP error"\
                f" occurred: {http_err}!"
            )
            return {}
        except Exception as err:
            self.logger.error(
                f"DeviceCodeConnect.authorize_request(): Other error"\
                f" occurred: {err}!"
            )
            return {}
    

    def process_authorize_response(self, json_ctx: Dict) -> Dict:
        """
        Construct an authorization request url. This method should be 
        overwritten by its child class.

        Args:
            json_ctx (Dict): json context in form of dictionary

        Returns:
            Dict: input json context
        """
        device_code = json_ctx['device_code']
        user_code = json_ctx['user_code']
        verification_uri = json_ctx['verification_uri']
        expires_in = json_ctx['expires_in']
        interval = json_ctx.get('interval', 10)

        print(f"Please go to {verification_uri} and enter this user"\
                f"code:\n{user_code}")
        self.logger.debug("DeviceCodeConnect.process_authorize_response(): "\
                f"Device_code [{device_code}]; Expires [{expires_in}]")

        ctx = self.prepare_grant_request(json_ctx)
        url = ctx.get("url", "")
        payload = ctx.get("payload", {})
        headers = ctx.get("headers", {})

        for _ in range(0, expires_in, interval):
            response = requests.post(url, data=payload, headers=headers)
            if response.status_code == 200:
                a_token = dict(response.json()).get('access_token', '')
                json_ctx["access_token"] = a_token
                r_token = dict(response.json()).get('refresh_token', '')
                json_ctx["refresh_token"] = a_token
                break
            elif response.status_code == 400:
                print('Waiting for user to authorize...')
            else:
                print('OAuth Device Code Flow Error Occurred:', 
                        response.json())
                break

            time.sleep(interval)
        
        return json_ctx
    

    def prepare_grant_request(self, add_params = {}) -> Dict:
        """
        Prepare grant token request
        """
        url = re.sub(
            "\{.*?\}",f"{self.tenant_id}", self.grant_token_request_url
        )

        code = add_params.get(self.K_DEVICE_CODE)

        if not code:
            raise ValueError(
                "DeviceCodeConnect.grant_request(): Missing required params "\
                "- code!"
        )
        payload = {
            "client_id": self.client_id,
            "scope": self.scope,
            "grant_type": self.grant_type,
            "device_code": code,
        }

        headers = {"Content-Type": self.content_type}
        self.logger.debug(
            f"DeviceCodeConnect.prepare_grant_request(): Payload => {payload}"
        )

        add_params["url"] = url
        add_params["headers"] = headers
        add_params["payload"] = payload

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
        return {}
    

    def prepare_token_refresh(self, ctx: Dict) -> Dict: 
        add_params = {
            DeviceCodeConnect.T_REFRESH_TOKEN: ctx["refresh_token"],
            DeviceCodeConnect.K_CLIENT_SECRET: self.client_secret_env
        }
        return add_params


    def refresh_token_request(self, add_params: Dict) -> Dict:
        """
        Make grant request to get refresh token.

        Args:
            add_params (Dict): additional parameters may required for 
                grant token request.

        Returns:
            dict: a dictionary of grant token response results.
        """
        url = re.sub(
            "\{.*?\}",f"{self.tenant_id}", self.grant_token_request_url
        )

        refresh_tok = add_params.get(self.T_REFRESH_TOKEN)
        sec_env = add_params.get(self.K_CLIENT_SECRET)
        secret = os.environ[str(sec_env)]

        payload = {
            "client_id": self.client_id,
            "scope": self.scope,
            "grant_type": self.T_REFRESH_TOKEN,
            "refresh_token": refresh_tok,
            "client_secret": secret
        }

        headers = {"Content-Type": self.content_type}
        self.logger.debug(
            f"AuthCodeConnect.refresh_token_request(): Payload => {payload}"
        )

        try:
            response = requests.post(url, data=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as http_err:
            self.logger.error(
                f"AuthCodeConnect.refresh_token_request(): "\
                f"HTTP error occurred: {http_err}!"
            )
            return {}
        except Exception as err:
            self.logger.error(
                f"AuthCodeConnect.refresh_token_request(): "\
                f"Other error occurred: {err}!"
            )
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
