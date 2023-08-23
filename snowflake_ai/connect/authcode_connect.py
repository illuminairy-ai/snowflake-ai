# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains OAuthConnect implementation of authorization
code flow.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.3.0"


import base64
import hashlib
import os
import requests
import sys
import re
from typing import Optional, Dict, Tuple, List
import logging
import streamlit as st
from urllib.parse import urlencode

from snowflake_ai.common import AppConfig, OAuthConnect



class AuthCodeConnect(OAuthConnect):
    """
    This class represents an application connect for 
    OAuth Authorization code grant flow.

    To use this class, create an instance as following :

        >>> from snowflake_ai.connect import AuthCodeConnect
        ...
        >>> connect = AuthCodeConnect()
        >>> connect.authorize_request()
        >>> connect.grant_request()
    """

    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))


    K_CODE_VERIFIER = "code_verifier"
    K_CODE_CHALLENGE = "code_challenge"
    K_AUTH_CODE = "auth_code"
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
        self.logger = AuthCodeConnect._logger


    @st.cache_data
    def generate_pkce_pair() -> Tuple[str, str]:
        code_verifier = base64.urlsafe_b64encode(os.urandom(32))\
                .rstrip(b"=").decode("utf-8")
        code_challenge = hashlib.sha256(code_verifier.encode("utf-8"))\
                .digest()
        code_challenge = base64.urlsafe_b64encode(code_challenge)\
                .rstrip(b"=").decode("utf-8")
        return code_verifier, code_challenge


    def authorize_request(self, add_params: Dict) -> str:
        """
        Construct an authorization request url. It is overridden to add
        additional params value for authcode flow.

        Args:
            add_params (Dict): dictionary contains code challenge for PKCE 
                (Proof Key for Code Exchange) 

        Returns:
            str: url string.
        """ 
        if (not add_params) or (not add_params.get(self.K_CODE_CHALLENGE)):
            raise ValueError(
                "AuthcodeConnect.authorize_request(): Additional params dict"\
                " doesn't contain code challenge!"
            )

        url = re.sub(
            "\{.*?\}",f"{self.tenant_id}", self.auth_request_url
        )

        params = {
            "client_id": self.client_id,
            "response_type": self.auth_response_type,
            "redirect_uri": self.redirect_uri,
            "response_mode": self.auth_response_mode,
            "scope": self.scope,
            "code_challenge": add_params[self.K_CODE_CHALLENGE],
            "code_challenge_method": self.code_challenge_method,
            "state": add_params[self.K_CODE_CHALLENGE]
        }

        query_string = urlencode(params)
        auth_url = f"{url}?{query_string}"
        self.logger.debug(
            f"AuthcodeConnect.authorize_request(): Auth_url - [{auth_url}]"
        )
        return auth_url
    

    def prepare_grant_request(self, add_params={}) -> Dict:
        """
        After redirect, prepare parameters for grant token request.
        """
        query_string = st.experimental_get_query_params()
        code: List = query_string.get(self.T_CODE)
        code_verifier, _ = self.generate_pkce_pair()

        add_params = {
            AuthCodeConnect.K_AUTH_CODE: code[0],
            AuthCodeConnect.K_CODE_VERIFIER: code_verifier,
            AuthCodeConnect.K_CLIENT_SECRET: self.client_secret_env
        }
        return add_params


    def grant_request(self, add_params: Dict) -> Dict:
        """
        Make grant request to get access token. This method should be
        overwritten by its child class.

        Args:
            add_params (Dict): additional parameters may required for 
                grant token request.

        Returns:
            dict: a dictionary of grant token response results.
        """
        url = re.sub(
            "\{.*?\}",f"{self.tenant_id}", self.grant_token_request_url
        )

        code = add_params.get(self.K_AUTH_CODE)
        code_verifier = add_params.get(self.K_CODE_VERIFIER)
        sec_env = add_params.get(self.K_CLIENT_SECRET)
        secret = os.environ[str(sec_env)]

        if not code or not code_verifier:
            raise ValueError(
                "AuthCodeConnect.grant_request(): Missing required params "\
                "- code or code_verifier!"
        )
        payload = {
            "client_id": self.client_id,
            "client_secret": secret,
            "scope": self.scope,
            "grant_type": self.grant_type,
            "code": code,
            "redirect_uri": self.redirect_uri,
            "code_verifier": code_verifier,
            "state" : "loggedin"
        }

        headers = {"Content-Type": self.content_type}
        self.logger.debug(
            f"AuthCodeConnect.grant_request(): Payload => {payload}"
        )

        try:
            response = requests.post(url, data=payload, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as http_err:
            self.logger.error(
                f"AuthCodeConnect.grant_request(): HTTP error occurred:"\
                f" {http_err}!"
            )
            return {}
        except Exception as err:
            self.logger.error(
                f"AuthCodeConnect.grant_request(): Other error occurred:"\
                f" {err}!"
            )
            return {}


    def prepare_token_refresh(self, ctx: Dict) -> Dict: 
        add_params = {
            AuthCodeConnect.T_REFRESH_TOKEN: ctx["refresh_token"],
            AuthCodeConnect.K_CLIENT_SECRET: self.client_secret_env
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
        self.redirect_uri = params.get("redirect_uri", '')
        self.code_challenge_method = params.get(
            "code_challenge_method", 'S256'
        )
        self.auth_response_mode = params.get("auth_response_mode", "query")
        return self
