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
__version__ = "0.4.0"


import sys
from typing import Optional, Dict, Tuple, List, Union
import logging
import jwt

from snowflake_ai.common import ConfigKey, ConfigType
from snowflake_ai.common import AppConfig, AppConnect



class OAuthConnect(AppConnect):
    """
    This class represents a generic application connect for 
    OAuth grant flow.

    To use this class, create instance from AppConfig or extend
    it in a child class for creation of a specific OAuth connect
    instance, e.g., AuthCodeConnect as following :

        >>> from snowflake_ai.connect import AuthCodeConnect
        ...
        >>> connect = AuthCodeConnect()
        >>> connect.authorize_request()
        >>> connect.grant_request()
    """

    K_OAUTH_CONN = ConfigType.OAuthConnects.value
    K_INIT_LIST = ConfigKey.INIT_LIST.value
    K_CLIENT_SECRET = "client_secret_env"

    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))

    _initialized = False

    # oauth_connects.<connect_name> : configs dict
    _init_connect_params = {}


    def __init__(
            self, 
            connect_key : Optional[str] = None,
            app_config: AppConfig = None
        ):
        super().__init__(connect_key, app_config)
        self.logger = OAuthConnect._logger

        self.oauth_connect_key = None

        if self.connect_key:
            _, self.oauth_connect_key = AppConfig.split_group_key(
                self.connect_key
            )
            self.setup_connect(self.connect_params)

        # setup default oauth connect
        if (connect_key is None) or (not connect_key):
            d_configs : Dict = None
            oauth_configs: Dict = None
            if self.app_config is None:
                d_configs = self._configs.get(ConfigType.AppConnects.value)
                if d_configs is not None:
                    oauth_configs = d_configs.get(OAuthConnect.K_OAUTH_CONN)
            else:
                oauth_configs = self.app_config.oauth_connect_configs
            k = AppConnect.search_default_key(oauth_configs)
            self.oauth_connect_key = k
            icps: Dict = self._init_connect_params.get(k)
            if icps is None:
                self.connect_params = self._configs\
                        [ConfigType.AppConnects.value]\
                        [OAuthConnect.K_OAUTH_CONN][k]
            else:
                self.connect_params = icps
                
            self.connect_type = self.connect_params.get(
                    ConfigKey.TYPE.value, '')
            self.connect_name = self.connect_params.get(
                    ConfigKey.NAME.value, '')
            self.connect_group = OAuthConnect.K_OAUTH_CONN
            self.connect_key = f"{OAuthConnect.K_OAUTH_CONN}.{k}"
            self.setup_connect(self.connect_params)



    @property
    def init_connect_params(self) -> Dict[str, object]:
        """
        Get a dictionary of initialized oauth connection config parameters.

        Returns:
            dict: initialized oauth connection config parameters.
        """   
        return OAuthConnect._init_connect_params
    

    def prepare_authorize_request(self, add_params: Dict = {}) -> Dict:
        pass


    def authorize_request(self, add_params: Dict = {}) -> Union[str, Dict]:
        """
        Construct an authorization request url. This method should be 
        overwritten by its child class.

        Args:
            add_params (Dict): additional parameters may be required for 
                authorization request.

        Returns:
            str: response json string.
        """
        return ""
    

    def process_authorize_response(self, json_ctx: Dict) -> Dict:
        """
        Construct an authorization request url. This method should be 
        overwritten by its child class.

        Args:
            json_ctx (Dict): json context in form of dictionary

        Returns:
            Dict: updated json context
        """
        return {}


    def prepare_grant_request(self, add_params: Dict = {}) -> Dict:
        pass


    def grant_request(self, add_params: Dict = {}) -> Dict:
        """
        Make grant request to get access token. This method should be
        overwritten by its child class.

        Args:
            add_params (Dict): additional parameters may required for 
                grant token request.

        Returns:
            dict: a dictionary of grant token response results.
        """   
        return add_params
    

    def process_grant_response(self, json_ctx: Dict = {}) -> Dict:
        pass


    def generate_pkce_pair(self) -> Tuple[str, str]:
        """
        Generate pkce pair for Authorization code grant flow. For other
        types of flows, empty strings are returned

        Returns:
            tuple(str, str): code_verifier, code_challenge
        """
        pass



    @staticmethod
    def is_jwt(token: str) -> bool:
        try:
            jwt.get_unverified_header(token)
            return True
        except jwt.InvalidTokenError:
            return False
        

    def decode_token(
        self, 
        token_result: Dict, 
        token_types: List[str]
    ) -> Dict:
        """
        Decode access and refresh token.

        Example:

            ctx = oc.decode_token(
                tok_res, ["access_token", "refresh_token"]
            )

        Returns:
            Dict(str, obj): dictionary of token attributes
        """
        rt = {}
        for tok_t in token_types:
            self.logger.debug(
                f"OauthConnect.decode_token(): Token type [{tok_t}]"
            )
            token = token_result.get(tok_t)
            if not token:
                self.logger.error(
                    "OAuthConnect.decode_token(): No token is found "\
                    "in the provided response result!"
                )
            elif self.is_jwt(token):
                self.logger.debug(
                    f"OauthConnect.decode_token(): JWT token [{token}]."
                )
                dtok = jwt.decode(token, options={
                    "verify_signature": self.verify_signature}
                )
                rt[tok_t] = token
                rt[f"decoded_{tok_t}"] = dtok
            else:
                self.logger.warn(
                    f"OauthConnect.decode_token(): Not JWT token [{token}]!"
                )
                rt[tok_t] = token

        return rt


    def prepare_token_refresh(self) -> Dict:
        pass


    def refresh_token_request(self, add_params: Dict) -> Dict:
        """
        Make grant request to get refresh token. This method should be
        overwritten by its child class.

        Args:
            add_params (Dict): additional parameters may required for 
                grant token request.

        Returns:
            dict: a dictionary of grant token response results.
        """
        pass


    def setup_connect(self, params: Optional[Dict] = None):
        """
        Setup OAuthConnect with proper parameters initialized; it
        should be extended by its child class.

        Args:
            params (Dict): input parameters for connection setup.

        Returns:
            OAuthConnect: self with proper parameters initialized
        """
        self.connect_type = params.get(
                ConfigKey.TYPE.value, AppConfig.T_OAUTH_CODE)
        self.content_type = params.get(
                "content_type", 
                "application/x-www-form-urlencoded"
            )
        self.auth_request_url = params.get("auth_request_url", '')
        self.auth_response_fields = params.get("auth_response_fields", [])
        self.auth_response_type = params.get("auth_response_type")
        self.tenant_id = params.get("tenant_id", '')
        self.client_id = params.get("client_id", '')
        self.scope = params.get("scope", '')
        self.auth_response_errors = params.get(
                "auth_response_errors", 
                ["error", "error_description"]
            )
        self.grant_token_request_url = params.get(
                "grant_token_request_url", ''
            )
        self.grant_type = params.get("grant_type", "authorization_code")
        self.grant_token_response_fields = params.get(
                "grant_token_response_fields", ["access_token"]
            )
        self.client_secret_env = params.get(
                self.K_CLIENT_SECRET, "SNOWFLAKE_DEFAULT_APP_SECRET"
            )
        self.verify_signature = params.get("verify_signature", False)
        return self


    def init_connects(self) -> int:
        """
        Initialize a list of OAuthConnect configuration parameter configs.

        Returns:
            int: 0 - if it hasn't been initialized; other integer means 
                number of parameters dict have been initialized.
        """
        if not OAuthConnect._initialized:
            conn_group_dict = AppConfig.filter_group_key(
                OAuthConnect.K_OAUTH_CONN, 
                AppConnect.K_APP_CONN,
                self._configs
            )
            lst = conn_group_dict.get(OAuthConnect.K_INIT_LIST)
            if lst is not None and len(lst) > 0 :
                for oconn in lst:
                    dc: Dict = self._configs[AppConnect.K_APP_CONN]\
                        [OAuthConnect.K_OAUTH_CONN]
                    params: Dict = dc.get(oconn)
                    if params is None:
                        raise ValueError(
                            "OAuthConnect.init_connets(): Error - ["\
                            f"{oconn}] doesn't exist in the configuration!"
                        )
                    else:
                        self.init_connect_params[oconn] = params
                self.logger.debug(
                    f"OAuthConnect.init_connects(): Initialized OAuth - "\
                    f"{self.init_connect_params}"
                )
                OAuthConnect._initialized = True
        
        return len(self.init_connect_params)
