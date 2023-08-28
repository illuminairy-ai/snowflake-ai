# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains StreamlitApp class representing an application
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
import toml
from functools import wraps
import streamlit as st
from types import ModuleType
from snowflake.snowpark import Session

from snowflake_ai.common import AppConfig, AppType, ConfigType
from snowflake_ai.apps import AppPage, BaseApp
from snowflake_ai.common import OAuthConnect
from snowflake_ai.connect import AuthCodeConnect, ConnectManager,\
        SnowConnect
from snowflake_ai.snowpandas import SetupManager, DataSetup



class StreamlitApp(BaseApp):
    """
    This class represents a Steamlit application's configurations and 
    its corresponding application. Since it is subclass of AppConfig,
    it has the following config directory bootstrapping precedence:

    1) input custom directory 
    2) snowflake_ai/conf subdir under current directory 
    3) snowflake_ai/conf subdir under user_home directory
    4) current directory
    5) home directory
    6) conf subdir under snowflake_ai library installation root dir

    Assuming a streamlit application app_1 under a default group named as
        streamlit_default is configured, this app can be created as:

        >>> from snowflake_ai.apps import StreamlitApp
        ... 
        ... # initialize application config for app_1
        >>> app = StreamlitApp("streamlit_default.app_1")
        ...
        ... # optionally the configs may be loaded from your custom dir
        ... # app = AppConfig("group_0.app_1", "custom dir", "config.toml")
    """

    _logger = logging.getLogger(__name__)

    K_PAGE = "page"
    K_PRE_PAGE = "previous_page"
    T_ST_APP = AppType.Streamlit.value


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
        if StreamlitApp.T_ST_APP != self.type:
            raise TypeError(
                "StreamlitApp.init(): Application type configuration Error."
            )
        self._setup_streamlit_config()
        self.pages : Dict[str, AppPage] = {}
        self.is_logged_in = False
        self.snow_connect = ConnectManager.create_default_snow_connect(self)
        self.default_setup = SetupManager.create_default_snow_setup(
            self, self.snow_connect
        )
        self.default_setup.load_module()


    def _setup_streamlit_config(
        self, 
        conf_file: Optional[str] = None,
        file_content: Optional[str] = None
    ) -> str:
        """
        Setup streamlit app config file.

        Args:
            conf_file (str): config file name
            file_content (str): custom config file content
        
        Retrun:
            str: streamlit configuration file path
        """
        app_dir = os.path.join(self.root_path, self.script_home)
        p = os.path.join(app_dir, ".streamlit/")
        if not exists(p):
            try:
                os.makedirs(p, exist_ok=True)
            except Exception as e:
                raise ValueError(
                    f"Streamlit._setup_streamlit_dir(): Error [{e}] - "\
                    "Cannot create streamlit configuration directory!"
                )

        if conf_file is None:
            conf_file = "config.toml"
        
        conf_path = os.path.join(p, conf_file)

        if not exists(conf_path):
            if file_content is None:
                file_content = self.get_streamlit_config()
            
            if file_content:
                with open(conf_path, "w") as f:
                    f.write(file_content)
        else:
            with open(conf_path, "r") as f:
                current_content = f.read()
            
            if file_content is None:
                file_content = self.get_streamlit_config()
            
            if current_content != file_content:
                if file_content:
                    with open(conf_path, "w") as f:
                        f.write(file_content)

        return conf_path


    def get_streamlit_config(self) :
        """
        Get streamlit app configuration from config file
        """
        # split app_key in form of group_key.app_key into two variables
        gk, k = AppConfig.split_group_key(self.app_key)
        config: Dict[str, Dict] = self.get_all_configs().get(
            ConfigType.Streamlits.value, {}
        )
        self.logger.debug(
            f"StreamlitApp.get_streamlit_config(): Group_key [{gk}]; App_"\
            f"key [{k}]; Config [{config}]."
        )
        if config:
            config = config.get(gk, {})
            if config:
                config = config.get(k, {})
        
        return toml.dumps(config)
    

    def add_page(self, page:AppPage):
        if self.pages:
            self.pages[page.page_id] = page
        

    def _add_page(
        self,
        page_id: str,
        title: Optional[str] = None,
        icon: Optional[str] = None,
        layout: Optional[str] = "wide",
        show_sidebar: Optional[bool] = True,
        func: Optional[Callable] = None,
    ):
        page = AppPage(
            page_id=page_id,
            title=title,
            icon=icon,
            layout=layout,
            show_sidebar=show_sidebar,
            func=func,
        )
        self.pages[page_id] = page


    def register_page(
        self,
        page_id: str,
        title: Optional[str] = None,
        icon: Optional[str] = None,
        layout: Optional[str] = "wide",
        show_sidebar: Optional[bool] = True,
    ) -> Callable:
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            self._add_page(
                page_id=page_id,
                title=title,
                icon=icon,
                layout=layout,
                show_sidebar=show_sidebar,
                func=wrapper,
            )
            return wrapper

        return decorator
    

    def set_pages_config(self):
        for _, pg in self.pages.items():
            st.set_page_config(
                page_title = pg.title,
                page_icon = pg.icon ,
                layout = pg.layout,
            )


    @staticmethod
    def get_current_page():
        query_params = st.experimental_get_query_params()
        return query_params.get(StreamlitApp.K_PAGE, ["/"])[0]
    

    def navigate_to(self, page_id: str):
        previous_page_id = self.get_current_page()
        st.experimental_set_query_params(
            previous_page = previous_page_id
        )
        st.experimental_set_query_params(page=page_id)
        self.run()


    def run(self):
        current_page_id = self.get_current_page()
        page: AppPage = self.pages.get(current_page_id)
        if page:
            page.func()
        else:
            st.error("Page not found!")


    def request_auth_code(
        self,
        oc:OAuthConnect
    ) -> str:
        oc = ConnectManager.create_default_oauth_connect(self)
        _, cc = AuthCodeConnect.generate_pkce_pair()
        params = {
            AuthCodeConnect.K_CODE_CHALLENGE: cc
        }
        return oc.authorize_request(params)


    def request_access_token(
        self,
        oc: OAuthConnect,
        ap: AppPage = None
    ) -> Dict :
        ok: bool = False
        params = oc.prepare_grant_request()
        auth_cd = params.get("auth_code")
        ctx = {}
        if auth_cd is not None and auth_cd:
            tok_res = oc.grant_request(params)
            self.logger.debug(
                f"StreamlitApp.request_access_token(): "\
                f"Token result - [{tok_res}]"
            )
            print()
            if tok_res:
                dc = ConnectManager.create_default_snow_connect(self)
                ctx = oc.decode_token(
                    tok_res, ["access_token", "refresh_token"]
                )
                self.logger.debug(
                    f"StreamlitApp.request_access_token(): "\
                    f"Decode token, Context [{ctx.items()}]"
                )                
                session = dc.create_session(ctx)
                if ap is not None:
                    ap.session = session
                    self.logger.debug(
                        f"StreamlitApp.request_access_token(): "\
                        f"Creation of snowflake session [OK]"
                    )
                tok: Dict = ctx.get("decoded_access_token")
                if tok:
                    fnm = tok.get("given_name", "")
                    lnm = tok.get("family_name", "")
                    st.sidebar.write(
                        f"Welcome, {fnm} {lnm}!"
                    )
                ok = True
        if not ok:
            st.error("Authorization failed.")

        return ctx


    def request_refresh_token(
        self,
        ctx: Dict,
        oc: OAuthConnect,
        ap: AppPage = None
    ) -> Dict :
        ok: bool = False
        params: Dict = oc.prepare_token_refresh(ctx) 
        refresh_tok = params.get("refresh_token")
        ctx = {}
        if  refresh_tok is not None and refresh_tok:
            tok_res = oc.refresh_token_request(params)
            self.logger.debug(
                f"StreamlitApp.request_refresh_token(): "\
                f"Refersh token result- [{tok_res}]"
            )              
            if tok_res:
                dc = ConnectManager.create_default_snow_connect(self)
                ctx = oc.decode_token(
                    tok_res, ["access_token", "refresh_token"]
                )
                if ctx.get("access_token"):
                    self.logger.debug(
                        f"StreamlitApp.request_refresh_token(): "\
                        f"Get access token [OK]"
                    )
                session = dc.create_session(ctx)
                if ap is not None:
                    ap.session = session
                    self.logger.debug(
                        f"StreamlitApp.request_refresh_token(): "\
                        f"Creation snowflake session [OK]"
                    )

                tok: Dict = ctx.get("decoded_access_token")
                if tok:
                    fnm = tok.get("given_name", "")
                    lnm = tok.get("family_name", "")
                    st.sidebar.write(
                        f"Welcome, {fnm} {lnm}!"
                    )
                ok = True
        if not ok:
            st.error("Authorization failed.")

        return ctx
    