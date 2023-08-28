# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains AppTab class representing an application page tab.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.5.0"


from typing import Optional, List, Dict, Callable
from functools import wraps
import logging

from snowflake.snowpark.session import Session
import streamlit as st
from streamlit import delta_generator as dg

from snowflake_ai.apps import TabRegistry



class AppPage:
    """
    Represents a page in a multi-page Streamlit application.

    Attributes:
        tab_id (str): A unique identifier for the page.
        func (Callable): The function that renders the page content.
        tab (DelaGenerator): streamlit container.
        title (str): The title of the page displayed in the navigation.
        icon (str): The title of the page displayed in the navigation.

    Example:
        >>> def page_home():
        ...     st.title("Home")
        ...     st.write("Welcome to the Home page!")
        ...
        >>> home_page = AppPage("home", "Home", page_home)
    """
    _logger = logging.getLogger(__name__)

    def __init__(
        self,
        page_id: str,
        title: Optional[str] = '',
        icon: Optional[str] = '',
        layout: Optional[str] = "wide",
        show_sidebar: Optional[bool] = True,
        show_tab: Optional[bool] = True,
        session: Session = None,
        func: Optional[Callable] = None,
    ):
        self.page_id = page_id
        if title is None:
            self.title = page_id
        else:
            self.title = title
        self.icon:str = icon
        self.layout:str = layout
        self.show_sidebar = show_sidebar
        self.show_tab = show_tab
        self.tab_registry = TabRegistry()
        self.session = session
        self.func = func


    def link_login(self, url):
        st.markdown(
            f"<a href='{url}' target = '_self'>Login</a>", 
            unsafe_allow_html=True
        )


    def link_logoff(self, url):
        st.markdown(
            f"<a href='{url}' target = '_self'>Logoff</a>", 
            unsafe_allow_html=True
        )


    def add_logo(self):
        st.markdown(
            """
            <style>
                [data-testid="stSidebarNav"] {
                    background-image: url(app/static/ecolab-logo.png);
                    background-repeat: no-repeat;
                    padding-top: 120px;
                    background-position: 8px 140px;
                    background-size: 280px 80px;
                }
                [data-testid="stSidebarNav"]::before {
                    content: "Insight Service";
                    margin-left: 20px;
                    margin-top: 20px;
                    font-size: 30px;
                    position: relative;
                    top: 100px;
                    color: #007AC3;
                }
            </style>
            """,
            unsafe_allow_html=True,
        )
        
        
    def render_sidebar(self):
        """
        Overwritten by its child class
        """
        self.add_logo()
        is_login = getattr(st.session_state, "login", False)
        sts: Dict = st.experimental_get_query_params()
        state = sts.get("state")
        loggedin = False
        if state:
            t = state[0]
            loggedin = True if t == "loggedin" else False
        else:
            loggedin = is_login

        if not loggedin:
            st.experimental_set_query_params(
                page="/", state="loggedoff"
            )
            setattr(st.session_state, "login", False)

        else:
            url = "http://localhost:8501/home"
            with st.sidebar:
                self.link_logoff(url)

            st.experimental_set_query_params(
                page="/", state="loggedin",
            )
            setattr(st.session_state, "login", True)


    def render(self):
        css = '''
            <style>
                .appview-container .main .block-container {{
                    max-width: 100%;
                    padding-top: 0rem;
                    padding-right: 0rem;
                    padding-left: 0rem;
                    padding-bottom: {1}rem; 
                }}

                .stTabs [data-baseweb="tab-list"]
                    button [data-testid="stMarkdownContainer"] 
                    p { font-size: 1.5rem; color: #1D8CCC}

                </style>
            '''
        st.markdown(css, unsafe_allow_html=True)

        if self.show_sidebar:
            self.render_sidebar()

        if self.show_tab:
            self.tab_registry.create_page_tabs()
            for t in self.tab_registry.tab_list:
                with t.get_tab():
                    t.func(self.session)

