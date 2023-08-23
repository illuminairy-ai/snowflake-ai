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
__version__ = "0.2.0"


from typing import Optional, List, Dict, Callable
from functools import wraps

from snowflake.snowpark.session import Session
import streamlit as st
from streamlit import delta_generator as dg



class PageTab:
    """
    Represents a pagetab in a multi-page Streamlit application.

    Attributes:
        tab_id (str): A unique identifier for the page.
        func (Callable): The function that renders the page content.
        tab (DelaGenerator): streamlit tab container.
        title (str): The title of the page displayed in the navigation.

    Example:
        >>> def page_home():
        ...     st.title("Home")
        ...     st.write("Welcome to the Home page!")
        ...
        >>> home_page = AppPage("home", "Home", page_home)
    """

    def __init__(
        self, 
        tab_id: str,
        func: Callable,
        tab: dg.DeltaGenerator = None,
        title: Optional[str] = None
    ):
        self.tab_id = tab_id
        self.func = func
        self._tab = tab
        if title is None:
            self.title = tab_id
        else:
            self.title = title


    def get_tab(self) -> dg.DeltaGenerator:
        """
        Get the underlying streamlit tab object

        Return:
            DeltaGenerator: Underlying tab object from streamlit for
                this AppTab object
        """
        return self._tab


    def set_tab(self, tab:dg.DeltaGenerator):
        """
        Set the underlying streamlit tab object.

        Args:
            tab (DeltaGenerator): Underlying tab object to be set
        """
        self._tab = tab
        return self



class TabRegistry:
    """
    A registry for managing tabs in a multi-page application.

    Attributes:
        tabs (List[PageTab]): A list of PageTab instances.

    Example:
        >>> hp = HomePage()
        >>> register = hp.tab_regitry.register
        >>> @register("home", "Home")
        ... def home_tab():
        ...     st.title("Home")
        ...     st.write("Welcome to the Home page!")
    """

    def __init__(self):
        self.tab_list: List[PageTab] = []
        self.tab_funcs: List[Callable] = []
        self.tabs: Dict[str, PageTab] = {}


    def register(
        self,
        tab_id: str,
        title: Optional[str] = None
    ) -> Callable:
        """
        Register a tab in the application page.

        Args:
            tab_id (str): A unique identifier for the tab.
            title (Optional[str]): The title of the tab displayed in the
                navigation. Defaults to None.

        Returns:
            Callable: The wrapped page tab function.

        Example:
            >>> registry = TabRegistry()
            >>> @registry.register("home", "Home")
            ... def home_tab():
            ...     st.title("Home")
            ...     st.write("Welcome to the Home page!")
        """

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            pt = PageTab(tab_id, wrapper, None, title)
            self.tab_list.append(pt)
            self.tab_funcs.append(func)
            return wrapper

        return decorator


    def create_page_tabs(self) -> Dict:
        """
        Create a dictionary of PageTab objects with their underlying
        streamlit container objects set.

        Return:
            dict: PageTab object ditionary
        """
        ids = [t.tab_id for t in self.tab_list]
        tbs = st.tabs(ids)

        td = dict(zip(ids, tbs))
        rd = {}

        for pt in self.tab_list:
            if pt.get_tab() is None and pt.tab_id in td:
                pt.set_tab(td[pt.tab_id])
                rd[pt.tab_id] = pt

        self.tabs = rd
        return rd