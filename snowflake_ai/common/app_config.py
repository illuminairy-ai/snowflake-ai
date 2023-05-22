# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains AppConfig class representing application
configrations.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.2.0"


import os
import sys
from os.path import exists
from pathlib import Path
from typing import Optional, Union, Dict, Tuple
import logging
import toml
from importlib.resources import read_text



class AppConfig:
    """
    This class represents the overall enterprise ai/ml application 
    configurations from a directory of configruation files and its
    instance represents a specific application configuration.

    The bootstrapping priority is the following: 

    1) input custom directory 
    2) snowflake_ai/conf subdir under current directory 
    3) snowflake_ai/conf subdir under user_home directory
    4) current directory
    5) home directory
    6) conf subdir under snowflake_ai library installation root dir

    Assuming there is a configruation of an application app_1 under a 
    default group named as group_0:

        >>> from snowflake_ai.common import AppConfig
        ... 
        ... # initialize application config for app_1 under group_0
        >>> ac = AppConfig("group_0.app_1")
        ...
        ... # optionally the configs may be loaded from your custom dir
        ... # ac = AppConfig("group_0.app_1", "custom dir", "config.toml")
        ...
        ... # get the default application which is streamlit app
        >>> app = ac.create_app()
    """

    DEFAULT_CONF_LIB_PATH = "snowflake_ai.conf"
    DEFAULT_CONF_FILE = "app_config.toml"
    DEFAULT_CONF_DIR = "./snowflake_ai/conf/"
    DEFAULT_CONN = "snowflake_0"
    DEFAULT_CURR_PATH = "."
    DEFAULT_HOME_PATH = "~"    

    T_SNOWFLAKE_CONN = "snowflake"
    T_FILE_CONN = "file"    
    T_ST_ML_APP = "streamlit_ml"
    T_OAUTH = "oauth"

    K_APPS = "apps"
    K_APP_CONN = "app_connects"
    K_DATA_SETUPS = "data_setups"
    K_ST_APPS = "streamlit_apps"
    K_MODEL_DEVS = "model_devs"
    K_MLOPS = "mlops"

    K_DEFAULT = "default"
    K_NAME = "name"
    K_TYPE = "type"
    K_RT_PATH = "root_path"
    K_VERSION = "version"
    K_ENV = "environment"
    K_AUTH_TYPE = "auth_type"

    K_OAUTH_CONN = "oauth_connects"
    K_MLOPS_PPL = "mlops_pipeline"

    RTK_LST = [
        K_APPS, K_APP_CONN, K_DATA_SETUPS, K_ST_APPS,
        K_MODEL_DEVS, K_MLOPS, 
    ]

    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s [%(levelname)s]  %(message)s'
    )
    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))

    _initialized = False
    _configs_root = ''
    _configs_path = ''
    _configs = {}
    _apps = {}


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
        self.logger = AppConfig._logger

        self.app_key = app_key.strip().lower()
        self.root_path = AppConfig._init_all_configs(config_dir, config_file)
        self.app_key, self.app_config = AppConfig.load_app_config(self.app_key)
        self.type =  self.get_all_configs()[AppConfig.K_APPS].get(
            AppConfig.K_TYPE, AppConfig.T_ST_ML_APP
        )
        self.root_path =  self.get_all_configs()[AppConfig.K_APPS].get(
            AppConfig.K_RT_PATH, os.path.abspath(AppConfig.DEFAULT_CURR_PATH)
        )
        self._init_app_base_config()
        if bool(self.app_key) and bool(self.app_config):
            self.apps[self.app_key] = self

        self.app_connect_keys = self.app_config.get(AppConfig.K_APP_CONN, [])
        self.data_setup_keys = self.app_config.get(AppConfig.K_DATA_SETUPS , [])


    @staticmethod
    def load_configs(
        config_dir : Optional[Union[str, None]] = None, 
        config_file : Optional[Union[str, None]] = None
    ) -> Tuple[str, str, Dict]:
        """
        Load configurations from input configuration directory and file if 
        supplied, or load them based on bootstrapping priorities.

        Args:
            config_dir (str): Configuration files directory.
            config_file (str): Configuration file name. If it is none or 
                empty, merging all toml files to create a complete config
                dictionary.

        Returns:
            Tuple[str, str, Dict]: root dir path and dir path from where
                the configurations are loaded, and the dictionary of all
                loaded configurations.

        Raises:
            ValueError: If config root, config file or path doesn't exist.
        """
        config_rt = None
        if (config_dir is None) or (not config_dir):
            config_rt = AppConfig.DEFAULT_CURR_PATH
            config_dir = AppConfig.DEFAULT_CONF_DIR
        configs = AppConfig._load_toml_files(config_dir, config_file)
        if config_rt != AppConfig.DEFAULT_CURR_PATH:
            config_rt = config_dir
        if config_rt:
            config_rt = os.path.abspath(config_rt)
        AppConfig._logger.info(
            f"AppConfig.load_configs(): 1:[config_root : {config_rt}];"\
            f" 1:[config_dir : {config_dir}"
        )

        home = Path.home()
        if not configs:
            config_rt = os.path.abspath(home)
            config_dir = os.path.join(home, AppConfig.DEFAULT_CONF_DIR)
            configs = AppConfig._load_toml_files(config_dir, config_file)
            AppConfig._logger.info(
                f"AppConfig.load_configs(): 2:[config_root : {config_rt}];"\
                f" 2:[config_dir : {config_dir}]"
            )
        
        if not configs:
            config_rt = os.path.abspath(AppConfig.DEFAULT_CURR_PATH)
            config_dir = config_rt
            configs = AppConfig._load_toml_files(config_dir, config_file)
            AppConfig._logger.info(
                f"AppConfig.load_configs(): 3:[config_root : {config_rt}];"\
                f" 3:[config_dir : {config_dir}]"
            )
        
        if not configs:
            config_rt = os.path.abspath(home)
            config_dir = config_rt
            configs = AppConfig._load_toml_files(config_dir, config_file)        
            AppConfig._logger.info(
                f"AppConfig.load_configs(): 4:[config_root : {config_rt}];"\
                f" 4:[config_dir : {config_dir}]"
            )
        
        if not configs:
            config_dir = AppConfig.DEFAULT_CONF_LIB_PATH
            configs = AppConfig.load_default_configs()
            AppConfig._logger.info(
                f"AppConfig.load_configs(): 5:[config_root : {config_rt}];"\
                f" 5:[config_dir : {config_dir}]"
            )
        
        if (config_rt is not None) and config_rt:
            config_rt =  os.path.abspath(config_rt)
            AppConfig._logger.info(
                f"AppConfig.load_configs(): 6:[config_root : {config_rt}];"\
                f" 6:[config_dir : {config_dir}]"
            )
        
        if (config_rt is None) or (not exists(config_rt)):
            s = f"AppConfig.load_configs(): Error - [{config_rt}] "\
                "doesn't exist!"
            AppConfig._logger.error(s)
            raise ValueError(s)

        return (config_rt, config_dir, configs)


    @staticmethod
    def _load_toml_files(
        config_dir : str, 
        config_file : Optional[str] = None
    ) -> Dict:
        rd = {}
        files_tsd = {}
        if (config_dir is not None) and (config_dir.strip()):
            config_dir = os.path.abspath(config_dir.strip())

        if not exists(config_dir):
            AppConfig._logger.warning(
                f"AppConfig._load_toml_files() directory {config_dir} "\
                "doesn't exist"
            )
            return rd

        if (config_file is not None) and bool(config_file.strip()):
            config_file_path = os.path.join(config_dir, config_file)
            with open(config_file_path, 'r') as f:
                try:
                    rd = toml.load(f)
                except Exception as e:
                    raise ValueError(
                        f"AppConfig._load_toml_files(): Cannot load "\
                        f"{config_file_path}, check format. Error - {e}"
                    )
            return rd
        else:
            toml_files = [
                f for f in os.listdir(config_dir) \
                    if f.lower().endswith(".toml")
            ]
            for toml_file in toml_files:
                config_file_path = os.path.join(config_dir, toml_file)
                with open(config_file_path, 'r') as f:
                    try:
                        toml_dict = toml.load(f)
                    except Exception as e:
                        raise ValueError(
                            f"AppConfig._load_toml_files(): Cannot load "\
                            f"files[{config_file_path}], please check format!"\
                            f" Error - {e}."
                        )
                file_ts = os.path.getmtime(config_file_path)

                for key, value in toml_dict.items():
                    key = key.strip().lower()
                    if key not in rd or file_ts > files_tsd[key]:
                        rd[key] = value
                        files_tsd[key] = file_ts
        
        AppConfig._logger.debug(
            f"DataConnect._load_toml_files(): From Dir => "\
            f"{config_dir}. Loaded Configs => {rd}"
        )
        return rd


    @staticmethod
    def load_default_configs() -> Dict:
        """
        Load default configuration from libary.

        Returns:
            dict: loaded configuration as a dictionary
        """
        config_file = read_text(
            AppConfig.DEFAULT_CONF_LIB_PATH, 
            AppConfig.DEFAULT_CONF_FILE
        )   
        configs = toml.loads(config_file)
        AppConfig._logger.info(
            f"Default app configuration loaded => {configs.keys}"
        )
        return configs
    

    @staticmethod
    def load_app_config(
        app_key: str, 
        configs: Optional[Union[Dict, None]] = None
    ) -> Tuple[str, Dict]:
        """
        Load app configuration from overall configurations

        Args:
            app_key (str): application key.
            configs (Dict): overall configuration dictionary

        Returns:
            Tuple[str, dict]: tuple of the app key string matched in
            a form of group.app_key and the dictionary of loaded
            application configurations
        """
        if configs is not None and configs.get(AppConfig.K_APPS) is None:
            s = f"AppConfig.load_app_config() Error: [apps] is missing"
            AppConfig._logger.error(s)
            raise ValueError(s)
        if app_key is None:
            return ('', {})
        
        if configs is None:
            configs = AppConfig.get_all_configs()

        rd = {}
        app_key = app_key.strip().lower()
        gk, ak = AppConfig.split_group_key(app_key)
        if not gk and bool(ak):
            k, rd = AppConfig.search_key_by_group(
                ak, AppConfig.K_APPS, configs
            )
        elif not gk and not ak:
            k, rd = '', {}
        elif gk and not ak:
            k, rd = f"{gk}.", AppConfig.filter_group_key(
                gk, AppConfig.K_APPS, configs
            )
        else:
            gs = dict(configs[AppConfig.K_APPS]).get(gk)
            if gs is not None:
                k = f"{gk}.{ak}" 
                rd =  configs[AppConfig.K_APPS][gk][ak] \
                    if dict(configs[AppConfig.K_APPS][gk]).get(ak) \
                        is not None else {}
            else:
                k, rd =  f"{gk}.{ak}", {}

        AppConfig._logger.info(
            f"AppConfig.load_app_config(): App[{k}] => {rd}"
        )
        return (k, rd)


    @staticmethod
    def filter_group_key(group_key: str, top_key:str, configs: Dict) \
        -> Dict:
        """
        Filter group section of configuration dictionary which has the
        structure of top_key: { group_key : { key1: "value1", key2: {} } }
        so that the returned dictionary group section doesn't have any
        nested dictionaries or subsections, i.e., {key1: "value1"} is
        returned as an example.

        Args:
            group_key (str): A string representing the group key
            top_key (str): A string that is the key of parent of group
                sections as the top level.
            configs (Dict): Configuration dictionary

        Returns:
            A dict containing group section's simple key and value pairs
            with any subsections filtered out.
        """
        if top_key not in configs or group_key not in configs[top_key]:
            return {}

        group_dict: dict = configs[top_key][group_key]

        filtered_dict = {k: v for k, v in group_dict.items() \
            if not isinstance(v, dict)}

        return filtered_dict


    @staticmethod
    def search_key_by_group(key: str, top_key:str, configs: Dict) \
        -> Tuple[str, Dict]:
        """
        Search sections of configs dictionary which has the structure of 
        top_key: { group_key: { key: {} } }, and return a tuple as 
        (group.key, dict of matched key section).

        Args:
            key (str): A string representing the input key to be searched.
            top_key (str): A string that is the key of parent of group
                sections as the top level.
            configs (Dict): Configuration dictionary

        Returns:
            A tuple containing the group_key.key and dict of matched
            key section.
        """
        if top_key not in configs:
            return '',  {}

        default_match = '',  {}
        first_match = '',  {}

        config_groups :dict = configs[top_key]
        for group_key in sorted(config_groups.keys()):
            group_dict = config_groups[group_key]
            if key in group_dict:
                if AppConfig.K_DEFAULT in group_key:
                    default_match = f"{group_key}.{key}", group_dict[key]
                elif first_match == ('', {}):
                    first_match = f"{group_key}.{key}", group_dict[key]

        if default_match != ('', {}):
            return default_match
        else:
            return first_match


    @staticmethod
    def split_group_key(key: str) -> Tuple[str, str]:
        """
        Split an input key by '.' and return group_key and the key without
        group prefix.

        Args:
            key: A string representing the input key to be split.

        Returns:
            A tuple containing the group_key and key without group prefix
            as strings.
        """
        if key is None:
            return '', ''
        key = key.strip().lower()
        split_keys = key.split('.')
        if len(split_keys) > 1:
            group_k = split_keys[0]
            k = split_keys[1]
        else:
            k = split_keys[0]
            group_k = ''
        return (group_k, k)


    @classmethod
    def get_qualified_key(cls, root_key: str, key: str) -> str:
        """
        Get fully qualified key in form of <group_key>.<key>

        Returns:
            A string representing a fully qualified key in config dict.
        """
        if root_key not in AppConfig.RTK_LST:
            raise ValueError(
                f"AppConfig.get_qualified_key(): Error - [{key}] not found!"
            )
        
        gk, k = AppConfig.split_group_key(key)
        if k:
            configs: dict = cls.get_all_configs()
            ret_k, _ = AppConfig.search_key_by_group(k, root_key, configs)
            if not ret_k:
                raise ValueError(
                    f"AppConfig.get_qualified_key(): Error - Key Empty!"
                )
            else:
                g_dict = dict(configs[root_key]).get(gk)
                if g_dict is not None:
                    ret_k = f"{gk}.{k}"
        else:
            raise ValueError(
                f"AppConfig.get_qualified_key(): Error - Input [{key}]"
            )
        return ret_k


    @classmethod
    def _init_all_configs(
        cls,
        config_dir : Optional[Union[str, None]] = None, 
        config_file : Optional[Union[str, None]] = None
    ) -> str:
        """
        Initialize the dictionary of all application configurations

        Returns:
            The root directory path where the configs are loaded from.
        """        
        if not cls._initialized:
            cls._configs_root, cls._configs_path, cls._configs = \
                AppConfig.load_configs(config_dir, config_file)
            cls._initialized = True
        return cls._configs_root


    def _init_app_base_config(self):
        self.name = self.app_config.get(AppConfig.K_NAME)
        self.group = AppConfig.split_group_key(self.app_key)[0]
        s = self.app_config.get(AppConfig.K_TYPE)
        if s is not None and s:
            self.type = s
        elif self.type is None or not self.type:
            self.type = AppConfig.T_ST_ML_APP
        self.version = self.app_config.get(AppConfig.K_VERSION)
        self.environment= self.app_config.get(AppConfig.K_ENV)
        s = self.app_config.get(AppConfig.K_RT_PATH)
        if s is not None and s:
            self.root_path = s
        elif self.root_path is None or not self.root_path:
            self.root_path = os.path.abspath(AppConfig.DEFAULT_CURR_PATH)
        self.app_dir = self.app_config.get("app_dir", ".")
        self.script_home_dir = self.app_config.get("script_home_dir", ".")
        self.group_config = AppConfig.filter_group_key(
            self.group, AppConfig.K_APPS, self.get_all_configs()
        )
    

    @classmethod
    def get_configs_root(cls) -> str:
        """
        Get root path of directory where configurations are loaded.

        Returns:
            str: root path string of directory containing all 
                configuration files.
        """
        if not cls._configs_root:
            cls._configs_root = cls._init_all_configs()
        return cls._configs_root
    

    @classmethod
    def get_configs_path(cls) -> str:
        """
        Get path of directory where configurations are loaded.

        Returns:
            str: path string of directory containing all 
                configuration files.
        """
        if not cls._configs_path:
            cls._init_all_configs()
        return cls._configs_path
    

    @classmethod
    def get_all_configs(cls) -> Dict[str, Dict]:
        """
        Get overall configurations.

        Returns:
            dict: all configurations.
        """ 
        if not cls._configs:
            cls._init_all_configs()
        return cls._configs


    @classmethod
    def get_all_apps(cls) -> Dict:
        """
        Get all initialized applications.

        Returns:
            dict: all initialized applications.
        """  
        return cls._apps


    @property
    def apps(self) -> Dict:
        """
        Get/set a dictionary of all initialized applications.

        Returns:
            dict: a dictionary of all initialized applications.
        """   
        return AppConfig._apps
    

    def get_app_type(self) -> str:
        """
        Get application configured type. Currently supported type
        is "streamlit_ml"

        Returns:
            str: application type string.
        """   
        return self.type


    def get_root_path(self) -> str:
        """
        Get overall apps configuration root path

        Returns:
            str: root directory path for all applications
        """   
        return self.root_path


    def get_app_dir(self) -> str:
        """
        Get the directory of the specified application

        Returns:
            str: the directory of the specified application
        """   
        return self.app_dir
    
