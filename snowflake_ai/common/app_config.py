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
__version__ = "0.3.0"


from enum import Enum
import os
import sys
from os.path import exists
from pathlib import Path
from typing import Optional, Union, Dict, Tuple, Any
import logging
import toml
from importlib.resources import read_text



class ConfigType(Enum):
    Apps = "apps"
    AppConnects = "app_connects"
    DataSetups = "data_setups"
    BaseApps = "base_apps"
    Streamlits = "streamlit_apps"
    MLPipelines = "ml_pipelines"
    MLOps = "ml_ops"
    OAuthConnects = "oauth_connects"
    DataConnects = "data_connects"
    FeatureStores = "feature_stores"
    ModelRegistries = "model_registries"
    PipelineFlows = "pipeline_flows"



class AppType(Enum):
    Default = "default"
    Streamlit = "streamlit"
    Notebook = "notebook"
    Console = "console"



class ConfigKey(Enum):
    APP_NAME = "app_name"
    NAME = "name"
    APP_SHORT_NAME = "app_short_name"
    TYPE = "type"
    VERSION = "version"
    DOMAIN_ENV = "domain_env"
    AUTH_TYPE = "auth_type"
    INIT_LIST = "init_list"
    ROOT_PATH = "root_path"
    APP_PATH = "app_path"
    SCRIPT = "script"
    SCRIPT_HOME = "script_home"
    CONN_OAUTH = "oauth_connect"
    CONN_DATA = "data_connect"
    STEPS = "steps"
    CLASS = "class"
    RUN = "run"
    ACCT = "account"
    DB = "database"
    WH = "warehouse"
    SCH = "schema"
    ROLE = "role"
    USER = "user"



class AppConfig:
    """
    This class represents the overall enterprise ai/ml application 
    configurations from a directory of configruation files and its
    instance represents a specific application configuration.

    The bootstrapping priority is the following: 

    1) input custom directory 
    2) .snowflake_ai/conf subdir under current directory 
    3) .snowflake_ai/conf subdir under user_home directory
    4) current directory
    5) home directory
    6) conf subdir under snowflake_ai library installation root dir

    Assuming there is a configruation of an application app_1 under a 
    default group named as group_def:

        >>> from snowflake_ai.common import AppConfig
        ... 
        ... # initialize application config for app_1 under group_def
        >>> ac = AppConfig("group_def.app_1")
        ...
        ... # optionally the configs may be loaded from your custom dir
        ... # ac = AppConfig("group_def.app_1", "custom dir", 
        ... #         "config.toml")
        ...
        ... # to create an actual application, e.g., notebook app
        >>> app = NotebookApp("business_group.app_1")
    """

    DEF_CONF_LIB_PATH = "snowflake_ai.conf"
    DEF_CONF_FILE = "app_config.toml"
    DEF_CONF_DIR = "./.snowflake_ai/conf/"
    DEF_CONN = "snflk_svc_def"
    DEF_CURR_PATH = "."
    DEF_HOME_PATH = "~"    

    T_DEFAULT = "default"
    T_DEF = "_def"
    T_CONN_SNFLK = "snowflake"
    T_CONN_FILE = "file"
    T_OAUTH = "oauth"
    T_OAUTH_DEVICE = "device_code"
    T_OAUTH_CODE = "auth_code"
    T_OAUTH_CRED = "client_credentials"
    T_AUTH_SNFLK = "snowflake"
    T_AUTH_KEYPAIR = "keypair"
    T_AUTH_EXT_BROWSER = "externalbrowser"
    T_AUTH_OAUTH = "oauth"    


    logging.basicConfig(
        level=logging.ERROR,
        format='%(asctime)s [%(levelname)s]  %(message)s'
    )
    _logger = logging.getLogger(__name__)
    _logger.addHandler(logging.StreamHandler(sys.stdout))

    _initialized = False
    _configs_root = ''
    _configs_path = ''

    # store all configs at class level
    _configs = {}

    # store appconfig ref by app at class level
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

        # get group_key.app_name , [apps.<group_key>.<app_name>] config
        self.app_key, self.app_config = AppConfig.load_app_config(
                self.app_key)
        self.app_name = self.app_config.get(ConfigKey.APP_NAME.value,
                self.app_key.lower().replace('.', '_') )
        self.app_group = AppConfig.split_group_key(self.app_key)[0]
        self.app_short_name = self.app_config.get(
                ConfigKey.APP_SHORT_NAME.value, 
                self.app_key.lower().replace('.', '_')
            )

        # set app global attrs, app type, and root path
        self.type =  self.get_all_configs()[ConfigType.Apps.value].get(
                ConfigKey.TYPE.value, AppType.Default.value)
        self.root_path =  self.get_all_configs()[ConfigType.Apps.value].get(
                ConfigKey.ROOT_PATH.value, os.path.abspath(AppConfig.DEF_CURR_PATH))

        # initialize app specific attrs, e.g., name, type, path
        self._init_app_base_config()
        if bool(self.app_key) and bool(self.app_config):
            self.apps[self.app_key] = self

        # get base app config
        _, self.app_base_config = AppConfig.get_group_item_config(
                self.app_key, ConfigType.BaseApps.value)
        self.app_connect_refs = self.app_base_config.get(
                ConfigType.AppConnects.value, [])
        self.data_setup_refs = self.app_base_config.get(
                ConfigType.DataSetups.value, [])        
        self.ml_pipeline_refs = self.app_base_config.get(
                ConfigType.MLPipelines.value, [])
        self.ml_ops_refs = self.app_base_config.get(
                ConfigType.MLOps.value, [])

        # get oauth and data connect configs groups
        d_conn: Dict = AppConfig.get_all_configs().get(
                ConfigType.AppConnects.value)
        self.oauth_connect_configs: Dict = {}
        if d_conn is not None:
            d_oauth_conn = d_conn.get(ConfigType.OAuthConnects.value)
            self.oauth_connect_configs = d_oauth_conn
        self.data_connect_configs: Dict  = {}
        if d_conn is not None:
            d_data_conn = d_conn.get(ConfigType.DataConnects.value)
            self.data_connect_configs = d_data_conn

        


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
            Tuple[str, str, Dict]: custom config root dir path, dir path
                from where the configurations are loaded, and the dict
                of all loaded configurations (merged all configs).

        Raises:
            ValueError: If config root, config file or path doesn't exist.
        """
        config_rt = None
        if (config_dir is None) or (not config_dir):
            config_rt = AppConfig.DEF_CURR_PATH
            config_dir = AppConfig.DEF_CONF_DIR
        configs = AppConfig._load_toml_files(config_dir, config_file)
        if config_rt != AppConfig.DEF_CURR_PATH:
            config_rt = config_dir
        if config_rt:
            config_rt = os.path.abspath(config_rt)
        AppConfig._logger.debug(
            f"AppConfig.load_configs(): Load from input default directory"\
            f" - Current_root_dir [{config_rt}]; Config_dir [{config_dir}]."
        )

        home = Path.home()
        if not configs:
            config_rt = os.path.abspath(home)
            config_dir = os.path.join(home, AppConfig.DEF_CONF_DIR)
            configs = AppConfig._load_toml_files(config_dir, config_file)
            AppConfig._logger.debug(
                f"AppConfig.load_configs(): Load from home default directory"\
                f" - Home [{home}]; Config_root [{config_rt}]; "\
                f"Config_dir [{config_dir}]."
            )
        
        if not configs:
            config_rt = os.path.abspath(AppConfig.DEF_CURR_PATH)
            config_dir = config_rt
            configs = AppConfig._load_toml_files(config_dir, config_file)
            AppConfig._logger.debug(
                f"AppConfig.load_configs(): Load from input directory "\
                f"after home default check - Config_root [{config_rt}];"\
                f" Config_dir [{config_dir}]."
            )
        
        if not configs:
            config_rt = os.path.abspath(home)
            config_dir = config_rt
            configs = AppConfig._load_toml_files(config_dir, config_file)        
            AppConfig._logger.debug(
                f"AppConfig.load_configs(): Load from home directory "\
                f"directly - Home [{home}]; Config_root [{config_rt}];"\
                f" Config_dir [{config_dir}]."
            )
        
        if not configs:
            config_dir = AppConfig.DEF_CONF_LIB_PATH
            configs = AppConfig.load_default_configs()
            AppConfig._logger.debug(
                f"AppConfig.load_configs(): Load from library default "\
                f"directory - Config_root [{config_rt}];"\
                f" Config_dir [{config_dir}]."
            )
        
        if (config_rt is not None) and config_rt:
            config_rt =  os.path.abspath(config_rt)
            AppConfig._logger.debug(
                f"AppConfig.load_configs(): Output current configuration"\
                f" bootstrap path - Config_root [{config_rt}];"\
                f" Config_dir [{config_dir}]."
            )
        
        if (config_rt is None) or (not exists(config_rt)):
            s = f"AppConfig.load_configs(): Error - config_root "\
                    f"[{config_rt}] doesn't exist!"
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
                f"AppConfig._load_toml_files(): Directory {config_dir}"\
                " doesn't exist!"
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
                        f"file [{config_file_path}], check format! "\
                        f"Error - {e}!"
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
                            f"list of files from [{config_file_path}], "\
                            f" check format! Error - {e}!"
                        )
                file_ts = os.path.getmtime(config_file_path)

                for key, value in toml_dict.items():
                    key = key.strip().lower()
                    if key not in rd or file_ts > files_tsd[key]:
                        rd[key] = value
                        files_tsd[key] = file_ts

        AppConfig._logger.info(
            f"DataConnect._load_toml_files(): Loaded configuration from "\
            f"path [{config_dir}]; Configuration keys [{rd.keys()}]."
        )        
        AppConfig._logger.debug(
            f"DataConnect._load_toml_files(): Loaded configuration from "\
            f"path [{config_dir}]; Detailed App_config [{rd}]."
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
            AppConfig.DEF_CONF_LIB_PATH, 
            AppConfig.DEF_CONF_FILE
        )   
        configs = toml.loads(config_file)
        AppConfig._logger.debug(
            f"AppConfig.load_default_configs(): Loaded default app"\
            f"configuration; Config keys [{configs.keys()}]."
        )
        return configs
    

    @staticmethod
    def get_group_item_config(
        group_item_key: str,
        root_key: str,
        configs: Optional[Union[Dict, None]] = None
    ) -> Tuple[str, Dict]:
        """
        Get sub configuration (dictionary obj) of the section from three level
        of the overall configuration <root>.<group>.<item>.

        Args:
            group_item_key (str): string in form of <group_key>.<item_key>.
            root_key (str): top level configuration section key, e.g., 
                "apps", "app_connects"
            configs (Dict): overall configuration dictionary

        Returns:
            Tuple[str, dict]: tuple of the key string matched in
            a form of group_key.item_key and the dictionary of loaded
            configuration section corresponding to the group_item_key.
        """
        root_key = root_key.strip().lower() if root_key \
                else ConfigType.Apps.value
        if configs is not None and \
                configs.get(root_key) is None:
            s = f"AppConfig.get_group_item_config(): Error "\
                    f"- [{root_key}] is missing!"
            AppConfig._logger.error(s)
            raise ValueError(s)

        if configs is None:
            configs = AppConfig.get_all_configs()

        rd = {}
        gp_itm_key = group_item_key.strip().lower()
        gk, ik = AppConfig.split_group_key(gp_itm_key)
        if (not gk) and ik:
            k, rd = AppConfig.search_key_by_group(
                ik, root_key, configs
            )
        elif (not gk) and (not ik):
            k, rd = '', {}
        elif gk and (not ik):
            k, rd = f"{gk}.", AppConfig.filter_group_key(
                gk, root_key, configs
            )
        else:
            gs = dict(configs[root_key]).get(gk)
            if gs is not None:
                k = f"{gk}.{ik}"
                rd =  configs[root_key][gk][ik] \
                    if dict(configs[root_key][gk]).get(ik) \
                        is not None else {}
            else:
                k, rd =  f"{gk}.{ik}", {}

        AppConfig._logger.debug(
            f"AppConfig.get_group_item_config(): Loaded group item "\
            f"configration with key [{k}]."
        )
        return (k, rd)


    @staticmethod
    def load_app_config(
        app_key: str, 
        configs: Optional[Union[Dict, None]] = None
    ) -> Tuple[str, Dict]:
        """
        Load app configuration from overall configurations, i.e.
        [apps.<group_key>.<app_name>] config section.

        Args:
            app_key (str): application key.
            configs (Dict): overall configuration dictionary

        Returns:
            Tuple[str, dict]: tuple of the app key string matched in
            a form of group_key.app_name and the dictionary of loaded
            application configurations
        """
        return AppConfig.get_group_item_config(
                app_key,
                ConfigType.Apps.value,
                configs
            )


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
        (group_key.key, dict of matched key section).

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
                if (AppConfig.T_DEF in group_key) or \
                        (AppConfig.T_DEFAULT in group_key):
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
        if root_key not in ConfigType._value2member_map_:
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
                f"AppConfig.get_qualified_key(): Error with key [{key}]!"
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
        # get app long name and group config
        self.name = self.app_config.get(ConfigKey.NAME.value)
        self.group_config = AppConfig.filter_group_key(
            self.app_group, ConfigType.Apps.value, self.get_all_configs()
        )

        s = self.app_config.get(ConfigKey.TYPE.value)
        if s is not None and s:
            self.type = s
        elif self.type is None or not self.type:
            self.type = AppType.Default.value
        self.version = self.app_config.get(ConfigKey.VERSION.value)
        self.domain_env = self.app_config.get(ConfigKey.DOMAIN_ENV.value)

        # app and root path (for app deployment)
        self.app_path = self.app_config.get(ConfigKey.APP_PATH.value, 
                AppConfig.DEF_CURR_PATH)
        s = self.app_config.get(ConfigKey.ROOT_PATH.value)
        if s is not None and s:
            self.root_path = s
        if self.root_path is None or not self.root_path:
            self.root_path = ""

        # set app script home path dir (for all app related scripts)
        self.script_home = self.app_config.get(ConfigKey.SCRIPT_HOME.value,
                 AppConfig.DEF_CURR_PATH)
    

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


    def get_app_path(self) -> str:
        """
        Get the directory of the specified application

        Returns:
            str: the directory of the specified application
        """   
        return self.app_path
    

    def get_app_namespaces(self) -> Tuple[str, str]:
        """
        Get application key in form of (group_key, app_name)

        Returns:
            tuple: (group_key, app_name)
        """   
        rs: Tuple[str, str] = ("" ,"")
        if self.app_key:
            s = self.app_key.rsplit('.', 1)
            if len(s) == 1:
                rs = ("", s[0])
            elif len(s) > 1:
                rs = (s[0], s[1])
        return rs
    