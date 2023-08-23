# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

"""
This module contains Pipeline class for ML flow focusing on model
building, training, validation and testing in DEV domain.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.4.0"


import logging
from typing import Any, Dict, Tuple
from datetime import *
from datetime import date as date

from enum import Enum
import shutil
import zipfile
import os
import hashlib
import pickle
import base64
import json
import pandas as pd

from snowflake.snowpark import Window as W
from snowflake.snowpark import DataFrame as SDF
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import col

from snowflake_ai.common import AppConfig, ConfigType, ConfigKey
from snowflake_ai.connect import AppConnect, SnowConnect
from snowflake_ai.connect import DataFrameFactory as DFF
from snowflake_ai.mlops import FlowContext, Pipeline, Step


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]  %(message)s'
)


class LibType(Enum):
    Default = "default"
    SKLearn = "scikit-learn"
    Pytorch = "pytorch"
    TensorFlow = "tensorflow"
    Databricks = "databricks"
    Snowflake = "snowflake"



class SnowRegistry:
    """
    This class provides Snowflake specific pipeline functionality
    """
    _logger = logging.getLogger(__name__)

    def __init__(
            self,
            reg_key: str,
            ctx: FlowContext,
            app_config: AppConfig = None
    ) -> None:
        self.logger = SnowRegistry._logger
        self.registry_key = reg_key
        self.context = ctx
        self.app_config = app_config
        self.app_configs: Dict = app_config.get_all_configs() if app_config \
                is not None else AppConfig.get_all_configs()
        self.data_connect: SnowConnect = None

        # load and init config
        self._init_config()



    def _init_config(self):
        gk, k = AppConfig.split_group_key(self.registry_key)
        reg_config: Dict = self.app_configs[ConfigType.MLOps.value][gk][k]
                
        if (reg_config[ConfigKey.TYPE.value] == AppConfig.T_CONN_SNFLK)\
                and (reg_config.get(ConfigKey.CONN_DATA.value) is not None):
            dconn_ref = reg_config.get(ConfigKey.CONN_DATA.value)
            ac = AppConnect.get_app_connects().get(dconn_ref)
            if ac is None:
                ac = SnowConnect(dconn_ref, self.app_config)
                AppConnect.get_app_connects()[dconn_ref] = ac

            self.data_connect = ac
            self.logger.debug(
                "SnowRegistry._init_config(): Initialize [{dconn_ref}] "\
                f"DataConnect for Registry [{self.registry_key}]."
            )
        
        # get registry table
        self.registry_table = reg_config.get(ConfigKey.REG_TABLE.value)
        if self.registry_table is None:
            self.logger.error(
                "SnowRegistry._init_config(): Configuration doesn't have "\
                f"registry table defined."
            )


    def update_model_registry(
        self,
        pipeline: Pipeline,
        step: Step,
        ref_id: str = "",
        model_name: str = "",
        model_type: str = "",
        model_path: str = "",
        lib_type: LibType = LibType.Default,
        model: Any = None,
        model_version: str = "",
        user_id: str = ""
    ):
        nmsp: str = self.context.data.get("app_namespace", "")
        app_nm: str = self.context.data.get("app_name", "")
        ts: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if not ref_id:
            ref_id = model_name.strip().replace(" ", "_")
        ref_id = ref_id.lower()
        mdl_id = SnowRegistry.to_model_id(ref_id)

        mdl_b64, tmp_f = "", "", ""
        if lib_type == LibType.Pytorch:
            mdl_b64, tmp_f = SnowRegistry.to_string_from_model_path(\
                    model_path, mdl_id)
        else:
            mdl_b64 = SnowRegistry.to_string_from_model(model)

        js_params = json.dumps(self.context.params)
        js_metrics = json.dumps(self.context.metrics)

        mdl_ver: str = model_version
        if (not model_version) and self.app_config:
            mdl_ver = self.app_config.version

        column_names = [
            "APP_NAMESPACE", "APP_NAME", "PIPELINE",
            "STEP", "REF_ID", "MODEL_NAME", "MODEL_ID", 
            "MODEL_TYPE",  "MODEL_PATH", "LIB_TYPE", 
            "MODEL_BIN", "STEP_PARAMS", "STEP_METRICS", 
            "USER", "VERSION", "TIMESTAMP"
        ]
    
        session = self.data_connect.create_service_session()

        if not user_id:
            user_id = session.get_current_role()

        df_mdl = pd.DataFrame([[
                nmsp, app_nm, pipeline.pipeline_key, 
                step.func.__name__, ref_id, model_name, mdl_id,
                model_type, model_path, lib_type.value, 
                mdl_b64, js_params, js_metrics,
                user_id, mdl_ver, ts
            ]],
            columns=column_names)

        tbl_nm = AppConfig.T_REG_TBL_DEF
        if self.registry_table is not None:
            tbl_nm = self.registry_table

        params: Dict = self.data_connect.connect_params
        db_nm = params.get(ConfigKey.DB.value, "")
        scm_nm = params.get(ConfigKey.SCH.value)
        session.write_pandas(
            df = df_mdl, 
            database = db_nm,
            schema = scm_nm,
            table_name = tbl_nm, 
            auto_create_table=True,
            overwrite=False
        )

        self.logger.debug(
            "SnowRegistry.update_model_registry(): Save Model "\
            f"[{model_name}] to Table [{tbl_nm}] [OK]."
        )
        # clean up 
        SnowRegistry.clean_up_model_file(tmp_f, model_path)


    def query_model_registry(
            self,
            pipeline: Pipeline,
            step: Step,
            model_name: str = "",
            ref_id: str = "",
            timestamp: str = ""
    ) -> Tuple[Any, str, Dict]:
        nmsp: str = self.context.data.get("app_namespace", "")
        app_nm: str = self.context.data.get("app_name", "")
        tbl_nm: str = self.registry_table if self.registry_table \
                else AppConfig.T_REG_TBL_DEF
        
        session = self.data_connect.create_service_session()
        sdf_mdl: SDF = DFF.create_sdf(tbl_nm, session)

        window_spec = W.partitionBy("APP_NAMESPACE", "APP_NAME", 
                "PIPELINE", "STEP", "MODEL_NAME", "REF_ID")

        sdf_mdl = sdf_mdl.withColumn("max_timestamp", F.max("TIMESTAMP")\
                .over(window_spec))

        sdf_mdl = sdf_mdl.filter(
            (col("APP_NAMESPACE") == nmsp) & 
            (col("APP_NAME") == app_nm) & 
            (col("PIPELINE") == pipeline.pipeline_key) & 
            (col("STEP") == step.func.__name__) & 
            (col("MODEL_NAME") == model_name) & 
            (col("REF_ID") == ref_id) & 
            (col("TIMESTAMP") == col("max_timestamp")) if not timestamp else\
            (col("TIMESTAMP") == timestamp)
        )

        r_mdl_res = sdf_mdl.select("MODEL_BIN", "MODEL_PATH", 
                "STEP_METRICS", "LIB_TYPE").collect()
        
        if len(r_mdl_res) == 0:
            self.logger.warn(
                "SnowRegistry.query_model_registry(): Model"\
                f"[{model_name}], Ref[{ref_id}], TS[{timestamp}] "\
                "query result is empty!"
            )
            return (None, "", {})

        mdl_b64 = r_mdl_res[0]["MODEL_BIN"]
        mdl_path: str = r_mdl_res[0]["MODEL_PATH"]
        js_metrics = r_mdl_res[0]["STEP_METRICS"]
        d_metrics: Dict = json.loads(js_metrics)

        if r_mdl_res[0]["LIB_TYPE"] == LibType.Pytorch.value:
            mdl = SnowRegistry.to_model_file_from_string(
                    mdl_b64, mdl_path)
        else:
            mdl_b64 = r_mdl_res[0]["MODEL_BIN"]
            pkl_mdl = base64.b64decode(mdl_b64)
            mdl = pickle.loads(pkl_mdl)

        return (mdl, mdl_path, d_metrics)


    @staticmethod
    def to_model_id(model_ref_id: str) -> str:
        return hashlib.sha256(model_ref_id.encode("utf-8"))\
                .hexdigest()    


    @staticmethod
    def to_string_from_model(model: Any) -> str:
        """
        convert ML model in python (generally from SKLearn) to B64 
        encoded string.
        """
        pkl_mdl_b64 = ""
        if model is not None: 
            pkl_mdl_bin = pickle.dumps(model)
            pkl_mdl_b64 = base64.b64encode(pkl_mdl_bin).decode()
        
        return pkl_mdl_b64


    @staticmethod
    def to_string_from_model_path(
            model_path: str,
            model_id: str = ""
    ) -> Tuple[str, str]:
        """
        Packege model path and files from file system to B64 encoded
        string and include a temporary file for the compressed file dir.
        """
        s_rs : str = ""
        temp_file : str = model_id + ".zip" if model_id else "model.zip"
        if model_path:
            shutil.make_archive(temp_file, 'zip', model_path)
            with open(temp_file, 'rb') as file:
                s_rs = base64.b64encode(file.read()).decode()

        return (s_rs, temp_file)
    

    @staticmethod
    def to_model_file_from_string(
            model_str: str,
            model_path: str,
            model_id: str = ""
    ) -> Any:
        decoded_mdl = base64.b64decode(model_str)

        temp_file = model_id + ".zip" if model_id else "model.zip"
        with open(temp_file, 'wb') as file:
            file.write(decoded_mdl)

        with zipfile.ZipFile(temp_file, 'r') as zip_ref:
            zip_ref.extractall(model_path)

        os.remove(temp_file)


    @staticmethod
    def clean_up_model_file(
            temp_file: str,
            dir_path: str = ""
    ):
        if temp_file is not None and temp_file:
            os.remove(temp_file)

        if dir_path:
            shutil.rmtree(dir_path)
    