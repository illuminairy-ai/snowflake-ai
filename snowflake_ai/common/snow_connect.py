# Copyright (C) 2023 Tony Liu
#
# This software may be modified and distributed under the terms
# of the BSD 3-Clause license. See the LICENSE file for details.

"""
This module contains SnowConnect class representing a specific Snowflake
connection.
"""

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "BSD 3-Clause"
__version__ = "0.1.0"


import os
import logging
import types
from typing import List, Dict, Optional, Union, Iterator

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

from pandas import DataFrame as DF

from snowflake.snowpark import Session
from snowflake.snowpark import Row
from snowflake.snowpark import DataFrame as SDF

from snowflake_ai.common import DataConnect



class SnowConnect(DataConnect):
    """
    This class provides specific Snowflake connection.

    Example:

    To use this class, just instantiate SnowConnect as follows:

        from snowflake_ai.common import SnowConnect

        connect: SnowConnect = SnowConnect()
        session = connect.get_session()
    """

    _logger = logging.getLogger(__name__)


    def __init__(
        self,
        config_dir : Optional[Union[str, None]] = None, 
        config_file : Optional[Union[str, None]] = None
    ):
        super().__init__(config_dir, config_file)
        self.logger = SnowConnect._logger
        self.session = self.get_session()

        # add Snowflake Session methods
        for method_name in dir(self.session):
            if not method_name.startswith('_'):
                method = getattr(self.session, method_name)
                if isinstance(method, types.MethodType):
                    setattr(self, method_name, method)
    



    def get_session(self) -> Session:
        """
        Get this snowflake connection's session. If there is no existing
        session exists, create a new session.

        Returns:
            Session: Snowflake connection's session
        """
        if not hasattr(self, 'session') or self.session is None:
            conn = self.get_connection()
            if (conn is None) or (not isinstance(conn, Session)):
                raise ValueError(
                    "SnowConnect.get_session(): "\
                    "Initialization error, cannot get Session"
                )
            self.session = conn
        return self.session
    

    def close_session(self):
        """
        Close current snowflake connection and session.
        """
        self.close_connection()


    def close_connection(self) -> int:
        """
        Close current snowflake connection and session.
        """
        if self.session is not None:
            self.session.close()
        return 0


    def create_connection(self, params: Dict):
        """
        Create a snowflake connection based on the specific configuration.
        A set of authentication types are supported:
            snowflake, keypair, oauth (to-do) and externalbrowser (to-do)

        Args:
            params (Dict): Configuration dictionary as input parameters.

        Returns:
            object: snowflake connection session
        """
        if params is None:
            raise ValueError(
                f"SnowConnect.create_connection params cannot be None"
            )
        auth = ""
        try:
            auth = params["auth_type"]
            if auth == "snowflake":
                self.session = self._do_snowflake_auth(params)
            elif auth == "keypair":
                self.logger.debug(f"keypair auth => {params}")
                self.session = self._do_keypair_auth(params)
            elif auth == "externalbrowser":
                self.session = self._do_externalbrowser_auth(params)
            elif auth == "oauth":
                self.session = self._do_oauth_auth(params)
        except Exception as e:
            self.logger.exception(
                f"SnowConnect.create_connection cannot use "\
                f"auth_type={auth} to creaet snowflake session: {e}"
            )
        return self.session


    def _do_snowflake_auth(self, params):
        pass_env: str = params["password_env"]
        password = os.environ[pass_env]
        session = None
        conn_params = {
            "account": params["account"],
            "user": params["user"],
            "authenticator": "snowflake",
            "password": password,
            "role": params["role"], 
            "warehouse": params["warehouse"],
            "database": params["database"],
            "schema": params["schema"]
        }
        try:
            session = Session.builder.configs(conn_params).create() 
        except Exception as e:
            self.logger.exception(
                f"Cannot connect to snowflake using _do_snowflake_auth(): {e}")
        return session


    def _do_keypair_auth(self, params):
        session, pkey, pkb, conn_params = None, None, bytes(), None
        kphrase_env = params["private_key_phrase_env"]
        kphrase = os.environ[kphrase_env]
        kpath_env = params["private_key_path_env"]
        kpath = os.environ[kpath_env]
        try:
            with open(kpath, "rb") as key:
                pkey = serialization.load_pem_private_key(
                    key.read(),
                    password = kphrase.encode(),
                    backend = default_backend()
                )
        except Exception as e:
            self.logger.exception(
                f"Cannot read snowflake user private key file: {e}"
            )
        if pkey:
            try:
                pkb = pkey.private_bytes(
                    encoding = serialization.Encoding.DER,
                    format = serialization.PrivateFormat.PKCS8,
                    encryption_algorithm = serialization.NoEncryption()
                )
            except Exception as e:
                self.logger.exception(
                    f"Cannot read snowflake user private key as bytes: {e}"
                )
        else: 
            raise ValueError(
                "Cannot read snowflake user private key file; "\
                f"check exception and key file path {kpath}."
            )
        if pkb:
            conn_params = {
                "account": params["account"],
                "user": params["user"],
                "private_key": pkb,
                "role": params["role"], 
                "warehouse": params["warehouse"],
                "database": params["database"],
                "schema": params["schema"]
            }
            try:
                session = Session.builder.configs(conn_params).create() 
                c = session.sql("select current_warehouse(), current_role()")\
                    .collect()
                self.logger.info(f"Snowflake connection [OK] => {c}")
            except Exception as ex:
                self.logger.exception(
                    "Cannot connect to snowflake using keyair auth in "\
                    f"_do_keypair_auth(): {ex}"
                )
        else: 
            raise ValueError(
                "Cannot read snowflake user private key bytes in "\
                "_do_keypair_auth()"
            )
        return session


    def _do_externalbrowser_auth(self, params):
        pass


    def _do_oauth_auth(self, params):
        # TO-DO
        pass


    def ddl(self, sql: str) -> List[Row]:
        """
        Excute Data Definition Language statement in Snowflake
        Args:
            sql (str): sql statement string

        Returns:
            List[Row]: list of Snowflake Snowpark Rows
        """
        cmds =  {
            'create', 'drop', 'alter', 'truncate', 'delete', 'replace',
            'rename', 'copy', 'clone', 'show', 'desc', 'undrop',
            'set', 'unset'
        }
        matches = [c for c in cmds if sql.upper().startswith(c.upper())]
        if not matches:
            raise ValueError(
                f"SnowConnect.ddl(): input sql doesn't seem to be DDL [{sql}]"
            )             
        return self.get_session().sql(sql).collect()


    def dml(self, sql: str) -> List[Row]:
        """
        Excute Data Manupilation Language statement in Snowflake
        Args:
            sql (str): sql statement string

        Returns:
            List[Row]: list of Snowflake Snowpark Rows
        """
        cmds =  {'insert', 'update', 'delete', 'call', 'explain'}
        matches = [c for c in cmds if sql.upper().startswith(c.upper())]
        if not matches:
            raise ValueError(
                f"SnowConnect.dml(): input sql doesn't seem to be DML [{sql}]"
            )             
        return self.get_session().sql(sql).collect()

    
    def tcl(self, sql: str) -> List[Row]:
        """
        Excute Tranction Control Language statement in Snowflake
        Args:
            sql (str): sql statement string

        Returns:
            List[Row]: list of Snowflake Snowpark Rows
        """
        cmds =  {'begin', 'commit', 'rollback', 'savepoint', 'release'}
        matches = [c for c in cmds if sql.upper().startswith(c.upper())]
        if not matches:
            raise ValueError(
                f"SnowConnect.tcl(): input sql doesn't seem to be TCL [{sql}]"
            )             
        return self.get_session().sql(sql).collect()

    
    def dcl(self, sql: str) -> List[Row]:
        """
        Excute Data Control Language statement in Snowflake
        Args:
            sql (str): sql statement string

        Returns:
            List[Row]: list of Snowflake Snowpark Rows
        """
        cmds =  {
            'grant', 'revoke', 'create', 'drop', 'alter', 'use', 'show',
            'desc', 'set', 'unset'
        }
        matches = [c for c in cmds if sql.upper().startswith(c.upper())]
        if not matches:
            raise ValueError(
                f"SnowConnect.dcl(): input sql doesn't seem to be DCL [{sql}]"
            )             
        return self.get_session().sql(sql).collect()
    

    def dql(self, sql: str) -> Iterator[DF]:
        """
        Excute Data Query Language (Select) statement in Snowflake
        Args:
            sql (str): sql statement string

        Returns:
            Iterator[DF]: Iterator of Pandas DataFrame list
        """
        cmds =  {'select'}
        matches = [c for c in cmds if sql.upper().startswith(c.upper())]
        if not matches:
            raise ValueError(
                f"SnowConnect.dql(): input sql doesn't seem to be DQL [{sql}]"
            )             
        return self.get_session().sql(sql).to_pandas_batches()
    

    def sdf(self, sql: str) -> SDF:
        """
        Excute Data Query Language (Select) statement in Snowflake
        Args:
            sql (str): sql statement string

        Returns:
            Dataframe: Snowflake Dataframe as result of the query
        """
        cmds =  {'select'}
        matches = [c for c in cmds if sql.upper().startswith(c.upper())]
        if not matches:
            raise ValueError(
                f"SnowConnect.sdf(): input sql doesn't seem to be DQL [{sql}]"
            )
        return self.get_session().sql(sql).to_df()