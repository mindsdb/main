"""
*******************************************************
 * Copyright (C) 2017 MindsDB Inc. <copyright@mindsdb.com>
 *
 * This file is part of MindsDB Server.
 *
 * MindsDB Server can not be copied and/or distributed without the express
 * permission of MindsDB Inc
 *******************************************************
"""

from mindsdb.interfaces.custom.custom_models import CustomModels
from mindsdb.interfaces.ai_table.ai_table import AITableStore
from mindsdb.interfaces.model.model_interface import ModelInterfaceWrapper
from mindsdb.interfaces.datastore.datastore import DataStoreWrapper
from mindsdb.api.mysql.mysql_proxy.datahub import init_datahub
from mindsdb.api.mysql.mysql_proxy.utilities import log
from mindsdb.utilities.config import Config


class SessionController():
    '''
    This class manages the server session
    '''

    def __init__(self, original_model_interface, original_data_store, company_id=None) -> object:
        """
        Initialize the session
        :param company_id:
        """

        self.username = None
        self.auth = False
        self.company_id = company_id
        self.logging = log

        self.integration = None
        self.integration_type = None
        self.database = None

        self.config = Config()
        self.ai_table = AITableStore(company_id=company_id)
        self.data_store = DataStoreWrapper(
            data_store=original_data_store,
            company_id=company_id
        )
        self.model_interface = ModelInterfaceWrapper(
            model_interface=original_model_interface,
            company_id=None
        )
        self.custom_models = CustomModels(company_id=company_id)
        self.datahub = init_datahub(
            model_interface=self.model_interface,
            custom_models=self.custom_models,
            ai_table=self.ai_table,
            data_store=self.data_store,
            company_id=company_id
        )

        self.prepared_stmts = {}

    def register_stmt(self, statement):
        i = 1
        while i in self.prepared_stmts and i < 100:
            i = i + 1
        if i == 100:
            raise Exception('Too many unclosed queries')

        self.prepared_stmts[i] = dict(
            type=None,
            statement=statement,
            fetched=0
        )
        return i

    def unregister_stmt(self, stmt_id):
        del self.prepared_stmts[stmt_id]
