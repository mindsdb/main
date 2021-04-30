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

from mindsdb.api.mysql.mysql_proxy.utilities import log


class SessionController():
    '''
    This class manages the server session
    '''

    def __init__(self, company_id=None) -> object:
        """
        Initialize the session
        :param socket:
        """

        self.username = None
        self.auth = False
        self.company_id = company_id
        self.logging = log

        self.integration = None
        self.integration_type = None
        self.database = None

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
