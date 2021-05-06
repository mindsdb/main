import base64

from mindsdb.api.mongo.classes.scram import Scram


class Session():
    def __init__(self, server_mindsdb_env):
        self.config = server_mindsdb_env['config']
        self.mindsdb_env = {'company_id': None}
        self.mindsdb_env.update(server_mindsdb_env)

    def init_scram(self, method):
        self.scram = Scram(method=method, get_salted_password=self.get_salted_password)

    def get_salted_password(self, username, method=None):
        real_user = self.config['api']['mongodb'].get('user', '')
        password = self.config['api']['mongodb'].get('password', '')
        if username != real_user:
            raise Exception(f'Wrong username {username}')

        salted_password = self.scram.salt_password(real_user, password)

        return base64.b64decode(self.scram.salt), salted_password
