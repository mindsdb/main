import os
import psutil
import tempfile
from pathlib import Path

from flask import request
from flask_restx import Resource
from flask import current_app as ca

from mindsdb.utilities.log import log
from mindsdb.api.http.namespaces.configs.util import ns_conf
from mindsdb.utilities.telemetry import (
    enable_telemetry,
    disable_telemetry,
    telemetry_file_exists,
    inject_telemetry_to_static
)



@ns_conf.route('/ping')
class Ping(Resource):
    @ns_conf.doc('get_ping')
    def get(self):
        '''Checks server avaliable'''
        return {'status': 'ok'}


@ns_conf.route('/ping_native')
class PingNative(Resource):
    @ns_conf.doc('get_ping_native')
    def get(self):
        ''' Checks server use native for learn or analyse.
            Will return right result only on Linux.
        '''
        if os.name != 'posix':
            return {'native_process': False}

        response = {
            'learn': False,
            'predict': False,
            'analyse': False
        }

        for process_type in response:
            p = Path(tempfile.gettempdir()).joinpath(f'mindsdb/processes/{process_type}/')
            if not p.is_dir():
                continue
            pids = [int(x.name) for x in p.iterdir()]
            for pid in pids:
                try:
                    psutil.Process(pid)
                except Exception:
                    p.joinpath(str(pid)).unlink()
                else:
                    response[process_type] = True

        return response


@ns_conf.route('/telemetry')
class Telemetry(Resource):
    @ns_conf.doc('get_telemetry_status')
    def get(self):
        storage_dir = ca.config_obj['storage_dir']
        status = "enabled" if telemetry_file_exists(storage_dir) else "disabled"
        return {"status": status}

    @ns_conf.doc('set_telemetry')
    def post(self):
        data = request.json
        action = data['action']
        if str(action).lower() in ["true", "enable", "on"]:
            enable_telemetry(ca.config_obj['storage_dir'])
        else:
            disable_telemetry(ca.config_obj['storage_dir'])
        inject_telemetry_to_static(ca.config_obj.paths['static'])
        return '', 200
