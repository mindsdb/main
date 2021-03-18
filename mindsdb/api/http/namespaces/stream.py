from flask import request
from flask_restx import Resource, abort
from flask import current_app as ca
from mindsdb.utilities.log import log
from mindsdb.api.http.namespaces.configs.streams import ns_conf
from mindsdb.streams.redis.redis_stream import RedisStream


def get_integration(name):
    integrations = ca.config_obj.get('integrations', {})
    return integrations.get(name)

def get_predictors():
    full_predictors_list = [*ca.mindsdb_native.get_models(), *ca.custom_models.get_models()]
    return [x["name"] for x in full_predictors_list
            if x["status"] == "complete" and x["current_phase"] == 'Trained']


@ns_conf.route('/')
class StreamList(Resource):
    @ns_conf.doc("get_streams")
    def get(self):
        return {'streams': ["foo", "bar"]}


@ns_conf.route('/<name>')
@ns_conf.param('name', 'Key-value storage stream')
class Stream(Resource):
    @ns_conf.doc("get_stream")
    def get(self, name):
        log.error("in get")
        if name == "fake":
            abort(404, f"Can\'t find steam: {name}")
        return {"name": name, "status": "fake"}

    @ns_conf.doc("put_stream")
    def put(self, name):
        params = request.json.get('params')
        if not isinstance(params, dict):
            abort(400, "type of 'params' must be dict")
        if 'integration_name' not in params:
            abort(400, "need to specify redis integration")
        integration_name = params['integration_name']
        integration_info = get_integration(integration_name)
        host = integration_info['host']
        port = integration_info['port']
        db = integration_info['db']
        predictor = params['predictor']
        input_stream = params['source_stream']
        if predictor not in get_predictors():
            abort(400, f"requested predictor '{predictor}' is not ready or doens't exist")
        stream = RedisStream(host, port, db, input_stream, predictor)
        log.error(f"running redis stream: host={host}, port={port}, db={db}, source_stream={input_stream}, predictor={predictor}")
        stream.start()
        return {"status": "success", "stream_name": name}, 200
