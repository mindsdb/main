import os
from flask import request
from flask_restx import Resource, abort
from flask import current_app as ca
from mindsdb.utilities.log import log
from mindsdb.api.http.namespaces.configs.streams import ns_conf

from mindsdb.interfaces.storage.db import session
from mindsdb.interfaces.storage.db import Stream as StreamDB
from mindsdb.streams.base.base_stream import StreamTypes


def get_integration(name):
    integrations = ca.config_obj.get('integrations', {})
    return integrations.get(name, {})

def get_predictors():
    full_predictors_list = [*request.naitve_interface.get_models(),*request.custom_models.get_models()]
    return [x["name"] for x in full_predictors_list
            if x["status"] == "complete" and x["current_phase"] == 'Trained']


def get_streams():
    streams = session.query(StreamDB).all()
    return [to_dict(stream) for stream in streams]


def to_dict(stream):
    return {"connect_info": stream.connection_params,
            "advanced_info": stream.advanced_params,
            "type": stream._type,
            "predictor": stream.predictor,
            "stream_in": stream.stream_in,
            "stream_out": stream.stream_out,
            "stream_anomaly": stream.stream_anomaly,
            "integration": stream.integration,
            "name": stream.name}


@ns_conf.route('/')
class StreamList(Resource):
    @ns_conf.doc("get_streams")
    def get(self):
        return {'streams': get_streams()}


@ns_conf.route('/<name>')
@ns_conf.param('name', 'Key-value storage stream')
class Stream(Resource):
    @ns_conf.doc("get_stream")
    def get(self, name):
        streams = get_streams()
        for stream in streams:
            if stream["name"] == name:
                return stream
        abort(404, f"Can\'t find steam: {name}")

    @ns_conf.doc("put_stream")
    def put(self, name):
        params = request.json.get('params')
        if not isinstance(params, dict):
            abort(400, "type of 'params' must be dict")
        for param in ["predictor", "stream_in", "stream_out", "integration_name"]:
            if param not in params:
                abort(400, f"'{param}' is missed.")
        integration_name = params['integration_name']
        integration_info = get_integration(integration_name)
        if not integration_info:
            abort(400, f"integration '{integration_name}' doesn't exist.")
        if integration_info["type"] not in ['redis', 'kafka']:
            abort(400, f"only integration of redis or kafka might be used to crate redis streams. got: '{integration_info.type}' type")
        connection_params = params.get('connect', {})
        advanced_params = params.get('advanced', {})
        predictor = params['predictor']
        stream_in = params['stream_in']
        stream_out = params['stream_out']
        stream_anomaly = params.get('stream_anomaly', stream_out)
        _type = params.get('type', 'forecast')
        if predictor not in get_predictors():
            abort(400, f"requested predictor '{predictor}' is not ready or doens't exist")
        stream = StreamDB(_type=_type, name=name, connection_params=connection_params, advanced_params=advanced_params,
                          predictor=predictor, stream_in=stream_in, stream_out=stream_out,
                          integration=integration_name, company_id=request.company_id, stream_anomaly=stream_anomaly)

        session.add(stream)
        session.commit()
        return {"status": "success", "stream_name": name}, 200

    @ns_conf.doc("delete_stream")
    def delete(self, name):
        try:
            session.query(StreamDB).filter_by(company_id=request.company_id, name=name).delete()
            session.commit()
        except Exception as e:
            log.error(e)
            abort(400, str(e))
        return '', 200
