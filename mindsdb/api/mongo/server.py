import socketserver as SocketServer
import socket
import struct
import bson
from bson import codec_options
from collections import OrderedDict
from abc import abstractmethod

from mindsdb.api.mongo.classes import RespondersCollection, Session

from mindsdb.api.mongo.responders import responders
import mindsdb.api.mongo.functions as helpers
from mindsdb.api.mongo.utilities import log

from mindsdb.interfaces.datastore.datastore import DataStore
from mindsdb.interfaces.model.model_interface import ModelInterface as NativeInterface

OP_REPLY = 1
OP_UPDATE = 2001
OP_INSERT = 2002
OP_QUERY = 2004
OP_GET_MORE = 2005
OP_DELETE = 2006
OP_KILL_CURSORS = 2007
OP_MSG = 2013

BYTE = '<b'
INT = '<i'
UINT = '<I'
LONG = '<q'


def unpack(format, buffer, start=0):
    end = start + struct.calcsize(format)
    return struct.unpack(format, buffer[start:end])[0], end


def get_utf8_string(buffer, start=0):
    end = buffer.index(b"\x00", start)
    s = buffer[start:end].decode('utf8')
    return s, end + 1


CODEC_OPTIONS = codec_options.CodecOptions(document_class=OrderedDict)


def decode_documents(buffer, start, content_size):
    docs = bson.decode_all(buffer[start:start + content_size], CODEC_OPTIONS)
    return docs, start + content_size


class OperationResponder():
    def __init__(self, responders):
        self.responders = responders

    @abstractmethod
    def handle(self, query_bytes):
        pass

    @abstractmethod
    def to_bytes(self, response, request_id):
        pass

    def get_match_responder(self, query):
        for responder in self.responders:
            if responder['match'](query):
                return responder['response']
        raise Exception('is no responder')


# NOTE probably, it need only for mongo version < 3.6
class OpInsertResponder(OperationResponder):
    def handle(self, buffer, request_id, mindsdb_env, session):
        flags, pos = unpack(UINT, buffer)
        namespace, pos = get_utf8_string(buffer, pos)
        query = bson.decode_all(buffer[pos:], CODEC_OPTIONS)
        responder = self.responders.find_match(query)
        assert responder is not None, 'query cant be processed'

        request_args = {
            'request_id': request_id
        }

        documents = responder.handle(query, request_args, mindsdb_env, session)

        return documents

    def to_bytes(self, response, request_id):
        pass


OP_MSG_FLAGS = {
    'checksumPresent': 0,
    'moreToCome': 1,
    'exhaustAllowed': 16
}


# NOTE used in mongo version > 3.6
class OpMsgResponder(OperationResponder):
    def handle(self, buffer, request_id, mindsdb_env, session):
        query = OrderedDict()
        flags, pos = unpack(UINT, buffer)

        checksum_present = bool(flags & (1 << OP_MSG_FLAGS['checksumPresent']))
        if checksum_present:
            msg_len = len(buffer) - 4
        else:
            msg_len = len(buffer)

        # sections
        while pos < msg_len:
            kind, pos = unpack(BYTE, buffer, pos)
            if kind == 0:
                # body
                section_size, _ = unpack(INT, buffer, pos)
                docs, pos = decode_documents(buffer, pos, section_size)
                query.update(docs[0])
            elif kind == 1:
                # Document
                section_size, pos = unpack(INT, buffer, pos)
                seq_id, pos = get_utf8_string(buffer, pos)
                docs_len = section_size - struct.calcsize(INT) - len(seq_id) - 1
                docs, pos = decode_documents(buffer, pos, docs_len)
                query[seq_id] = docs

        remaining = len(buffer) - pos
        if checksum_present:
            if remaining != 4:
                raise Exception('should be checksum at the end of message')
            # TODO read and check checksum
        elif remaining != 0:
            raise Exception('is bytes left after msg parsing')

        log.debug(f'GET OpMSG={query}')

        responder = self.responders.find_match(query)
        assert responder is not None, 'query cant be processed'

        request_args = {
            'request_id': request_id,
            'database': query['$db']
        }

        documents = responder.handle(query, request_args, mindsdb_env, session)

        return documents

    def to_bytes(self, response, request_id):
        flags = struct.pack("<I", 0)  # TODO
        payload_type = struct.pack("<b", 0)  # TODO
        payload_data = bson.BSON.encode(response)
        data = b''.join([flags, payload_type, payload_data])

        reply_id = 0  # TODO add seq here
        response_to = request_id

        header = struct.pack("<iiii", 16 + len(data), reply_id, response_to, OP_MSG)
        return header + data


# NOTE used in any mongo shell version
class OpQueryResponder(OperationResponder):
    def handle(self, buffer, request_id, mindsdb_env, session):
        # https://docs.mongodb.com/manual/reference/mongodb-wire-protocol/#wire-op-query
        flags, pos = unpack(UINT, buffer)
        namespace, pos = get_utf8_string(buffer, pos)
        is_command = namespace.endswith('.$cmd')
        num_to_skip, pos = unpack(INT, buffer, pos)
        num_to_return, pos = unpack(INT, buffer, pos)
        docs = bson.decode_all(buffer[pos:], CODEC_OPTIONS)

        query = docs[0]  # docs = [query, returnFieldsSelector]

        log.debug(f'GET OpQuery={query}')

        responder = self.responders.find_match(query)
        assert responder is not None, 'query cant be processed'

        request_args = {
            'num_to_skip': num_to_skip,
            'num_to_return': num_to_return,
            'request_id': request_id,
            'is_command': is_command
        }

        documents = responder.handle(query, request_args, mindsdb_env, session)

        return documents

    def to_bytes(self, request, request_id):
        flags = struct.pack("<i", 0)  # TODO
        cursor_id = struct.pack("<q", 0)  # TODO
        starting_from = struct.pack("<i", 0)  # TODO
        number_returned = struct.pack("<i", len([request]))
        reply_id = 123  # TODO
        response_to = request_id

        log.debug(f'RET docs={request}')

        data = b''.join([flags, cursor_id, starting_from, number_returned])
        data += b''.join([bson.BSON.encode(doc) for doc in [request]])

        message = struct.pack("<i", 16 + len(data))
        message += struct.pack("<i", reply_id)
        message += struct.pack("<i", response_to)
        message += struct.pack("<i", OP_REPLY)

        return message + data


class MongoRequestHandler(SocketServer.BaseRequestHandler):
    _stopped = False

    def _init_ssl(self):
        import ssl
        import tempfile
        import atexit
        import os

        from mindsdb.utilities.wizards import make_ssl_cert

        CERT_PATH = tempfile.mkstemp(prefix='mindsdb_cert_', text=True)[1]
        make_ssl_cert(CERT_PATH)
        atexit.register(lambda: os.remove(CERT_PATH))

        ssl_context = ssl.SSLContext()
        ssl_context.load_cert_chain(CERT_PATH)
        ssl_socket = ssl_context.wrap_socket(
            self.request,
            server_side=True,
            do_handshake_on_connect=True
        )
        self.request = ssl_socket

    def handle(self):
        log.debug('connect')
        log.debug(str(self.server.socket))

        self.session = Session(self.server.mindsdb_env)

        first_byte = self.request.recv(1, socket.MSG_PEEK)
        if first_byte == b'\x16':
            # TLS 'client hello' starts from \x16
            self._init_ssl()

        while True:
            header = self._read_bytes(16)
            if header is False:
                # connection closed by client
                break
            length, pos = unpack(INT, header)
            request_id, pos = unpack(INT, header, pos)
            response_to, pos = unpack(INT, header, pos)
            opcode, pos = unpack(INT, header, pos)
            log.debug(f'GET length={length} id={request_id} opcode={opcode}')
            msg_bytes = self._read_bytes(length - pos)
            answer = self.get_answer(request_id, opcode, msg_bytes)
            if answer is not None:
                self.request.send(answer)

    def get_answer(self, request_id, opcode, msg_bytes):
        if opcode not in self.server.operationsHandlersMap:
            raise NotImplementedError(f'Unknown opcode {opcode}')
        responder = self.server.operationsHandlersMap[opcode]
        assert responder is not None, 'error'
        response = responder.handle(msg_bytes, request_id, self.session.mindsdb_env, self.session)
        if response is None:
            return None
        return responder.to_bytes(response, request_id)

    def _read_bytes(self, length):
        buffer = b''
        while length:
            chunk = self.request.recv(length)
            if chunk == b'':
                log.debug('Connection closed')
                return False

            length -= len(chunk)
            buffer += chunk
        return buffer


class MongoServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def __init__(self, config):
        mongodb_config = config['api'].get('mongodb')
        assert mongodb_config is not None, 'is no mongodb config!'
        host = mongodb_config['host']
        port = mongodb_config['port']
        log.debug(f'start mongo server on {host}:{port}')

        super().__init__((host, int(port)), MongoRequestHandler)

        self.mindsdb_env = {
            'config': config,
            'data_store': DataStore(),
            'mindsdb_native': NativeInterface()
        }

        respondersCollection = RespondersCollection()

        opQueryResponder = OpQueryResponder(respondersCollection)
        opMsgResponder = OpMsgResponder(respondersCollection)
        opInsertResponder = OpInsertResponder(respondersCollection)

        self.operationsHandlersMap = {
            OP_QUERY: opQueryResponder,
            OP_MSG: opMsgResponder,
            OP_INSERT: opInsertResponder
        }

        respondersCollection.add(
            when={'drop': 'system.sessions'},
            result={'ok': 1}
        )
        respondersCollection.add(
            when={'update': 'system.version'},
            result={'ok': 1}
        )
        respondersCollection.add(
            when={'setFeatureCompatibilityVersion': helpers.is_true},
            result={'ok': 1}
        )
        # OpMSG=OrderedDict([('features', 1), ('$clusterTime', OrderedDict([('clusterTime', Timestamp(1599748325, 1)), ('signature', OrderedDict([('hash', b'\xb8\xc3\x03\x18\xca\xe6bh\xf0\xcb47,\x924\x8a >\xfc\x91'), ('keyId', 6870854312365391875)]))])), ('$configServerState', OrderedDict([('opTime', OrderedDict([('ts', Timestamp(1599748325, 1)), ('t', 1)]))])), ('$db', 'admin')])
        respondersCollection.add(
            when={'features': helpers.is_true},
            result={'ok': 1}
        )
        # OpMSG=OrderedDict([('serverStatus', 1), ('$clusterTime', OrderedDict([('clusterTime', Timestamp(1599748366, 1)), ('signature', OrderedDict([('hash', b'\xa1E}\xbbIU\xc2D\x95++\x82\x88\xb5\x84\xf5\xda)+B'), ('keyId', 6870854312365391875)]))])), ('$configServerState', OrderedDict([('opTime', OrderedDict([('ts', Timestamp(1599748366, 1)), ('t', 1)]))])), ('$db', 'admin')])
        respondersCollection.add(
            when={'serverStatus': helpers.is_true},
            result={'ok': 1}
        )
        # OpMSG=OrderedDict([('ismaster', 1), ('$db', 'admin'), ('$clusterTime', OrderedDict([('clusterTime', Timestamp(1599749031, 1)), ('signature', OrderedDict([('hash', b'6\x87\xd5Y\xa7\xc7\xcf$\xab\x1e\xa2{\xe5B\xe5\x99\xdbl\x8d\xf4'), ('keyId', 6870854312365391875)]))])), ('$client', OrderedDict([('application', OrderedDict([('name', 'MongoDB Shell')])), ('driver', OrderedDict([('name', 'MongoDB Internal Client'), ('version', '3.6.3')])), ('os', OrderedDict([('type', 'Linux'), ('name', 'Ubuntu'), ('architecture', 'x86_64'), ('version', '18.04')])), ('mongos', OrderedDict([('host', 'maxs-comp:27103'), ('client', '127.0.0.1:52148'), ('version', '3.6.3')]))])), ('$configServerState', OrderedDict([('opTime', OrderedDict([('ts', Timestamp(1599749031, 1)), ('t', 1)]))]))])

        respondersCollection.responders += responders


def run_server(config):
    SocketServer.TCPServer.allow_reuse_address = True
    with MongoServer(config) as srv:
        srv.serve_forever()
