import json
import re
import struct
from dataclasses import asdict, dataclass, fields
from datetime import datetime
from enum import StrEnum
from typing import Any, ClassVar, Dict, List, Optional, Pattern, Tuple, Union, get_args, get_origin


DATETIME_FORMAT: str = '%Y%m%d%H%M'
ENCODING: str = 'utf-8'
HEADER_FORMAT: str = '!B7s12s1sI'
HEADER_SIZE: int = struct.calcsize(HEADER_FORMAT)
HOST_ID_PATTERN: Pattern[str] = re.compile(r'^[a-z0-9]{7}$')
MSG_TYPE_PATTERN: Pattern[str] = re.compile(r'^[A-Z]{1}$')
PROTOCOL_VERSION: int = 1
TIMESTAMP_PATTERN: Pattern[str] = re.compile(r'^\d{12}$')


class IncompletePacketError(Exception):
    def __init__(self, packet_size: int, expected_size: int):
        super().__init__(f'Packet too small: received {packet_size} bytes, but expected at least {expected_size} bytes.')
        self.packet_size = packet_size
        self.expected_size = expected_size


class UnknownAliasError(Exception):
    def __init__(self, alias: str):
        super().__init__(f'Unknown alias: {alias!r}.')
        self.alias = alias


class UnknownFieldNameError(Exception):
    def __init__(self, field_name: str):
        super().__init__(f'Unknown field name: {field_name!r}.')
        self.field_name = field_name


class UnknownMessageTypeError(Exception):
    def __init__(self, msg_type: str):
        super().__init__(f'Unknown message type: {msg_type!r}.')
        self.msg_type = msg_type


class MessageTypes(StrEnum):
    STATUS = 'S'
    REQUEST = 'R'
    ACK = 'A'


def is_optional_type(type_: Any) -> bool:
    origin: Optional[Any] = get_origin(type_)
    args: Tuple[Any] = get_args(type_)

    return origin is Union and len(args) == 2 and type(None) in args


def pack(host_id: str, timestamp: datetime, msg_type: str, payload: bytes) -> bytes:
    if not HOST_ID_PATTERN.match(host_id):
        raise ValueError(f'Invalid host_id pattern: {host_id}')

    if not MSG_TYPE_PATTERN.match(msg_type):
        raise ValueError(f'Invalid msg_type pattern: {msg_type}')

    if not msg_type in MessageTypes._member_map_.values():
        raise UnknownMessageTypeError(msg_type)

    encoded_host_id: bytes = host_id.encode(ENCODING)
    formatted_timestamp: bytes = timestamp.strftime(DATETIME_FORMAT).encode(ENCODING)
    encoded_msg_type: bytes = msg_type.encode(ENCODING)
    payload_size: int = len(payload)

    header: bytes = struct.pack(HEADER_FORMAT, PROTOCOL_VERSION, encoded_host_id, formatted_timestamp, encoded_msg_type, payload_size)

    return header + payload


def unpack(packet: bytes) -> Tuple[int, str, datetime, str, int, bytes]:
    if len(packet) < HEADER_SIZE:
        raise IncompletePacketError(len(packet), HEADER_SIZE)

    version, encoded_host_id, formatted_timestamp, encoded_msg_type, payload_size = struct.unpack(HEADER_FORMAT, packet[:HEADER_SIZE])

    host_id: str = encoded_host_id.decode(ENCODING).strip()
    timestamp: datetime = datetime.strptime(formatted_timestamp.decode(ENCODING), DATETIME_FORMAT)
    msg_type: str = encoded_msg_type.decode(ENCODING).strip()

    payload = packet[HEADER_SIZE:HEADER_SIZE + payload_size]

    return version, host_id, timestamp, msg_type, payload_size, payload


@dataclass
class Status:
    host_name: Optional[str] = None
    ram_total: Optional[float] = None
    ram_usage: Optional[float] = None
    cpu_total: Optional[int] = None
    cpu_usage: Optional[float] = None
    cpu_temperature: Optional[float] = None
    disk_usage: Optional[float] = None
    disk_total: Optional[float] = None

    FIELD_ALIAS: ClassVar[Dict[str, str]] = {
        'host_name': 'HNM',
        'ram_total': 'RTT',
        'ram_usage': 'RUG',
        'cpu_total': 'CTT',
        'cpu_usage': 'CUG',
        'cpu_temperature': 'CTP',
        'disk_total': 'DTT',
        'disk_usage': 'DUG'
    }

    def __post_init__(self):
        for field in fields(Status):
            field_value: Any = getattr(self, field.name)
            expected_type: Any = field.type
            expected_args: Tuple[Any] = get_args(expected_type)

            if field_value is None:
                if type(None) not in expected_args:
                    raise ValueError(f'The field {field.name!r} cannot be None.')
            else:
                if is_optional_type(expected_type):
                    expected_type = expected_args[0]

                if not isinstance(field_value, expected_type):
                    raise TypeError(
                        f"The field {field.name!r} must be of type '{expected_type}'. "
                        f"Received: '{type(field_value)}'."
                    )

    def serialize(self) -> bytes:
        field_types: Dict[str, Any] = {field.name: field.type for field in fields(Status)}

        data: Dict[str, Any] = {}

        for field_name, field_value in asdict(self).items():
            if field_value is not None:
                field_type: Any = field_types[field_name]

                if field_type == datetime or (is_optional_type(field_type) and datetime in get_args(field_type)):
                    field_value = field_value.strftime(DATETIME_FORMAT)

                if field_type == float or (is_optional_type(field_type) and float in get_args(field_type)):
                    field_value = round(field_value, 3)

                data[Status.FIELD_ALIAS[field_name]] = field_value

        return json.dumps(data, separators=(',', ':')).encode(ENCODING)

    @staticmethod
    def deserialize(serialized_status: bytes) -> 'Status':
        field_types: Dict[str, Any] = {field.name: field.type for field in fields(Status)}
        alias_to_field: Dict[str, str] = {value: key for key, value in Status.FIELD_ALIAS.items()}

        data: Dict[str, Any] = json.loads(serialized_status.decode(ENCODING))
        kwargs: Dict[str, Any] = {}

        for field_alias, field_value in data.items():
            field_name: Optional[str] = alias_to_field.get(field_alias)

            if not field_name:
                raise UnknownAliasError(field_alias)

            field_type: Any = field_types[field_name]

            if field_type == datetime or (is_optional_type(field_type) and datetime in get_args(field_type)):
                field_value = datetime.strptime(field_value, DATETIME_FORMAT)

            kwargs[field_name] = field_value

        return Status(**kwargs)


@dataclass
class Request:
    requested_fields: List[str]

    FIELD_ALIAS: ClassVar[Dict[str, str]] = Status.FIELD_ALIAS

    def __post_init__(self):
        valid_field_names = list(Request.FIELD_ALIAS.keys())
        for field_name in self.requested_fields:
            if field_name not in valid_field_names:
                raise UnknownFieldNameError(field_name)

    def serialize(self) -> bytes:
        return b' '.join([Request.FIELD_ALIAS[field_name].encode(ENCODING) for field_name in self.requested_fields])

    @staticmethod
    def deserialize(serialized_request: bytes) -> 'Request':
        alias_to_field: Dict[str, str] = {value: key for key, value in Request.FIELD_ALIAS.items()}
        request_fields: List[str] = []

        for field_alias in serialized_request.decode(ENCODING).strip().split(' '):
            field_name: Optional[str] = alias_to_field.get(field_alias)

            if not field_name:
                raise UnknownAliasError(field_alias)

            request_fields.append(field_name)

        return Request(request_fields)


def create_acknowledge_packet(host_id: str, timestamp: datetime) -> bytes:
    return pack(host_id=host_id, timestamp=timestamp, msg_type=MessageTypes.ACK, payload=b'')


def create_request_packet(host_id: str, timestamp: datetime, request: Request) -> bytes:
    return pack(host_id=host_id, timestamp=timestamp, msg_type=MessageTypes.REQUEST, payload=request.serialize())


def create_status_packet(host_id: str, timestamp: datetime, status: Status) -> bytes:
    return pack(host_id=host_id, timestamp=timestamp, msg_type=MessageTypes.STATUS, payload=status.serialize())


def parse_packet(packet: bytes) -> Union[Status, Request, None]:
    version, host_id, timestamp, msg_type, payload_size, payload = unpack(packet)

    match msg_type:
        case MessageTypes.STATUS:
            return Status.deserialize(payload)
        case MessageTypes.REQUEST:
            return Request.deserialize(payload)
        case MessageTypes.ACK:
            return None
        case _:
            raise UnknownMessageTypeError(msg_type)
