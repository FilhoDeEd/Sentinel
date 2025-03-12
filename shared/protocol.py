import json
import re
import struct
from dataclasses import dataclass, asdict
from datetime import datetime
from enum import StrEnum
from typing import Any, ClassVar, Dict, List, Optional, Pattern, Tuple


DATETIME_FORMAT: str = '%Y%m%d%H%M'
ENCODING: str = 'utf-8'
HEADER_FORMAT: str = '!B1sI7s'
HEADER_SIZE: int = struct.calcsize(HEADER_FORMAT)
HOST_ID_PATTERN: Pattern[str] = re.compile(r'^[a-z0-9]{7}$')
MSG_TYPE_PATTERN: Pattern[str] = re.compile(r'^[A-Z]{1}$')
PROTOCOL_VERSION: int = 1


class MessageTypes(StrEnum):
    STATUS = 'S'
    REQUEST = 'R'
    ACK = 'A'


class UnknownAliasError(Exception):
    def __init__(self, alias: str):
        super().__init__(f'Unknown alias: {alias!r}.')
        self.alias = alias


class UnknownFieldNameError(Exception):
    def __init__(self, field_name: str):
        super().__init__(f'Unknown field name: {field_name!r}.')
        self.field_name = field_name


class IncompletePacketError(Exception):
    def __init__(self, packet_size: int, expected_size: int):
        super().__init__(f'Packet too small: received {packet_size} bytes, but expected at least {expected_size} bytes.')
        self.packet_size = packet_size
        self.expected_size = expected_size


def pack(host_id: str, msg_type: str, payload: bytes) -> bytes:
    if not HOST_ID_PATTERN.match(host_id):
        raise ValueError(f'Invalid host_id pattern: {host_id}')

    if not MSG_TYPE_PATTERN.match(msg_type):
        raise ValueError(f'Invalid msg_type pattern: {msg_type}')

    header: bytes = struct.pack(HEADER_FORMAT, PROTOCOL_VERSION, msg_type.encode(ENCODING), len(payload), host_id.encode(ENCODING))

    return header + payload


def unpack(packet: bytes) -> Tuple[int, str, int, str, bytes]:
    if len(packet) < HEADER_SIZE:
        raise IncompletePacketError(len(packet), HEADER_SIZE)

    version, msg_type, payload_size, host_id = struct.unpack(HEADER_FORMAT, packet[:HEADER_SIZE])
    msg_type = msg_type.decode(ENCODING).strip()
    host_id = host_id.decode(ENCODING).strip()

    payload = packet[HEADER_SIZE:HEADER_SIZE + payload_size]

    return version, msg_type, payload_size, host_id, payload


@dataclass
class HostStatus:
    timestamp: datetime
    host_name: Optional[str] = None
    ram_total: Optional[float] = None
    ram_usage: Optional[float] = None
    cpu_total: Optional[int] = None
    cpu_usage: Optional[float] = None
    cpu_temperature: Optional[float] = None
    disk_usage: Optional[float] = None
    disk_total: Optional[float] = None

    FIELD_ALIAS: ClassVar[Dict[str, str]] = {
        'timestamp': 'TMS',
        'host_name': 'HNM',
        'ram_total': 'RTT',
        'ram_usage': 'RUG',
        'cpu_total': 'CTT',
        'cpu_usage': 'CUG',
        'cpu_temperature': 'CTP',
        'disk_total': 'DTT',
        'disk_usage': 'DUG'
    }

    def serialize(self) -> bytes:
        field_types: Dict[str, Any] = HostStatus.__annotations__

        data: Dict[str, Any] = {}

        for field_name, field_value in asdict(self).items():
            if field_value is not None:
                field_type: Any = field_types[field_name]

                if field_type == datetime or field_type == Optional[datetime]:
                    field_value = field_value.strftime(DATETIME_FORMAT)

                if field_type == float or field_type == Optional[float]:
                    field_value = round(field_value, 3)

                data[HostStatus.FIELD_ALIAS[field_name]] = field_value

        return json.dumps(data, separators=(',', ':')).encode(ENCODING)

    @staticmethod
    def deserialize(serialized_host_status: bytes) -> 'HostStatus':
        field_types: Dict[str, Any] = HostStatus.__annotations__
        alias_to_field: Dict[str, str] = {value: key for key, value in HostStatus.FIELD_ALIAS.items()}

        data: Dict[str, Any] = json.loads(serialized_host_status.decode(ENCODING))
        kwargs: Dict[str, Any] = {}

        for field_alias, field_value in data.items():
            field_name: Optional[str] = alias_to_field.get(field_alias)

            if not field_name:
                raise UnknownAliasError(field_alias)

            field_type: Any = field_types[field_name]

            if field_type == datetime or field_type == Optional[datetime]:
                field_value = datetime.strptime(field_value, DATETIME_FORMAT)

            kwargs[field_name] = field_value

        return HostStatus(**kwargs)


def create_status_packet(host_id: str, host_status: HostStatus) -> bytes:
    return pack(host_id=host_id, msg_type=MessageTypes.STATUS, payload=host_status.serialize())


def create_acknowledge_packet(host_id: str) -> bytes:
    return pack(host_id=host_id, msg_type=MessageTypes.ACK, payload=b'')


def create_request_packet(host_id: str, request_fields: List[str]):
    payload: bytes = b''

    for field_name in request_fields:
        field_alias: Optional[str] = HostStatus.FIELD_ALIAS.get(field_name)

        if not field_alias:
            raise UnknownAliasError(field_name)
        
        payload += field_alias.encode(ENCODING)

    return pack(host_id=host_id, msg_type=MessageTypes.REQUEST, payload=payload)


