from pprint import pprint


import re
import json
import struct
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, ClassVar, Dict, Optional, Pattern


ID_PATTERN: Pattern[str] = re.compile(r'^[a-z0-9]{7}$')

HEADER_FORMAT: str = '!B1sI7s'
HEADER_SIZE: int = struct.calcsize(HEADER_FORMAT)

ENCODING: str = 'utf-8'
DATETIME_FORMAT: str = '%Y%m%d%H%M'


class UnknownAliasError(Exception):
    def __init__(self, alias: str):
        super().__init__(f'Unknown alias encountered: {alias!r}')
        self.alias = alias


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

    @staticmethod
    def serialize(host_status: 'HostStatus') -> bytes:
        field_types: Dict[str, Any] = HostStatus.__annotations__

        data: Dict[str, Any] = {}

        for field_name, field_value in asdict(host_status).items():
            if field_value is not None:
                field_type: Any = field_types[field_name]

                if field_type == datetime or field_type == Optional[datetime]:
                    field_value = field_value.strftime(DATETIME_FORMAT)

                if field_type == float or field_type == Optional[float]:
                    field_value = round(field_value, 3)

                data[HostStatus.FIELD_ALIAS[field_name]] = field_value

        return json.dumps(data).encode(ENCODING)

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


def parse_status_update(message: str) -> HostStatus | None:
    try:
        parts = message.split(",")
        client_id, ram, temp = parts[0], float(parts[1]), float(parts[2])

        if not ID_PATTERN.match(client_id):
            raise ValueError("Invalid ID format")

        return HostStatus(client_id, ram, temp)
    except (ValueError, IndexError):
        return None
