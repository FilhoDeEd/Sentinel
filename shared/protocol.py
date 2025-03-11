from pprint import pprint


import re
import struct
import json
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, ClassVar, Dict, Optional


ID_PATTERN = re.compile(r'^[a-z0-9]{7}$')

HEADER_FORMAT = '!B1sI7s'
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


ENCODING = 'utf-8'
DATETIME_FORMAT = '%Y%m%d%H%M'


@dataclass
class HostStatus:
    host_id: str
    timestamp: datetime
    host_name: Optional[str] = None
    ram_total: Optional[float] = None
    ram_usage: Optional[float] = None
    cpu_total: Optional[int] = None
    cpu_usage: Optional[float] = None
    cpu_temperature: Optional[float] = None
    disk_usage: Optional[float] = None
    disk_total: Optional[float] = None
    uptime: Optional[int] = None

    FIELD_ALIAS: ClassVar[Dict[str, str]] = {
        'host_id': 'HID',
        'timestamp': 'TMS',
        'host_name': 'HNM',
        'ram_total': 'RTT',
        'ram_usage': 'RUG',
        'cpu_total': 'CTT',
        'cpu_usage': 'CUG',
        'cpu_temperature': 'CTP',
        'disk_total': 'DTT',
        'disk_usage': 'DUG',
        'uptime': 'UPT',
    }

    @staticmethod
    def serialize(host_status: 'HostStatus') -> bytes:
        annotations: Dict[str, Any] = HostStatus.__annotations__
        data: Dict[str, Any] = {}

        for attr, value in asdict(host_status).items():
            if value is not None:
                attr_type: Any = annotations[attr]

                if attr_type == datetime or attr_type == Optional[datetime]:
                    value = value.strftime(DATETIME_FORMAT)

                if attr_type == float or attr_type == Optional[float]:
                    value = round(value, 3)

                data[HostStatus.FIELD_ALIAS[attr]] = value

        return json.dumps(data).encode(ENCODING)

    @staticmethod
    def deserialize(data: bytes) -> 'HostStatus':
        pass



status = HostStatus(
    host_id="abc1234",
    timestamp=datetime(2023, 1, 3, 3, 58),
    ram_total=16.029413,
    ram_usage=8.5,
    cpu_total=8,
    cpu_usage=45.3,
    cpu_temperature=65.2,
    disk_usage=100.0,
    disk_total=50.0,
    uptime=3600
)

print(HostStatus.serialize(status))


def parse_status_update(message: str) -> HostStatus | None:
    try:
        parts = message.split(",")
        client_id, ram, temp = parts[0], float(parts[1]), float(parts[2])

        if not ID_PATTERN.match(client_id):
            raise ValueError("Invalid ID format")

        return HostStatus(client_id, ram, temp)
    except (ValueError, IndexError):
        return None
