from typing import Optional
from dataclasses import dataclass
from enum import Enum


@dataclass
class Log:
    url: str
    ctn: str
    result: str
    log_timestamp: str


class ResponseResolution(Enum):
    approve = "Одобрение"
    reject = "Отказ"


class RejectReason(Enum):
    blacklist = "Клиент находится в черном списке"
    income = "Клиент не имеет достаточный уровень дохода"


@dataclass
class ResultRequest:
    ctn: str
    iin: str
    smartphoneID: int
    income: int
    creationDate: str


@dataclass
class ResultResponse:
    result: ResponseResolution
    reason: Optional[RejectReason]

    def json(self):
        return {key: value for key, value in self.__dict__.items() if value is not None}
