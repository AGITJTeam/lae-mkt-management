from dataclasses import dataclass
from typing import Optional

@dataclass
class LogsModel:
    idLog: int
    idPoliciesHdr: int
    idReceipsHdr: Optional[int]
    idEmployeeUsr: int
    nameEmployeeUSR: str
    userNameUSR: str
    idEmployeeCsr1: Optional[int]
    nameEmployeeCSR1: Optional[str]
    userNameCSR1: Optional[str]
    idEmployeeCsr2: Optional[int]
    nameEmployeeCSR2: Optional[str]
    userNameCSR2: Optional[str]
    idEmployeeUw: Optional[int]
    nameEmployeeUW: Optional[str]
    userNameUw: Optional[str]
    suspenseToIdEmployee: Optional[int]
    nameEmployeeSuspense: Optional[str]
    userNameSuspense: Optional[str]
    comments: str
    typeLog: str
    dateLog: str