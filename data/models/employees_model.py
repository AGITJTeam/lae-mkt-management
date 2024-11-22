from dataclasses import dataclass
from typing import Optional

@dataclass
class EmployeeModel:
    idemployee: int
    firstName: str
    middleName: Optional[str]
    lastName1: str
    lastName2: Optional[str]
    fullName: str
    fullNameWithUserName: str
    firstNameLastNameWithUserName: str
    mailAddress: Optional[str]
    mailCity: Optional[str]
    mailState: Optional[str]
    mailZipCode: Optional[str]
    physicalAddress: Optional[str]
    physicalCity: Optional[str]
    physicalState: Optional[str]
    physicalZipCode: Optional[str]
    pyshicalCounty: Optional[str]
    phone: Optional[str]
    cellPhone: Optional[str]
    emailPersonal: Optional[str]
    emailWork: Optional[str]
    username: str
    birthDay: str
    image: Optional[str]
    idGender: int
    idMarital: int
    dateCreated: Optional[str]
    lastUpdated: Optional[str]
    auth0UserId: str
    active: bool
