from dataclasses import dataclass, fields, is_dataclass
from typing import get_type_hints, List, Type, TypeVar, Any, Optional

T = TypeVar("T")

def fromDictRecursive(cls: Type[T], data: dict) -> T:
    if not is_dataclass(cls):
        raise TypeError(f"{cls} is not a dataclass")

    type_hints = get_type_hints(cls)
    init_args = {}

    for field in fields(cls):
        field_name = field.name
        field_type = type_hints.get(field_name)
        field_value = data.get(field_name)

        if is_dataclass(field_type) and isinstance(field_value, dict):
            init_args[field_name] = fromDictRecursive(field_type, field_value)

        elif (hasattr(field_type, '__origin__') and
              field_type.__origin__ == list and
              is_dataclass(field_type.__args__[0])):

            init_args[field_name] = [
                fromDictRecursive(field_type.__args__[0], item)
                for item in field_value or []
            ]

        else:
            init_args[field_name] = field_value

    return cls(**init_args)

@dataclass
class Appointments:
    createdAt: str
    csr: Optional[str]
    date: str
    date_formatted: Optional[str]
    id: int
    office: Optional[str]
    quote_number: int
    type_appt: Optional[str]
    usr: Optional[str]

@dataclass
class Car:
    CarMake: str
    CarModel: str
    CarYear: str
    IsOver50K: Optional[str]
    VIN: str
    commercial_id: int
    coverage_type: Optional[str]
    createdAt: str
    customer_id: int
    damaged: Optional[str]
    id: int
    special_equipment: Optional[str]
    stated_value: Optional[str]
    type: str
    updatedAt: str
    vehicle_type: str
    webquote_id: int

@dataclass
class Customer:
    LaeId: int
    address: str
    birthday: str
    city: str
    createdAt: str
    email: str
    firstname: str
    gender: str
    has_license: str
    id: int
    language_pref: str
    lastname: str
    marital_status: str
    opt_in: str
    phonenumber: str
    state: str
    updatedAt: str
    zipcode: str

@dataclass
class WebquotesGoResponse:
    agent: str
    appointments: Appointments
    appt_id: int
    assigned_to: str
    calls: int
    cars: list[Car]
    createdAt: str
    customer: Customer
    customer_id: int
    excluded: str
    finished_quote: Optional[str]
    from_date: str
    full_link: str
    gclid: Optional[str]
    hey_mkt_conv_id: int
    id: int
    is_over_50k: Optional[str]
    itc_link: str
    lead_source: str
    notes: str
    office_sold_at: Optional[str]
    office_worked: str
    quote_number: str
    referer: str
    region_worked: str
    sale_type: Optional[str]
    sold_date: Optional[str]
    status: str
    submission_date: str
    to_date: str
    toall: bool
    transfered_to: Optional[str]
    updatedAt: str
