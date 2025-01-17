from dataclasses import dataclass
from typing import Optional

@dataclass
class WebquotesModel:
    ID: str
    Form: str
    name: str
    email: str
    phone: str
    submission_date: str
    submission_on_time: str
    Birthday: str
    model_year: str
    Make: str
    Model: str
    notes: str
    status: str
    agent: str
    zip: str
    State: str
    Preference: str
    marital_status: str
    licence_status: str
    Gender: str
    residence_status: str
    Garaged: str
    assignedTo: str
    toAll: str
    workedAt: str
    region_worked_at: str
    sold_at: str
    date_sold: str
    Referer: str
    campaign_id: str
    GCLID: Optional[str]
    calls: int