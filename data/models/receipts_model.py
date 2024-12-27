from dataclasses import dataclass
from typing import Optional

@dataclass
class ReceiptModel:
    idreceiptHdr: int
    idPoliciesHdr: int
    idCustomer: int
    nameCustomer: str
    idOffice: int
    office: str
    idEmployeeUSR: int
    nameEmployeeUSR: str
    userNameUSR: str
    idEmployeeCSR1: int
    nameEmployeeCSR1: str
    userNameCSR1: str
    idEmployeeCSR2: int
    nameEmployeeCSR2: Optional[str]
    userNameCSR2: Optional[str]
    statusReceipt: str | int
    transactionType: str
    date: str
    totalAmntReceipt: float
    amountPaid: float
    balanceDue: float
    balanceDueDate: Optional[str]
    retained: float
    fiduciary: float
    nonFiduciary: float
    correction: bool
    comments: Optional[str]
    void: bool
    dateCreated: str
    lastUpdated: Optional[str]
    active: bool
    receiptsBalancePayments: list
    receiptsDTL: list
    policiesHDR: list
