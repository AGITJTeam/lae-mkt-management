from dataclasses import dataclass

@dataclass
class ReceiptsPayrollModel:
    statusReceipt: int
    txnType: str
    idReceiptHdr: int
    date: str
    balanceDue: int
    idCustomer: int
    firstName: str
    lastName1: str
    customerName: str
    invoiceItemDesc: str
    payee: str
    fiduciary: float
    nonFiduciary: float
    amountII: int
    amountPaidRec: float
    payMethods: str
    totalAmntReceipt: float
    memo: str
    usr: str
    csr: str
    csR2: str
    agency: str
    officeRec: str
    officePol: str
    bankAccount: str
    retained: int
    void: bool
    for_t: str
