from dataclasses import dataclass
from typing import Optional

@dataclass
class PoliciesDtlModel:
    idPoliciesHdr: int
    idPoliciesDTL: int
    idCustomer: int
    nameCustomer: Optional[str]
    idProduct: int
    producTname: str
    active: bool
