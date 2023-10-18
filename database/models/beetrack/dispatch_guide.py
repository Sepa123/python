from typing import Optional , List, Any
from datetime import datetime
from pydantic import BaseModel


class DistpatchGuideD(BaseModel):
    guide : str
    beecode : str
    identifier : str
    account_id : int
    contact_name : str
    contact_phone : str
    contact_identifier: str
    contact_email : str
    contact_address : str
    promised_date : str

class DistpatchGuide(BaseModel):
    resource : str
    event : str
    account_name: str
    dispatch_guide : DistpatchGuideD
    items : List[str]
    tags : List[str]
    group : List[str]