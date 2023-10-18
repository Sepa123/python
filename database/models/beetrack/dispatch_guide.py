from typing import Optional , List, Any
from datetime import datetime
from pydantic import BaseModel

class Extras(BaseModel):
    name:str
    value : str

class tags(BaseModel):
    name:str
    value : str

class Items(BaseModel):
    name:str
    quantity : int
    delivered_quantity : int
    code : str
    extras : List[Extras]

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
    items : List[Items]
    tags : List[tags]
    group : List[str]