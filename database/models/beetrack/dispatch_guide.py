from typing import Optional , List, Any
from datetime import datetime
from pydantic import BaseModel

class Extras(BaseModel):
    name: Optional[str]
    value : Optional[str]

class tags(BaseModel):
    name: Optional[str]
    value : Optional[str]

class Items(BaseModel):
    id :  Optional[int]
    name: Optional[str]
    description :  Optional[str]
    quantity : Optional[int]
    original_quantity:  Optional[int]
    delivered_quantity :  Optional[int]
    code :  Optional[str]
    extras : Optional [List[Extras]]

class DistpatchGuideD(BaseModel):
    guide : Optional[str]
    beecode : Optional[str]
    identifier : Optional[str]
    account_id :  Optional[int]
    contact_name : Optional[str]
    contact_phone : Optional[str]
    contact_identifier: Optional[str]
    contact_email : Optional[str]
    contact_address : Optional[str]
    promised_date : Optional[str]
    min_delivery_time : Optional[str]
    max_delivery_time : Optional[str]


class Groups(BaseModel):
    name : Optional[str]
    group_category : Optional[str]
    group_category_id : Optional[str]
    associated_at :  Optional[int]


class DistpatchGuide(BaseModel):
    resource : Optional[str]
    event : Optional[str]
    account_name: Optional[str]
    dispatch_guide :  Optional[DistpatchGuideD]
    items :  Optional[List[Items]]
    tags : Optional[List[tags]]
    group : Optional[List[Groups]]