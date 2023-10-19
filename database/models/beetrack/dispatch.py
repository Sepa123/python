from typing import Optional , List, Any
from datetime import datetime
from pydantic import BaseModel

class tags(BaseModel):
    name: Optional[str]
    value : Optional[str]

class Extras(BaseModel):
    name: Optional[str]
    value : Optional[str]

class EvaluationAnswers(BaseModel):
    name: Optional[str]
    value : Optional[str]

## este es el original
# class Items(BaseModel):
#     name: Optional[str]
#     quantity : Optional[int]
#     delivered_quantity : Optional[int]
#     code : Optional[str]
#     extras : Optional[List[Extras]]

#este es el de dispatch_guide
class Items(BaseModel):
    id :  Optional[int]
    name: Optional[str]
    description :  Optional[str]
    quantity : Optional[int]
    original_quantity:  Optional[int]
    delivered_quantity :  Optional[int]
    code :  Optional[str]
    extras : Optional [List[Extras]]
#este es el de dispatch_guide
class Groups(BaseModel):
    name : Optional[str]
    group_category : Optional[str]
    group_category_id : Optional[str]
    associated_at :  Optional[int]

class Place(BaseModel):
    name: Optional[str]
    id : Optional[int]

class Waypoint(BaseModel):
    latitude: Optional[float]
    longitude : Optional[float] 


### esta cosa es de dispatch_guide
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



# resource : Optional[str] v
#     event : Optional[str] v
#     route : Optional[int]
#     account_id : Optional[int]
#     date : Optional[str]
#     truck : Optional[str]
#     truck_driver : Optional[str]
#     started : bool
#     started_at : Optional[str]
#     ended : bool
#     ended_at : Optional[str]

class Dispatch(BaseModel):
    resource : Optional[str]
    event : Optional[str]
    route : Optional[int]
    account_name: Optional[str]
    account_id : Optional[int]
    date : Optional[str]
    identifier : Optional[str]
    truck : Optional[str]
    truck_driver : Optional[str]
    truck_identifier : Optional[str]
    started : Optional[bool]
    started_at : Optional[str]
    ended : Optional[bool]
    ended_at : Optional[str]
    status : Optional[int]
    substatus : Optional[str]
    substatus_code : Optional[str]
    estimated_at : Optional[str]
    contact_identifier : Optional[str]
    tags : Optional[List[tags]]
    is_pickup :  Optional[bool]
    is_trunk :  Optional[bool]
    evaluation_answers : Optional[List[EvaluationAnswers]]
    items : Optional[List[Items]]
    arrived_at : Optional[str]
    place : Optional[Place]
    waypoint : Optional[Waypoint]
    dispatch_guide : Optional[DistpatchGuideD]
    groups : Optional[List[Groups]]


class DispatchInsert(BaseModel):
    resource : Optional[str]
    event : Optional[str]
    identifier : Optional[str]
    truck_identifier : Optional[str]
    status : Optional[int]
    substatus : Optional[str]
    substatus_code : Optional[str]
    contact_identifier : Optional[str]
    bultos : Optional[str]
    comuna : Optional[str]
    driver : Optional[str]
    item_name : Optional[str]
    item_quantity : Optional[int]
    item_delivered_quantity : Optional[int]
    item_code : Optional[str]
    arrived_at: Optional[str]
    latitud : Optional[str]
    longitud : Optional[str]