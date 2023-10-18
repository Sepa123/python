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


class Items(BaseModel):
    name: Optional[str]
    quantity : Optional[int]
    delivered_quantity : Optional[int]
    code : Optional[str]
    extras : Optional[List[Extras]]


class Place(BaseModel):
    name: Optional[str]
    id : Optional[int]

class Waypoint(BaseModel):
    latitude: Optional[float]
    longitude : Optional[float] 

class Dispatch(BaseModel):
    resource : Optional[str]
    event : Optional[str]
    account_name: Optional[str]
    account_id : Optional[int]
    identifier : Optional[str]
    truck_identifier : Optional[str]
    status : Optional[int]
    substatus : Optional[str]
    substatus_code : Optional[str]
    estimated_at : Optional[str]
    contact_identifier : Optional[str]
    tags : Optional[List[tags]]
    is_pickup : bool
    is_trunk : bool
    evaluation_answers : Optional[List[EvaluationAnswers]]
    items : Optional[List[Items]]
    arrived_at : Optional[str]
    place : Optional[Place]
    waypoint : Optional[Waypoint]

