from typing import Optional , List, Any
from datetime import datetime
from pydantic import BaseModel

class tags(BaseModel):
    name:str
    value : str

class Extras(BaseModel):
    name:str
    value : str

class EvaluationAnswers(BaseModel):
    name:str
    value : str


class Items(BaseModel):
    name:str
    quantity : int
    delivered_quantity : int
    code : str
    extras : List[Extras]


class Place(BaseModel):
    name:str
    id : int

class Waypoint(BaseModel):
    latitude: float
    longitude : float 

class Dispatch(BaseModel):
    resource : str
    event : str
    account_name: str
    account_id : int
    identifier : str
    truck_identifier : str
    status : int
    substatus : str
    substatus_code : str
    estimated_at : str
    contact_identifier : str
    tags : List[tags]
    is_pickup : bool
    is_trunk : bool
    evaluation_answers : List[EvaluationAnswers]
    items : List[Items]
    arrived_at : str
    place : Place
    waypoint : Optional[Waypoint]

