from typing import Optional , List, Any
from datetime import datetime
from pydantic import BaseModel

class Route(BaseModel):
    resource : Optional[str]
    event : Optional[str]
    route : Optional[int]
    account_id : Optional[int]
    date : Optional[str]
    truck : Optional[str]
    truck_driver : Optional[str]
    started : bool
    started_at : Optional[str]
    ended : bool
    ended_at : Optional[str]
