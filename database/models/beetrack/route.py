from typing import Optional , List, Any
from datetime import datetime
from pydantic import BaseModel

class Route(BaseModel):
    resource : str
    event : str
    route : int
    account_id : int
    date : str
    truck : str
    truck_driver : str
    started : bool
    started_at : str
    ended : bool
    ended_at : str
