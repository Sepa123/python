from typing import Optional, List, Any
from pydantic import BaseModel




##### TAGS ITEMS


class Tag(BaseModel):
    name: Optional[str]
    value: Optional[str]


class Item(BaseModel):
    id: Optional[int]
    name: Optional[str]
    description: Optional[str]
    quantity: Optional[int]
    original_quantity: Optional[int]
    delivered_quantity: Optional[int]
    code: Optional[str]
    extras: Optional[List[Tag]]


## Actualización de Guia


class ActualizacionGuia(BaseModel):
    resource: Optional[str]
    event: Optional[str]
    account_name: Optional[str]
    account_id: Optional[int]
    guide: Optional[int]
    identifier: Optional[int]
    beecode: Optional[str]
    mode: Optional[int]
    position: Optional[int]
    route_id: Optional[str]
    dispatch_id: Optional[int]
    truck_identifier: Optional[str]
    truck_type: Optional[str]
    status: Optional[int]
    estimated_at: Optional[str]
    max_delivery_time: Optional[str]
    min_delivery_time: Optional[str]
    is_pickup: Optional[bool]
    is_trunk: Optional[bool]
    locked: Optional[bool]
    substatus: Optional[str]
    substatus_code: Optional[str]
    contact_name: Optional[str]
    contact_phone: Optional[str]
    contact_identifier: Optional[str]
    contact_email: Optional[str]
    contact_address: Optional[str]
    tags: Optional[List[Tag]]
    items: Optional[List[Item]]
    groups: Optional[List[Any]]
    evaluation_answers: Optional[List[Any]]
    truck_groups: Optional[List[Any]]
    kpi_distance: Optional[str]
    real_distance: Optional[str]
    time_of_management: Optional[str]
    management_latitude: Optional[str]
    management_longitude: Optional[str]






#### Creación de Ruta

class CreacionRuta(BaseModel):
    resource: Optional[str]
    event: Optional[str]
    account_name: Optional[str]
    route: Optional[int]
    account_id: Optional[int]
    date: Optional[str]
    truck: Optional[str]
    truck_driver: Optional[str]
    started: Optional[bool]
    started_at: Optional[str]
    ended: Optional[bool]
    ended_at: Optional[str]
    start_form_answers: Optional[List[Any]]



#### Creació de Guia

class DispatchGuide(BaseModel):
    guide: Optional[int]
    beecode: Optional[str]
    identifier: Optional[int]
    account_id: Optional[int]
    promised_date: Optional[str]
    min_delivery_time: Optional[str]
    max_delivery_time: Optional[str]
    contact_name: Optional[str]
    contact_phone: Optional[str]
    contact_identifier: Optional[str]
    contact_email: Optional[str]
    contact_address: Optional[str]


class CreacionGuia(BaseModel):
    resource: Optional[str]
    event: Optional[str]
    account_name: Optional[str]
    dispatch_guide: Optional[DispatchGuide]
    tags: Optional[List[Tag]]
    items: Optional[List[Item]]
    groups: Optional[List[Any]]
