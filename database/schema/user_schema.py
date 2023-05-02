from pydantic import BaseModel
from typing import Optional

class userSchema(BaseModel):
    id: Optional[int]
    username: str
    password: str

class loginSchema(BaseModel):
    username: str
    password: str

def users_schema(users_db):
    item = []

    for data in users_db:
        dictionary = {}
        dictionary["id"] = data[0]
        dictionary["username"] = data[1]
        item.append(dictionary)
    
    return item


    
   
    