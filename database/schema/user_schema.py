from pydantic import BaseModel
from typing import Optional

class userSchema(BaseModel):
    id: Optional[int]
    username: str
    password: str

class loginSchema(BaseModel):
    username: str
    password: str

def user_schema(user_db):
    dictionary = {}
    dictionary["id"] = user_db[0]
    dictionary["full_name"] = user_db[1]
    dictionary["mail"] = user_db[2]
    dictionary["password"] = user_db[3]
    dictionary["active"] = user_db[4]
    dictionary["rol_id"] = user_db[5]
    
    return dictionary

def users_schema(users_db):
    item = []
    for data in users_db:
        dictionary = {}
        dictionary["id"] = data[0]
        dictionary["full_name"] = data[1]
        dictionary["mail"] = data[2]
        dictionary["password"] = data[3]
        dictionary["active"] = data[4]
        dictionary["rol_id"] = data[5]

        item.append(dictionary)
    
    return item



    
   
    