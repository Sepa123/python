from pydantic import BaseModel
from typing import Optional
import re


def get_number(data : str):
    return [int(s) for s in str.split(data) if s.isdigit()][0]

class cargaEasy(BaseModel):
    total: int
    verificado: int

def cargaEasy_schema(data_db):
    item = []
    # print(data_db[0][0])
    print()
    dictionary = {}
    dictionary["total"] = get_number(data_db[0][0])
    dictionary["verificado"] = get_number(data_db[1][0])
    item.append(dictionary)
    print(dictionary)
    return item