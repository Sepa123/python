def roles_list(rol):
  return  {
    "id" : rol[0],
    "name":rol[1],
    "description" : rol[2],
    "extra_data": eval(rol[3]),
    "is_sub_rol": rol[4]
}

def roles_list_schema(roles):
    return [roles_list(rol) for rol in roles ]
