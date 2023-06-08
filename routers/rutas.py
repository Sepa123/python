from fastapi import APIRouter, status,HTTPException
from fastapi.responses import FileResponse
from openpyxl import Workbook
import re
import json
from typing import List

## conexion

from database.client import reportesConnection

## modelos y schemas

from database.models.ruta_manual import RutaManual
from database.schema.ruta_manual import convert_to_json

router = APIRouter(tags=["rutas"], prefix="/api/rutas")

conn = reportesConnection()

@router.get("/buscar/{pedido_id}",status_code=status.HTTP_202_ACCEPTED)
async def get_ruta_manual(pedido_id : str):
    results = conn.get_ruta_manual(pedido_id)
    
    if results is None or results == []:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="El codigo del producto no existe")
    
    json_data = convert_to_json(results)
    # print(results)
    print("/buscar/ruta")

    return json_data


@router.post("/agregar",status_code=status.HTTP_201_CREATED)
async def insert_ruta_manual(rutas : List[List[RutaManual]]):
    # try:
        print(len(rutas))
        id_ruta = conn.read_id_ruta()[0]
        nombre_ruta = conn.get_nombre_ruta_manual(rutas[0][0].Created_by)[0][0]

        check = conn.check_producto_existe(rutas[0][0].Codigo_pedido)
        check = re.sub(r'\(|\)', '',check[0])
        check = check.split(",")

        print(check)
        
        if(check[0] == "1"):
            print("codigo pedido repetido")
            raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED, 
                                detail=f"El Producto {rutas[0][0].Codigo_pedido} se encuentra en la ruta {check[1]}")
        for ruta in rutas:
            for producto in ruta:
                data = producto.dict()
                data["Id_ruta"] = id_ruta
                data["Agrupador"] = nombre_ruta
                data["Nombre_ruta"] = nombre_ruta
                # data["Pistoleado"] = True 
                # conn.update_verified(data["Codigo_producto"])
                # conn.write_rutas_manual(data)
        return { "message": f"La Ruta {nombre_ruta} fue guardada exitosamente" }
    # except:
    #     print("error")
    #     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Error con la consulta")

# @router.put("/agregar",status_code=status.HTTP_201_CREATED)
# async def insert_ruta_manual(rutas : List[List[RutaManual]]):
