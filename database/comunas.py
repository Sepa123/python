import psycopg2
import codecs
from decouple import config
import os, sys, codecs

# import datetime
# import pytz

# Especifica el timezone que deseas utilizar
timezone = 'America/Santiago'

# Configura las opciones de conexión con el timezone especificado
options = f'--timezone={timezone}'



class ComunasConnection():
    conn = None
    def __init__(self) -> None:
        try:
            self.conn = psycopg2.connect(config("POSTGRES_DB_CARGA"), options=options)
            # self.conn.encoding("")
            self.conn.autocommit = True
            self.conn.set_client_encoding("UTF-8")
        except psycopg2.OperationalError as err:
            print(err)
            self.conn.close()
            self.conn = psycopg2.connect(config("POSTGRES_DB_CARGA"), options=options)
            # self.conn.encoding("")
            self.conn.autocommit = True
            self.conn.set_client_encoding("UTF-8")

    def __def__(self):
        self.conn.close()

    def closeDB(self):
        self.conn.close()

    def get_regiones_chile(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT *
                FROM public.op_regiones;
            """)
            return cur.fetchall()
        
    def get_comunas_chile_by_id_region(self,id_region):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT  comuna_name, id_region
                FROM public.op_comunas
                where id_region = '{id_region}'
            """)
            return cur.fetchall()
        
    def get_comunas_chile(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""
                SELECT  comuna_name, id_region, id_comuna
                FROM public.op_comunas
            """)
            return cur.fetchall()