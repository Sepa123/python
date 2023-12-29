import psycopg2
import codecs
from decouple import config
import os, sys, codecs


# Especifica el timezone que deseas utilizar
timezone = 'America/Santiago'

# Configura las opciones de conexión con el timezone especificado
options = f'--timezone={timezone}'


class HelaConnection():
    conn = None
    def __init__(self) -> None:
        try:
            self.conn = psycopg2.connect(config("POSTGRES_DB_CARGA"), options=options)
            # self.conn.encoding("")
            self.conn.autocommit = True
            self.conn.set_client_encoding("UTF-8")
        except psycopg2.OperationalError as err:
            print(err)
            print("Se conectara ???")
            self.conn.close()
            self.conn = psycopg2.connect(config("POSTGRES_DB_CARGA"), options=options)
            # self.conn.encoding("")
            self.conn.autocommit = True
            self.conn.set_client_encoding("UTF-8")
        
    def __def__(self):
        self.conn.close()

    
    ##Login User hela

    def read_only_one(self, data):
        with self.conn.cursor() as cur:
            print (data)
            cur.execute(f"""
           SELECT id, nombre ,mail,"password" ,activate ,rol_id  FROM hela.usuarios WHERE lower(mail)= lower('{data}')
            """)
            return cur.fetchone()

    ##Cambiar Contraseña Hela

    def cambiar_password(self,password,mail):
        with self.conn.cursor() as cur:
                cur.execute(f"""        
                UPDATE hela.usuarios
                SET "password"='{password}'
                WHERE mail='{mail}'
                """)
                row = cur.rowcount
                
        self.conn.commit()

        return row

      ## Asignar rutas activas
   
    def insert_ruta_asignada(self,data):
        with self.conn.cursor() as cur: 
            
            cur.execute("""
            INSERT INTO hela.ruta_asignada
            (asigned_by, id_ruta, nombre_ruta, patente, driver, cant_producto, estado, region)
            VALUES(%(asigned_by)s, %(id_ruta)s, %(nombre_ruta)s, %(patente)s, %(conductor)s, %(cantidad_producto)s, true, %(region)s);
            """,data)
        self.conn.commit()
    

    ## Obtener patente y drive por nombre_ruta

    def read_id_ruta_activa_by_nombre(self, nombre_ruta):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT patente,driver, id_ruta, region
            FROM hela.ruta_asignada where nombre_ruta = '{nombre_ruta}'
            """)
            return cur.fetchone()

    ##Bitacora de recepcion de productos

    def insert_data_bitacora_recepcion(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO hela.bitacora_recepcion
            (id_usuario, cliente, guia, ids_usuario, latitud, longitud,origen)
            VALUES(%(id_usuario)s, %(cliente)s, %(n_guia)s,  %(ids_usuario)s, %(latitud)s, %(longitud)s, %(observacion)s);
            """,data)
        
        self.conn.commit()


    ##Registrar nuevo usuario

    def insert_nuevo_usuario(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO hela.usuarios (nombre, mail, "password", activate, rol_id) VALUES( %(Nombre)s,  %(Mail)s,  %(Password)s , true, %(Rol)s );

            """,data)
        self.conn.commit()

    
    ## Actualizar ruta asignada

    def update_ruta_asignada(self,patente,driver,nombre_ruta):
        with self.conn.cursor() as cur:
                cur.execute(f"""        
                UPDATE hela.ruta_asignada
                set patente='{patente}', driver='{driver}'
                WHERE nombre_ruta = '{nombre_ruta}'
                """)
        self.conn.commit()

    def get_nombre_usuario_hela(self, id_usuario : int):
        with self.conn.cursor() as cur:
            cur.execute(f"""
            SELECT nombre
            FROM hela.usuarios
            WHERE id = {id_usuario}
            """)
            return cur.fetchone()
