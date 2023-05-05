import psycopg2
from decouple import config

### Conexion usuario (ahora conexion a  )
class UserConnection():
    conn = None
    def __init__(self) -> None:
        try:
            self.conn = psycopg2.connect(config("POSTGRES_DB_LOCAL"))
        except psycopg2.OperationalError as err:
            print(err)
            self.conn.close()
        
    def __def__(self):
        self.conn.close()
    
    def write(self, data):
        with self.conn.cursor() as cur:
            cur.execute("""
            INSERT INTO public.users(username, password) VALUES(%(username)s, %(password)s);

            """,data)
        self.conn.commit()

    
    def read_all(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT id, username FROM users
            """)
            return cur.fetchall()
        
    def read_only_one(self, data : str):
        
        with self.conn.cursor() as cur:
            cur.execute("""
            SELECT username,password FROM users WHERE username=%(username)s 

            """, data)
            return cur.fetchone()
        
class reportesConnection():
    conn = None
    def __init__(self) -> None:
        try:
            # self.conn = psycopg2.connect(database="postgres",host="44.199.104.254",user="wms_readonly",password="TY2022#",port="5432")
            self.conn = psycopg2.connect(config("POSTGRES_DB_CARGA"))

        except psycopg2.OperationalError as err:
            print(err)
            self.conn.close()
        
    def __def__(self):
        self.conn.close()

    def read_cargas_easy(self):

        with self.conn.cursor() as cur:

            cur.execute("""
            Select 'Verificados: '||  count(*) as Cuenta from areati.ti_wms_carga_easy twce
            Where to_char(created_at,'yyyy-mm-dd')=to_char(current_date,'yyyy-mm-dd') and verified= true
            union
            Select 'Total: '|| count(*) as Cuenta from areati.ti_wms_carga_easy twce
            Where to_char(created_at,'yyyy-mm-dd')=to_char(current_date,'yyyy-mm-dd')
            """)
            return cur.fetchall()
        
class transyanezConnection():
    conn = None
    def __init__(self) -> None:
        try:
            self.conn = psycopg2.connect(config("POSTGRES_DB_TR"))

        except psycopg2.OperationalError as err:
            print(err)
            self.conn.close()
    
    def get_vehiculos_portal(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            select
            --coalesce (TRIM(a.company_name) > '','-') as "Compañía",
            case when a.company_name <> '' then a.company_name
            else '(ID : ' || u.agencies[1] || ' )' -- || u.full_name
            end "Compañia",
            --u.agencies[1] || ' - ' || u.full_name as "ID - Nombre Colaborador",
            (select ra."name" from public.regions ra where ra.id =u.region) as "Región Origen",
            v.patent as "Patente",
            case
            when v.active then 'Activo'
            else 'Pendiente'
            end as "Estado",
            '(' || v.type || ') ' || vt."name" as "Tipo",
            -- v.carasteristic,
            vc."name" as "Caracteristica",
            --v.brand,
            vb."name" as "Marca",
            v.model as "Modelo",
            v.year as "Año",
            r."name" as "Region",
            c."name" as "Comuna"
            --v.agency_id,
            --v.driver_user_id,
            --v.peoneta_user_id
            --lower(u.mail) as "E-Mail"
            from "transyanez".vehicle v
            left join public.regions r on r.id=v.region
            left join public.communes c on c.id=v.commune
            left join public.agency a on a.id = v.agency_id
            left join "user".users u on cast(u.agencies[1] as integer) = cast(v.agency_id as integer)
            left join "transyanez".vehicle_type vt on vt.id = v."type"
            left join "transyanez".vehicle_characteristic vc on vc.id = v.carasteristic
            left join "transyanez".vehicle_brand vb on vb.id = v.brand
            where v.patent <> ''
            group by 1,2,3,4,5,6,7,8,9,10,11
            order by 1 asc
            """)
            return cur.fetchall()


