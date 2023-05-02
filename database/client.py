import psycopg2

class UserConnection():
    conn = None
    def __init__(self) -> None:
        try:
            self.conn = psycopg2.connect(database="postgres",host="localhost",user="postgres",password="1234",port="5432")
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
            # cur.execute("""
            # SELECT * FROM users WHERE username=%(username)s AND password=%(password)s

            # """,data)
            cur.execute("""
            SELECT username,password FROM users WHERE username=%(username)s 

            """, data)
            return cur.fetchone()
        
            