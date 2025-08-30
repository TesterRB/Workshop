import mysql.connector
from mysql.connector import Error


class DataWarehouse:
    def __init__(self, host, user, password, database):
        """Inicializa la conexión a MySQL"""
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None

    def connect(self):
        """Conexión a la base de datos"""
        try:
            self.conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            if self.conn.is_connected():
                print("Conexión exitosa a la base de datos")
        except Error as e:
            print(f"Error de conexión: {e}")

    def insert_data(self, table, data):
        """Inserta datos en una tabla"""
        try:
            cursor = self.conn.cursor()
            placeholders = ", ".join(["%s"] * len(data))
            columns = ", ".join(data.keys())
            sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(data.values()))
            self.conn.commit()
            print(f"Datos insertados en {table}")
        except Error as e:
            print(f"Error al insertar datos en {table}: {e}")

    def get_data(self, query):
        """Obtiene datos con una consulta SQL"""
        try:
            cursor = self.conn.cursor(dictionary=True)
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f"Error al obtener datos: {e}")
            return []

    def close(self):
        """Cierra la conexión"""
        if self.conn.is_connected():
            self.conn.close()
            print("Conexión cerrada")
