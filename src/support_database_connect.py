# database agent
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors

# data processing
import pandas as pd

# functions typing
from typing import Optional, Tuple, List, Union, Dict


drop_all_tables = """
DROP TABLE IF EXISTS ccaa, provinces, economicos, demograficos, generacion_renovables, demanda_evolucion CASCADE;
"""

ccaa_table = """
CREATE TABLE ccaa (
    ccaa_id INT PRIMARY KEY,
    ccaa VARCHAR(100) NOT NULL
);
"""

provinces_table = """
CREATE TABLE provinces (
    province_id INT PRIMARY KEY,
    province_name VARCHAR(100) NOT NULL,
    ccaa_id INT REFERENCES ccaa(ccaa_id) ON DELETE SET NULL
);
"""

economicos_table = """
CREATE TABLE economicos (
    economicos_id SERIAL PRIMARY KEY,
    province_id INT REFERENCES provinces(province_id) ON DELETE SET NULL,
    year INT NOT NULL,
    total NUMERIC NOT NULL,
    ccaa_id INT REFERENCES ccaa(ccaa_id) ON DELETE SET NULL
);
"""

demograficos_table = """
CREATE TABLE demograficos (
    demograficos_id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    province_id INT REFERENCES provinces(province_id) ON DELETE SET NULL,
    age VARCHAR(20) NOT NULL,
    nationality VARCHAR(100) NOT NULL,
    gender CHAR(1) CHECK (gender IN ('H', 'M')),
    total INT NOT NULL,
    ccaa_id INT REFERENCES ccaa(ccaa_id) ON DELETE SET NULL
);
"""

generacion_renovables_table = """
CREATE TABLE generacion_renovables (
    generacion_id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    month INT CHECK (month BETWEEN 1 AND 12),
    value NUMERIC NOT NULL,
    type VARCHAR(100) NOT NULL,
    ccaa_id INT REFERENCES ccaa(ccaa_id) ON DELETE SET NULL
);
"""

demanda_evolucion_table = """
CREATE TABLE demanda_evolucion (
    demanda_id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    month INT CHECK (month BETWEEN 1 AND 12),
    value NUMERIC NOT NULL,
    ccaa_id INT REFERENCES ccaa(ccaa_id) ON DELETE SET NULL
);
"""


def establecer_conn(database_name, postgres_pass, usuario, host="localhost", autocommit=True):
    """
    Establece una conexión a una base de datos de PostgreSQL.

    Params:
        - database_name (str): El nombre de la base de datos a la que conectarse.
        - postgres_pass (str): La contraseña del usuario de PostgreSQL.
        - usuario (str): El nombre del usuario de PostgreSQL.
        - host (str, opcional): La dirección del servidor PostgreSQL. Por defecto es "localhost".

    Returns:
        psycopg2.extensions.connection: La conexión establecida a la base de datos PostgreSQL.

    """

    # Crear la conexión a la base de datos PostgreSQL
    conn = psycopg2.connect(
        host=host,
        user=usuario,
        password=postgres_pass,
        database=database_name
    )

    # Establecer la conexión en modo autocommit
    conn.autocommit = autocommit # No hace necesario el uso del commit al final de cada sentencia de insert, delete, etc.
    
    return conn

def crear_db(database_name):
    # conexion a postgres
    conn = establecer_conn("postgres", "admin", "postgres") # Nos conectamos a la base de datos de postgres por defecto para poder crear la nueva base de datos
    
    # creamos un cursor con la conexion que acabamos de crear
    cur = conn.cursor()
    
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
    
    # Almacenamos en una variable el resultado del fetchone. Si existe tendrá una fila sino será None
    bbdd_existe = cur.fetchone()
    
    # Si bbdd_existe es None, creamos la base de datos
    if not bbdd_existe:
        cur.execute(f"CREATE DATABASE {database_name};")
        print(f"Base de datos {database_name} creada con éxito")
    else:
        print(f"La base de datos ya existe")
        
    # Cerramos el cursor y la conexion
    cur.close()
    conn.close()

