import csv
import os
import psycopg2
from db_connections import get_postgres_connection, get_mysql_pilar3_connection

def populate_tbl_tesistas():
    """
    Puebla la tabla tbl_tesistas en PostgreSQL a partir de tblTesistas en MySQL,
    asegurándose de incluir el id_antiguo para el mapeo.
    """
    print("--- Poblando tbl_tesistas (con id_antiguo) ---")
    pg_conn = None
    mysql_conn = None
    try:
        pg_conn = get_postgres_connection()
        mysql_conn = get_mysql_pilar3_connection()

        if pg_conn is None or mysql_conn is None:
            raise Exception("No se pudo conectar a una de las bases de datos.")

        pg_cur = pg_conn.cursor()
        mysql_cur = mysql_conn.cursor(dictionary=True)

        # 1. Obtener mapeo de DNI a id_usuario nuevo de PostgreSQL
        pg_cur.execute("SELECT id, num_doc_identidad FROM tbl_usuarios WHERE num_doc_identidad IS NOT NULL")
        user_map = {row[1]: row[0] for row in pg_cur.fetchall()}
        print(f"  Se mapearon {len(user_map)} usuarios desde PostgreSQL por DNI.")

        # 2. Leer tesistas de MySQL
        mysql_cur.execute("SELECT Id, DNI, Codigo, IdCarrera, Activo FROM tblTesistas")
        source_tesistas = mysql_cur.fetchall()
        print(f"  Se encontraron {len(source_tesistas)} tesistas en la tabla de origen.")

        # 3. Preparar los datos para la inserción
        tesistas_to_insert = []
        unmapped_users = 0
        for tesista in source_tesistas:
            dni = tesista['DNI']
            id_usuario = user_map.get(dni)
            
            if id_usuario:
                tesistas_to_insert.append((
                    id_usuario,
                    tesista['Codigo'],
                    tesista['IdCarrera'],
                    1 if tesista['Activo'] == 'A' else 0,
                    tesista['Id']  # id_antiguo
                ))
            else:
                unmapped_users += 1
        
        print(f"  Se prepararon {len(tesistas_to_insert)} registros de tesistas para insertar.")
        if unmapped_users > 0:
            print(f"  ADVERTENCIA: Se ignoraron {unmapped_users} tesistas porque su DNI no fue encontrado en tbl_usuarios.")

        # 4. Limpiar e insertar los datos en PostgreSQL
        print("  Limpiando la tabla tbl_tesistas...")
        pg_cur.execute("TRUNCATE TABLE public.tbl_tesistas RESTART IDENTITY CASCADE;")
        
        if tesistas_to_insert:
            insert_query = """
            INSERT INTO tbl_tesistas (
                id_usuario, codigo_estudiante, id_estructura_academica, estado, id_antiguo
            ) VALUES (%s, %s, %s, %s, %s)
            """
            pg_cur.executemany(insert_query, tesistas_to_insert)
            pg_conn.commit()
            print(f"  Se insertaron {len(tesistas_to_insert)} registros en tbl_tesistas.")

        print("--- Poblado de tbl_tesistas completado. ---")

    except Exception as e:
        print(f"  ERROR CRÍTICO durante el poblado de tbl_tesistas: {e}")
        if pg_conn:
            pg_conn.rollback()
    finally:
        if pg_conn: pg_conn.close()
        if mysql_conn: mysql_conn.close()

if __name__ == '__main__':
    populate_tbl_tesistas()
