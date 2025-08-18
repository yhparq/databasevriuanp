
import csv
import os
import psycopg2
from db_connections import get_postgres_connection

def migrate_estructura_academica():
    """
    Puebla la tabla tbl_estructura_academica en PostgreSQL desde un archivo CSV.
    """
    postgres_conn = None
    
    try:
        postgres_conn = get_postgres_connection()
        if not postgres_conn:
            raise Exception("No se pudo establecer la conexión a PostgreSQL.")

        pg_cursor = postgres_conn.cursor()

        # 1. Cargar datos desde el CSV
        print("Leyendo datos desde tbl_estructura_academica_rows.csv...")
        csv_path = os.path.join(os.path.dirname(__file__), '..', 'tbl_estructura_academica_rows.csv')
        
        records_to_insert = []
        with open(csv_path, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                # Mapear las columnas del CSV a la tabla
                mapped_record = (
                    int(row['id']),
                    row['nombre'],
                    int(row['id_especialidad']),
                    int(row['id_sede']),
                    int(row['estado_ea'])
                )
                records_to_insert.append(mapped_record)
        
        print(f"Se encontraron {len(records_to_insert)} registros para insertar.")

        # 2. Insertar los registros en la tabla
        if records_to_insert:
            print("Limpiando e insertando registros en tbl_estructura_academica...")
            
            # Limpiar la tabla antes de insertar para evitar conflictos
            pg_cursor.execute("TRUNCATE TABLE public.tbl_estructura_academica RESTART IDENTITY CASCADE;")
            
            insert_query = """
                INSERT INTO tbl_estructura_academica (id, nombre, id_especialidad, id_sede, estado_ea) 
                VALUES (%s, %s, %s, %s, %s)
            """
            pg_cursor.executemany(insert_query, records_to_insert)
            postgres_conn.commit()
            print("Migración de tbl_estructura_academica completada.")

    except (Exception, psycopg2.Error) as e:
        print(f"Error durante la migración de tbl_estructura_academica: {e}")
        if postgres_conn:
            postgres_conn.rollback()
        raise e
    finally:
        if postgres_conn:
            postgres_conn.close()
