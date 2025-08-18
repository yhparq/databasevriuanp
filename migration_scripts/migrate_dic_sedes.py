
import csv
import os
import psycopg2
from db_connections import get_postgres_connection

def migrate_dic_sedes():
    """
    Puebla la tabla dic_sedes en PostgreSQL desde el archivo dic_sedes_rows.csv.
    """
    postgres_conn = None
    
    try:
        postgres_conn = get_postgres_connection()
        if not postgres_conn:
            raise Exception("No se pudo establecer la conexión a PostgreSQL.")

        pg_cursor = postgres_conn.cursor()

        # Cargar datos desde el CSV
        print("Leyendo datos desde dic_sedes_rows.csv...")
        csv_path = os.path.join(os.path.dirname(__file__), '..', 'dic_sedes_rows.csv')
        
        records_to_insert = []
        with open(csv_path, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                mapped_record = (
                    int(row['id']),
                    row['nombre'] # Leer del CSV
                )
                records_to_insert.append(mapped_record)
        
        print(f"Se encontraron {len(records_to_insert)} sedes para insertar.")

        # Insertar los registros en la tabla
        if records_to_insert:
            print("Limpiando e insertando registros en dic_sedes...")
            pg_cursor.execute("TRUNCATE TABLE public.dic_sedes RESTART IDENTITY CASCADE;")
            
            # Usar el nombre de columna correcto de la BD
            insert_query = "INSERT INTO dic_sedes (id, nombre) VALUES (%s, %s)"
            pg_cursor.executemany(insert_query, records_to_insert)
            
            postgres_conn.commit()
            print("Migración de dic_sedes completada.")

    except (Exception, psycopg2.Error) as e:
        print(f"Error durante la migración de dic_sedes: {e}")
        if postgres_conn:
            postgres_conn.rollback()
        raise e
    finally:
        if postgres_conn:
            postgres_conn.close()
