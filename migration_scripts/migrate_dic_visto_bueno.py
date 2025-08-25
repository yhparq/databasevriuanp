
import csv
import os
import psycopg2
from db_connections import get_postgres_connection

def migrate_dic_visto_bueno():
    """
    Puebla la tabla dic_visto_bueno en PostgreSQL desde un archivo CSV.
    """
    postgres_conn = None
    try:
        postgres_conn = get_postgres_connection()
        if not postgres_conn:
            raise Exception("No se pudo establecer la conexión a PostgreSQL.")

        pg_cursor = postgres_conn.cursor()

        csv_path = os.path.join(os.path.dirname(__file__), '..', 'dic_visto_bueno_rows.csv')
        
        records_to_insert = []
        with open(csv_path, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                mapped_record = (
                    int(row['id']),
                    row['descripcion'],
                    int(row['id_etapa'])
                )
                records_to_insert.append(mapped_record)
        
        print(f"Se encontraron {len(records_to_insert)} registros en el CSV de visto bueno.")

        if records_to_insert:
            pg_cursor.execute("TRUNCATE TABLE public.dic_visto_bueno RESTART IDENTITY CASCADE;")
            
            insert_query = """
                INSERT INTO dic_visto_bueno (id, descripcion, id_etapa) 
                VALUES (%s, %s, %s)
            """
            pg_cursor.executemany(insert_query, records_to_insert)
            postgres_conn.commit()
            print("Migración de dic_visto_bueno completada.")

    except (Exception, psycopg2.Error) as e:
        print(f"Error durante la migración de dic_visto_bueno: {e}")
        if postgres_conn:
            postgres_conn.rollback()
        raise e
    finally:
        if postgres_conn:
            postgres_conn.close()
