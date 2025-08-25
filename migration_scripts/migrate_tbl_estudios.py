import csv
import os
import sys
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from migration_scripts.db_connections import get_postgres_connection

def migrate_tbl_estudios():
    """
    Migrates studies data from a CSV file to the tbl_estudios table in PostgreSQL,
    filtering for records with a valid university ID (<= 33).
    """
    csv_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'tbl_estudios_rows.csv'))

    if not os.path.exists(csv_file_path):
        print(f"Error: CSV file not found at {csv_file_path}")
        return

    conn = None
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        print("Connected to PostgreSQL. Starting migration for tbl_estudios...")

        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            records_to_insert = []
            for row in reader:
                id_universidad = int(row['id_universidad']) if row['id_universidad'] else None

                # Apply the filter condition
                if id_universidad and id_universidad <= 33:
                    id_usuario = int(row['id_usuario']) if row['id_usuario'] else None
                    id_grado_academico = int(row['id_grado_academico']) if row['id_grado_academico'] else None
                    id_tipo_obtencion = int(row['id_tipo_obtencion']) if row['id_tipo_obtencion'] else None
                    fecha_emision = row['fecha_emision'] if row['fecha_emision'] else None

                    records_to_insert.append((
                        id_usuario,
                        id_universidad,
                        id_grado_academico,
                        row['titulo_profesional'],
                        row['especialidad'],
                        fecha_emision,
                        row['resolucion'],
                        id_tipo_obtencion
                    ))

            if records_to_insert:
                print(f"Found {len(records_to_insert)} records to insert after filtering.")
                
                # Truncate before inserting
                cur.execute("TRUNCATE TABLE public.tbl_estudios RESTART IDENTITY CASCADE;")

                insert_query = """
                    INSERT INTO tbl_estudios (
                        id_usuario, id_universidad, id_grado_academico, titulo_profesional,
                        especialidad, fecha_emision, resolucion, id_tipo_obtencion
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                cur.executemany(insert_query, records_to_insert)
                conn.commit()
                print("Migration for tbl_estudios completed successfully.")
            else:
                print("No valid records found to insert after filtering.")

    except Exception as e:
        print(f"An error occurred during tbl_estudios migration: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("PostgreSQL connection closed.")

if __name__ == "__main__":
    migrate_tbl_estudios()
