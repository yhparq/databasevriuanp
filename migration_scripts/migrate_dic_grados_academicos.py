import csv
import os
import sys

# Add the parent directory to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from migration_scripts.db_connections import get_postgres_connection

def migrate_dic_grados_academicos():
    """
    Migrates academic degrees data from a CSV file to the PostgreSQL database.
    """
    # Path to the CSV file, assuming it's in the parent directory
    csv_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dic_grados_academicos_rows.csv'))

    if not os.path.exists(csv_file_path):
        print(f"Error: CSV file not found at {csv_file_path}")
        return

    conn = None
    try:
        conn = get_postgres_connection()
        cur = conn.cursor()
        print("Connected to PostgreSQL. Starting migration for dic_grados_academicos...")

        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                nombre = row['nombre']
                abreviatura = row['abreviatura']
                
                # Check if the record already exists
                cur.execute("SELECT id FROM dic_grados_academicos WHERE abreviatura = %s OR nombre = %s", (abreviatura, nombre))
                if cur.fetchone():
                    print(f"Skipping existing record: {nombre} ({abreviatura})")
                    continue

                # Insert new record
                cur.execute(
                    "INSERT INTO dic_grados_academicos (nombre, abreviatura, estado_dic_grados_academicos) VALUES (%s, %s, %s)",
                    (nombre, abreviatura, True)
                )
                print(f"Inserted record: {nombre} ({abreviatura})")

        conn.commit()
        print("Migration for dic_grados_academicos completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("PostgreSQL connection closed.")

if __name__ == "__main__":
    migrate_dic_grados_academicos()
