import os
import sys
import psycopg2

# Add parent directory to Python path to allow module imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from migration_scripts.db_connections import get_postgres_connection, get_mysql_absmain_connection

def migrate_tbl_grado_docente():
    """
    Populates the tbl_grado_docente table by combining data from PostgreSQL
    (tbl_docentes, tbl_estudios) and MySQL (tblDocentes).
    """
    pg_conn = None
    mysql_conn = None
    try:
        pg_conn = get_postgres_connection()
        mysql_conn = get_mysql_absmain_connection()
        
        if not pg_conn or not mysql_conn:
            raise Exception("Database connection failed.")

        pg_cursor = pg_conn.cursor()
        mysql_cursor = mysql_conn.cursor(dictionary=True)

        print("Connections established. Starting migration for tbl_grado_docente.")

        # 1. Get degree hierarchy from PostgreSQL
        pg_cursor.execute("SELECT id, nombre FROM dic_grados_academicos")
        grados_data = pg_cursor.fetchall()
        
        # Define hierarchy (higher number is better)
        hierarchy = {'Doctor': 5, 'Magíster': 4, 'Segunda Especialidad': 3, 'Título Profesional': 2, 'Bachiller': 1}
        grados_map = {row[0]: {'nombre': row[1], 'rank': hierarchy.get(row[1], 0)} for row in grados_data}

        # 2. Get old category data from MySQL
        mysql_cursor.execute("SELECT Codigo, Categoria, fechaasc FROM tblDocentes")
        mysql_docentes_data = {row['Codigo']: row for row in mysql_cursor.fetchall()}
        print(f"Fetched {len(mysql_docentes_data)} records from MySQL tblDocentes.")

        # 3. Get docentes who have studies from PostgreSQL
        pg_cursor.execute("""
            SELECT DISTINCT d.id as docente_id, d.id_usuario, d.codigo_airhs
            FROM tbl_docentes d
            JOIN tbl_estudios es ON d.id_usuario = es.id_usuario
        """)
        docentes_with_studies = pg_cursor.fetchall()
        print(f"Found {len(docentes_with_studies)} docentes with studies to process.")

        records_to_insert = []
        for docente_id, usuario_id, codigo_airhs in docentes_with_studies:
            
            # 4. Find the highest academic degree for the current user
            pg_cursor.execute("SELECT id_grado_academico FROM tbl_estudios WHERE id_usuario = %s", (usuario_id,))
            user_degrees = pg_cursor.fetchall()
            
            highest_degree_rank = -1
            highest_degree_id = None
            for degree_id_tuple in user_degrees:
                degree_id = degree_id_tuple[0]
                if degree_id in grados_map and grados_map[degree_id]['rank'] > highest_degree_rank:
                    highest_degree_rank = grados_map[degree_id]['rank']
                    highest_degree_id = degree_id
            
            if not highest_degree_id:
                continue # Skip if no valid degree found

            highest_degree_name = grados_map[highest_degree_id]['nombre']

            # 5. Get category and antiquity from MySQL data
            mysql_data = mysql_docentes_data.get(codigo_airhs)
            if not mysql_data:
                continue # Skip if no matching record in old MySQL table

            categoria_descripcion = mysql_data.get('Categoria')
            antiguedad_categoria = mysql_data.get('fechaasc')

            records_to_insert.append((
                docente_id,
                highest_degree_name,
                categoria_descripcion,
                antiguedad_categoria,
                True  # estado_tbl_grado_docente
            ))

        # 6. Insert all records into the target table
        if records_to_insert:
            print(f"Prepared {len(records_to_insert)} records for insertion.")
            pg_cursor.execute("TRUNCATE TABLE public.tbl_grado_docente RESTART IDENTITY CASCADE;")
            
            insert_query = """
                INSERT INTO tbl_grado_docente (
                    id_docente, grado_academico, categoria_descripcion, 
                    antiguedad_categoria, estado_tbl_grado_docente
                ) VALUES (%s, %s, %s, %s, %s)
            """
            pg_cursor.executemany(insert_query, records_to_insert)
            pg_conn.commit()
            print("Migration for tbl_grado_docente completed successfully.")
        else:
            print("No valid records found to migrate.")

    except (Exception, psycopg2.Error) as e:
        print(f"An error occurred during migration: {e}")
        if pg_conn:
            pg_conn.rollback()
    finally:
        if pg_conn:
            pg_conn.close()
        if mysql_conn:
            mysql_conn.close()
        print("Database connections closed.")

if __name__ == "__main__":
    migrate_tbl_grado_docente()