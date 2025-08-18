
import csv
import os
import psycopg2
import mysql.connector
from db_connections import get_mysql_absmain_connection, get_postgres_connection

def populate_tbl_docentes():
    """
    Puebla la tabla tbl_docentes de PostgreSQL vinculando los docentes de MySQL
    con los usuarios existentes en PostgreSQL mediante su nombre completo.
    """
    postgres_conn = None
    mysql_conn = None
    
    try:
        postgres_conn = get_postgres_connection()
        mysql_conn = get_mysql_absmain_connection()

        if not all([postgres_conn, mysql_conn]):
            raise Exception("No se pudieron establecer todas las conexiones.")

        pg_cursor = postgres_conn.cursor()
        mysql_cursor = mysql_conn.cursor(dictionary=True)

        # 1. Obtener todos los usuarios de PostgreSQL y crear un mapa de nombre a ID
        print("Leyendo usuarios desde PostgreSQL para mapeo...")
        pg_cursor.execute("SELECT id, lower(trim(nombres) || ' ' || trim(apellidos)) FROM tbl_usuarios")
        user_map = {name: user_id for user_id, name in pg_cursor.fetchall()}
        print(f"Se mapearon {len(user_map)} usuarios.")

        # 2. Leer todos los docentes de la tabla original de MySQL
        print("Leyendo docentes desde MySQL...")
        mysql_cursor.execute("SELECT * FROM tblDocentes")
        docentes_records = mysql_cursor.fetchall()
        print(f"Se encontraron {len(docentes_records)} docentes para procesar.")

        # 3. Vincular y preparar para la inserción
        docentes_to_insert = []
        docentes_no_vinculados = []

        for docente in docentes_records:
            nombres = docente.get('Nombres', '').strip().lower()
            apellidos = docente.get('Apellidos', '').strip().lower()
            full_name_key = f"{nombres} {apellidos}"

            user_id = user_map.get(full_name_key)

            if user_id:
                # Mapeo de datos para la tabla tbl_docentes
                mapped_docente = (
                    docente.get('Id'),
                    user_id,
                    docente.get('IdCategoria'),
                    docente.get('Codigo'),
                    1,  # id_especialidad (valor por defecto)
                    docente.get('Activo')
                )
                docentes_to_insert.append(mapped_docente)
            else:
                # Si no se encuentra el usuario, se guarda para el reporte
                docentes_no_vinculados.append(docente)

        # 4. Insertar los registros en tbl_docentes
        if docentes_to_insert:
            print(f"\nInsertando {len(docentes_to_insert)} registros en tbl_docentes...")
            # Antes de insertar, es buena práctica limpiar la tabla
            print("Limpiando la tabla tbl_docentes...")
            pg_cursor.execute("TRUNCATE TABLE public.tbl_docentes RESTART IDENTITY CASCADE;")
            
            insert_query = """
                INSERT INTO tbl_docentes (id, id_usuario, id_categoria, codigo_airhs, id_especialidad, estado_docente) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            pg_cursor.executemany(insert_query, docentes_to_insert)
            postgres_conn.commit()
            print("Poblado de tbl_docentes completado.")

        # 5. Exportar los no vinculados a un CSV
        if docentes_no_vinculados:
            report_path = os.path.join(os.path.dirname(__file__), '..', 'docentes_no_vinculados.csv')
            print(f"\nExportando {len(docentes_no_vinculados)} docentes no vinculados a: {report_path}")
            
            with open(report_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=docentes_no_vinculados[0].keys())
                writer.writeheader()
                writer.writerows(docentes_no_vinculados)
            print("Reporte de docentes no vinculados creado.")

    except (Exception, psycopg2.Error, mysql.connector.Error) as e:
        print(f"Error durante el poblado de tbl_docentes: {e}")
        if postgres_conn:
            postgres_conn.rollback()
        raise e
    finally:
        if postgres_conn:
            postgres_conn.close()
        if mysql_conn:
            mysql_conn.close()
