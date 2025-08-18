import csv
import os
import psycopg2
import mysql.connector
from db_connections import get_mysql_pilar3_connection, get_postgres_connection

def populate_tbl_tesistas():
    """
    Puebla la tabla tbl_tesistas, truncando los códigos de estudiante de más de 6
    caracteres y registrándolos en un CSV.
    """
    postgres_conn = None
    mysql_conn = None
    
    try:
        postgres_conn = get_postgres_connection()
        mysql_conn = get_mysql_pilar3_connection()

        if not all([postgres_conn, mysql_conn]):
            raise Exception("No se pudieron establecer todas las conexiones.")

        pg_cursor = postgres_conn.cursor()
        mysql_cursor = mysql_conn.cursor(dictionary=True)

        # 1. Cargar mapa de Estructura Académica
        print("Cargando mapeo de estructura académica desde CSV...")
        ea_map = {}
        csv_path = os.path.join(os.path.dirname(__file__), '..', 'tbl_estructura_academica_rows.csv')
        with open(csv_path, mode='r', encoding='utf-8') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                ea_map[int(row['id_carrera'])] = int(row['id'])
        print(f"Se mapearon {len(ea_map)} estructuras académicas.")

        # 2. Cargar mapa de Usuarios
        print("Leyendo usuarios desde PostgreSQL para mapeo...")
        pg_cursor.execute("SELECT id, lower(trim(nombres) || ' ' || trim(apellidos)) FROM tbl_usuarios")
        user_map = {name: user_id for user_id, name in pg_cursor.fetchall()}
        print(f"Se mapearon {len(user_map)} usuarios.")

        # 3. Leer tesistas de MySQL
        print("Leyendo tesistas desde MySQL...")
        mysql_cursor.execute("SELECT * FROM tblTesistas")
        tesistas_records = mysql_cursor.fetchall()
        print(f"Se encontraron {len(tesistas_records)} registros de tesistas para procesar.")

        # 4. Vincular, truncar códigos si es necesario, y preparar para la inserción
        tesistas_to_insert = []
        tesistas_no_vinculados = []
        tesistas_codigo_largo = []

        for tesista in tesistas_records:
            nombres = tesista.get('Nombres', '').strip().lower()
            apellidos = tesista.get('Apellidos', '').strip().lower()
            full_name_key = f"{nombres} {apellidos}"
            
            user_id = user_map.get(full_name_key)
            id_carrera = tesista.get('IdCarrera')
            id_estructura_academica = ea_map.get(id_carrera)

            if user_id and id_estructura_academica:
                codigo_estudiante = tesista.get('Codigo', '')
                if len(codigo_estudiante) > 6:
                    tesistas_codigo_largo.append(tesista)
                    codigo_estudiante = codigo_estudiante[:6]

                mapped_tesista = (
                    tesista.get('Id'),
                    user_id,
                    codigo_estudiante,
                    id_estructura_academica,
                    tesista.get('Activo')
                )
                tesistas_to_insert.append(mapped_tesista)
            else:
                motivo = ""
                if not user_id: motivo += "Usuario no encontrado. "
                if not id_estructura_academica: motivo += "Estructura académica no encontrada."
                tesistas_no_vinculados.append({**tesista, 'motivo_omitido': motivo})

        # 5. Insertar los registros
        if tesistas_to_insert:
            print(f"\nInsertando {len(tesistas_to_insert)} registros en tbl_tesistas...")
            pg_cursor.execute("TRUNCATE TABLE public.tbl_tesistas RESTART IDENTITY CASCADE;")
            
            insert_query = """
                INSERT INTO tbl_tesistas (id, id_usuario, codigo_estudiante, id_estructura_academica, estado) 
                VALUES (%s, %s, %s, %s, %s)
            """
            pg_cursor.executemany(insert_query, tesistas_to_insert)
            postgres_conn.commit()
            print("Poblado de tbl_tesistas completado.")

        # 6. Exportar reportes
        if tesistas_no_vinculados:
            report_path = os.path.join(os.path.dirname(__file__), '..', 'tesistas_no_vinculados.csv')
            print(f"Exportando {len(tesistas_no_vinculados)} tesistas no vinculados a: {report_path}")
            fieldnames = list(tesistas_records[0].keys()) + ['motivo_omitido']
            with open(report_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(tesistas_no_vinculados)

        if tesistas_codigo_largo:
            report_path = os.path.join(os.path.dirname(__file__), '..', 'tesistas_con_codigo_largo.csv')
            print(f"Exportando {len(tesistas_codigo_largo)} registros con código largo a: {report_path}")
            fieldnames = list(tesistas_records[0].keys())
            with open(report_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(tesistas_codigo_largo)

    except (Exception, psycopg2.Error, mysql.connector.Error) as e:
        print(f"Error durante el poblado de tbl_tesistas: {e}")
        if postgres_conn:
            postgres_conn.rollback()
        raise e
    finally:
        if postgres_conn:
            postgres_conn.close()
        if mysql_conn:
            mysql_conn.close()
