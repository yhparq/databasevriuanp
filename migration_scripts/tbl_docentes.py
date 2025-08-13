import csv
import os
from db_connections import get_mysql_absmain_connection, get_postgres_connection

def migrate_tbl_docentes():
    """
    Migra datos de tblDocentes (MySQL) a tbl_docentes (PostgreSQL)
    con una lógica compleja de vinculación de usuarios.
    Los docentes que no se pueden vincular se guardan en un archivo CSV.
    """
    mysql_conn = None
    postgres_conn = None
    try:
        mysql_conn = get_mysql_absmain_connection()
        postgres_conn = get_postgres_connection()
        if not all([mysql_conn, postgres_conn]):
            raise Exception("No se pudieron establecer todas las conexiones de base de datos.")

        mysql_cursor = mysql_conn.cursor(dictionary=True)
        postgres_cursor = postgres_conn.cursor()

        # Seleccionamos todas las columnas de MySQL para tener un reporte completo
        mysql_cursor.execute("SELECT * FROM tblDocentes")
        mysql_docentes = mysql_cursor.fetchall()

        docentes_to_insert = []
        docentes_skipped = [] # Lista para guardar los docentes omitidos

        print(f"Procesando {len(mysql_docentes)} docentes de MySQL...")

        for docente in mysql_docentes:
            user_id = None
            
            # Intento 1: Buscar por DNI
            if docente['DNI']:
                postgres_cursor.execute("SELECT id FROM tbl_usuarios WHERE num_doc_identidad = %s", (docente['DNI'],))
                result = postgres_cursor.fetchone()
                if result:
                    user_id = result[0]
            
            # Intento 2: Si no se encuentra por DNI, buscar por correo
            if not user_id and docente['Correo']:
                postgres_cursor.execute("SELECT id FROM tbl_usuarios WHERE correo = %s", (docente['Correo'],))
                result = postgres_cursor.fetchone()
                if result:
                    user_id = result[0]

            if user_id:
                docentes_to_insert.append((
                    docente['Id'],
                    user_id,
                    docente['IdCategoria'],
                    docente['Codigo'],
                    1, # id_especialidad por defecto
                    docente['Activo']
                ))
            else:
                # Si no se encuentra, se añade a la lista de omitidos
                print(f"  AVISO: Omitiendo docente DNI {docente['DNI']}. No se encontró usuario. Se guardará en el reporte.")
                docentes_skipped.append(docente)
        
        # Insertar los registros que sí se pudieron vincular
        if docentes_to_insert:
            insert_query = """
                INSERT INTO tbl_docentes (id, id_usuario, id_categoria, codigo_airhs, id_especialidad, estado_docente) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            postgres_cursor.executemany(insert_query, docentes_to_insert)
            postgres_conn.commit()
        
        # Crear el reporte CSV con los docentes omitidos
        if docentes_skipped:
            report_path = os.path.join(os.path.dirname(__file__), '..', 'docentes_no_migrados.csv')
            print(f"\nCreando reporte de docentes no migrados en: {report_path}")
            
            with open(report_path, 'w', newline='', encoding='utf-8') as csvfile:
                # Usar las claves del primer diccionario de la lista como cabeceras
                fieldnames = docentes_skipped[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                writer.writerows(docentes_skipped)
            
            print("Reporte creado exitosamente.")

        print(f"\nMigración de docentes completada.")
        print(f"  Registros insertados: {len(docentes_to_insert)}")
        print(f"  Registros omitidos (guardados en el reporte): {len(docentes_skipped)}")

    except Exception as e:
        print(f"Error en la migración de tbl_docentes: {e}")
        if postgres_conn:
            postgres_conn.rollback()
        raise e
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()
