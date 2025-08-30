
import psycopg2
from db_connections import get_postgres_connection, get_mysql_pilar3_connection

def migrate_tbl_correcciones_jurados():
    """
    Migra las correcciones de jurados desde tblCorrects (MySQL), usando
    mapeo de id_antiguo para docentes y trámites.
    """
    print("--- Iniciando migración de tbl_correcciones_jurados (Lógica ID Antiguo) ---")
    pg_conn = None
    mysql_conn = None
    try:
        pg_conn = get_postgres_connection()
        mysql_conn = get_mysql_pilar3_connection()

        if pg_conn is None or mysql_conn is None:
            raise Exception("No se pudo conectar a una de las bases de datos.")

        pg_cur = pg_conn.cursor()
        mysql_cur = mysql_conn.cursor(dictionary=True)

        # 1. Crear mapas de IDs nuevos
        pg_cur.execute("SELECT id, id_antiguo FROM tbl_docentes WHERE id_antiguo IS NOT NULL")
        docente_map = {row[1]: row[0] for row in pg_cur.fetchall()}
        print(f"  Se mapearon {len(docente_map)} docentes por id_antiguo.")

        pg_cur.execute("SELECT id, id_antiguo FROM tbl_tramites WHERE id_antiguo IS NOT NULL")
        tramites_map = {row[1]: row[0] for row in pg_cur.fetchall()}

        # 2. Crear mapa de búsqueda para la conformación inicial
        pg_cur.execute("SELECT id, id_tramite, id_docente, id_orden FROM tbl_conformacion_jurados")
        conformacion_map = {(row[1], row[2]): (row[0], row[3]) for row in pg_cur.fetchall()}
        print(f"  Se mapearon {len(conformacion_map)} registros de conformación para búsqueda.")

        # 3. Leer correcciones de MySQL
        mysql_cur.execute("SELECT IdTramite, IdDocente, Fecha, Mensaje FROM tblCorrects")
        source_correcciones = mysql_cur.fetchall()

        # 4. Preparar datos, filtrando los que no tienen coincidencia
        correcciones_to_insert = []
        unmatched_count = 0

        for corr in source_correcciones:
            id_tramite_antiguo = corr['IdTramite']
            id_docente_antiguo = corr['IdDocente']
            
            new_tramite_id = tramites_map.get(id_tramite_antiguo)
            new_docente_id = docente_map.get(id_docente_antiguo)

            if not new_tramite_id or not new_docente_id:
                unmatched_count += 1
                continue

            match = conformacion_map.get((new_tramite_id, new_docente_id))
            
            if match:
                id_conformacion_jurado, id_orden = match
                correcciones_to_insert.append((
                    id_conformacion_jurado, id_orden, corr['Mensaje'], corr['Fecha'], 1
                ))
            else:
                unmatched_count += 1
        
        print(f"  Se prepararon {len(correcciones_to_insert)} correcciones para insertar.")
        print(f"  Se ignoraron {unmatched_count} correcciones sin coincidencia.")

        # 5. Insertar los datos
        if correcciones_to_insert:
            insert_query = """
            INSERT INTO tbl_correcciones_jurados (
                id_conformacion_jurado, orden, mensaje_correccion, "Fecha_correccion", estado_correccion
            ) VALUES (%s, %s, %s, %s, %s)
            """
            pg_cur.executemany(insert_query, correcciones_to_insert)
            pg_conn.commit()
            print(f"  Se insertaron {len(correcciones_to_insert)} registros.")

        print("--- Migración de tbl_correcciones_jurados completada. ---")

    except Exception as e:
        print(f"  ERROR CRÍTICO en migración de correcciones: {e}")
        if pg_conn: pg_conn.rollback()
    finally:
        if pg_conn: pg_conn.close()
        if mysql_conn: mysql_conn.close()

if __name__ == '__main__':
    migrate_tbl_correcciones_jurados()
