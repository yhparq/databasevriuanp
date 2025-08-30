
import psycopg2
from db_connections import get_postgres_connection, get_mysql_pilar3_connection

def get_tipo_evento(motivo):
    motivo_lower = motivo.lower()
    if 'intento' in motivo_lower: return 1
    if 'sorteo' in motivo_lower: return 4
    return 7

def migrate_tbl_asignacion_jurado():
    """
    Migra el historial de cambios de jurado desde tesJuCambios, usando
    mapeo de id_antiguo para docentes y trámites.
    """
    print("--- Iniciando migración de tbl_asignacion_jurado (Lógica ID Antiguo) ---")
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

        pg_cur.execute("SELECT id FROM tbl_usuarios WHERE correo = 'sistema@vriunap.pe'")
        system_user_id = pg_cur.fetchone()[0]

        # 2. Leer datos de origen, ordenados para la lógica de iteración
        mysql_cur.execute("SELECT * FROM tesJuCambios ORDER BY IdTramite, Fecha ASC")
        source_data = mysql_cur.fetchall()

        # 3. Procesar y preparar datos para la inserción
        data_to_insert = []
        iteracion_tracker = {}
        unmapped_jurados = 0
        jurado_fields = ['IdJurado1', 'IdJurado2', 'IdJurado3', 'IdJurado4']

        for row in source_data:
            id_tramite_antiguo = row['IdTramite']
            new_tramite_id = tramites_map.get(id_tramite_antiguo)
            if not new_tramite_id:
                continue

            current_iteracion = iteracion_tracker.get(id_tramite_antiguo, 0) + 1
            iteracion_tracker[id_tramite_antiguo] = current_iteracion
            id_tipo_evento = get_tipo_evento(row['Motivo'])

            for i, field in enumerate(jurado_fields):
                old_docente_id = row[field]
                if old_docente_id and old_docente_id > 0:
                    new_docente_id = docente_map.get(old_docente_id)
                    if new_docente_id:
                        data_to_insert.append((
                            new_tramite_id, 5, i + 1, current_iteracion, id_tipo_evento,
                            new_docente_id, system_user_id, row['Fecha'], 0
                        ))
                    else:
                        unmapped_jurados += 1
        
        print(f"  Se prepararon {len(data_to_insert)} registros para insertar.")
        if unmapped_jurados > 0:
            print(f"  ADVERTENCIA: Se ignoraron {unmapped_jurados} jurados sin mapeo.")

        # 4. Insertar los datos
        if data_to_insert:
            insert_query = """
            INSERT INTO tbl_asignacion_jurado (
                tramite_id, id_etapa, id_orden, iteracion, id_tipo_evento,
                docente_id, id_usuario_asignador, fecha_evento, estado
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            pg_cur.executemany(insert_query, data_to_insert)
            pg_conn.commit()
            print(f"  Se insertaron {len(data_to_insert)} registros.")

        print("--- Migración de tbl_asignacion_jurado completada ---")

    except Exception as e:
        print(f"  ERROR CRÍTICO en migración de asignación de jurados: {e}")
        if pg_conn: pg_conn.rollback()
    finally:
        if pg_conn: pg_conn.close()
        if mysql_conn: mysql_conn.close()

if __name__ == '__main__':
    migrate_tbl_asignacion_jurado()
