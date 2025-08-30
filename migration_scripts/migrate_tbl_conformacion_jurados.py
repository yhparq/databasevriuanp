
import psycopg2
from db_connections import get_postgres_connection, get_mysql_pilar3_connection

def migrate_tbl_conformacion_jurados_combined():
    """
    Altera la tabla tbl_conformacion_jurados para permitir nulos y luego
    migra la conformación INICIAL de jurados desde tesTramites (MySQL).
    """
    print("--- Iniciando migración combinada de tbl_conformacion_jurados (Alter + Insert) ---")
    pg_conn = None
    mysql_conn = None
    try:
        pg_conn = get_postgres_connection()
        mysql_conn = get_mysql_pilar3_connection()

        if pg_conn is None or mysql_conn is None:
            raise Exception("No se pudo conectar a una de las bases de datos.")

        pg_cur = pg_conn.cursor()
        mysql_cur = mysql_conn.cursor(dictionary=True)

        # --- PASO 1: Alterar la tabla ---
        print("  Alterando la tabla tbl_conformacion_jurados para aceptar NULLs...")
        pg_cur.execute("ALTER TABLE tbl_conformacion_jurados ALTER COLUMN id_asignacion DROP NOT NULL;")
        pg_cur.execute("ALTER TABLE tbl_conformacion_jurados ALTER COLUMN fecha_asignacion DROP NOT NULL;")
        print("  Alteración completada.")

        # --- PASO 2: Proceder con la migración ---
        # Obtener el ID del usuario 'sistema'
        pg_cur.execute("SELECT id FROM tbl_usuarios WHERE correo = 'sistema@vriunap.pe'")
        system_user_id = pg_cur.fetchone()[0]

        # Obtener mapeo de trámites
        pg_cur.execute("SELECT id, id_antiguo FROM tbl_tramites WHERE id_antiguo IS NOT NULL")
        tramites_map = {row[1]: row[0] for row in pg_cur.fetchall()}

        # Obtener las fechas de la primera asignación
        mysql_cur.execute("""
            SELECT IdTramite, MAX(Fecha) as fecha_asignacion
            FROM logTramites
            WHERE Accion = 'Proyecto enviado a Revisión'
            GROUP BY IdTramite
        """)
        fechas_asignacion = {row['IdTramite']: row['fecha_asignacion'] for row in mysql_cur.fetchall()}

        # Leer datos de la conformación inicial desde tesTramites
        mysql_cur.execute("SELECT Id, IdJurado1, IdJurado2, IdJurado3, IdJurado4 FROM tesTramites")
        source_tramites = mysql_cur.fetchall()
        print(f"  Se encontraron {len(source_tramites)} trámites en tesTramites.")

        # Preparar los datos para la inserción
        jurados_to_insert = []
        jurado_fields = ['IdJurado1', 'IdJurado2', 'IdJurado3', 'IdJurado4']

        for tramite in source_tramites:
            id_antiguo = tramite['Id']
            new_tramite_id = tramites_map.get(id_antiguo)
            fecha_asignacion = fechas_asignacion.get(id_antiguo)

            if not new_tramite_id:
                continue

            for i, field in enumerate(jurado_fields):
                id_docente = tramite[field]
                if id_docente and id_docente > 0:
                    jurados_to_insert.append((
                        new_tramite_id, id_docente, i + 1, 5, system_user_id,
                        None, fecha_asignacion, 1
                    ))
        
        print(f"  Se prepararon {len(jurados_to_insert)} registros para insertar.")

        # Limpiar la tabla de destino antes de insertar
        print("  Limpiando la tabla tbl_conformacion_jurados...")
        pg_cur.execute("TRUNCATE TABLE public.tbl_conformacion_jurados RESTART IDENTITY CASCADE;")

        # Insertar los datos en PostgreSQL
        if jurados_to_insert:
            insert_query = """
            INSERT INTO tbl_conformacion_jurados (
                id_tramite, id_docente, id_orden, id_etapa, 
                id_usuario_asignador, id_asignacion, fecha_asignacion, estado_cj
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            pg_cur.executemany(insert_query, jurados_to_insert)
            print(f"  Se insertaron {len(jurados_to_insert)} registros.")

        pg_conn.commit()
        print("--- Migración de tbl_conformacion_jurados (Inicial) completada. ---")

    except Exception as e:
        print(f"  ERROR CRÍTICO en migración de conformación de jurados: {e}")
        if pg_conn:
            pg_conn.rollback()
    finally:
        if pg_conn:
            pg_conn.close()
        if mysql_conn:
            mysql_conn.close()

if __name__ == '__main__':
    migrate_tbl_conformacion_jurados_combined()
