from db_connections import get_mysql_absmain_connection, get_postgres_connection

def migrate_dic_disciplinas():
    """
    Migra datos de ocdeDisciplinas (MySQL) a dic_disciplinas (PostgreSQL),
    estableciendo el estado por defecto en 1.
    """
    mysql_conn = None
    postgres_conn = None
    try:
        mysql_conn = get_mysql_absmain_connection()
        postgres_conn = get_postgres_connection()
        if not all([mysql_conn, postgres_conn]):
            raise Exception("No se pudieron establecer las conexiones.")

        mysql_cursor = mysql_conn.cursor(dictionary=True)
        postgres_cursor = postgres_conn.cursor()

        mysql_cursor.execute("SELECT Id, IdSubArea, Nombre FROM ocdeDisciplinas")
        records = mysql_cursor.fetchall()
        
        records_to_insert = [(rec['Id'], rec['IdSubArea'], rec['Nombre'], 1) for rec in records]

        postgres_cursor.execute("TRUNCATE TABLE public.dic_disciplinas RESTART IDENTITY CASCADE;")
        
        postgres_cursor.executemany(
            "INSERT INTO dic_disciplinas (id, id_subarea, nombre, estado_disciplina) VALUES (%s, %s, %s, %s)",
            records_to_insert
        )
        
        postgres_conn.commit()
        print(f"Migrados {len(records_to_insert)} registros a dic_disciplinas.")

    except Exception as e:
        print(f"Error en la migración de dic_disciplinas: {e}")
        raise e
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()
