from db_connections import get_mysql_absmain_connection, get_postgres_connection

def migrate_dic_lineas_universidad():
    """
    Migra datos de dicLineasVRI (MySQL) a dic_lineas_universidad (PostgreSQL),
    estableciendo el estado por defecto en 1.
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

        mysql_cursor.execute("SELECT Id, Nombre FROM dicLineasVRI")
        records = mysql_cursor.fetchall()
        
        # Preparar los datos para la inserción, añadiendo el estado por defecto
        records_to_insert = [(rec['Id'], rec['Nombre'], 1) for rec in records]

        postgres_cursor.execute("TRUNCATE TABLE public.dic_lineas_universidad RESTART IDENTITY CASCADE;")
        
        postgres_cursor.executemany(
            "INSERT INTO dic_lineas_universidad (id, nombre, estado_linea_universidad) VALUES (%s, %s, %s)",
            records_to_insert
        )
        
        postgres_conn.commit()
        print(f"Migrados {len(records_to_insert)} registros a dic_lineas_universidad.")

    except Exception as e:
        print(f"Error en la migración de dic_lineas_universidad: {e}")
        raise e
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()
