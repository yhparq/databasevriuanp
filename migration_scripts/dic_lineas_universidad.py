from db_connections import get_mysql_absmain_connection, get_postgres_connection

def migrate_dic_lineas_universidad():
    """
    Migra datos de dicLineasVRI (MySQL) a dic_lineas_universidad (PostgreSQL),
    mapeando las columnas correctamente.
    """
    mysql_conn = None
    postgres_conn = None
    try:
        mysql_conn = get_mysql_absmain_connection()
        postgres_conn = get_postgres_connection()
        if not all([mysql_conn, postgres_conn]):
            raise Exception("No se pudieron establecer todas las conexiones de base de datos.")

        mysql_cursor = mysql_conn.cursor()
        postgres_cursor = postgres_conn.cursor()

        # 1. Mapeo correcto: Se seleccionan las 3 columnas de MySQL.
        mysql_cursor.execute("SELECT Id, Nombre, Estado FROM dicLineasVRI")
        
        records = mysql_cursor.fetchall()
        
        # 2. La sentencia INSERT ahora incluye la columna de estado.
        postgres_cursor.executemany(
            "INSERT INTO dic_lineas_universidad (id, nombre, estado_linea_universidad) VALUES (%s, %s, %s)",
            records
        )
        
        postgres_conn.commit()
        print(f"Migrados {len(records)} registros a dic_lineas_universidad.")

    except Exception as e:
        print(f"Error en la migración de dic_lineas_universidad: {e}")
        # 3. Re-lanzar la excepción para detener el script principal.
        raise e
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()