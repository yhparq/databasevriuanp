from db_connections import get_mysql_absmain_connection, get_postgres_connection

def migrate_dic_carreras():
    """
    Migra datos de dicCarreras (MySQL) a dic_carreras (PostgreSQL),
    mapeando solo las columnas necesarias y estableciendo valores por defecto.
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

        # 1. Mapeo correcto: Se seleccionan solo las 3 columnas necesarias de MySQL.
        # 2. Valor por defecto: Se añade el valor '1' para 'estado_carrera'.
        mysql_cursor.execute("SELECT Id, IdFacultad, Nombre, 1 FROM dicCarreras")
        
        records = mysql_cursor.fetchall()
        
        # 3. La sentencia INSERT ahora incluye las 4 columnas de destino requeridas.
        postgres_cursor.executemany(
            "INSERT INTO dic_carreras (id, id_facultad, nombre, estado_carrera) VALUES (%s, %s, %s, %s)",
            records
        )
        
        postgres_conn.commit()
        print(f"Migrados {len(records)} registros a dic_carreras.")

    except Exception as e:
        print(f"Error en la migración de dic_carreras: {e}")
        # 4. Re-lanzar la excepción para detener el script principal.
        raise e
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()