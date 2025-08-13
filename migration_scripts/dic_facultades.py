from db_connections import get_mysql_absmain_connection, get_postgres_connection

def migrate_dic_facultades():
    """
    Migra datos de dicFacultades (MySQL) a dic_facultades (PostgreSQL),
    mapeando las columnas correctamente, estableciendo valores por defecto
    y excluyendo registros con IdArea = 0.
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

        # Se añade la condición WHERE para excluir facultades con IdArea = 0
        mysql_cursor.execute("SELECT Id, Nombre, Abrev, IdArea, 1 FROM dicFacultades WHERE IdArea != 0")
        
        records = mysql_cursor.fetchall()
        
        postgres_cursor.executemany(
            "INSERT INTO dic_facultades (id, nombre, abreviatura, id_area, estado_facultad) VALUES (%s, %s, %s, %s, %s)",
            records
        )
        
        postgres_conn.commit()
        print(f"Migrados {len(records)} registros a dic_facultades.")

    except Exception as e:
        print(f"Error en la migración de dic_facultades: {e}")
        raise e
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()