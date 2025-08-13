from db_connections import get_mysql_absmain_connection, get_postgres_connection

def migrate_dic_categoria():
    """
    Migra datos de dicCategorias (MySQL) a dic_categoria (PostgreSQL),
    mapeando las columnas correctamente y estableciendo valores por defecto.
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

        # 1. Mapeo correcto: Se seleccionan las 4 columnas de MySQL.
        # 2. Valor por defecto: Se añade el valor '1' para 'estado_categoria'.
        mysql_cursor.execute("SELECT Id, Tipo, Nombre, Abrev, 1 FROM dicCategorias")
        
        records = mysql_cursor.fetchall()
        
        # 3. La sentencia INSERT ahora incluye todas las columnas de destino requeridas.
        postgres_cursor.executemany(
            "INSERT INTO dic_categoria (id, tipo, nombre, abreviatura, estado_categoria) VALUES (%s, %s, %s, %s, %s)",
            records
        )
        
        postgres_conn.commit()
        print(f"Migrados {len(records)} registros a dic_categoria.")

    except Exception as e:
        print(f"Error en la migración de dic_categoria: {e}")
        # 4. Re-lanzar la excepción para detener el script principal.
        raise e
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()