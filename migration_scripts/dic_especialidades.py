from db_connections import get_mysql_absmain_connection, get_postgres_connection

def migrate_dic_especialidades():
    """
y
    Migra datos de dicEspecialis (MySQL) a dic_especialidades (PostgreSQL)
    con una lógica de mapeo y filtrado específica.
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

        # 1. Consulta SQL con toda la lógica de filtrado requerida.
        #    - Mapea Denominacion a la columna de nombre.
        #    - Filtra donde Denominacion no es nulo ni vacío.
        #    - Filtra donde IdCarrera no es 0.
        #    - Añade el estado por defecto 0.
        sql_query = """
            SELECT Id, IdCarrera, Denominacion, 0 
            FROM dicEspecialis 
            WHERE Denominacion IS NOT NULL 
              AND Denominacion != '' 
              AND IdCarrera != 0
        """
        mysql_cursor.execute(sql_query)
        
        records = mysql_cursor.fetchall()
        
        # 2. La sentencia INSERT mapea los datos a las columnas correctas en PostgreSQL.
        postgres_cursor.executemany(
            "INSERT INTO dic_especialidades (id, id_carrera, nombre, estado_especialidad) VALUES (%s, %s, %s, %s)",
            records
        )
        
        postgres_conn.commit()
        print(f"Migrados {len(records)} registros a dic_especialidades.")

    except Exception as e:
        print(f"Error en la migración de dic_especialidades: {e}")
        raise e
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()