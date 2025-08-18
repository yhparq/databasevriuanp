from db_connections import get_mysql_absmain_connection, get_postgres_connection

def migrate_tbl_sublineas_vri():
    """
    Migra datos de tblLineas (MySQL) a tbl_sublineas_vri (PostgreSQL).
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

        mysql_cursor.execute("SELECT Id, id_lineaV, Nombre, IdDiscip, IdCarrera, fecha, Estado FROM tblLineas")
        records = mysql_cursor.fetchall()
        
        records_to_insert = []
        for rec in records:
            records_to_insert.append((
                rec['Id'],
                rec['id_lineaV'],
                rec['Nombre'],
                rec['IdDiscip'],
                rec['IdCarrera'],
                rec['fecha'],
                rec['fecha'], # Usando la misma fecha para fecha_modificacion
                rec['Estado']
            ))

        postgres_cursor.execute("TRUNCATE TABLE public.tbl_sublineas_vri RESTART IDENTITY CASCADE;")
        
        postgres_cursor.executemany(
            """INSERT INTO tbl_sublineas_vri (
                id, id_linea_universidad, nombre, id_disciplina, id_carrera, 
                fecha_registro, fecha_modificacion, estado_sublinea_vri
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            records_to_insert
        )
        
        postgres_conn.commit()
        print(f"Migrados {len(records_to_insert)} registros a tbl_sublineas_vri.")

    except Exception as e:
        print(f"Error en la migración de tbl_sublineas_vri: {e}")
        raise e
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()
