
from db_connections import get_mysql_absmain_connection, get_postgres_connection

def migrate_dic_areas_ocde():
    """
    Migra datos de ocdeAreas (MySQL) a dic_areas_ocde (PostgreSQL).
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

        mysql_cursor.execute("SELECT Id, Nombre, 1 FROM ocdeAreas")
        
        records = mysql_cursor.fetchall()
        
        postgres_cursor.executemany(
            "INSERT INTO dic_areas_ocde (id, nombre, estado_area) VALUES (%s, %s, %s)",
            records
        )
        
        postgres_conn.commit()
        print(f"Migrados {len(records)} registros a dic_areas_ocde.")

    except Exception as e:
        print(f"Error en la migración de dic_areas_ocde: {e}")
        raise e
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()