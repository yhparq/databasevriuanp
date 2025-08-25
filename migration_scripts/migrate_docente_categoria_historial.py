
import psycopg2
import mysql.connector
from db_connections import get_mysql_absmain_connection, get_postgres_connection

def migrate_docente_categoria_historial():
    """
    Migra el historial de categorías de docentes desde la tabla tblDocentes de MySQL
    a la tabla tbl_docente_categoria_historial de PostgreSQL.

    La lógica se basa en los docentes que ya existen en la tabla tbl_docentes de PostgreSQL,
    asegurando la integridad de los datos.
    """
    postgres_conn = None
    mysql_conn = None
    
    try:
        # 1. Establecer conexiones a las bases de datos
        postgres_conn = get_postgres_connection()
        mysql_conn = get_mysql_absmain_connection()

        if not all([postgres_conn, mysql_conn]):
            raise Exception("No se pudieron establecer todas las conexiones necesarias.")

        pg_cursor = postgres_conn.cursor()
        mysql_cursor = mysql_conn.cursor(dictionary=True)

        # 2. Obtener la lista de IDs de docentes válidos desde PostgreSQL
        print("Paso 1: Obteniendo la lista de docentes válidos desde PostgreSQL...")
        pg_cursor.execute("SELECT id FROM public.tbl_docentes")
        # Usamos un set para una búsqueda mucho más rápida (O(1) en promedio)
        valid_docente_ids = {row[0] for row in pg_cursor.fetchall()}
        print(f"Se encontraron {len(valid_docente_ids)} docentes válidos en PostgreSQL.")

        # 3. Leer todos los registros de la tabla de docentes original en MySQL
        print("\nPaso 2: Leyendo todos los docentes desde MySQL...")
        mysql_cursor.execute("SELECT Id, IdCategoria, FechaAsc, ResolAsc, Activo FROM tblDocentes")
        all_mysql_docentes = mysql_cursor.fetchall()
        print(f"Se encontraron {len(all_mysql_docentes)} registros en la tabla de origen.")

        # 4. Cruzar, mapear y filtrar los datos
        print("\nPaso 3: Mapeando y filtrando los registros para el historial...")
        historial_to_insert = []
        
        for docente in all_mysql_docentes:
            docente_id = docente.get('Id')
            
            # Verificar si el docente de MySQL existe en la base de datos de PostgreSQL
            if docente_id in valid_docente_ids:
                # Mapear los campos según lo acordado
                mapped_record = (
                    docente_id,
                    docente.get('IdCategoria'),
                    docente.get('FechaAsc'),
                    docente.get('ResolAsc'),
                    bool(docente.get('Activo')) # Convertir a booleano (0=False, cualquier otro número=True)
                )
                historial_to_insert.append(mapped_record)
        
        print(f"Se prepararon {len(historial_to_insert)} registros de historial para insertar.")

        # 5. Limpiar la tabla de destino e insertar los nuevos datos
        if historial_to_insert:
            print("\nPaso 4: Limpiando la tabla de destino (tbl_docente_categoria_historial)...")
            pg_cursor.execute("TRUNCATE TABLE public.tbl_docente_categoria_historial RESTART IDENTITY;")
            
            print(f"Paso 5: Insertando {len(historial_to_insert)} registros en la tabla de destino...")
            insert_query = """
                INSERT INTO public.tbl_docente_categoria_historial 
                (id_docente, id_categoria, fecha_resolucion, resolucion, estado) 
                VALUES (%s, %s, %s, %s, %s)
            """
            pg_cursor.executemany(insert_query, historial_to_insert)
            
            # Confirmar la transacción
            postgres_conn.commit()
            print("¡Migración del historial de categorías completada con éxito!")
        else:
            print("\nNo se encontraron registros de historial para migrar.")

    except (Exception, psycopg2.Error, mysql.connector.Error) as e:
        print(f"\nERROR: Ocurrió un problema durante la migración: {e}")
        if postgres_conn:
            postgres_conn.rollback() # Revertir cambios en caso de error
    finally:
        # 6. Cerrar conexiones
        print("\nCerrando conexiones a las bases de datos.")
        if postgres_conn:
            postgres_conn.close()
        if mysql_conn:
            mysql_conn.close()

if __name__ == "__main__":
    migrate_docente_categoria_historial()
