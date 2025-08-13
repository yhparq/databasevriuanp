import csv
import os
from db_connections import get_postgres_connection

def migrate_tbl_usuarios():
    """
    Migra los datos desde un archivo CSV a la tabla tbl_usuarios en PostgreSQL.
    """
    print("Iniciando la migración de datos desde xddd.csv a tbl_usuarios...")

    postgres_conn = None
    try:
        # 1. Obtener conexión a PostgreSQL
        postgres_conn = get_postgres_connection()
        if postgres_conn is None:
            print("Error: No se pudo establecer la conexión con PostgreSQL. Abortando.")
            return

        cur = postgres_conn.cursor()

        # 2. Definir la ruta al archivo CSV (está en el directorio padre)
        csv_file_path = os.path.join(os.path.dirname(__file__), '..', 'xddd.csv')

        # 3. Abrir y leer el archivo CSV
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            header = next(reader) # Omitir la cabecera

            # Columnas en la tabla de destino (excluyendo 'id')
            table_columns = [
                "nombres", "apellidos", "tipo_doc_identidad", "num_doc_identidad", 
                "correo", "correo_google", "telefono", "pais", "direccion", 
                "sexo", "fecha_nacimiento", "contrasenia", "ruta_foto", "estado"
            ]
            
            insert_sql = f"INSERT INTO tbl_usuarios ({', '.join(table_columns)}) VALUES ({', '.join(['%s'] * len(table_columns))})"

            # 4. Iterar sobre las filas del CSV e insertar en la base de datos
            count = 0
            for row in reader:
                # Tomamos los datos desde la segunda columna hasta la penúltima
                # para que coincida con `table_columns`
                data_to_insert = row[1:15] 
                

                # Convertir strings vacíos o la palabra "NULL" a None para que se inserten como NULL en la BD
                processed_data = [None if val == '' or val.upper() == 'NULL' else val for val in data_to_insert]

                cur.execute(insert_sql, processed_data)
                count += 1

        # 5. Confirmar la transacción
        postgres_conn.commit()
        print(f"Migración completada. Se han insertado {count} registros en tbl_usuarios.")

    except FileNotFoundError:
        print(f"Error: No se encontró el archivo en la ruta: {csv_file_path}")
    except Exception as e:
        print(f"Ha ocurrido un error durante la migración: {e}")
        if postgres_conn:
            postgres_conn.rollback() # Revertir cambios en caso de error
    finally:
        # 6. Cerrar la conexión
        if postgres_conn:
            cur.close()
            postgres_conn.close()
            print("Conexión a PostgreSQL cerrada.")

if __name__ == '__main__':
    migrate_tbl_usuarios()