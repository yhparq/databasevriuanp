
import csv
import psycopg2
import os

# --- Configuración de la Base de Datos ---
DB_NAME = "postgres"  # <-- CAMBIA ESTO SI TU BASE DE DATOS TIENE OTRO NOMBRE
DB_USER = "admin"
DB_PASS = "admin123"
DB_HOST = "localhost"
DB_PORT = "5432"
TABLE_NAME = "tbl_usuarios"

# --- Configuración del Archivo CSV ---
CSV_FILENAME = "xddd.csv"

def clean_value(value, column_name):
    """Convierte valores vacíos o placeholders a None y maneja tipos de datos."""
    if not value or value.strip().startswith('VACIO_') or value.strip() == '*':
        return None
    
    # Para la columna 'estado', asegurarse de que sea un entero
    if column_name == 'estado':
        try:
            return int(value)
        except (ValueError, TypeError):
            return None # O un valor por defecto, ej: 0
            
    # Para la columna 'fecha_nacimiento', asegurarse de que sea una fecha válida o None
    if column_name == 'fecha_nacimiento':
        # Intenta analizar el formato AAAA-MM-DD
        try:
            # Simple validación de formato
            if len(value.split('-')) == 3:
                return value
            else:
                return None
        except:
            return None

    return value.strip()

def create_table(cur):
    """Crea la tabla en la base de datos si no existe."""
    create_table_command = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id INTEGER PRIMARY KEY,
        nombres TEXT,
        apellidos TEXT,
        tipo_doc_identidad TEXT,
        num_doc_identidad TEXT,
        correo TEXT,
        correo_google TEXT,
        telefono TEXT,
        pais TEXT,
        direccion TEXT,
        sexo CHAR(1),
        fecha_nacimiento DATE,
        contrasenia TEXT,
        ruta_foto TEXT,
        estado INTEGER,
        roles TEXT
    );
    """
    try:
        print(f"Creando tabla '{TABLE_NAME}' si no existe...")
        cur.execute(create_table_command)
        print("Tabla verificada/creada exitosamente.")
    except psycopg2.Error as e:
        print(f"Error al crear la tabla: {e}")
        raise

def migrate_data():
    """Conecta a la BD, lee el CSV e inserta los datos."""
    conn = None
    try:
        # Establecer conexión con la base de datos
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()
        print("Conexión a PostgreSQL exitosa.")

        # 1. Crear la tabla
        create_table(cur)

        # 2. Leer el archivo CSV e insertar los datos
        print(f"Leyendo datos de '{CSV_FILENAME}' para la migración...")
        with open(CSV_FILENAME, mode='r', encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)
            header = next(csv_reader)  # Omitir la cabecera

            insert_sql = f"""
            INSERT INTO {TABLE_NAME} ({', '.join(header)})
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """

            count = 0
            for row in csv_reader:
                # Limpiar cada valor de la fila antes de insertar
                cleaned_row = [clean_value(val, header[i]) for i, val in enumerate(row)]
                
                try:
                    cur.execute(insert_sql, cleaned_row)
                    count += 1
                    if count % 1000 == 0:
                        print(f"{count} filas insertadas...")
                except psycopg2.Error as e:
                    print(f"Error al insertar la fila: {row}")
                    print(f"Error de base de datos: {e}")
                    conn.rollback() # Revertir la transacción actual por si acaso
                    
        # Confirmar los cambios en la base de datos
        conn.commit()
        print(f"¡Migración completada! Se han insertado {count} filas en la tabla '{TABLE_NAME}'.")

    except psycopg2.OperationalError as e:
        print(f"Error de conexión: No se pudo conectar a la base de datos '{DB_NAME}'.")
        print(f"Detalles: {e}")
    except FileNotFoundError:
        print(f"Error: El archivo '{CSV_FILENAME}' no se encontró en el directorio '{os.getcwd()}'.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Conexión a PostgreSQL cerrada.")

if __name__ == "__main__":
    migrate_data()
