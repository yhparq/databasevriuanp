import psycopg2
import sys
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# --- Configuración de Conexión a Supabase ---
# Usando la URL de la base de datos desde las variables de entorno.
DATABASE_URL = os.getenv("DATABASE_URL")

def test_connection():
    """
    Intenta conectarse a la base de datos de Supabase y reporta el resultado.
    """
    if not DATABASE_URL:
        print("\n❌ ERROR DE CONFIGURACIÓN:")
        print("La variable de entorno DATABASE_URL no está definida.")
        print("Asegúrate de que tu archivo .env contiene la línea: DATABASE_URL=...")
        sys.exit(1)

    conn = None
    try:
        print("--- Intentando conectar a Supabase usando la URL... ---")
        
        conn = psycopg2.connect(DATABASE_URL)
        
        print("\n✅ ¡CONEXIÓN EXITOSA!")
        print("La conexión a la base de datos de Supabase se ha establecido correctamente.")
        
        cur = conn.cursor()
        cur.execute('SELECT version();')
        db_version = cur.fetchone()
        print(f"Versión de PostgreSQL: {db_version[0]}")
        cur.close()

    except psycopg2.OperationalError as e:
        print("\n❌ ERROR DE CONEXIÓN:")
        print("No se pudo establecer la conexión con la base de datos.")
        print("Verifica tu string de conexión, la red, el firewall y la configuración de DNS/IPv6.")
        print(f"\nDetalles del error: {e}")
        sys.exit(1) # Salir con código de error

    except psycopg2.Error as e:
        print(f"\n❌ OCURRIÓ UN ERROR INESPERADO DE PSYCOPG2: {e}")
        sys.exit(1) # Salir con código de error

    finally:
        if conn is not None:
            conn.close()
            print("\n--- Conexión cerrada. ---")

if __name__ == '__main__':
    test_connection()
