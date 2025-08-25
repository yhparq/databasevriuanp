import psycopg2
from dotenv import load_dotenv
import os

# Load environment variables from .env
# Asegúrate de que el archivo .env esté en la raíz del proyecto
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

print("--- Intentando conectar a Supabase usando variables de entorno... ---")
print(f"Host: {HOST}")
print(f"Puerto: {PORT}")
print(f"Usuario: {USER}")

# Connect to the database
try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    print("\n✅ ¡CONEXIÓN EXITOSA!")
    
    # Create a cursor to execute SQL queries
    cursor = connection.cursor()
    
    # Example query
    cursor.execute("SELECT NOW();")
    result = cursor.fetchone()
    print("Hora actual del servidor de base de datos:", result)

    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("\n--- Conexión cerrada. ---")

except Exception as e:
    print(f"\n❌ ERROR DE CONEXIÓN: {e}")
    print("\nEsto confirma que el problema es de red, ya que las credenciales se cargaron correctamente pero no se pudo establecer la conexión.")
