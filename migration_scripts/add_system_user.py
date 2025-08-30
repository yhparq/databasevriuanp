
import psycopg2
from db_connections import get_postgres_connection

def add_system_user():
    """
    Agrega un usuario 'sistema' a la tabla tbl_usuarios si no existe.
    """
    print("--- Agregando usuario 'sistema' ---")
    conn = None
    try:
        conn = get_postgres_connection()
        if conn is None:
            raise Exception("No se pudo conectar a PostgreSQL.")
        
        cur = conn.cursor()

        # Verificar si el usuario ya existe
        cur.execute("SELECT id FROM tbl_usuarios WHERE correo = %s", ('sistema@vriunap.pe',))
        if cur.fetchone():
            print("El usuario 'sistema' ya existe en la base de datos.")
            return

        # Insertar el nuevo usuario
        insert_query = """
        INSERT INTO tbl_usuarios (nombres, apellidos, num_doc_identidad, correo, estado)
        VALUES (%s, %s, %s, %s, %s)
        """
        user_data = ('Sistema', 'Usuario del Sistema', '99999999', 'sistema@vriunap.pe', 1)
        
        cur.execute(insert_query, user_data)
        conn.commit()
        
        print("Usuario 'sistema' agregado exitosamente.")

    except Exception as e:
        print(f"  ERROR CRÍTICO al agregar el usuario 'sistema': {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    add_system_user()
