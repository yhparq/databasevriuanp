
import psycopg2
import mysql.connector
from db_connections import get_mysql_absmain_connection, get_postgres_connection

def migrate_docentes_with_placeholders():
    """
    Migra datos de tblDocentes a tbl_usuarios, generando correos electrónicos
    de marcador de posición para los registros que no tienen uno.
    """
    mysql_conn = None
    postgres_conn = None
    
    try:
        mysql_conn = get_mysql_absmain_connection()
        postgres_conn = get_postgres_connection()

        if not all([mysql_conn, postgres_conn]):
            raise Exception("No se pudieron establecer las conexiones a la base de datos.")

        mysql_cursor = mysql_conn.cursor(dictionary=True)
        postgres_cursor = postgres_conn.cursor()

        print("Leyendo registros de tblDocentes desde MySQL...")
        mysql_cursor.execute("SELECT * FROM tblDocentes")
        docentes_records = mysql_cursor.fetchall()
        print(f"Se encontraron {len(docentes_records)} registros de docentes.")

        usuarios_to_insert = []
        emails_used = set()
        
        for record in docentes_records:
            email = record.get('Correo')
            dni = record.get('DNI')

            # Si el correo está vacío o es nulo, generar un marcador de posición
            if not email or not email.strip():
                if dni and dni.strip():
                    email = f"dni.{dni.strip()}@unap.edu.pe"
                else:
                    # Si no hay DNI, no podemos generar un correo único, así que omitimos
                    print(f"AVISO: Omitiendo docente Id {record.get('Id')} por no tener DNI ni correo.")
                    continue
            
            # Verificar si el correo ya ha sido usado en este lote
            if email in emails_used:
                print(f"AVISO: Correo duplicado '{email}' encontrado para docente Id {record.get('Id')}. Omitiendo registro.")
                continue
            
            emails_used.add(email)

            # Mapear los campos
            mapped_record = (
                record.get('Nombres'),
                record.get('Apellidos'),
                'DNI', # tipo_doc_identidad
                dni,
                email,
                None,  # correo_google
                record.get('NroCelular'),
                None,  # pais
                record.get('Direccion'),
                record.get('Sexo'),
                record.get('FechaNac'),
                record.get('Clave'),
                None,  # ruta_foto
                record.get('Activo')
            )
            usuarios_to_insert.append(mapped_record)

        # Insertar los registros en tbl_usuarios
        if usuarios_to_insert:
            print(f"Insertando {len(usuarios_to_insert)} registros de docentes en tbl_usuarios...")
            
            insert_query = """
                INSERT INTO tbl_usuarios (
                    nombres, apellidos, tipo_doc_identidad, num_doc_identidad, 
                    correo, correo_google, telefono, pais, direccion, sexo, 
                    fecha_nacimiento, contrasenia, ruta_foto, estado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            postgres_cursor.executemany(insert_query, usuarios_to_insert)
            postgres_conn.commit()
            print("Migración de docentes a usuarios completada exitosamente.")
        else:
            print("No se encontraron registros de docentes válidos para migrar.")

    except (Exception, psycopg2.Error, mysql.connector.Error) as e:
        print(f"Error durante la migración de docentes: {e}")
        if postgres_conn:
            postgres_conn.rollback()
        raise e
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()
