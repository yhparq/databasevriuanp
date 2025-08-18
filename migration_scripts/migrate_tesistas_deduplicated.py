import csv
import os
import psycopg2
import mysql.connector
from db_connections import get_mysql_pilar3_connection, get_postgres_connection

def migrate_tesistas_deduplicated():
    """
    Migra tesistas a usuarios, deduplicando por nombre completo.
    Maneja DNIs duplicados modificándolos y exporta reportes en CSV.
    """
    postgres_conn = None
    mysql_conn = None
    
    try:
        postgres_conn = get_postgres_connection()
        mysql_conn = get_mysql_pilar3_connection()

        if not all([postgres_conn, mysql_conn]):
            raise Exception("No se pudieron establecer todas las conexiones.")

        pg_cursor = postgres_conn.cursor()
        mysql_cursor = mysql_conn.cursor(dictionary=True)

        # 1. Obtener usuarios existentes (docentes)
        print("Leyendo usuarios existentes desde PostgreSQL...")
        pg_cursor.execute("SELECT lower(trim(nombres) || ' ' || trim(apellidos)), correo, num_doc_identidad FROM tbl_usuarios")
        existing_users_raw = pg_cursor.fetchall()
        
        existing_names = {row[0] for row in existing_users_raw}
        existing_emails = {row[1] for row in existing_users_raw if row[1]}
        existing_dnis = {row[2] for row in existing_users_raw if row[2]}
        print(f"Se encontraron {len(existing_names)} usuarios existentes.")

        # 2. Leer todos los registros de tblTesistas
        print("Leyendo registros de tblTesistas desde MySQL...")
        mysql_cursor.execute("SELECT * FROM tblTesistas")
        tesistas_records = mysql_cursor.fetchall()
        print(f"Se encontraron {len(tesistas_records)} registros de tesistas para procesar.")

        # 3. Procesar, deduplicar y manejar conflictos
        tesistas_to_insert = []
        skipped_by_name = []
        modified_dni_records = []
        
        processed_full_names = set()
        processed_dnis = set(existing_dnis)

        for record in tesistas_records:
            nombres = record.get('Nombres', '').strip()
            apellidos = record.get('Apellidos', '').strip()
            
            if not nombres or not apellidos:
                continue

            full_name_key = f"{nombres.lower()} {apellidos.lower()}"
            
            # Omitir si el nombre completo es duplicado
            if full_name_key in processed_full_names or full_name_key in existing_names:
                skipped_by_name.append({**record, 'motivo_omitido': 'Nombre completo duplicado'})
                continue
            
            processed_full_names.add(full_name_key)
            
            # Manejar DNI
            dni = record.get('DNI', '').strip()
            original_dni = dni
            if not dni:
                dni = f"dd_{full_name_key.replace(' ','_')}"[:12] # Usar nombre para DNI vacío
                record_with_mod_dni = {**record, 'dni_modificado': dni}
                modified_dni_records.append(record_with_mod_dni)
            elif dni in processed_dnis:
                dni = f"dd{original_dni}"
                record_with_mod_dni = {**record, 'dni_modificado': dni}
                modified_dni_records.append(record_with_mod_dni)
            
            processed_dnis.add(dni)

            # Manejar Correo
            email = record.get('Correo', '').strip()
            if not email or email in existing_emails:
                email = f"dni.{original_dni or full_name_key.replace(' ','_')}@unap.edu.pe"
            
            existing_emails.add(email)

            # Mapear para la inserción
            mapped_record = (
                nombres, apellidos, None, dni, email, None, record.get('NroCelular'),
                None, record.get('Direccion'), record.get('Sexo'), None,
                record.get('Clave'), None, 1
            )
            tesistas_to_insert.append(mapped_record)

        # 4. Insertar los nuevos registros
        if tesistas_to_insert:
            print(f"\nInsertando {len(tesistas_to_insert)} nuevos registros de tesistas en tbl_usuarios...")
            insert_query = """
                INSERT INTO tbl_usuarios (
                    nombres, apellidos, tipo_doc_identidad, num_doc_identidad, correo,
                    correo_google, telefono, pais, direccion, sexo, fecha_nacimiento,
                    contrasenia, ruta_foto, estado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            pg_cursor.executemany(insert_query, tesistas_to_insert)
            postgres_conn.commit()
            print("Migración de tesistas completada.")

        # 5. Exportar reportes CSV
        if skipped_by_name:
            report_path = os.path.join(os.path.dirname(__file__), '..', 'tesistas_duplicados_por_nombre.csv')
            print(f"\nExportando {len(skipped_by_name)} duplicados por nombre a: {report_path}")
            fieldnames = list(tesistas_records[0].keys()) + ['motivo_omitido']
            with open(report_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(skipped_by_name)
        
        if modified_dni_records:
            report_path = os.path.join(os.path.dirname(__file__), '..', 'tesistas_con_dni_modificado.csv')
            print(f"Exportando {len(modified_dni_records)} registros con DNI modificado a: {report_path}")
            fieldnames = list(tesistas_records[0].keys()) + ['dni_modificado']
            with open(report_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(modified_dni_records)

    except (Exception, psycopg2.Error, mysql.connector.Error) as e:
        print(f"Error durante la migración de tesistas: {e}")
        if postgres_conn:
            postgres_conn.rollback()
        raise e
    finally:
        if postgres_conn:
            postgres_conn.close()
        if mysql_conn:
            mysql_conn.close()
