import csv
import os
import psycopg2
import mysql.connector
from db_connections import get_mysql_pilar3_connection, get_postgres_connection

def migrate_tbl_tramites():
    """
    Migra los datos de tesTramites (MySQL) a tbl_tramites (PostgreSQL),
    aplicando reglas de remapeo para el campo 'id_etapa'.
    """
    mysql_conn = None
    postgres_conn = None
    
    try:
        mysql_conn = get_mysql_pilar3_connection()
        postgres_conn = get_postgres_connection()
        if not all([mysql_conn, postgres_conn]):
            raise Exception("No se pudieron establecer las conexiones.")

        mysql_cursor = mysql_conn.cursor(dictionary=True)
        postgres_cursor = postgres_conn.cursor()

        # Mapa de remapeo para el campo Estado -> id_etapa
        # La tabla de origen tiene estados de 0 a 14.
        etapa_map = {
            0: 1,   # Regla especial: Estado 0 se convierte en etapa 1
            1: 1,
            2: 2,   # Sin equivalente explícito, se mapea a sí mismo
            3: 2,
            4: 4,   # Sin equivalente explícito, se mapea a sí mismo
            5: 3,
            6: 4,
            7: 5,
            8: 8,   # Sin equivalente explícito, se mapea a sí mismo
            9: 10,
            10: 11,
            11: 11,  # Sin equivalente explícito, se mapea a sí mismo
            12: 12,
            13: 13,
            14: 14   # Sin equivalente explícito, se mapea a sí mismo
        }

        mysql_cursor.execute("SELECT * FROM tesTramites")
        tramites_records = mysql_cursor.fetchall()
        print(f"Se encontraron {len(tramites_records)} trámites en MySQL.")

        tramites_to_insert = []
        tramites_no_mapeados = []

        for tramite in tramites_records:
            estado_antiguo = tramite.get('Estado')
            id_etapa_nuevo = etapa_map.get(estado_antiguo)

            if id_etapa_nuevo is not None:
                mapped_tramite = (
                    tramite.get('Id'),
                    tramite.get('Codigo'),
                    id_etapa_nuevo,
                    tramite.get('IdLinea'),
                    1, # id_modalidad (defecto)
                    1, # id_tipo_trabajo (defecto)
                    1, # id_denominacion (defecto)
                    tramite.get('FechRegProy'),
                    1  # estado_tramite (defecto)
                )
                tramites_to_insert.append(mapped_tramite)
            else:
                tramites_no_mapeados.append(tramite)

        # Insertar los registros mapeados
        if tramites_to_insert:
            print(f"Insertando {len(tramites_to_insert)} trámites mapeados en tbl_tramites...")
            postgres_cursor.execute("TRUNCATE TABLE public.tbl_tramites RESTART IDENTITY CASCADE;")
            
            insert_query = """
                INSERT INTO tbl_tramites (
                    id_antiguo, codigo_proyecto, id_etapa, id_sublinea_vri, id_modalidad,
                    id_tipo_trabajo, id_denominacion, fecha_registro, estado_tramite
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            postgres_cursor.executemany(insert_query, tramites_to_insert)
            postgres_conn.commit()
            print("Migración de tbl_tramites completada.")

        # Exportar los no mapeados a un CSV
        if tramites_no_mapeados:
            report_path = os.path.join(os.path.dirname(__file__), '..', 'tramites_no_mapeados.csv')
            print(f"\nExportando {len(tramites_no_mapeados)} trámites no mapeados a: {report_path}")
            
            with open(report_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=tramites_no_mapeados[0].keys())
                writer.writeheader()
                writer.writerows(tramites_no_mapeados)
            print("Reporte de trámites no mapeados creado.")

    except (Exception, psycopg2.Error, mysql.connector.Error) as e:
        print(f"Error durante la migración de tbl_tramites: {e}")
        if postgres_conn:
            postgres_conn.rollback()
        raise e
    finally:
        if mysql_conn:
            mysql_conn.close()
        if postgres_conn:
            postgres_conn.close()
