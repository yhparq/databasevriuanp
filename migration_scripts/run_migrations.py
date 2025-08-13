import psycopg2
from db_connections import get_postgres_connection

# Importar todas las funciones de migración
from tbl_usuarios import migrate_tbl_usuarios
from dic_areas_ocde import migrate_dic_areas_ocde
from dic_subareas_ocde import migrate_dic_subareas_ocde
from dic_facultades import migrate_dic_facultades
from dic_categoria import migrate_dic_categoria
from dic_lineas_universidad import migrate_dic_lineas_universidad
from dic_carreras import migrate_dic_carreras
from dic_especialidades import migrate_dic_especialidades
from tbl_docentes import migrate_tbl_docentes # Nuevo

def clean_destination_tables():
    """
    Vacía las tablas de destino en PostgreSQL antes de la migración.
    """
    print("--- Limpiando las tablas de destino en PostgreSQL ---")
    conn = None
    try:
        conn = get_postgres_connection()
        if conn is None:
            raise Exception("No se pudo conectar a PostgreSQL para la limpieza.")
        
        cur = conn.cursor()
        
        tables_to_clean = [
            "tbl_docentes", # Nuevo
            "tbl_usuarios",
            "dic_especialidades",
            "dic_carreras",
            "dic_facultades",
            "dic_subareas_ocde",
            "dic_areas_ocde",
            "dic_categoria",
            "dic_lineas_universidad"
        ]
        
        for table in reversed(tables_to_clean):
            print(f"  Limpiando tabla: {table}...")
            cur.execute(f"TRUNCATE TABLE public.{table} RESTART IDENTITY CASCADE;")

        conn.commit()
        print("--- Limpieza completada exitosamente ---")
        return True

    except Exception as e:
        print(f"  ERROR CRÍTICO durante la limpieza de tablas: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()

def run_all_migrations():
    """
    Ejecuta todas las migraciones en un orden específico para satisfacer
    las restricciones de clave foránea.
    """
    print("\n--- Iniciando el proceso de migración completo ---")
    
    migrations = [
        ("tbl_usuarios", migrate_tbl_usuarios),
        ("dic_areas_ocde", migrate_dic_areas_ocde),
        ("dic_subareas_ocde", migrate_dic_subareas_ocde),
        ("dic_facultades", migrate_dic_facultades),
        ("dic_categoria", migrate_dic_categoria),
        ("dic_lineas_universidad", migrate_dic_lineas_universidad),
        ("dic_carreras", migrate_dic_carreras),
        ("dic_especialidades", migrate_dic_especialidades),
        ("tbl_docentes", migrate_tbl_docentes), # Nuevo
    ]

    for name, migrate_function in migrations:
        print(f"--- Ejecutando migración para: {name} ---")
        try:
            migrate_function()
        except Exception as e:
            print(f"  La migración de '{name}' falló. Se detiene el proceso.")
            return

    print("\n--- Todas las migraciones se han completado exitosamente ---")

if __name__ == '__main__':
    if clean_destination_tables():
        run_all_migrations()
    else:
        print("\nLa migración no se ejecutó debido a un error durante la limpieza.")
