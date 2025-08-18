import psycopg2
from db_connections import get_postgres_connection

# Importar las funciones de migración
from migrate_docentes_with_placeholders import migrate_docentes_with_placeholders
from migrate_tesistas_deduplicated import migrate_tesistas_deduplicated
from migrate_estructura_academica import migrate_estructura_academica
from populate_tbl_docentes import populate_tbl_docentes
from populate_tbl_tesistas import populate_tbl_tesistas
from dic_areas_ocde import migrate_dic_areas_ocde
from dic_subareas_ocde import migrate_dic_subareas_ocde
from dic_facultades import migrate_dic_facultades
from dic_categoria import migrate_dic_categoria
from dic_lineas_universidad import migrate_dic_lineas_universidad
from dic_carreras import migrate_dic_carreras
from dic_especialidades import migrate_dic_especialidades
from migrate_dic_sedes import migrate_dic_sedes
from migrate_dic_disciplinas import migrate_dic_disciplinas
from migrate_tbl_sublineas_vri import migrate_tbl_sublineas_vri
from migrate_dic_denominaciones import migrate_dic_denominaciones
from migrate_dic_etapas import migrate_dic_etapas
from migrate_dic_modalidades import migrate_dic_modalidades
from migrate_dic_tipo_trabajos import migrate_dic_tipo_trabajos
from migrate_tbl_tramites import migrate_tbl_tramites

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
            "tbl_tramites",
            "tbl_sublineas_vri",
            "dic_disciplinas",
            "tbl_tesistas",
            "tbl_docentes",
            "tbl_usuarios",
            "tbl_estructura_academica",
            "dic_sedes",
            "dic_especialidades",
            "dic_carreras",
            "dic_facultades",
            "dic_subareas_ocde",
            "dic_areas_ocde",
            "dic_categoria",
            "dic_lineas_universidad",
            "dic_denominaciones",
            "dic_etapas",
            "dic_modalidades",
            "dic_tipo_trabajos"
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
    Ejecuta todas las migraciones en un orden específico.
    """
    print("\n--- Iniciando el proceso de migración completo ---")
    
    migrations = [
        ("migrate_docentes_with_placeholders", migrate_docentes_with_placeholders),
        ("migrate_tesistas_deduplicated", migrate_tesistas_deduplicated),
        ("dic_areas_ocde", migrate_dic_areas_ocde),
        ("dic_subareas_ocde", migrate_dic_subareas_ocde),
        ("dic_facultades", migrate_dic_facultades),
        ("dic_categoria", migrate_dic_categoria),
        ("dic_lineas_universidad", migrate_dic_lineas_universidad),
        ("dic_carreras", migrate_dic_carreras),
        ("dic_especialidades", migrate_dic_especialidades),
        ("migrate_dic_sedes", migrate_dic_sedes),
        ("migrate_dic_disciplinas", migrate_dic_disciplinas),
        ("migrate_estructura_academica", migrate_estructura_academica),
        ("migrate_dic_denominaciones", migrate_dic_denominaciones),
        ("migrate_dic_etapas", migrate_dic_etapas),
        ("migrate_dic_modalidades", migrate_dic_modalidades),
        ("migrate_dic_tipo_trabajos", migrate_dic_tipo_trabajos),
        ("populate_tbl_docentes", populate_tbl_docentes),
        ("populate_tbl_tesistas", populate_tbl_tesistas),
        ("migrate_tbl_sublineas_vri", migrate_tbl_sublineas_vri),
        ("migrate_tbl_tramites", migrate_tbl_tramites),
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
