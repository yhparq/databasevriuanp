import psycopg2
from db_connections import get_postgres_connection

# Importar las funciones de migración
from migrate_docentes_with_placeholders import migrate_docentes_with_placeholders
from migrate_tesistas_deduplicated import migrate_tesistas_deduplicated
from migrate_estructura_academica import migrate_estructura_academica
from populate_tbl_docentes import populate_tbl_docentes
from populate_tbl_tesistas import populate_tbl_tesistas
from migrate_dic_areas_ocde import migrate_dic_areas_ocde
from migrate_dic_subareas_ocde import migrate_dic_subareas_ocde
from migrate_dic_facultades import migrate_dic_facultades
from migrate_dic_categoria import migrate_dic_categoria
from migrate_dic_lineas_universidad import migrate_dic_lineas_universidad
from migrate_dic_carreras import migrate_dic_carreras
from migrate_dic_especialidades import migrate_dic_especialidades
from migrate_dic_sedes import migrate_dic_sedes
from migrate_dic_disciplinas import migrate_dic_disciplinas
from migrate_tbl_sublineas_vri import migrate_tbl_sublineas_vri
from migrate_dic_denominaciones import migrate_dic_denominaciones
from migrate_dic_etapas import migrate_dic_etapas
from migrate_dic_modalidades import migrate_dic_modalidades
from migrate_dic_tipo_trabajos import migrate_dic_tipo_trabajos
from migrate_tbl_tramites import migrate_tbl_tramites
from migrate_dic_acciones import migrate_dic_acciones
from migrate_dic_servicios import migrate_dic_servicios
from migrate_dic_tipo_archivo import migrate_dic_tipo_archivo
from migrate_dic_visto_bueno import migrate_dic_visto_bueno
from migrate_dic_universidades import migrate_dic_universidades
from migrate_dic_nivel_admins import migrate_dic_nivel_admins
from migrate_dic_orden_jurado import migrate_dic_orden_jurado
from migrate_docente_categoria_historial import migrate_docente_categoria_historial
from migrate_dic_grados_academicos import migrate_dic_grados_academicos
from migrate_dic_obtencion_studios import migrate_dic_obtencion_studios
from migrate_tbl_estudios import migrate_tbl_estudios
from populate_tbl_grado_docente import populate_tbl_grado_docente
from add_system_user import add_system_user
from migrate_tbl_conformacion_jurados import migrate_tbl_conformacion_jurados_combined
from migrate_tbl_asignacion_jurado import migrate_tbl_asignacion_jurado
from migrate_tbl_correcciones_jurados import migrate_tbl_correcciones_jurados

from migrate_dic_tipoevento_jurado import migrate_dic_tipoevento_jurado

def prepare_destination_tables(conn):
    """
    Añade las columnas 'id_antiguo' necesarias para el mapeo.
    Esta función es idempotente.
    """
    print("--- Preparando tablas de destino (añadiendo columnas si es necesario) ---")
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE public.tbl_docentes ADD COLUMN IF NOT EXISTS id_antiguo INTEGER;")
            cur.execute("ALTER TABLE public.tbl_tesistas ADD COLUMN IF NOT EXISTS id_antiguo INTEGER;")
            conn.commit()
        print("--- Tablas preparadas exitosamente ---")
        return True
    except Exception as e:
        print(f"  ERROR CRÍTICO durante la preparación de tablas: {e}")
        conn.rollback()
        return False

def clean_destination_tables(conn):
    """
    Vacía las tablas de destino en PostgreSQL antes de la migración.
    """
    print("--- Limpiando las tablas de destino en PostgreSQL ---")
    try:
        with conn.cursor() as cur:
            tables_to_clean = [
                "tbl_correcciones_jurados", "tbl_asignacion_jurado", "tbl_conformacion_jurados",
                "tbl_grado_docente", "tbl_estudios", "tbl_docente_categoria_historial",
                "tbl_tramites", "tbl_sublineas_vri", "dic_disciplinas",
                "tbl_tesistas", "tbl_docentes", "tbl_usuarios",
                "tbl_estructura_academica", "dic_sedes", "dic_especialidades",
                "dic_carreras", "dic_facultades", "dic_subareas_ocde",
                "dic_areas_ocde", "dic_categoria", "dic_lineas_universidad",
                "dic_denominaciones", "dic_etapas", "dic_modalidades",
                "dic_tipo_trabajos", "dic_universidades", "dic_nivel_admins",
                "dic_orden_jurado", "dic_tipoevento_jurado", "dic_grados_academicos",
                "dic_obtencion_studios"
            ]
            for table in reversed(tables_to_clean):
                print(f"  Limpiando tabla: {table}...")
                cur.execute(f"TRUNCATE TABLE public.{table} RESTART IDENTITY CASCADE;")
            conn.commit()
        print("--- Limpieza completada exitosamente ---")
        return True
    except Exception as e:
        print(f"  ERROR CRÍTICO durante la limpieza de tablas: {e}")
        conn.rollback()
        return False

def run_all_migrations():
    """
    Ejecuta todas las migraciones en un orden específico.
    """
    conn = None
    try:
        conn = get_postgres_connection()
        if conn is None:
            raise Exception("No se pudo conectar a PostgreSQL.")

        if not prepare_destination_tables(conn):
            raise Exception("Falló la preparación de las tablas.")

        if not clean_destination_tables(conn):
            raise Exception("Falló la limpieza de las tablas.")
        
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
            ("migrate_docente_categoria_historial", migrate_docente_categoria_historial),
            ("migrate_tbl_sublineas_vri", migrate_tbl_sublineas_vri),
            ("migrate_tbl_tramites", migrate_tbl_tramites),
            ("migrate_dic_acciones", migrate_dic_acciones),
            ("migrate_dic_servicios", migrate_dic_servicios),
            ("migrate_dic_tipo_archivo", migrate_dic_tipo_archivo),
            ("migrate_dic_visto_bueno", migrate_dic_visto_bueno),
            ("migrate_dic_universidades", migrate_dic_universidades),
            ("migrate_dic_nivel_admins", migrate_dic_nivel_admins),
            ("migrate_dic_orden_jurado", migrate_dic_orden_jurado),
            ("migrate_dic_tipoevento_jurado", migrate_dic_tipoevento_jurado),
            ("migrate_dic_grados_academicos", migrate_dic_grados_academicos),
            ("migrate_dic_obtencion_studios", migrate_dic_obtencion_studios),
            ("migrate_tbl_estudios", migrate_tbl_estudios),
            ("populate_tbl_grado_docente", populate_tbl_grado_docente),
            ("add_system_user", add_system_user),
            ("migrate_tbl_conformacion_jurados", migrate_tbl_conformacion_jurados_combined),
            ("migrate_tbl_asignacion_jurado", migrate_tbl_asignacion_jurado),
            ("migrate_tbl_correcciones_jurados", migrate_tbl_correcciones_jurados),
        ]

        for name, migrate_function in migrations:
            print(f"--- Ejecutando migración para: {name} ---")
            migrate_function()

        print("\n--- Todas las migraciones se han completado exitosamente ---")

    except Exception as e:
        print(f"\nLa migración no se ejecutó debido a un error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    run_all_migrations()
