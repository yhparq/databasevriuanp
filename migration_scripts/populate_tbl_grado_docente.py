
import psycopg2
from db_connections import get_postgres_connection

def populate_tbl_grado_docente():
    """
    Populates the tbl_grado_docente table based on data from tbl_estudios
    and tbl_docente_categoria_historial.
    """
    conn = None
    try:
        print("Connecting to the PostgreSQL database...")
        conn = get_postgres_connection()
        cur = conn.cursor()

        print("Truncating tbl_grado_docente table...")
        cur.execute("TRUNCATE TABLE public.tbl_grado_docente RESTART IDENTITY;")

        print("Populating tbl_grado_docente with processed data...")
        sql_query = """
        INSERT INTO public.tbl_grado_docente (
            id_docente,
            grado_academico,
            categoria_descripcion,
            antiguedad_categoria,
            estado_tbl_grado_docente
        )
        WITH
        -- CTE to get the highest academic degree for each user
        GradosRankeados AS (
            SELECT
                e.id_usuario,
                ga.nombre AS grado_academico,
                -- Assign a rank to the academic degree to select the highest one
                ROW_NUMBER() OVER(PARTITION BY e.id_usuario ORDER BY
                    CASE
                        WHEN ga.nombre ILIKE '%doctor%' THEN 1
                        WHEN ga.nombre ILIKE '%maestro%' OR ga.nombre ILIKE '%magister%' THEN 2
                        WHEN ga.nombre ILIKE '%título profesional%' OR ga.nombre ILIKE '%ingeniero%' OR ga.nombre ILIKE '%licenciado%' THEN 3
                        WHEN ga.nombre ILIKE '%bachiller%' THEN 4
                        ELSE 5
                    END
                ) as rn_grado
            FROM
                public.tbl_estudios e
            JOIN
                public.dic_grados_academicos ga ON e.id_grado_academico = ga.id
        ),
        -- CTE to get the most recent category for each docent
        CategoriasRecientes AS (
            SELECT
                dch.id_docente,
                dch.id_categoria,
                dch.fecha_resolucion,
                -- Assign a rank based on the resolution date to get the most recent one
                ROW_NUMBER() OVER(PARTITION BY dch.id_docente ORDER BY dch.fecha_resolucion DESC) as rn_categoria
            FROM
                public.tbl_docente_categoria_historial dch
            WHERE
                dch.fecha_resolucion IS NOT NULL
        ),
        -- CTE to get the list of docents that exist in tbl_estudios
        DocentesEnEstudios AS (
            SELECT DISTINCT
                d.id AS id_docente,
                d.id_usuario
            FROM
                public.tbl_docentes d
            INNER JOIN
                public.tbl_estudios e ON d.id_usuario = e.id_usuario
        )
        -- Final query that joins the information
        SELECT
            de.id_docente,
            gr.grado_academico,
            cat.nombre AS categoria_descripcion,
            cr.fecha_resolucion AS antiguedad_categoria,
            true AS estado_tbl_grado_docente
        FROM
            DocentesEnEstudios de
        LEFT JOIN
            GradosRankeados gr ON de.id_usuario = gr.id_usuario AND gr.rn_grado = 1 -- Only the highest degree
        LEFT JOIN
            CategoriasRecientes cr ON de.id_docente = cr.id_docente AND cr.rn_categoria = 1 -- Only the most recent category
        LEFT JOIN
            public.dic_categoria cat ON cr.id_categoria = cat.id;
        """
        cur.execute(sql_query)

        conn.commit()
        print("Successfully populated tbl_grado_docente.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        if conn is not None:
            conn.rollback()
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")

if __name__ == '__main__':
    populate_tbl_grado_docente()
