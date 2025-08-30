
# Lógica de Migración para tbl_conformacion_jurados

Este archivo documenta las reglas para migrar los datos de jurados desde la tabla `tesTramites` (MySQL) a la tabla `tbl_conformacion_jurados` (PostgreSQL).

## Reglas de Mapeo

- **`id`**: Se genera automáticamente (serial).
- **`id_tramite`**: Se obtiene buscando en `tbl_tramites` el `id` que corresponde al `id_antiguo` (que es el `Id` de `tesTramites`).
- **`id_docente`**: Se crean 4 registros por cada trámite, uno para cada campo de jurado: `IdJurado1`, `IdJurado2`, `IdJurado3`, y `IdJurado4`.
- **`id_orden`**: Se asigna según el número del jurado (ej: 1 para `IdJurado1`, 2 para `IdJurado2`, etc.).
- **`id_etapa`**: Valor por defecto: `5`.
- **`estado_cj`**: Valor por defecto: `1`.

## Puntos Pendientes (Campos NOT NULL)

La siguiente lógica necesita ser definida, ya que estos campos no aceptan valores nulos:

- **`id_usuario_asignador`**: (Pendiente de definir)
- **`id_asignacion`**: (Pendiente de definir)
- **`fecha_asignacion`**: (Pendiente de definir)
