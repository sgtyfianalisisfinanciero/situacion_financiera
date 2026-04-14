# Referencia de la API interna

Documentación de los módulos, clases y funciones públicas del
proyecto. Para el detalle completo de cada función (parámetros,
tipos, excepciones), consultar los docstrings en el código fuente.

## `generar_hogares` (punto de entrada)

Script principal. Se ejecuta desde la línea de comandos.

### Funciones

**`load_instruments(path)`** — Lee el catálogo YAML y devuelve un
diccionario plano `{id_canónico: metadata}`.

**`build_code_map(instruments)`** — Extrae el mapeo
`{id_canónico: código_bde}` del catálogo. Lanza `RuntimeError` si
no hay ninguna serie con provider `bde`.

**`export_excel(df, path)`** — Exporta un DataFrame a un fichero
Excel. Crea el directorio padre si no existe.

**`main()`** — Parsea argumentos CLI y ejecuta el pipeline:
descarga, transformación, generación de gráficos y Word.

### Argumentos CLI

| Flag | Descripción |
|---|---|
| `--download-only` | Solo descarga y exporta datos (sin gráficos ni Word) |
| `--full` | Fuerza re-descarga completa (borra el feather existente) |
| `--lookback Q` | Número de trimestres a re-verificar para revisiones (defecto: 4) |

## `src.providers.base`

### `DataProvider` (ABC)

Clase base abstracta para todos los providers de datos. Define la
interfaz que cualquier fuente de datos debe implementar.

**`fetch(codes, start=None, end=None)`** — Descarga series para un
rango de fechas. Devuelve un DataFrame con `DatetimeIndex` y una
columna por serie. Si `start` y `end` son `None`, el provider decide
el rango.

**`is_available()`** — Comprueba si el servicio externo responde.
Devuelve `bool`.

## `src.providers.bde`

### `BdeProvider(DataProvider)`

Implementación concreta para el Banco de España.

**Constructor**: `BdeProvider(language="es", timeout=30)`

- `language` — Idioma para metadatos de la API (se envía como
  parámetro `idioma` en la petición HTTP).
- `timeout` — Segundos máximos por petición HTTP.

**`fetch(codes, start=None, end=None)`** — Descarga series del BdE.
Divide en lotes de 10, traduce `start` al parámetro `rango` de la
API (solo acepta `"30M"`, `"60M"` o `"MAX"`), y recorta el resultado
si se pidió un `start` concreto. El parámetro `end` se acepta por
compatibilidad con la interfaz pero es ignorado (la API siempre
devuelve hasta el último dato).

**`is_available()`** — Hace un ping ligero al endpoint `favoritas`
de la API.

### Constantes del módulo

| Constante | Valor | Descripción |
|---|---|---|
| `DEFAULT_TIMEOUT` | 30 | Timeout HTTP por defecto (segundos) |
| `BATCH_SIZE` | 10 | Máximo de series por petición a la API |

## `src.store`

### `SeriesStore`

Persistencia local en feather con actualización incremental.

**Constructor**: `SeriesStore(path)`

- `path` — Ruta al fichero feather.

**`exists()`** — `True` si el fichero feather existe en disco.

**`load()`** — Carga el DataFrame almacenado. Devuelve un DataFrame
vacío si el fichero no existe.

**`save(df)`** — Guarda un DataFrame en el feather. Crea el
directorio padre si no existe.

**`update(provider, codes, lookback_quarters=4)`** — Actualización
incremental. En la primera ejecución, descarga todo. En las
siguientes, calcula la fecha de inicio como `último_dato - lookback`,
descarga solo desde ahí, fusiona con lo existente (los datos frescos
prevalecen sobre los antiguos para capturar revisiones), y guarda.
Si hay series nuevas en `codes` que no están en el feather existente,
descarga su histórico completo.

### Constantes del módulo

| Constante | Valor | Descripción |
|---|---|---|
| `DEFAULT_LOOKBACK_QUARTERS` | 4 | Trimestres de lookback por defecto |

## `src.pipeline.engine`

Motor genérico de transformaciones, replicado del proyecto
diariospython.

### `TransformationRule` (dataclass, frozen)

Agrupa la definición de una transformación derivada.

- `output_name` — nombre de la columna de salida.
- `dependencies` — lista de columnas que deben existir para que
  la regla se ejecute.
- `compute` — función `(DataFrame) -> Series` que calcula el
  resultado.

### `apply_transformations(df, rules)`

Aplica una lista de reglas secuencialmente. Cada regla puede
depender de columnas creadas por reglas anteriores. Si una
dependencia falta, la regla se salta con un warning. No modifica
el DataFrame de entrada (trabaja sobre una copia).

## `src.pipeline.rules`

Reglas de transformación específicas de hogares. Cada función
pública devuelve una lista de `TransformationRule`.

**`normalize_rules(catalog)`** — Genera reglas de conversión de
unidades a partir del campo `unit` del catálogo. Produce columnas
con sufijo `_BN` (miles de millones de euros).

**`aggregation_rules()`** — Totales derivados (flujos totales,
otros activos + préstamos).

**`composition_rules()`** — Ratios de cada categoría de activo
sobre el total.

**`growth_rate_rules()`** — Tasas de variación interanual. Sufijo
`_YOY`.

**`rolling_rules()`** — Sumas móviles de 4 trimestres. Sufijo
`_4Q`.

**`all_rules(catalog)`** — Todas las reglas anteriores en el orden
correcto (normalización, agregaciones, composición, tasas, sumas).

## `src.charts`

Driver de generación de gráficos. Lee `series/charts.yaml` y
produce PNGs en `output/charts/`.

**`generate_charts(config_path, data_path, out_dir)`** — Genera
todos los gráficos definidos en el YAML. Despacha cada uno al
artist adecuado según su tipo (`line`, `stacked_area`,
`stacked_bar`). Devuelve la lista de IDs generados. Los gráficos
que fallan se registran como warning y no interrumpen el resto.

## `src.artists.stacked`

Artists locales para gráficos que tesorotools aún no soporta.
Siguen la misma interfaz que `LinePlot` (constructor + `plot()`).

### `StackedAreaPlot`

Gráfico de áreas apiladas. Usado para composición de activos
financieros.

**Constructor**: `StackedAreaPlot(out_path, data, series, *,
scale=1, start_date=None, end_date=None, baseline=False,
format=None, legend=None)`

**`plot()`** — Genera y guarda el PNG. Devuelve el `Axes`.

### `StackedBarPlot`

Gráfico de barras apiladas con soporte para valores negativos.
Usado para variación neta de activos/pasivos por componente.

**Constructor**: `StackedBarPlot(out_path, data, series, *,
scale=1, start_date=None, end_date=None, baseline=True,
format=None, legend=None)`

**`plot()`** — Genera y guarda el PNG. Devuelve el `Axes`.

## `src.report`

Generación del documento Word usando `tesorotools.render`.

**`generate_report(charts_dir, config_path, output_path)`** —
Construye el informe Word. Descubre qué PNGs existen en
`charts_dir`, lee títulos y subtítulos de `charts.yaml`, y
ensambla el documento con `Report`/`Section`/`Images` de
tesorotools. Los gráficos faltantes se omiten sin error.
