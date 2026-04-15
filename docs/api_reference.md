# Referencia de la API interna

Documentación de los módulos, clases y funciones públicas del
proyecto. Para el detalle completo de cada función (parámetros,
tipos, excepciones), consultar los docstrings en el código fuente.

Los módulos compartidos (`DataProvider`, `BdeProvider`,
`TransformationRule`, `apply_transformations`, factories de reglas,
artists) están en tesorotools y se documentan allí. Aquí solo se
documenta el código local del proyecto.

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
descarga, transformación, gráficos, tablas y Word.

### Argumentos CLI

| Flag | Descripción |
|---|---|
| `--download-only` | Solo descarga y exporta datos (sin gráficos ni Word) |
| `--full` | Fuerza re-descarga completa (borra el feather existente) |
| `--lookback Q` | Número de trimestres a re-verificar para revisiones (defecto: 4) |

Los flags se pueden combinar: `--full --lookback 2`.

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

## `src.pipeline.rules`

Reglas de transformación específicas de hogares. Usa las factories
de `tesorotools.pipeline.rules` (`scale_rule`, `sum_rule`,
`ratio_rule`, `yoy_rule`, `rolling_sum_rule`). Cada función pública
devuelve una lista de `TransformationRule`.

**`normalize_rules(catalog)`** — Genera reglas de conversión de
unidades a partir del campo `unit` del catálogo. Produce columnas
con sufijo `_BN` (miles de millones de euros).

**`aggregation_rules()`** — Totales derivados (flujos totales,
otros activos + préstamos).

**`composition_rules()`** — Ratios de cada categoría de activo
sobre el total.

**`mortgage_type_rules()`** — Volúmenes y proporciones de hipotecas
por tipo de interés (variable, mixto, fijo). Agrega los plazos de
fijación en las tres categorías.

**`dudosidad_rules()`** — Ratios de dudosidad: dudosos / crédito
total para hogares, vivienda y consumo.

**`amortization_rules()`** — Amortizaciones hipotecarias
(stock_{t-1} - stock_t + flujos) y renegociaciones acumuladas
(cumsum).

**`stock_change_rules()`** — Cambios de stock (delta trimestral y
4Q) y residuos de revalorización (delta - transacciones) para
activos y pasivos. Necesario para los gráficos de descomposición
VNA/VNP.

**`deuda_pib_decomposition_rules()`** — Descompone la variación
intertrimestral del ratio deuda/PIB en contribución de la deuda
y contribución del PIB.

**`growth_rate_rules()`** — Tasas de variación interanual. Sufijo
`_YOY`. Incluye stocks de crédito (12 periodos) y cuentas
financieras (4 periodos).

**`rolling_rules()`** — Sumas móviles de 4 trimestres. Sufijo
`_4Q`. Incluye VNA, VNP, OFN y componentes de flujos de
activos/pasivos.

**`all_rules(catalog)`** — Todas las reglas anteriores en el orden
correcto: normalización → agregaciones → hipotecas tipo →
composición → dudosidad → amortizaciones → descomposición
deuda/PIB → tasas YOY → rolling 4Q → cambios de stock.

## `src.charts`

Driver de generación de gráficos. Lee `series/charts.yaml` y
produce PNGs en `output/charts/`.

**`generate_charts(config_path, data_path, out_dir)`** — Genera
todos los gráficos definidos en el YAML. Despacha cada uno al
artist de tesorotools adecuado según su tipo (`line`,
`stacked_area`, `stacked_bar`). Devuelve la lista de IDs
generados. Los gráficos que fallan se registran como warning.

## `src.tables`

Generación de tablas formateadas para el informe Word.

**`generate_tables(config_path, data_path, out_dir)`** — Lee
`series/tables.yaml`, extrae los últimos N periodos de cada
serie, formatea los números, y guarda feathers en `out_dir`.
Devuelve la lista de IDs generados.

## `src.report`

Generación del documento Word desde template YAML.

**`generate_report(template_path, output_path)`** — Carga
`series/template.yaml` con el `TemplateLoader` de tesorotools
(que interpreta los tags `!report`, `!section`, `!image`, etc.),
construye el `Report`, y lo renderiza a un fichero `.docx`.

## Ficheros de configuración YAML

### `series/instruments.yaml`

Catálogo de las 59 series. Cada entrada tiene un ID canónico,
un `display_name`, y un bloque `providers.bde` con `code` y
`unit`.

### `series/charts.yaml`

Definiciones de los 25 gráficos. Cada entrada tiene tipo, series,
formato, escala, fechas, leyenda.

### `series/tables.yaml`

Definiciones de las 3 tablas. Cada entrada tiene series, periodos,
frecuencia, decimales, y opcionalmente series YOY.

### `series/template.yaml`

Estructura del informe Word con custom tags de tesorotools.
Define secciones, imágenes (referenciando PNGs), tablas
(referenciando feathers), y textos. Las rutas `imports` son
relativas a la ubicación del template.
