# Guía de desarrollo

Recetas prácticas para las operaciones más comunes del proyecto.

## Añadir una nueva serie

1. Buscar el código de la serie en la web del Banco de España
   (https://www.bde.es/webbe/es/estadisticas/) o en el catálogo
   del paquete R `bdeseries`.

2. Añadir la entrada en `series/instruments.yaml`:

   ```yaml
   MI_NUEVA_SERIE:
     display_name: "descripción legible"
     providers:
       bde:
         code: "CODIGO_BDE_AQUI"
   ```

3. Ejecutar el pipeline. Si el feather ya existe, el store detectará
   que `MI_NUEVA_SERIE` no está en los datos almacenados y
   descargará su histórico completo automáticamente:

   ```bash
   uv run --active python generar_hogares.py --download-only
   ```

4. Verificar que la nueva columna aparece en el Excel y feather de
   salida.

No hace falta tocar código Python para añadir series: el catálogo
YAML es la única fuente de verdad.

## Eliminar una serie

Borrar la entrada de `instruments.yaml`. En la siguiente ejecución,
la serie seguirá en el feather existente (el store no borra columnas
viejas), pero ya no se actualizará. Si se quiere un feather limpio,
ejecutar con `--full`:

```bash
uv run --active python generar_hogares.py --download-only --full
```

## Forzar una re-descarga completa

```bash
uv run --active python generar_hogares.py --download-only --full
```

Esto borra el feather existente y descarga todo el histórico desde
cero. Útil si se sospecha que los datos locales están corruptos o
si se ha cambiado el catálogo significativamente.

## Cambiar la ventana de lookback

Por defecto, el store re-verifica los últimos 4 trimestres (12
meses) de datos para capturar revisiones estadísticas. Para
cambiar esto:

```bash
# Re-verificar solo 2 trimestres (6 meses)
uv run --active python generar_hogares.py --lookback 2

# No re-verificar nada, solo añadir datos nuevos
uv run --active python generar_hogares.py --lookback 0

# Re-verificar 8 trimestres (2 años)
uv run --active python generar_hogares.py --lookback 8
```

## Ejecutar los tests

```bash
uv run --active python -m unittest discover tests
```

Para ejecutar un test concreto:

```bash
uv run --active python -m unittest tests.test_bde_provider
```

## Comprobar el estilo del código

```bash
uv run --active ruff check .
uv run --active ruff format --check .
```

Para corregir automáticamente:

```bash
uv run --active ruff check --fix .
uv run --active ruff format .
```

## Añadir un gráfico nuevo

1. Añadir la entrada en `series/charts.yaml`:

   ```yaml
   mi_nuevo_grafico:
     type: line          # line | stacked_area | stacked_bar
     title: "Titulo del grafico"
     subtitle: "Unidades"
     source: "Fuente: ..."
     series:
       SERIE_1: "Etiqueta 1"
       SERIE_2: "Etiqueta 2"
     format: { units: "%", decimals: 1 }
     start_date: "2015-01-01"
     baseline: true       # mostrar linea en y=0
     legend: { ncol: 2 }  # columnas de la leyenda
     scale: 100           # multiplicar datos (ej. ratio -> %)
   ```

2. Para que el gráfico aparezca en el informe Word, añadir
   una entrada `!image` en la sección correspondiente de
   `series/template.yaml`. La clave debe ser el nombre del
   PNG (ej. `mi_nuevo_grafico.png: !image`).

3. Ejecutar el pipeline para verificar:

   ```bash
   uv run --active python generar_hogares.py
   ```

## Modificar un gráfico existente

Los parámetros de cada gráfico están en `series/charts.yaml`.
No hace falta tocar código Python para cambiar series, escalas,
fechas o formato de ejes.

## Leer los datos desde otro script

El feather de salida se puede leer desde cualquier script Python
o notebook sin necesidad de importar nada del proyecto:

```python
import pandas as pd

# datos_hogares.feather tiene 'date' como columna
df = pd.read_feather("output/datos_hogares.feather")
df = df.set_index("date")
print(df["STOCK_VIVIENDA"].tail())

# datos_transformados.feather ya tiene DatetimeIndex
df = pd.read_feather("output/datos_transformados.feather")
print(df.loc["2025":, "TEDR_FIJO"])
```

## Estructura del DataFrame de salida

El feather y el Excel contienen un DataFrame transformado con:

- **Índice**: `DatetimeIndex` llamado `"date"`, sin timezone,
  normalizado a medianoche. Rango desde 1994 (para las series
  trimestrales más antiguas) hasta el último dato disponible.

- **Columnas originales** (59): IDs canónicos como
  `STOCK_VIVIENDA`, `TEDR_CONSUMO`, `CF_RIQUEZA_NETA`. Los
  valores están en las unidades originales de la API del BdE
  (K_EUR, M_EUR, BN_EUR o PCT según la serie).

- **Columnas derivadas** (~90): generadas por el pipeline de
  transformaciones. Se distinguen por sus sufijos:
  - `_BN`: valor normalizado a miles de millones de euros.
  - `_YOY`: tasa de variación interanual (decimal, 0.04 = 4%).
  - `_4Q`: suma móvil de 4 trimestres (incl. componentes).
  - `_DELTA_Q`, `_DELTA_4Q`: cambios de stock por periodo.
  - `CF_PCT_*`: composición de activos como fracción del total.
  - `CF_REVAL_*`: residuos de revalorización.
  - `DUDOSIDAD_*`: ratio dudosos/crédito total (decimal).
  - `HIPOT_PCT_*`: proporción de hipotecas por tipo.
  - `DEUDA_PIB_VAR_*`: descomposición del ratio deuda/PIB.
  - `AMORTIZACIONES_VIVIENDA`, `RENEGOCIACIONES_ACUM`.

- **Valores**: `float64`. Las celdas donde no hay dato (por
  frecuencias mixtas o por falta de historia para calcular un
  YOY) contienen `NaN`.