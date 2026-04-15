# Situación Financiera de los Hogares

Pipeline automatizado para el informe de Situación Financiera de
los Hogares: descarga 59 series temporales de la API del Banco de
España, aplica transformaciones (normalización, agregaciones,
ratios de dudosidad, tasas interanuales, sumas móviles), genera
17 gráficos y 3 tablas con estilo tesorotools, y ensambla un
informe Word con los resultados.

## Instalación rápida

```bash
uv venv <ruta-del-entorno-fuera-de-onedrive>
uv sync --active
```

## Uso

```bash
# Pipeline completo: descarga + transformación + gráficos + tablas + Word
uv run --active python generar_hogares.py

# Solo descarga y exporta datos (sin gráficos ni Word)
uv run --active python generar_hogares.py --download-only

# Re-descarga completa (borra datos locales)
uv run --active python generar_hogares.py --full

# Los flags se pueden combinar
uv run --active python generar_hogares.py --full --lookback 2
```

### Salidas

| Fichero | Descripción |
|---|---|
| `output/store_bde.feather` | Almacén incremental con códigos BdE crudos (no tocar) |
| `output/datos_hogares.xlsx` | Excel con datos transformados (IDs canónicos) |
| `output/datos_hogares.feather` | Feather con columna `date` (para consumo genérico) |
| `output/datos_transformados.feather` | Feather con `DatetimeIndex` (para gráficos y tablas) |
| `output/charts/*.png` | 17 gráficos PNG con estilo tesorotools |
| `output/tables/*.feather` | 3 tablas formateadas como feather |
| `output/informe_hogares.docx` | Informe Word con gráficos, tablas y marcadores |

## Documentación

- [Arquitectura](docs/architecture.md) — diseño del proyecto,
  decisiones técnicas, diagramas.
- [Diccionario de datos](docs/data_dictionary.md) — referencia de
  las 59 series, unidades, fuentes.
- [Guía de desarrollo](docs/development_guide.md) — recetas para
  añadir series, gráficos y tablas, ejecutar tests.
- [Referencia de API](docs/api_reference.md) — módulos, clases y
  funciones públicas.
- [Contribuir](docs/contributing.md) — herramientas de calidad
  de código, pre-commit hooks, convenciones.