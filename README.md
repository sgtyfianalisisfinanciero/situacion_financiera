# Situación Financiera de los Hogares

Pipeline automatizado para el informe de Situación Financiera de
los Hogares: descarga 53 series temporales de la API del Banco de
España, aplica transformaciones (normalización, agregaciones, tasas
interanuales, sumas móviles), genera 16 gráficos con estilo
tesorotools y ensambla un informe Word con los resultados.

## Instalación rápida

```bash
uv venv <ruta-del-entorno-fuera-de-onedrive>
uv sync --active
```

## Uso

```bash
# Pipeline completo: descarga + transformación + gráficos + Word
uv run --active python generar_hogares.py

# Solo descarga y exporta datos (sin gráficos ni Word)
uv run --active python generar_hogares.py --download-only

# Re-descarga completa (borra datos locales)
uv run --active python generar_hogares.py --full
```

### Salidas

| Fichero | Descripción |
|---|---|
| `output/datos_hogares.xlsx` | Excel con datos transformados |
| `output/datos_hogares.feather` | Feather con columna `date` (compatible con pandas genérico) |
| `output/datos_transformados.feather` | Feather con `DatetimeIndex` (usado por los gráficos) |
| `output/charts/*.png` | 16 gráficos PNG con estilo tesorotools |
| `output/informe_hogares.docx` | Informe Word con gráficos embebidos |

## Documentación

- [Arquitectura](docs/architecture.md) — diseño del proyecto,
  decisiones técnicas, diagramas.
- [Diccionario de datos](docs/data_dictionary.md) — referencia de
  las 53 series, unidades, fuentes.
- [Guía de desarrollo](docs/development_guide.md) — recetas para
  añadir series y gráficos, ejecutar tests, leer los datos.
- [Referencia de API](docs/api_reference.md) — módulos, clases y
  funciones públicas.
- [Contribuir](docs/contributing.md) — herramientas de calidad
  de código, pre-commit hooks, convenciones.
