# Situación Financiera de los Hogares

Descarga automatizada de las 53 series temporales que alimentan el
informe de Situación Financiera de los Hogares. Los datos se obtienen
de la API pública del Banco de España y se exportan a Excel y Feather.

## Instalación rápida

```bash
uv venv <ruta-del-entorno-fuera-de-onedrive>
uv sync --active
```

## Uso

```bash
uv run --active python generar_hogares.py
uv run --active python generar_hogares.py --download-only
uv run --active python generar_hogares.py --full
```

## Documentación

- [Arquitectura](docs/architecture.md) — diseño del proyecto,
  decisiones técnicas, diagramas.
- [Diccionario de datos](docs/data_dictionary.md) — referencia de
  las 53 series, unidades, fuentes.
- [Guía de desarrollo](docs/development_guide.md) — recetas para
  añadir series, ejecutar tests, leer los datos.
- [Referencia de API](docs/api_reference.md) — módulos, clases y
  funciones públicas.
- [Contribuir](docs/contributing.md) — herramientas de calidad
  de código, pre-commit hooks, convenciones.
