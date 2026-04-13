# Contribuir al proyecto

Esta guía explica las herramientas de calidad de código que usa el
proyecto, cómo funcionan, y qué hacer cuando bloquean un commit.

## Herramientas de calidad

El proyecto usa tres herramientas que se ejecutan automáticamente
antes de cada `git commit` mediante pre-commit hooks:

**ruff** es un linter y formateador de código Python. Comprueba
dos cosas: que el código no tiene errores de estilo ni patrones
problemáticos (lint), y que está formateado de forma consistente
(format). La configuración está en `pyproject.toml` bajo
`[tool.ruff]`, donde se fija el límite de línea a 80 caracteres.

**pyright** es un verificador de tipos estático. Analiza el código
sin ejecutarlo y comprueba que los tipos son correctos: que no se
pasa un `str` donde se espera un `int`, que no se llama a un método
que no existe, etc. Está configurado en modo **strict**, que es el
nivel más exigente. La configuración está en `pyproject.toml` bajo
`[tool.pyright]`.

**pre-commit** es el framework que orquesta las dos anteriores.
Intercepta cada `git commit` y ejecuta ruff y pyright sobre los
ficheros modificados. Si alguno falla, el commit se bloquea y hay
que corregir los errores antes de volver a intentarlo. La
configuración está en `.pre-commit-config.yaml`.

## Instalación inicial

Las herramientas se instalan como dependencias de desarrollo. Si
vienes de una instalación limpia:

```bash
uv sync --active --group dev
```

Después hay que activar los hooks de pre-commit (solo la primera
vez):

```bash
uv run --active pre-commit install
```

Esto registra un script en `.git/hooks/pre-commit` que se ejecutará
antes de cada commit.

## Flujo de trabajo diario

### Antes de hacer commit

No hace falta hacer nada especial. Los hooks se disparan solos al
hacer `git commit`. Si todo está bien, el commit se crea
normalmente. Si hay errores, el commit se bloquea y se muestran los
mensajes de error en la terminal.

### Si ruff bloquea el commit

Ruff puede bloquear por dos motivos: errores de lint (código
problemático) o errores de formato (estilo inconsistente).

Para errores de lint:

```bash
uv run --active ruff check .
```

Esto muestra los errores con su ubicación y explicación. Muchos se
corrigen automáticamente con:

```bash
uv run --active ruff check --fix .
```

Para errores de formato:

```bash
uv run --active ruff format .
```

Esto reformatea todos los ficheros. Después se pueden añadir los
cambios al commit.

### Si pyright bloquea el commit

Pyright bloquea cuando encuentra errores de tipos. Para ver los
errores:

```bash
uv run --active pyright
```

Cada error incluye el fichero, la línea, y una explicación del
problema. Los errores más comunes y cómo resolverlos:

**`reportMissingTypeArgument`** — un tipo genérico sin especificar
sus parámetros. Por ejemplo `dict` en lugar de `dict[str, int]`.
Hay que añadir los tipos concretos.

**`reportUnknownVariableType`** — pyright no puede inferir el tipo
de una variable. Suele pasar con el retorno de funciones de
librerías externas. Se resuelve añadiendo una anotación de tipo
explícita o usando `cast()`.

**`reportArgumentType`** — se está pasando un argumento de un tipo
incorrecto a una función. Hay que comprobar qué tipo espera la
función y ajustar el código.

### Ejecutar los checks manualmente

Para comprobar que todo está bien sin hacer commit:

```bash
uv run --active ruff check .
uv run --active ruff format --check .
uv run --active pyright
```

El `--check` de `ruff format` solo comprueba sin modificar nada.

## Convenciones del código

### Idioma

Los ficheros `.py` están íntegramente en inglés: docstrings,
comentarios, nombres de variables y funciones. Las únicas
excepciones son los string literals que representan campos de APIs
externas (como `"idioma"` o `"rango"` de la API del BdE) y los
nombres de ficheros de salida destinados a usuarios hispanohablantes
(como `datos_hogares.xlsx`).

Los ficheros `.md` de documentación están en español con ortografía
correcta (tildes, eñes).

El `instruments.yaml` usa `display_name` en español porque es
orientado a presentación.

### Tipos

El proyecto usa pyright en modo strict. Todo el código debe tener
tipos explícitos y completos. Algunas reglas prácticas:

Evitar `Any`. Si una función externa devuelve `Any` (como
`resp.json()` o `yaml.safe_load()`), usar `cast()` para darle un
tipo concreto lo antes posible.

Usar `TypedDict` para estructuras de datos con claves conocidas,
como las respuestas de APIs o las entradas del catálogo YAML.

Las funciones privadas (prefijo `_`) también necesitan tipos en
modo strict.

### Formato

Líneas de máximo 80 caracteres. Ruff se encarga de formatear
automáticamente con `ruff format .`. No hace falta preocuparse
por el estilo manualmente: escribir el código, ejecutar el
formateador, y listo.

## Cómo funciona por dentro

### Los pre-commit hooks

Cuando se hace `git commit`, git ejecuta el script
`.git/hooks/pre-commit` que instaló pre-commit. Este script lee
`.pre-commit-config.yaml` y ejecuta los hooks definidos:

1. **ruff (lint)**: ejecuta `ruff check --fix` sobre los ficheros
   staged. Si hay errores que no se pueden corregir automáticamente,
   falla.
2. **ruff (format)**: ejecuta `ruff format` sobre los ficheros
   staged. Si algún fichero cambia, falla (porque el staged ya no
   coincide con lo que hay en disco).
3. **pyright**: ejecuta `pyright` sobre todo el proyecto (no solo
   los ficheros staged, porque un cambio en un fichero puede causar
   errores de tipos en otro).

Si cualquiera de los tres falla, el commit no se crea. Hay que
corregir, hacer `git add` de los cambios, y repetir el
`git commit`.

### La configuración de pyright

En `pyproject.toml`:

```toml
[tool.pyright]
pythonVersion = "3.14"
extraPaths = ["."]
typeCheckingMode = "strict"
reportUnknownMemberType = "warning"
reportUnknownVariableType = "warning"
reportAttributeAccessIssue = "warning"
```

El modo strict es el más exigente de pyright. Las tres reglas
rebajadas a warning son por limitaciones de los stubs de pandas,
no por nuestro código. Si pandas mejora sus stubs en el futuro,
estas excepciones se pueden eliminar.

El `extraPaths = ["."]` permite a pyright encontrar el paquete
`src` desde la raíz del proyecto.

### La configuración de ruff

```toml
[tool.ruff]
line-length = 80
```

Ruff usa por defecto un conjunto amplio de reglas de lint y un
formateador compatible con Black. El límite de línea a 80 es más
restrictivo que el valor por defecto de 88, pero mejora la
legibilidad en pantallas partidas y en diffs de git.
