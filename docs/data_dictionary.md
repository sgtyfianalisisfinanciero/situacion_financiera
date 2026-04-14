# Diccionario de datos

Referencia completa de las 53 series temporales que descarga el
proyecto. Cada serie tiene un ID canónico (usado en el código y como
nombre de columna en los DataFrames), un código de proveedor (el
código real que se envía a la API del BdE), y una descripción de qué
representa.

Los datos llegan en crudo desde la API. Las unidades indicadas aquí
son las originales del proveedor, antes de cualquier transformación.

## Stocks de crédito a hogares

Fuente original: BCE (Distributional Credit Flows). Redistribuidas
por el BdE. Frecuencia mensual. Unidades: euros (sin redondear).

Estas series representan el saldo vivo (stock) de préstamos
concedidos a hogares e instituciones sin fines de lucro al servicio
de los hogares (ISFLSH), desglosado por finalidad.

| ID canónico | Código BdE | Descripción |
|---|---|---|
| `STOCK_PRESTAMOS` | `DCF_M.N.ES.W0.S1M.S1.N.L.LE.FD.T._Z.XDC._T.M.V.N._T` | Total de préstamos a hogares |
| `STOCK_VIVIENDA` | `DCF_M.N.ES.W0.S1M.S1.N.L.LE.F4B.T._Z.XDC._T.M.V.N._T` | Préstamos para adquisición de vivienda |
| `STOCK_CONSUMO` | `DCF_M.N.ES.W0.S1M.S1.N.L.LE.F4A.T._Z.XDC._T.M.V.N._T` | Préstamos al consumo |
| `STOCK_OTROS` | `DCF_M.N.ES.W0.S1M.S1.N.L.LE.F4C.T._Z.XDC._T.M.V.N._T` | Préstamos para otros fines |

## Flujos brutos de crédito

Fuente: BdE (nuevas operaciones). Frecuencia mensual. Unidades:
millones de euros.

Representan el volumen de nuevas operaciones de crédito concedidas
en el mes por entidades de crédito y establecimientos financieros de
crédito (EFC) a hogares.

| ID canónico | Código BdE | Descripción |
|---|---|---|
| `FLUJOS_HIPOTECARIO_CON_RENEG` | `DN_1TI2TIE42` | Crédito hipotecario total (incluye renegociaciones) |
| `FLUJOS_HIPOTECARIO_SIN_RENEG` | `DN_1TI2TIE98` | Crédito hipotecario sin renegociaciones |
| `RENEGOCIACIONES` | `DN_1TI2TIE97` | Renegociaciones de crédito hipotecario |
| `FLUJOS_CONSUMO` | `DN_1TI2TIE47` | Crédito al consumo |
| `FLUJOS_OTROS` | `DN_1TI2TIE51` | Crédito para otros fines |
| `FLUJOS_TARJETAS` | `DN_1TI2TIE80` | Tarjetas de crédito con pago aplazado y tarjetas revolving |

### Desglose por plazo del crédito hipotecario

| ID canónico | Código BdE | Descripción |
|---|---|---|
| `FLUJOS_HIPOT_1A` | `DN_1TI2TIE43` | Hipotecas a plazo hasta 1 año |
| `FLUJOS_HIPOT_1_5A` | `DN_1TI2TIE44` | Hipotecas a plazo de 1 a 5 años |
| `FLUJOS_HIPOT_5_10A` | `DN_1TI2TIE45` | Hipotecas a plazo de 5 a 10 años |
| `FLUJOS_HIPOT_10A` | `DN_1TI2TIE46` | Hipotecas a plazo superior a 10 años |

## Tipos de interés

Fuente: BdE. Frecuencia mensual. Unidades: porcentaje (valor
crudo, ej. 2.77 = 2,77%).

Los tipos de interés se dividen en dos categorías: los del stock
(aplicados al saldo vivo de préstamos existentes) y los de nuevas
operaciones (TEDR: Tipo Efectivo Definición Restringida, aplicados
al flujo de nuevos préstamos en el mes).

### Tipos del stock

| ID canónico | Código BdE | Descripción |
|---|---|---|
| `TI_STOCK_VIVIENDA` | `DN_1TI1T0050` | Tipo medio del stock de préstamos para vivienda |
| `TI_STOCK_CONSUMO` | `DN_1TI1T0051` | Tipo medio del stock de préstamos para consumo y otros |

### TEDR de nuevas operaciones

| ID canónico | Código BdE | Descripción |
|---|---|---|
| `TEDR_VIVIENDA` | `DN_1TI2T0002` | TEDR de nuevas hipotecas para vivienda |
| `TEDR_RENEG_VIVIENDA` | `DN_1TI2T0136` | TEDR de hipotecas renegociadas |
| `TEDR_VIVIENDA_SIN_RENEG` | `DN_1TI2T0137` | TEDR de nuevas hipotecas sin renegociaciones |
| `TEDR_CONSUMO` | `DN_1TI2T0007` | TEDR de nuevos créditos al consumo |
| `TEDR_OTROS` | `DN_1TI2T0011` | TEDR de nuevos créditos para otros fines |

### TEDR por tipo de hipoteca

| ID canónico | Código BdE | Descripción |
|---|---|---|
| `TEDR_VARIABLE` | `DN_1TI2T0003` | TEDR de hipotecas a tipo variable |
| `TEDR_FIJO` | `DN_1TI2T0006` | TEDR de hipotecas a tipo fijo |
| `TEDR_MIXTO_1_5` | `DN_1TI2T0004` | TEDR de hipotecas a tipo mixto, periodo fijo inicial de 1 a 5 años |
| `TEDR_MIXTO_5_10` | `DN_1TI2T0005` | TEDR de hipotecas a tipo mixto, periodo fijo inicial de 5 a 10 años |

## Cuentas financieras: activos

Fuente: BdE (Cuentas Financieras de la Economía Española, sector
S.14 hogares). Frecuencia trimestral. Unidades: millones de euros.

Representan los saldos (posiciones) de activos financieros en poder
de los hogares al final de cada trimestre.

| ID canónico | Código BdE | Descripción |
|---|---|---|
| `CF_RIQUEZA_NETA` | `DMZ10N0000H.Q` | Activos financieros netos (activos menos pasivos) |
| `CF_TOTAL_ACTIVO` | `DMZ10S0000H.Q` | Total activos financieros |
| `CF_EFECTIVO_DEPOSITOS` | `DMZ10S1000H.Q` | Efectivo y depósitos |
| `CF_VALORES_DEUDA` | `DMZ10S4000H.Q` | Valores representativos de deuda (bonos) |
| `CF_PRESTAMOS_ACTIVO` | `DMZ10S8000H.Q` | Préstamos concedidos por hogares |
| `CF_PARTICIPACIONES` | `DMZ10S5000H.Q` | Participaciones en el capital y en fondos de inversión |
| `CF_SEGUROS` | `DMZ10S2000H.Q` | Seguros, pensiones y garantías estandarizadas |
| `CF_OTROS_ACTIVOS` | `DMZ10S3000H.Q` | Otros activos financieros |

## Cuentas financieras: pasivos

Misma fuente y frecuencia. Representan las deudas de los hogares.

| ID canónico | Código BdE | Descripción |
|---|---|---|
| `CF_TOTAL_PASIVO` | `DMZ10S000H0.Q` | Total pasivos financieros |
| `CF_DEUDA_HOGARES` | `DMZ10S800H0.Q` | Préstamos recibidos (deuda de los hogares) |
| `CF_CREDITOS_COMERCIALES` | `DMZ10S840H0.Q` | Créditos comerciales y anticipos |
| `CF_OTRAS_CUENTAS` | `DMZ10S310H0.Q` | Otras cuentas pendientes de cobro/pago |

## Cuentas financieras: operaciones (activo)

Frecuencia trimestral. Unidades: millones de euros. Representan
los flujos netos de inversión/desinversión de los hogares en cada
tipo de activo durante el trimestre (variación neta de activos,
VNA).

| ID canónico | Código BdE | Descripción |
|---|---|---|
| `CF_VNA` | `DMZ10F0000H.Q` | Variación neta total de activos financieros |
| `CF_VAR_EFECTIVO` | `DMZ10F1000H.Q` | Variación de efectivo y depósitos |
| `CF_VAR_VALORES` | `DMZ10F4000H.Q` | Variación de valores de deuda |
| `CF_VAR_ACCIONES` | `DMZ10F51Z0H.Q` | Variación de acciones y otras participaciones |
| `CF_VAR_FONDOS` | `DMZ10F5200H.Q` | Variación de participaciones en FI y acciones de sociedades de inversión |
| `CF_VAR_SEGUROS` | `DMZ10F2000H.Q` | Variación de seguros, pensiones y garantías |

## Cuentas financieras: operaciones (pasivo)

Frecuencia trimestral. Unidades: millones de euros. Representan
los flujos netos de endeudamiento/desendeudamiento (variación neta
de pasivos, VNP).

| ID canónico | Código BdE | Descripción |
|---|---|---|
| `CF_VNP` | `DMZ10F000H0.Q` | Variación neta total de pasivos |
| `CF_VAR_PRESTAMOS` | `DMZ10F800H0.Q` | Variación neta de préstamos recibidos |
| `CF_VAR_CRED_COMERCIALES` | `DMZ10F840H0.Q` | Variación de créditos comerciales |
| `CF_VAR_OTROS_PASIVOS` | `DMZ10F310H0.Q` | Variación de otros pasivos |

## Magnitudes macroeconómicas y ratios

Series complementarias de distintas fuentes y frecuencias.

| ID canónico | Código BdE | Frecuencia | Unidad | Descripción |
|---|---|---|---|---|
| `CF_OFN` | `DMZ10A0000H.Q` | Trimestral | Millones EUR | Operaciones financieras netas (VNA menos VNP) |
| `CF_DEUDA_PIB` | `DMZ10C80ZH0_TPRPIB.Q` | Trimestral | Ratio | Deuda de hogares como porcentaje del PIB |
| `CF_DEUDA_MILLONES` | `DMZ10S80ZH0.Q` | Trimestral | Millones EUR | Deuda total de hogares |
| `PIB` | `DTNSEC2010_PIBPM` | Trimestral | Millones EUR | PIB a precios corrientes (precios de mercado) |
| `CAP_NEC_FINANCIACION` | `DSPC102020CB90000_SS14A_TPRB6B000.T` | Trimestral | Millones EUR | Capacidad (+) o necesidad (-) de financiación de los hogares |
| `AHORRO_BRUTO` | `DSPC102020CB8B000_SS14A_TPRB6B000.T` | Trimestral | Millones EUR | Ahorro bruto de los hogares |

## Nota sobre las unidades

Los datos llegan de la API en las unidades indicadas arriba, sin
transformaciones. Las series de stocks DCF llegan en euros (no
millones), mientras que los flujos DN llegan en millones de euros.
Los tipos de interés llegan como porcentaje directo (ej. 2.77).

El pipeline de transformaciones (`src/pipeline/rules.py`) convierte
estas unidades crudas en magnitudes normalizadas:

- **`_BN`**: miles de millones de euros (K_EUR ÷ 1e6, M_EUR ÷ 1e3,
  BN_EUR sin cambio). Series en PCT se ignoran.
- **`_YOY`**: tasa de variación interanual (12 periodos para
  mensuales, 4 para trimestrales). Opera sobre observaciones
  no-NaN para manejar frecuencias mixtas.
- **`_4Q`**: suma móvil de 4 trimestres para anualizar flujos.
- **`CF_PCT_*`**: composición de activos como fracción del total.
- **`FLUJOS_TOTAL_BN`**, **`CF_OTROS_Y_PRESTAMOS_BN`**:
  agregaciones por suma (con `min_count=1` para propagar NaN
  correctamente).