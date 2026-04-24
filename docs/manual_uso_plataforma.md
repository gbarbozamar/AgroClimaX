# Manual Simple de Uso de AgroClimaX

## 1. Que es AgroClimaX

AgroClimaX es una plataforma de monitoreo y alerta agroclimatica. Su objetivo es ayudarte a:

- ver rapidamente el estado hidrico de una zona
- identificar areas con riesgo de estres
- comparar capas satelitales y climaticas
- analizar una unidad territorial concreta
- ajustar parametros del motor de alertas cuando haga falta

La plataforma combina informacion de:

- Sentinel-1
- Sentinel-2
- ERA5
- forecast meteorologico
- capas espaciales de apoyo como CONEAT, secciones y H3

## 2. Como leer la pantalla principal

La pantalla esta organizada en tres partes:

- barra superior: seleccion de area, actualizacion y acciones rapidas
- mapa central: visualizacion espacial
- panel derecho: detalle, estado, drivers, forecast, historial y configuracion

En la parte superior tambien vas a ver los indicadores principales:

- `Humedad Suelo (S1)`: proxy superficial expresado en porcentaje
- `NDMI Vegetacion (S2)`: indice espectral en escala adimensional
- `SPI-30 (ERA5)`: indice estandarizado de precipitacion
- `Area en Alerta`: porcentaje del area afectada
- `Risk Score`: score compuesto de riesgo de 0 a 100
- `Confianza`: score de calidad del dato de 0 a 100
- `Persistencia`: dias consecutivos en el estado actual

Cada indicador tiene un boton `i` con:

- unidad de la metrica
- explicacion breve
- referencia de la fuente usada

## 3. Primer uso recomendado

Si es la primera vez que usas la plataforma, te conviene seguir este recorrido:

1. Abri la plataforma.
2. Mirá el color del banner principal.
3. Revisá los KPI de arriba.
4. Observá el mapa en capa `Alerta`.
5. En el selector de area, cambiá de `Uruguay (nacional)` a un departamento.
6. En el panel derecho, revisá:
   - `Estado Hidrico Actual`
   - `Motores de Riesgo`
   - `Forecast 7 Dias`
   - `Serie Temporal`

Con eso ya tenes una lectura operativa inicial.

## 4. Como cambiar de area

En la barra superior hay un selector de area.

Podes elegir:

- `Uruguay (nacional)`
- un departamento concreto

Cuando cambiás el area:

- el mapa se actualiza
- cambian los KPI
- cambia el panel derecho
- cambia el estado de alerta visible

## 5. Como interpretar el estado de alerta

La plataforma usa cuatro niveles principales:

- `Verde`: condicion estable
- `Amarillo`: vigilancia
- `Naranja`: alerta
- `Rojo`: emergencia

No se disparan solo por una regla fija. El sistema combina:

- magnitud del estres observado
- persistencia temporal
- anomalia respecto al comportamiento reciente
- confirmacion meteorologica
- vulnerabilidad del suelo

Por eso conviene leer siempre:

- el estado
- el `Risk Score`
- la `Confianza`
- los `Motores de Riesgo`

## 6. Como usar el mapa

El mapa permite ver distintas capas. Las mas importantes son:

- `Alerta`: vista sintetica de riesgo
- `RGB`: composicion visual de Sentinel-2
- `NDVI`: vigor de vegetacion
- `NDMI`: humedad relativa de vegetacion
- `NDWI`: agua superficial / humedad
- `SAVI`: vegetacion con correccion de suelo
- `SAR VV`: senal radar Sentinel-1
- `Termal`: temperatura superficial
- `CONEAT`: cartografia de apoyo MGAP
- `Secciones`: secciones policiales
- `Predios`: unidades productivas importadas
- `H3`: grilla operativa fallback

### Recomendacion simple de uso del mapa

- empezá por `Alerta`
- si ves una zona comprometida, pasá a `NDMI` y `SAR VV`
- si necesitás contexto territorial, activá `Secciones`, `Predios` o `H3`
- si necesitás soporte de suelo, activá `CONEAT`

## 7. Como seleccionar una unidad en el mapa

Podés hacer clic sobre:

- un departamento
- una seccion
- un hexagono H3
- un predio importado

Cuando seleccionás una unidad:

- queda resaltada en el mapa
- el panel derecho muestra su estado
- cambian los drivers, forecast e historial para esa unidad

## 8. Como dibujar una parcela manualmente

Si querés analizar un poligono puntual:

1. presioná `Dibujar Parcela`
2. dibujá la geometria en el mapa
3. cerrá el poligono
4. esperá el calculo

El sistema va a devolver un analisis custom para esa geometria.

Si querés borrar el analisis:

- usá `Limpiar`

## 9. Como usar la capa de secciones

La capa `Secciones` sirve para ver el territorio dividido en unidades administrativas mas finas que el departamento.

Uso recomendado:

1. activá `Secciones`
2. elegí un departamento
3. hacé clic sobre la seccion de interes
4. revisá el estado e indices de esa unidad

Es util cuando querés:

- ver diferencias dentro de un mismo departamento
- ubicar rapidamente focos de deterioro
- trabajar con una unidad mas operativa que el mapa nacional

## 10. Como usar la capa H3

La capa `H3` usa hexagonos para dividir el territorio en unidades comparables.

Se usa cuando:

- no hay predios reales cargados
- querés una lectura espacial mas fina
- necesitás comparar zonas con una grilla homogénea

Uso recomendado:

1. activá `H3`
2. hacé zoom si hace falta
3. seleccioná un hexagono
4. revisá el detalle de esa unidad en el panel derecho

## 11. Como cargar predios o potreros

En el panel derecho, dentro de `Unidades Productivas`, podés subir:

- `.geojson`
- `.json`
- `.zip` con shapefile

### Pasos

1. elegí la categoria:
   - `Predio`
   - `Potrero`
   - `Lote`
2. seleccioná el archivo
3. presioná `Importar capa`
4. esperá el mensaje de confirmacion
5. activá la capa `Predios`

Si necesitás un archivo base:

- usá `Descargar plantilla`

## 12. Como leer el panel derecho

El panel derecho tiene varias secciones importantes:

### Estado Hidrico Actual

Resume:

- nivel actual
- explicacion
- accion sugerida
- modo de datos
- calibracion usada

### Motores de Riesgo

Muestra que componentes empujan el riesgo:

- magnitud
- persistencia
- anomalia
- confirmacion meteorologica
- vulnerabilidad del suelo

### Forecast 7 Dias

Permite ver si la situacion:

- mejora
- se mantiene
- empeora

### Humedad Superficial del Suelo

Muestra:

- humedad derivada de Sentinel-1
- NDMI de Sentinel-2

### SPI-30

Muestra:

- valor actual
- categoria interpretativa

### Serie Temporal

Sirve para ver la evolucion reciente de la unidad seleccionada.

### Indicadores Tecnicos

Resume:

- calibracion activa
- unidad operativa
- suelo/cobertura
- forecast
- modo de datos
- pipeline

## 13. Como usar la pestana Settings

La pestana `Settings` sirve para cambiar la configuracion del motor de alertas sin tocar codigo.

Hay dos modos:

- `Global`
- `Por cobertura`

### Global

Modifica la configuracion base para todo el sistema.

### Por cobertura

Permite crear un ajuste particular para:

- `pastura_cultivo`
- `forestal`
- `humedal`
- `suelo_desnudo_urbano`

### Que se puede ajustar

- umbrales de estados
- pesos del risk score
- pesos del confidence score
- histeresis
- ventanas de calculo
- calibracion
- reglas espaciales
- parametros meteorologicos

### Recomendacion de uso

- usá `Global` para cambios generales
- usá `Por cobertura` solo si querés diferenciar comportamiento por tipo de superficie

### Botones principales

- `Recargar`: trae la configuracion actual
- `Guardar`: aplica los cambios
- `Reset global`: vuelve a la configuracion por defecto
- `Borrar override`: elimina un override de cobertura

## 14. Como interpretar Risk Score y Confianza

### Risk Score

Es un score interno de 0 a 100.

- mas alto = mas riesgo
- no es porcentaje fisico
- sirve para sintetizar varias señales en una sola escala

### Confianza

Tambien es un score de 0 a 100.

- mas alto = mejor calidad y coherencia del dato
- baja si hay problemas de frescura, cobertura, acuerdo o calibracion

Interpretacion recomendada:

- `riesgo alto + confianza alta`: señal fuerte
- `riesgo alto + confianza baja`: señal a revisar con cautela
- `riesgo bajo + confianza alta`: situacion estable con buena evidencia

## 15. Como usar CONEAT

La capa `CONEAT` es una capa de apoyo cartografico.

Sirve para:

- contextualizar el territorio
- entender mejor diferencias espaciales
- apoyar lectura de vulnerabilidad o aptitud

Importante:

- esta capa se visualiza mejor con zoom relativamente cercano
- si no se ve a gran escala, acercate

## 16. Flujo de uso recomendado para trabajo diario

### Opcion A: monitoreo nacional rapido

1. abrir la plataforma
2. revisar KPI superiores
3. mirar el estado nacional
4. revisar top drivers
5. pasar a departamentos de interes

### Opcion B: analisis de una zona puntual

1. elegir el departamento
2. activar `Secciones`, `Predios` o `H3`
3. seleccionar una unidad
4. revisar estado, risk score, confianza y forecast
5. comparar con `NDMI` y `SAR VV`

### Opcion C: analisis de un predio propio

1. importar archivo geoespacial
2. activar `Predios`
3. seleccionar el predio
4. revisar el detalle de la unidad
5. si hace falta, ajustar reglas desde `Settings`

## 17. Buenas practicas de uso

- no te quedes solo con un color; mirá tambien `Risk Score` y `Confianza`
- compará siempre `Alerta`, `NDMI` y `SAR VV`
- usá `Forecast 7 Dias` para anticipar deterioros
- si una señal parece extraña, revisá la fuente con el tooltip `i`
- si trabajás con una cobertura particular, revisá si conviene un ajuste `Por cobertura`

## 18. Limitaciones que conviene tener presentes

- algunas metricas son proxies y no equivalen a una medicion de campo directa
- el sistema combina datos observados, derivados y reglas configurables
- la interpretacion final siempre mejora si se complementa con observacion local

## 19. Si algo no carga o se ve raro

Probá esto:

1. recargá la pagina con `Ctrl + F5`
2. verificá que estés en la URL correcta
3. cambiá de area y volvé a la anterior
4. si una capa no aparece, acercá el zoom
5. si importaste un archivo y no ves la capa, revisá el mensaje de importacion

## 20. Resumen corto

Si querés usar AgroClimaX de la forma mas simple posible:

1. mirá el color general y los KPI
2. elegí un departamento o unidad
3. revisá `Risk Score`, `Confianza` y `Forecast`
4. apoyate en `NDMI`, `SAR VV`, `Secciones`, `Predios` o `H3`
5. si necesitás cambiar reglas, usá `Settings`

Con ese flujo ya podés usar la plataforma de forma intuitiva y operativa.
