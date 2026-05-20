# Edge IIoT Gateway — System Learning & Architecture Guide

> **Para uso personal de ingeniería.**
> Este documento no es un tutorial. No da soluciones: obliga a construirlas y a justificarlas.
>
> **v5.0 — mayo 2026.** Reestructuración del roadmap orientada a demostrabilidad, valor
> IIoT industrial, y diferenciación de dominio en ese orden.
> El sistema funciona de extremo a extremo hasta Fase 11. Este documento refleja
> comportamiento real, límites medidos, deuda activa, y roadmap justificado.
> La v4.0 era estratégica. Esta versión reordena la estrategia: primero sistema fiable,
> después sistema demostrable, después sistema industrialmente convincente, finalmente
> diferenciación motorsport.

---

## CÓMO LEER ESTE DOCUMENTO

Este documento tiene tres usos simultáneos:

**Como guía de aprendizaje activo:** lee solo la sección en la que estás trabajando. No
adelantes. Cada capa tiene preguntas que no puedes responder sin haber construido lo que
viene antes.

**Como log de decisiones arquitectónicas:** cada decisión de diseño está justificada con
el problema concreto que resuelve. Si no ves la justificación, la decisión está mal
tomada o mal documentada. Actualiza antes de continuar.

**Como referencia de evolución:** las fases futuras no existen porque "quedan bien". Existen
porque resuelven un límite conocido del sistema actual. Si no puedes nombrar el límite
antes de nombrar la solución, la fase no está lista para construirse.

**Regla que no cambia:** un sistema mínimo que funciona de extremo a extremo, con fallos
manejados y decisiones justificadas, comunica más capacidad técnica que una arquitectura
avanzada con piezas desconectadas. La complejidad sin propósito no es experiencia: es ruido.

---

## PRINCIPIOS OPERATIVOS

Estos principios aplican a todas las fases, presentes y futuras. Si una decisión viola uno
de estos principios, es la decisión la que está mal, no el principio.

**P1 — El sistema funciona o no funciona. No hay gradientes.**
Un módulo funcional en aislamiento que no está integrado no cuenta. El único criterio es:
el dato entra por un extremo y llega verificablemente al otro, incluyendo escenarios de
fallo. Si no puedes demostrarlo con output observable y automatizado, no funciona.

**P2 — El flujo de datos es la arquitectura.**
La arquitectura no es el diagrama de módulos. Es el camino que recorre un dato desde que
cambia en el sensor hasta que llega a su destino, incluyendo los desvíos por fallo. Si no
puedes dibujar ese camino de memoria, el diseño no está claro.

**P3 — Cierra cada capa antes de añadir la siguiente.**
Una capa está cerrada cuando tiene comportamiento observable verificable, tests que lo
demuestran, y manejo explícito de sus fallos posibles. Añadir complejidad sobre una capa
no cerrada multiplica deuda técnica, no la suma.

**P4 — La complejidad tiene coste de mantenimiento mental.**
No añadas un patrón hasta que tengas el problema concreto que ese patrón resuelve. Si no
puedes nombrar el problema antes de nombrar la solución, es sobreingeniería prematura.

**P5 — Las interfaces entre módulos son más importantes que los módulos.**
Antes de implementar cualquier módulo: define qué recibe, qué devuelve, qué excepciones
puede lanzar, qué garantías ofrece.

**P6 — Los datos corruptos son peores que los datos ausentes.**
Un hueco en la serie temporal es visible y medible. Un valor incorrecto con timestamp
erróneo contamina el historial. Validar en el punto de entrada es más barato que limpiar
datos en InfluxDB después.

**P7 — Escala cuando tengas el problema, no cuando lo anticipes.**
Los límites del sistema están medidos. Escalar antes de alcanzarlos es sobreingeniería.
Escalar cuando los benchmarks lo demuestren es ingeniería.

---

## ESTADO ACTUAL DEL SISTEMA (mayo 2026)

### Pipeline de extremo a extremo — Fase 11

```
OPC UA Server (sensores industriales)          AC Shared Memory (Windows host)
      │  suscripciones event-driven                   │  loop ~100 Hz por señal
      │  latencia: ms–s                               │  ctypes.Structure + mmap
      ▼                                               ▼
DataChangeHandler (síncrono)              UDP Emitter (ac_emitter.py, Windows)
      │  StatusCode → QualityStatus                   │  threshold-driven por señal
      │  node_id → sensor_id (sensors.yaml)           │  JSON por paquete, UDP puerto 9000
      │  SensorReading.from_opc_notification()        ▼
      │  queue.put_nowait()              ACProtocol.datagram_received() (síncrono)
      │                                               │  SensorReading.from_ac_packet()
      │                                               │  queue.put_nowait()
      ▼                                               │
asyncio.Queue(maxsize=2500) ←──────────────────────────┘
      │
      ▼
worker()
      │  forward_pending() si pending > 0  ←── StoreAndForward SQLite
      │  await queue.get(timeout=5.0)             (FIFO, at-least-once)
      │                                                │
      │  destination.send()                           │
      │  True  ──────────────────────────────────────▶│ delete_batch()
      │  False ──────────────────────────────────────▶│ save_batch()
      ▼
MQTTDestination (aiomqtt)
      │  reconexión en send() si MqttError
      ▼
Broker MQTT
      │
      ▼
Telegraf → InfluxDB → Grafana

asyncio.TaskGroup (main.py)
      ├─ connect_with_retry()   OPC UA con backoff exponencial
      ├─ worker()               consumidor único
      ├─ ac_adapter()           UDP datagram endpoint
      └─ http_server()          /health + /metrics (stdlib pura)
```

### Componentes y estado real

| Componente | Estado | Observación crítica |
|---|---|---|
| `asyncio.Queue` + pipeline | **Funcional** | `maxsize=2500`. Backpressure por descarte con log. |
| OPC UA suscripciones | **Funcional** | `DataChangeHandler` síncrono + `put_nowait()`. Reconexión exponencial operativa. |
| Store & Forward SQLite | **Funcional** | FIFO, at-least-once. Throughput real máximo: ~490 msg/s. |
| Strategy Pattern destinos | **Funcional** | `BaseDestination` ABC. `MQTTDestination` con reconexión en `send()`. |
| Contrato Pydantic | **Funcional** | `SensorReading` con validadores NaN/Inf/timezone/quality. 14 unit tests. |
| Telegraf + InfluxDB | **Funcional** | Tags: `sensor_id`, `quality`, `unit`. Field: `value`. Timestamp: SourceTimestamp OPC UA. |
| Grafana | **Funcional** | Dashboard filtrable por `sensor_id` con variable dinámica. |
| `/health` + `/metrics` | **Funcional** | Tres estados verificados. asyncio HTTP stdlib. |
| Mapping YAML externo | **Funcional** | `config/sensors.yaml`. 5 sensores OPC UA simultáneos. |
| Adaptador AC UDP | **Funcional** | Pipeline híbrido OPC UA + AC verificado en Grafana. 6 señales. |

### Payload que circula por el pipeline

El worker no distingue la fuente. `opc_node_id=null` es el discriminador — no un campo
de error.

```json
// Fuente OPC UA
{
  "sensor_id": "planta1.maquina1.temperatura",
  "timestamp": "2026-05-10T09:00:00+00:00",
  "value": 67.9,
  "quality": "Good",
  "unit": "°C",
  "opc_node_id": "ns=2;i=2",
  "received_at": "2026-05-10T09:00:00.003+00:00",
  "raw_value": 67.9
}

// Fuente AC
{
  "sensor_id": "motorsport.spa.coche1.rpms",
  "timestamp": "2026-05-10T09:07:08.026+00:00",
  "value": 6297.0,
  "quality": "Good",
  "unit": "rpm",
  "opc_node_id": null,
  "received_at": "2026-05-10T09:07:24.544+00:00",
  "raw_value": 6297.0
}
```

### Semántica temporal — limitación activa

| Campo | OPC UA | AC | Limitación |
|---|---|---|---|
| `timestamp` | `SourceTimestamp` del servidor | `datetime.now(UTC)` del emisor Windows | Para AC: momento de lectura de Shared Memory, no timestamp de ciclo del simulador |
| `received_at` | `datetime.now(UTC)` del gateway | `datetime.now(UTC)` del gateway | Momento real de ingesta |
| `received_at - timestamp` | Latencia real del pipeline OPC UA | Latencia UDP + delta de polling | **No está siendo medida ni logueada** |

El número "~1ms LAN" es una estimación, no un benchmark. Esta es deuda de verificabilidad activa.

---

## CAPA I — FOUNDATION
### El pipeline existe y transporta datos

*Fases 0–5. Completadas. Esta capa no se toca.*

---

### Lo que se construyó

**Fase 0 — asyncio.Queue fundacional**

`asyncio.Queue(maxsize=2500)` como contrato central del sistema. Productores usan
`put_nowait()` — no `await queue.put()` — porque los handlers OPC UA y AC son síncronos:
no pueden hacer `await`. Si la llamada bloqueara, el event loop quedaría congelado. El
descarte con log es la única alternativa correcta: un dato perdido es observable; un event
loop congelado no lo es.

**Fase 1 — OPC UA Polling (eliminado)**

El código de polling ya no existe. Se documenta porque fue la base conceptual que permitió
entender lo que las suscripciones hacen por debajo: el ciclo de petición-respuesta, la
latencia de reconexión, el manejo de errores de I/O async. Sin esa comprensión, las
suscripciones son magia negra.

**Fase 2 — OPC UA Suscripciones event-driven**

`DataChangeHandler` recibe notificaciones del servidor y las traduce al modelo interno:
`StatusCode → QualityStatus`, `SourceTimestamp → timestamp`, `node_id → sensor_id` vía
`sensors.yaml`. La suscripción es push, no pull: el servidor notifica cuando cambia el
valor, no cuando el gateway pregunta.

`connect_with_retry()` gestiona reconexión exponencial (backoff 2s → 60s). El
`asyncio.Event` señaliza fallo de suscripción y lo detecta la corutina de reconexión.

Limitación estructural: `DataChangeHandler` es síncrono por contrato de `asyncua`. No
puede hacer `await`. La cola es el único puente entre el contexto síncrono del handler y
el contexto async del worker.

**Fase 3 — Store & Forward SQLite**

`StoreAndForward` como buffer persistente para at-least-once delivery cuando MQTT no está
disponible.

Decisiones de diseño con razonamiento:

- Conexión persistente en `__init__`, no por llamada: evita latencia de apertura de
  archivo por operación.
- `asyncio.Lock` sobre todas las operaciones: SQLite serializa writes. El lock asyncio
  garantiza que no hay dos corutinas compitiendo por la misma conexión.
- `executemany` para batch inserts: reduce overhead de parsing SQL y número de
  transacciones. La transacción la gestiona el context manager `with self._conn:`.
- `delete_batch()` solo tras confirmación: si el proceso muere entre envío y borrado, el
  mensaje se reenvía. Correcto para at-least-once: puede llegar dos veces, nunca cero.

**Fase 4 — Gateway integrado + shutdown limpio**

`asyncio.TaskGroup` lanza cuatro tareas en paralelo. Shutdown garantiza que si la cola
tiene mensajes cuando llega la señal de cierre, el sistema espera hasta 10 segundos para
procesarlos. Si el timeout expira, drena la cola manualmente a SQLite.

**Comportamiento real del shutdown — limitación conocida:**
`shutdown()` cancela tareas individuales dentro de un `TaskGroup` activo. Cancelar una
tarea en un `TaskGroup` provoca que el `TaskGroup` cancele todas las demás — es el
comportamiento por diseño de Python 3.11+. El shutdown funciona correctamente en la
práctica, pero el orden de cierre lo controla el `TaskGroup`, no la lógica explícita de
`shutdown()`. Este mecanismo debe estar bajo control explícito antes de añadir complejidad
nueva. **Esta es deuda activa que bloquea la Fase 12.**

**Fase 5 — BaseDestination + Strategy Pattern**

`BaseDestination` ABC con contrato `send() -> bool` e `is_available() -> bool`. El worker
no sabe qué hay detrás. `MQTTDestination` mantiene conexión persistente (no por mensaje)
para amortizar el overhead de handshake TCP + MQTT CONNECT. En caso de `MqttError`:
recrea el cliente, devuelve `False` — el mensaje actual ya se perdió, el siguiente usará
la conexión restaurada.

Consecuencia documentada: el primer mensaje que encuentra un fallo de MQTT siempre va a
SQLite aunque la reconexión funcione inmediatamente. Introduce latencia de un ciclo antes
de que los mensajes live usen la conexión restaurada. No es un bug — es el comportamiento
correcto para at-least-once, pero tiene implicaciones de latencia conocidas.

---

### Pregunta de ingeniería que cierra esta capa

*¿Puedes dibujar de memoria el camino que recorre un dato desde que cambia en el sensor OPC
UA hasta que llega al broker MQTT, incluyendo el desvío por SQLite cuando MQTT no está
disponible?*

Si no, esta capa no está cerrada.

---

## CAPA II — DATA CONTRACT
### El pipeline solo ve datos correctos

*Fase 6. Completada.*

---

### Lo que se construyó

`SensorReading` es el único modelo de datos que circula por el pipeline. La validación
ocurre en el punto de entrada — antes de la cola — para que el interior del sistema solo
vea datos que cumplen el contrato.

### Campos y garantías

| Campo | Tipo | Garantía |
|---|---|---|
| `sensor_id` | `str` (min 3 chars) | Nunca vacío |
| `timestamp` | `datetime` (tz-aware) | Timestamp naive → `ValidationError` |
| `value` | `float \| None` | `None` solo si `quality=BAD`. NaN → `ValidationError`. ±Inf → `ValidationError` |
| `quality` | `QualityStatus` | Enum: Good / Bad / Uncertain |
| `unit` | `str` | Nunca vacío |
| `opc_node_id` | `str \| None` | `None` para fuentes no-OPC UA. No es un campo de error. |
| `received_at` | `datetime` (tz-aware) | Momento de ingesta en el gateway |
| `raw_value` | `float \| None` | Igual que `value` actualmente. Reserva arquitectónica para calibración futura. |

### Invariantes activos

- `value=None` con `quality=Good` → `ValidationError` (bug del servidor, no dato legítimo)
- `value=None` con `quality=Uncertain` → `ValidationError`
- `value=None` con `quality=Bad` → aceptado (OPC UA envía `Variant(Null)` en lecturas Bad)

### Por qué NaN es especialmente peligroso

NaN propagado a InfluxDB hace que `mean()` devuelva NaN para toda la ventana temporal que
lo contiene. Un sensor con fallo intermitente que emite NaN contamina el histórico de esa
ventana de forma no recuperable sin reescritura de datos. Rechazar en validación es el
único momento en que el coste es cero.

### Constructores semánticos

`from_opc_notification()` — recibe tipos nativos de `asyncua` y construye `SensorReading`.
Si la API de `asyncua` cambia, solo cambia este método.

`from_ac_packet()` — recibe el dict JSON del paquete UDP. `timestamp` viene del emisor
Windows. `received_at` es `datetime.now(UTC)` del gateway.

### Estado de testing

14 unit tests cubren: timezone naive rechazado, UTC aceptado, NaN rechazado, ±Inf
rechazado, zero aceptado, negativo aceptado, Bad+None aceptado, Good+None rechazado,
Uncertain+None rechazado, campos del payload MQTT.

**Gap activo:** no existe ningún test de integración del pipeline. El criterio P1 ("el dato
llega verificablemente al otro extremo") se verifica solo manualmente. Esta es la deuda
más importante del sistema. **Bloquea cualquier fase nueva.**

---

## CAPA III — INFRASTRUCTURE
### El pipeline llega al destino correcto

*Fases 7–8. Completadas.*

---

### Por qué Telegraf y no escritura directa a InfluxDB

Si el gateway escribe directamente a InfluxDB: tiene dos responsabilidades (transporte y
persistencia), un fallo de InfluxDB afecta al flujo de transporte, y el Store & Forward
tendría que gestionar también la cola de escrituras. Con Telegraf como bridge: el gateway
es indiferente a qué base de datos existe detrás. Si InfluxDB se sustituye por
TimescaleDB, el gateway no cambia — solo cambia la configuración de Telegraf.

### Schema InfluxDB

```
measurement: sensor_readings
tags:         sensor_id, quality, unit
fields:       value (float)
timestamp:    SensorReading.timestamp (SourceTimestamp OPC UA o timestamp emisor AC)
```

Tags son índices en InfluxDB. Una query `WHERE sensor_id = '...'` es eficiente solo si
`sensor_id` es tag. El coste es cardinalidad: cada combinación única de tags crea una
serie nueva.

| Sensores | Series activas |
|---|---|
| 10 | ~30 |
| 50 | ~150 |
| 200 | ~600 |

Muy por debajo del límite problemático de InfluxDB (~100.000 series en hardware de
laboratorio).

Por qué el timestamp del `SensorReading` y no `now()` de Telegraf: si Telegraf usa
`now()`, los datos quedan indexados por momento de llegada a Telegraf, no por momento de
medición. Eso invalida cualquier análisis de latencia o correlación temporal.

### Grafana

Dashboard con variable dinámica `$sensor_id`. Permite ver OPC UA y AC en los mismos
paneles filtrando por sensor. No es observabilidad de producción — es la herramienta de
verificación durante desarrollo. Para visualización operacional en tiempo real (< 1s de
latencia), Grafana no es el stack correcto: su overhead de query HTTP introduce cientos de
ms por panel.

---

## CAPA IV — OBSERVABILITY
### El sistema reporta su propio estado

*Fase 9. Completada.*

---

### Principio de observabilidad

Hay una diferencia entre "el proceso está vivo" y "el sistema está funcionando". Un proceso
puede estar vivo con la cola al 95% de capacidad, MQTT desconectado, y SQLite acumulando
deuda. Los endpoints de observabilidad distinguen estos estados.

### `/health` — ¿Puedo operar el sistema?

```json
{
  "status": "healthy",
  "queue_size": 0,
  "sql_pending": 0,
  "mqtt_connected": true,
  "opc_ua_connected": true
}
```

| Estado | Criterio |
|---|---|
| `healthy` | MQTT conectado + OPC UA conectado + cola < 75% de `maxsize` |
| `degraded` | cola ≥ 75% de `maxsize` (≥1875 de 2500) |
| `unhealthy` | MQTT desconectado O OPC UA desconectado |

El estado MQTT usa `_connected` — flag actualizado por `send()`, sin I/O en cada health
check. El estado OPC UA usa `asyncio.Event` — sin estado duplicado.

### `/metrics` — ¿Qué ha hecho el sistema?

```json
{
  "pending": 0,
  "queueSize": 0,
  "cont_total": 76,
  "cont_fails": 0
}
```

`cont_proce_total` se incrementa cuando `send()` devuelve `True`. No distingue entre
mensajes OPC UA y AC, ni entre mensajes directos y reenviados desde SQLite. Esta
granularidad es deuda activa — necesaria para operación real.

### Servidor HTTP

Implementado con `asyncio.start_server()` — stdlib pura, sin dependencias externas. Lee
la primera línea del request para extraer el path. HTTP/1.0 behavior (cierra conexión
tras respuesta). Dos endpoints de solo lectura no justifican una dependencia de framework.

### Lo que falta para observabilidad real

- Latencia `received_at - timestamp` no está siendo medida ni logueada. Es la métrica más
  reveladora del pipeline.
- Contadores no distinguen fuente (OPC UA vs AC) ni tipo de mensaje (directo vs reenvío).
- No hay alerta cuando SQLite supera un umbral de pendientes — la señal de degradación
  existe en el health check pero no hay mecanismo de notificación.

---

## CAPA V — SCALING LIMITS
### Los límites están medidos, no estimados

*Mediciones de hardware real, mayo 2026.*

---

### Benchmarks verificados

**Worker throughput — `benchmark_worker.py` (10.000 msgs pre-cargados en cola)**

```
MockDestination (sin I/O):          66.254 msg/s
MQTTDestination (broker Docker):     8.320 msg/s

Cuello de botella: latencia de publicación MQTT, no procesamiento Python.
```

**Nota crítica:** el benchmark con MQTTDestination requiere broker vivo para reproducirse.
MockDestination está definida pero no integrada en el benchmark principal. Los números no
son reproducibles sin infraestructura externa. **Esta es deuda activa.**

**SQLite throughput — `benchmark_sqlite.py` (save_batch() con sleep de cadencia)**

```
100 msg/s objetivo  →  88.9 msg/s real  (déficit: 11%)
500 msg/s objetivo  → 360.6 msg/s real  (déficit: 28%)
1000 msg/s objetivo → 488.2 msg/s real  (déficit: 51%)

Límite efectivo: ~490 msg/s sostenido.
```

**Nota crítica:** mide `save_batch()` en aislamiento. No incluye overhead del worker ni
contención con `forward_pending()`. El throughput real bajo carga combinada puede ser
menor.

### Carga operacional actual

```
OPC UA (5 sensores, ~1 Hz):         ~5 msg/s
AC (6 señales, threshold-driven):   ~90 msg/s pico
Combinado:                          ~95 msg/s

Margen hasta límite SQLite (~490):  ~5x
Margen hasta límite worker MQTT:    ~87x
```

### Escenario futuro — AC con 20 señales completas

```
AC (20 señales, ~60 Hz efectivo):   ~1.200 msg/s
SQLite bajo MQTT caído:             déficit ~710 msg/s
```

Con 20 señales AC y MQTT caído, SQLite no puede absorber la tasa de ingesta. La cola de
persistencia crece indefinidamente. **Este límite justifica la política de retención por
criticidad de señal (Fase C).** No es una limitación para arreglar hoy — es una
limitación para tener en cuenta cuando se expandan las señales AC.

### Cuándo NO escalar

El worker aguanta 8.300 msg/s con MQTT real. La carga actual es ~95 msg/s. Margen: ~87x.
No hay justificación para múltiples workers, Kafka, Redis, ni ninguna otra capa de
transporte hasta que los benchmarks demuestren que el límite actual se ha alcanzado.

### Cuándo sí escalar

Cuando `benchmark_worker.py` en condiciones de carga real (no pre-cargando la cola) muestre
que el worker es el cuello de botella, o cuando la latencia de extremo a extremo medida
supere el SLA del sistema. No antes.

---

## CAPA VI — DOMAIN
### El pipeline habla dos idiomas

*Fases 10–11. Completadas.*

---

### Naming convention ISA-95 aplicada a dos dominios

```
Industrial:   planta1.maquina1.temperatura
Motorsport:   motorsport.spa.coche1.rpms
```

El mismo esquema jerárquico. El primer componente identifica el dominio. El worker trata
el `sensor_id` como opaco — no necesita saber el dominio para transportar el dato.

### Configuración externa — `sensors.yaml`

Fuente de verdad del mapping `(maquina, variable) → {sensor_id, unit}` para el dominio
OPC UA. Se carga en cada reconexión — si el YAML cambia mientras el gateway está caído, la
siguiente reconexión carga el YAML actualizado. Nodos fuera del mapping: log WARNING +
descarte. Nunca silencioso, nunca crash.

Limitación activa: la configuración de señales AC (señales activas, umbrales) está
hardcodeada en `ac_emitter.py`. No hay contraparte YAML para el dominio motorsport. Con 6
señales esto es aceptable. Con 20 señales y umbrales variables por señal y por circuito,
se convierte en deuda operacional.

### Arquitectura del emisor AC — decisiones clave

`PhysicsData(ctypes.Structure, _pack_=4)` replica el struct C# de AC en memoria. El
orden de `_fields_` es crítico — define el offset de cada campo. Un campo fuera de orden
produce valores numéricos plausibles pero incorrectos, sin error observable.

El emisor lee a 100 Hz pero emite solo cuando el valor supera el umbral por señal. La
frecuencia efectiva de emisión depende de la dinámica del vehículo, no del polling. En
marcha estable, RPM puede no cambiar más de 50 rpm durante segundos — la frecuencia
efectiva baja a 0. Esto es correcto: el dato UDP solo viaja cuando hay información nueva.

### Por qué UDP y no OPC UA como wrapper de AC

OPC UA es event-driven (notifica cuando cambia). AC genera estado continuo a ciclo fijo
(~100 Hz). Wrappear AC en OPC UA requiere polling del servidor OPC UA, lo que introduce
jitter de ±20–50ms. El `PublishingInterval=10ms` no está garantizado. Además es una capa
extra de proceso, protocolo, y punto de fallo sin ventaja sobre UDP directo. El diseño
correcto sigue la semántica del dominio.

### Señales AC implementadas (6 de ~20 disponibles)

| Señal | Umbral | Frecuencia efectiva | sensor_id |
|---|---|---|---|
| `rpms` | 50 rpm | ~15 Hz | `motorsport.spa.coche1.rpms` |
| `speed_kmh` | 0.5 km/h | ~15 Hz | `motorsport.spa.coche1.speed_kmh` |
| `gas` | 0.01 | ~10 Hz | `motorsport.spa.coche1.throttle` |
| `brake` | 0.01 | ~10 Hz | `motorsport.spa.coche1.brake` |
| `gear` | cualquier cambio | evento | `motorsport.spa.coche1.gear` |
| `tyre_core_temp FL` | 0.1°C | ~5 Hz | `motorsport.spa.coche1.tyre_temp_fl` |

### Señales pendientes de implementar

| Señal | Struct | Frecuencia efectiva | Notas |
|---|---|---|---|
| `tyreCoreTemperature[1-3]` FR/RL/RR | Physics | ~5 Hz | Mismo struct, campos de array |
| `wheelSlip[4]` | Physics | ~60 Hz | Alta frecuencia — impacto en carga |
| `accG[3]` | Physics | ~60 Hz | Alta frecuencia — impacto en carga |
| `suspensionTravel[4]` | Physics | ~60 Hz | Alta frecuencia — impacto en carga |
| `tyrePressure[4]` | Physics | ~10 Hz | — |
| `fuel` | Physics | ~1 Hz | — |
| `lapTime` / `lastLap` | Graphics | evento | Requiere `GraphicsData` struct |

**Nota:** `wheelSlip`, `accG`, y `suspensionTravel` a ~60 Hz efectivo con 4 canales cada
uno pueden generar ~720 msg/s adicionales. Eso lleva la carga total a ~1.200 msg/s si MQTT
cae — por encima del límite de SQLite. La política de retención por criticidad de señal
(Fase C) es prerequisito para activar estos canales.

---

## CAPA VII — RELIABILITY NARRATIVE
### El sistema tiene garantías, no esperanzas

*Síntesis de las capas anteriores como narrativa de reliability engineering.*

---

### Puntos de pérdida de datos y sus protecciones

| Punto | Condición de pérdida | Protección | Estado |
|---|---|---|---|
| Handler OPC UA → Cola | Cola llena | `put_nowait()` + log WARNING | Activo |
| Handler AC → Cola | Cola llena | `put_nowait()` + log WARNING; siguiente dato en ≤67ms | Activo |
| Cola → Worker | Proceso muere sin shutdown limpio | Graceful shutdown: vaciado a SQLite en 10s | Activo |
| Worker → MQTT | Broker no disponible | Store & Forward SQLite (at-least-once) | Activo |
| SQLite → MQTT | Fallo durante forward | `delete_batch()` solo tras confirmación de envío | Activo |
| AC alta frecuencia + MQTT caído | SQLite > 490 msg/s | **Sin protección activa** — déficit acumulado | Límite conocido |
| MQTT → Telegraf | Telegraf caído | Buffer interno de Telegraf | Fuera del control del gateway |
| Telegraf → InfluxDB | InfluxDB caído | Modo SYNC en Telegraf | Fuera del control del gateway |
| Validación de entrada | NaN, Inf, timestamp naive | Pydantic `ValidationError` + descarte con log | Activo |
| Reconexión OPC UA | Suscripción caída | Backoff exponencial 2s → 60s | Activo |
| Reconexión MQTT | `MqttError` en `send()` | Recreación de cliente en el mismo ciclo | Activo — latencia de un ciclo |

### Las tres garantías verificables del sistema

1. **At-least-once delivery para OPC UA a carga nominal:** cuando MQTT cae y vuelve,
   todos los mensajes OPC UA que llegaron durante la interrupción llegan al broker en orden
   FIFO desde SQLite. Verificable con fault injection: matar el broker, esperar, levantar,
   observar vaciado de SQLite.

2. **Ningún dato corrupto en el pipeline:** NaN, Inf, timestamps naive, y la combinación
   `value=None` con `quality≠BAD` son rechazados en el punto de entrada. El interior del
   pipeline no necesita defensas contra corrupción de datos.

3. **El estado del sistema es observable en cualquier momento:** `/health` y `/metrics`
   responden sin I/O adicional. El health check no verifica conectividad activa — usa el
   último estado conocido. Esto es correcto para un endpoint de baja latencia.

### La garantía que falta

At-least-once delivery para AC a alta frecuencia bajo MQTT caído no está garantizada. Es
un límite de diseño conocido y documentado, no un bug. La solución requiere una política
de retención diferenciada (Fase C) que no está implementada.

---

## DEUDA TÉCNICA ACTIVA (mayo 2026)

En orden de impacto y urgencia:

| Deuda | Impacto | Urgencia | Acción |
|---|---|---|---|
| Sin tests de integración del pipeline | P1 violado. El sistema funciona pero no se puede verificar sin Grafana | **Crítica — bloquea Fase A** | Tests E2E con MockDestination |
| Shutdown con TaskGroup no controlado explícitamente | El orden de cierre es correcto por coincidencia, no por diseño | **Alta — bloquea Fase A** | Refactorizar antes de añadir complejidad |
| Benchmark requiere broker MQTT vivo | Números no reproducibles por terceros | **Alta** | Integrar MockDestination en benchmark |
| Latencia `received_at - timestamp` no medida | El número "~1ms" es una estimación no verificable | **Alta** | 10 líneas de código. Hacerlo ahora. |
| `main()` huérfano en `worker.py` | Confusión de punto de entrada | **Media — cosmética pero visible** | Borrar |
| Contadores sin distinción de fuente/tipo | `cont_total` agrega OPC UA + AC + reenvíos | **Media** | Resolver en Fase A |
| Señales AC sin validación de rango | `quality=GOOD` con valores físicamente imposibles | **Media — resolver al expandir señales** | Implementar en Fase B |
| Configuración AC hardcodeada en emisor | No configurable externamente | **Baja — aceptable con 6 señales** | Resolver al expandir a 20 señales |
| `raw_value == value` siempre | Campo reservado sin función real | **Baja — deuda conceptual** | Documentar o implementar calibración |

---

## ROADMAP DE EVOLUCIÓN

Las siguientes fases son letras para distinguirlas del historial numérico de construcción.
Cada fase existe porque resuelve un límite conocido del sistema actual. El orden no es
arbitrario: refleja una secuencia de valor acumulable donde cada fase amplifica el impacto
de la anterior.

La narrativa estratégica del roadmap es:

```
Fase A + B  →  sistema fiable y verificable
Fase D      →  sistema demostrable
Fase E + F  →  sistema industrialmente convincente
Fase G      →  diferenciación de dominio motorsport
Fase H + I  →  capacidades enterprise y arquitectura edge/cloud
```

Un proyecto que llega a Fase F sin haber pasado por D es técnicamente interesante e
indemostrable. Un proyecto que llega a G sin haber pasado por E es un motorsport hobby
sin credibilidad industrial. El orden importa.

---

### FASE A — Consolidación de production readiness
**Prioridad: MUST HAVE. Prerequisito para todo lo demás.**

**Límite que resuelve:** el sistema funciona pero no puede verificar que funciona sin
inspección manual de Grafana. Eso viola P1. La deuda de verificabilidad no es estética —
hace que cualquier cambio futuro sea un riesgo no medible.

**Motivación arquitectónica:** el principio P1 dice que el sistema funciona o no funciona.
Actualmente, la verificación de que el dato llega al destino es manual (mirar Grafana).
Eso no es verificable de forma reproducible. Un sistema que solo puedes verificar mirando
una pantalla no puedes confiar en él cuando cambia.

**Qué construir:**

*Tests de integración del pipeline (E2E):*
Cinco tests mínimos que verifican las invariantes del sistema con `MockDestination`:
- Un dato OPC UA válido llega al destino.
- Un dato con timestamp naive es rechazado antes de la cola.
- Un dato con `quality=BAD` y `value=None` llega al destino correctamente.
- Cuando `send()` devuelve `False`, el dato aparece en SQLite.
- Cuando SQLite tiene pendientes y el destino vuelve a estar disponible, los mensajes
  llegan en orden FIFO.

Estos tests son documentación ejecutable del contrato del sistema.

*Shutdown bajo control explícito:*
Refactorizar el shutdown para que el orden de cierre sea consecuencia del diseño, no del
comportamiento de `TaskGroup`. El mecanismo debe ser el mismo que el efecto.

*Benchmarks reproducibles sin infraestructura externa:*
Integrar `MockDestination` en `benchmark_worker.py` como benchmark primario. El benchmark
con MQTT real se convierte en benchmark secundario (requiere infraestructura, documenta
throughput con I/O real).

*Latencia medida y logueada:*
`received_at - timestamp` calculado para cada mensaje en el worker. Logueado en nivel
DEBUG y expuesto como estadística (p50, p95, p99) en `/metrics`.

*Limpieza:*
Borrar `main()` huérfano en `worker.py`. Documentar `raw_value` explícitamente como
reserva para calibración futura.

**Qué añade técnicamente:** transforma el sistema de "funciona en mi máquina" a "funciona
y puede demostrarlo". Tests de integración son distintos de unit tests — verifican el
contrato del sistema, no el comportamiento de componentes individuales.

**Qué riesgo evita:** sin tests E2E, cualquier cambio en el pipeline puede romper la
garantía de entrega sin que sea evidente hasta que alguien mire Grafana. Con tests E2E, el
CI detecta la rotura en segundos.

**Qué tradeoff introduce:** los tests de integración son más lentos que los unit tests y
más frágiles ante cambios de interfaz interna. Ese coste es aceptable — son la única forma
de verificar las garantías del sistema sin infraestructura externa.

**Qué NO hacer:**
No implementar CI/CD completo con deployment automatizado — no hay entorno de producción.
Un GitHub Actions que ejecute los tests en cada push es suficiente. No añadir nuevas
features a `SensorReading` mientras esta fase no esté cerrada.

**Valor profesional:** la diferencia entre un engineer que "sabe que funciona" y uno que
"puede demostrar que funciona" es exactamente la diferencia entre junior y mid. Los tests
de integración son la señal más clara de madurez de ingeniería.

---

### FASE B — Señales AC completas + política de retención
**Prioridad: HIGH VALUE.**

**Límite que resuelve:** con 6 señales el límite de SQLite (~490 msg/s) no es operacional.
Con 20 señales a frecuencia real (~1.200 msg/s bajo MQTT caído), el límite se activa sin
aviso. La política de retención hace ese límite explícito y operable antes de que sea un
problema.

**Motivación arquitectónica:** actualmente la limitación "SQLite no escala para AC a alta
frecuencia" es un límite documentado pero no activo — con 6 señales la carga es ~95 msg/s.
Con 20 señales (incluyendo `wheelSlip`, `accG`, `suspensionTravel` a ~60 Hz efectivo), la
carga sube a ~1.200 msg/s cuando MQTT cae. SQLite no puede absorber eso. La expansión de
señales hace el límite operacionalmente real.

**Qué construir:**

*Política de retención por criticidad de señal:*
Clasificar cada señal en una de dos categorías:

```
CRITICAL: rpms, brake, throttle, gear, speed_kmh
  → Store & Forward garantizado
  → Pérdida de datos en fallo inaceptable

BEST_EFFORT: tyre_temp, wheel_slip, acc_g, suspension, fuel
  → Descarte aceptado bajo presión de MQTT
  → La serie tiene gaps pero no hay garantía prometida
```

La clasificación se configura en `config/ac_signals.yaml` — el mismo patrón que
`sensors.yaml` para OPC UA. El worker consulta la clasificación antes de decidir si
persistir en SQLite o descartar.

*Validación de rango en `from_ac_packet()`:*
RPM > 20.000 es físicamente imposible en un coche de carretera. Brake > 1.0 también. Sin
validación de rango, `quality=GOOD` puede acompañar a un valor incorrecto derivado de un
error en el struct offset (un campo `_fields_` en el orden incorrecto). La validación de
rango es la segunda línea de defensa contra errores de struct.

*Configuración unificada para señales AC:*
`config/ac_signals.yaml` define: nombre de señal, sensor_id, umbral de emisión, unidad,
criticidad, rango válido [min, max]. Elimina el hardcoding en `ac_emitter.py`.

**Qué añade técnicamente:** introduce el concepto de QoS diferenciada por señal dentro de
un mismo pipeline. El worker necesita consultar la clasificación de cada mensaje para tomar
la decisión de persistencia — esto añade una lógica de routing simple pero conceptualmente
importante.

**Qué riesgo evita:** evita que la expansión de señales reviente SQLite sin aviso. La
política de retención hace el límite explícito y operable antes de que sea un problema.

**Qué tradeoff introduce:** `BEST_EFFORT` significa que hay señales que pueden perderse
durante un fallo de MQTT. Eso tiene que estar documentado claramente en el contrato del
sistema. Un engineer que lea `/health: healthy` tiene que entender que eso no garantiza que
`wheel_slip` no tuvo gaps en los últimos 30 segundos.

**Qué NO hacer:** no implementar MQTT QoS 2 (exactly-once) — el overhead de handshake es
inaceptable para telemetría a ~100 msg/s. No implementar un segundo worker para señales
CRITICAL — el worker actual tiene margen de 87x antes de ser el cuello de botella.

**Valor profesional:** diseño de QoS diferenciada en un pipeline de telemetría es exactamente
el tipo de decisión que toma un engineer en sistemas industriales o de motorsport reales.
No es una feature — es una decisión de arquitectura con consecuencias medibles.

---

### FASE D — Reproducibilidad del entorno
**Prioridad: MUST HAVE antes de cualquier expansión de funcionalidad.**

**Límite que resuelve:** el sistema es técnicamente correcto pero no reproducible. Un
proyecto no reproducible no es un portfolio — es un repositorio privado que requiere al
autor presente para ejecutarse. Esta fase cierra la brecha entre "el sistema funciona" y
"el sistema puede ser evaluado por un tercero".

La Fase D se adelanta a la expansión de dominio porque la demostrabilidad tiene más valor
inmediato que las features avanzadas. Un sistema con Docker Compose, datos industriales
realistas y un README técnico honesto comunica más capacidad que un sistema con análisis
sofisticado que no se puede ejecutar en 5 minutos.

**Motivación:** actualmente el proyecto requiere conocimiento implícito sobre cómo arrancar
cada componente. Eso es deuda operacional que hace el proyecto no demostrable en una
entrevista o revisión técnica.

**Qué construir:**

*Docker Compose para el stack completo:*
```yaml
services:
  mosquitto:     broker MQTT — imagen oficial, configuración mínima
  influxdb:      persistencia — imagen oficial, bucket y token pre-configurados
  telegraf:      bridge MQTT → InfluxDB — configuración como volumen
  grafana:       visualización — dashboard importado via provisioning
  opc-ua-sim:    servidor OPC UA simulado — imagen propia, datos industriales realistas
  gateway:       el sistema — imagen propia construida desde el repo
```

El gateway y el servidor OPC UA son las únicas imágenes que hay que construir. El resto
son imágenes oficiales parametrizadas. El stack debe arrancar con un único comando y estar
listo en menos de 60 segundos.

*Servidor OPC UA simulado integrado:*
Un servidor OPC UA embebido en el stack que expone nodos con datos industriales con
comportamiento físicamente plausible: temperatura con variación lenta, presión con ciclos,
señales de proceso con ruido. No datos aleatorios — datos que parecen provenir de una
planta real. Esto convierte el simulador en un activo técnico demostrable, no en un
generador de números.

Los nodos expuestos deben corresponder exactamente al `sensors.yaml` del gateway — sin
configuración manual adicional después de `docker compose up`.

*Script de demo reproducible:*
Un script que arranca el stack, espera a que esté sano (verificando `/health`), y ejecuta
una inyección de datos controlada. La demo debe ser determinista: mismos datos de entrada,
mismo output observable. El script debe incluir un escenario de fault injection: matar
el broker MQTT, esperar 10 segundos, levantarlo, y verificar que SQLite vació su deuda.
Ese escenario demuestra una garantía real del sistema, no una feature.

*README orientado a un engineer externo:*
- Qué problema resuelve el sistema (3 frases, sin marketing).
- Prerequisitos del entorno (Docker, versión mínima).
- Cómo arrancarlo (`docker compose up`).
- Qué demuestra la demo y cómo verificarlo.
- Las 5 decisiones arquitectónicas más importantes con su razonamiento.
- Los límites conocidos del sistema — con números. La honestidad técnica sobre los límites
  es más convincente que ocultar deuda.

**Qué añade técnicamente:** introduce la separación entre el sistema y su entorno de
ejecución. El gateway no debería saber si corre en un host de desarrollo o en un Docker
network — y con esta fase, no lo sabe. El simulador OPC UA introduce la noción de
"testbed de datos industriales" reutilizable.

**Qué riesgo evita:** evita que el proyecto quede descartado en una revisión técnica por
no poder ejecutarse. Un reviewer que tiene que pedir instrucciones de arranque va a concluir
que el proyecto no está terminado.

**Qué tradeoff introduce:** dockerizar el gateway añade una capa entre el proceso y el
sistema operativo. Los problemas de red entre contenedores (resolución de hostname, MTU,
latencia Docker network vs loopback) son reales y distintos de los problemas del sistema.
Hay que documentarlos, no ignorarlos.

**Qué NO hacer:** no añadir Kubernetes ni Helm charts — no hay justificación para
orquestación con un único nodo de laboratorio. No añadir CD automatizado. No construir
el simulador OPC UA con datos completamente aleatorios — eso no demuestra nada. No
añadir autenticación al stack de demo — la complejidad de certificados OPC UA en un
entorno Docker de demo no vale el tiempo.

**Valor profesional:** la reproducibilidad es una propiedad de ingeniería, no una
conveniencia. Un sistema que requiere conocimiento implícito para ejecutarse tiene coste
de mantenimiento y coste de transferencia de conocimiento no documentados. Docker Compose
no es el objetivo — el objetivo es que el sistema no tenga dependencias ocultas.

---

### FASE E — Simulación industrial realista
**Prioridad: HIGH VALUE para credibilidad IIoT.**

**Límite que resuelve:** el servidor OPC UA simulado de la Fase D genera datos funcionales
pero no físicamente creíbles. Un reviewer con experiencia industrial reconoce
inmediatamente la diferencia entre datos aleatorios y datos que se comportan como una
planta real. La simulación industrial realista convierte el simulador de un fixture de
test en un activo técnico demostrable.

**Motivación arquitectónica:** un sistema de telemetría industrial que solo demuestra
transporte de datos vale menos que uno que demuestra comprensión del comportamiento físico
que está transportando. La diferencia entre "transporto datos de temperatura" y "entiendo
que la temperatura de un motor tiene deriva térmica, tiempo de respuesta y ruido de sensor
característico" es la diferencia entre un integrador de protocolos y un engineer de IIoT.

**Qué construir:**

*Señales con comportamiento físicamente plausible:*

Temperatura: valor base + deriva lenta (tendencia de calentamiento o enfriamiento sobre
minutos) + ruido gaussiano de baja amplitud (ruido de sensor). El valor no cambia de forma
discontinua. Oscilación dentro de rango operacional configurable.

```python
temp = base + drift_rate * t + random.gauss(0, noise_sigma)
```

Presión: comportamiento cíclico que refleja ciclos de proceso (compresor, bomba) con
período configurable. No sinusoide pura — ciclos asimétricos con subida más rápida que
bajada, como en un sistema real.

Vibración: nivel de ruido base con anomalías inyectables. La vibración "normal" tiene
ruido gaussiano. La vibración "con fallo incipiente" añade armónicos de mayor amplitud
en ventanas temporales controladas. Esto permite demostrar que el sistema detecta un
cambio de comportamiento que precede al fallo.

*`quality=BAD` programable:*
El simulador debe poder degradar la calidad de un sensor durante una ventana temporal
configurada (inicio, duración, señales afectadas). Esto simula un sensor desconectado,
con fallo de comunicación, o en mantenimiento. Permite demostrar cómo el sistema gestiona
datos BAD — uno de los comportamientos más importantes de OPC UA en producción.

*Degradación temporal de sensor:*
Un sensor puede degradarse gradualmente: el ruido aumenta, el rango de valores se estrecha,
el valor deriva fuera del rango operacional normal. Esto simula el comportamiento real de
un sensor que está fallando. Con esta feature, la simulación puede demostrar un patrón
que en producción requeriría meses de datos reales para observar.

**Qué añade técnicamente:** desacopla el simulador del gateway — el simulador tiene su
propia lógica de dominio. Introduce el concepto de "datos industrialmente creíbles" como
propiedad del sistema, no solo de los datos. El simulador pasa de ser un mock a ser un
testbed reutilizable.

**Qué riesgo evita:** evita que un reviewer con experiencia descarte el proyecto por tener
datos de temperatura que cambian 30°C en un segundo. La credibilidad técnica se destruye
con detalles físicamente imposibles.

**Qué tradeoff introduce:** la simulación más realista es más compleja de configurar y
mantener. El equilibrio correcto es configurabilidad declarativa (parámetros en YAML) sin
lógica de simulación en código de producción del gateway. El simulador es un componente
separado — su complejidad no contamina el gateway.

**Qué NO hacer:** no implementar modelos físicos de primer principio (ecuaciones
diferenciales de transferencia de calor, CFD, FEA). No usar Modelica ni SimPy. No
construir digital twins completos. El objetivo es "físicamente plausible", no "físicamente
exacto". La diferencia es fundamental: uno requiere conocimiento del dominio, el otro
requiere conocer la planta específica.

No introducir machine learning en la simulación. No generar datos desde modelos
probabilísticos complejos. Un ingeniero de planta tiene que poder leer los parámetros del
simulador y reconocer que corresponden a comportamiento razonable.

**Valor profesional:** el conocimiento de cómo se comportan físicamente las señales
industriales no es trivial. Un engineer que puede diseñar una simulación creíble de
comportamiento de planta demuestra que ha trabajado con sistemas reales, o que tiene la
formación para entender qué hacen los datos que transporta. Esto es lo que distingue a un
engineer de IIoT de un engineer de integración de protocolos.

---

### FASE F — Mantenimiento predictivo básico
**Prioridad: DIFERENCIADOR industrial. Alto valor para posicionamiento profesional.**

**Límite que resuelve:** el sistema actual transporta datos y los almacena, pero no produce
ningún análisis online. Un sistema de telemetría que solo almacena datos para análisis
posterior no resuelve el problema de detección temprana de fallos — que es exactamente el
problema que justifica la monitorización continua en planta.

**Motivación arquitectónica:** condition monitoring en sistemas industriales reales (líneas
de producción, turbinas, compresores) no es solo almacenar series temporales. Es detectar
desviaciones del comportamiento normal mientras el sistema está operando. La diferencia
entre análisis histórico ("¿cuándo falló?") y monitorización online ("¿va a fallar?") es
el núcleo del mantenimiento predictivo. Esta fase introduce ese análisis online de forma
mínima y justificada.

La comparación funcional de este módulo con plataformas como OSIsoft PI System o Siemens
MindSphere no es exagerada: ambas hacen exactamente esto — análisis estadístico online de
señales de proceso para detección de anomalías — con más infraestructura y a mayor escala.
La diferencia es la escala, no el concepto.

**Qué construir:**

*Análisis estadístico online por señal:*
Para cada señal monitoreada, mantener en memoria un buffer de ventana deslizante de N
muestras (N configurable por señal en el YAML). Calcular sobre ese buffer:

```
media_móvil(t) = mean(buffer[-N:])
desv_std(t)    = std(buffer[-N:])
umbral_sup(t)  = media_móvil(t) + k * desv_std(t)
umbral_inf(t)  = media_móvil(t) - k * desv_std(t)
```

Donde `k` es el factor de umbral configurable (típicamente k=2 para ~95% de confianza
bajo distribución normal). El umbral es dinámico — se adapta al comportamiento reciente
de la señal, no a un umbral estático configurado offline.

*Clasificación de anomalías — puntuales vs persistentes:*
Una anomalía es puntual si el valor vuelve al rango normal en la siguiente muestra. Es
persistente si permanece fuera del rango durante M muestras consecutivas (M configurable).
Solo las anomalías persistentes generan alerta. Las puntuales se loguean como WARNING pero
no se elevan — en sistemas industriales reales, los glitches puntuales de sensor son
frecuentes y no son señal de fallo.

*Endpoint `/alerts`:*
```json
{
  "alerts": [
    {
      "sensor_id": "planta1.maquina1.temperatura",
      "timestamp": "2026-05-10T09:15:00+00:00",
      "value": 94.3,
      "mean": 67.1,
      "std": 2.4,
      "threshold": 71.9,
      "type": "persistent",
      "duration_samples": 8
    }
  ],
  "window_size": 60,
  "k_factor": 2.0
}
```

El endpoint devuelve las alertas activas en el momento de la consulta. No hay historial
de alertas — ese es trabajo de InfluxDB. El gateway solo mantiene el estado online.

*Integración con el pipeline existente:*
El análisis se ejecuta en el worker, después de `destination.send()` y antes de volver a
`queue.get()`. No bloquea el pipeline — el cálculo estadístico sobre un buffer en memoria
es O(N) con N pequeño (~60 muestras), despreciable frente a la latencia de MQTT.

**Qué añade técnicamente:** introduce estado de análisis en el gateway. El gateway pasa
de ser un transductor sin estado (recibe, valida, envía) a un componente con memoria de
corto plazo de cada señal. Esto es una decisión arquitectónica, no una feature — tiene
implicaciones en el consumo de memoria, en el comportamiento bajo reconexión (¿el buffer
se pierde si el gateway reinicia?), y en la separación de responsabilidades.

**Qué riesgo evita:** evita que el proyecto se perciba como "otro pipeline de telemetría".
La detección de anomalías online es el primer paso hacia condition monitoring real — y
es exactamente lo que diferencia un sistema de monitorización de un sistema de logging.

**Qué tradeoff introduce:** el análisis online vs el análisis histórico (queries de
InfluxDB) son complementarios, no equivalentes. El análisis online detecta anomalías con
latencia de segundos pero pierde el contexto histórico cuando el gateway reinicia. El
análisis histórico tiene latencia de minutos pero puede correlacionar señales a lo largo
de semanas. La implementación correcta usa ambos — esta fase introduce el online, las
fases históricas ya están en InfluxDB.

El buffer en memoria hace que el sistema tenga estado que no se persiste. Si el gateway
reinicia, el buffer se vacía y los primeros N muestras post-reinicio no tienen análisis.
Esto es correcto y debe estar documentado explícitamente.

**Qué NO hacer:** no implementar modelos ML (isolation forest, autoencoders, LSTM para
predicción). No usar TensorFlow, PyTorch, scikit-learn. No implementar Kafka Streams ni
Spark Streaming. No construir alertas complejas con correlación de múltiples señales.
El objetivo es media móvil + desviación estándar + umbral dinámico. Nada más.

No persistir el historial de alertas en el gateway — eso es responsabilidad de InfluxDB.
No implementar notificaciones push (email, Slack, PagerDuty) — eso requiere integración
externa que no está en el scope del sistema edge.

**Valor profesional:** condition monitoring con análisis estadístico online es una
competencia central en IIoT industrial. Los ingenieros de planta no quieren machine
learning black-box — quieren poder explicar por qué sonó una alarma. Media ± 2σ es
explicable, auditable y tiene décadas de uso en control estadístico de proceso (SPC).
Implementar esto correctamente demuestra comprensión del dominio industrial, no solo
conocimiento de frameworks ML.

---

### FASE G — Análisis de sesión motorsport
**Prioridad: DIFERENCIADOR de dominio. Especialización avanzada sobre sistema maduro.**

**Límite que resuelve:** el dominio motorsport del sistema transporta datos de telemetría
de vuelta pero no tiene estructura de sesión. Los datos de rpm, temperatura de neumático
o frenada llegan a InfluxDB como una serie temporal sin contexto de vuelta o sesión. Eso
hace que las preguntas de análisis de racing (delta entre vueltas, comportamiento por
sector, comparación entre sesiones) no puedan responderse directamente.

**Contexto estratégico:** la analítica motorsport no es la siguiente evolución natural del
sistema — es una especialización de dominio sobre una plataforma industrial ya madura.
El sistema llega a esta fase habiendo demostrado reproducibilidad (Fase D), credibilidad
de datos (Fase E), y capacidad de análisis online (Fase F). La capa motorsport es entonces
una extensión semántica de dominio, no un cambio de arquitectura.

Esta distinción importa. Un sistema que llega a análisis motorsport sin haber demostrado
capacidades industriales convincentes se percibe como un proyecto de hobbyist con un
pipeline encima. Un sistema que llega a motorsport como especialización de una plataforma
industrial madura se percibe como arquitectura de ingeniería real aplicada a un dominio
de alto rendimiento.

**Motivación arquitectónica:** los datos de telemetría de vuelta que llegan ahora a InfluxDB
son una serie temporal sin estructura de sesión. Saber que `rpm=8000` a las 14:23:45 es
útil. Saber que `rpm=8000` en el segundo 34 de la vuelta 7, sector 2, sesión de
clasificación del sábado — eso es análisis de racing real. Esta fase añade esa semántica
sin cambiar el transporte.

**Qué construir:**

*Detección de vuelta desde señales:*
`lapTime` y `lastLap` de `GraphicsData` de AC son los eventos naturales de inicio/fin de
vuelta. Requieren implementar el struct `GraphicsData` en `ac_emitter.py` y añadir un canal
de eventos al pipeline (distinto del canal de señales continuas). Una vuelta es un evento
discreto, no una señal continua — el modelo de dato es diferente.

*Modelo de sesión en InfluxDB:*
Añadir `session_id` y `lap_number` como tags en el schema. Esto cambia la cardinalidad:

```
Antes:  sensor_id × quality × unit = ~600 series con 200 sensores
Después: sensor_id × quality × unit × session_id × lap_number
```

`session_id` es un timestamp de inicio de sesión (fecha-circuito-tipo). `lap_number` es
un entero por sesión. La cardinalidad con 10 vueltas por sesión sigue siendo manejable,
pero hay que calcularla antes de desplegarlo.

*Análisis de delta entre vueltas:*
Query en InfluxDB que calcula `current_lap[signal] - reference_lap[signal]` por sector.
No es una feature de ingeniería — es una query. Pero justifica que el schema esté diseñado
para responder esa pregunta eficientemente.

*Detección de anomalías por sector:*
Umbral estadístico por señal por sector: si `tyre_temp_fl` en el sector 2 está más de 2σ
por encima de la media de las últimas 5 vueltas en ese sector, es una anomalía. Esto puede
implementarse como query de InfluxDB o como procesamiento en el gateway. En el gateway es
más inmediato pero añade lógica de estado. Como query es más simple y aprovecha el
historial ya almacenado.

**Qué añade técnicamente:** introduce análisis de series temporales con semántica de dominio
motorsport. La diferencia entre "datos de telemetría" y "análisis de racing" es exactamente
esta capa. Es lo que distingue un pipeline de telemetría de un sistema de análisis de
rendimiento de vuelta.

**Qué riesgo evita:** evita que el proyecto sea "telemetría sin contexto" — datos que llegan
a InfluxDB pero no responden ninguna pregunta de dominio útil. El análisis de vuelta es la
pregunta de dominio central en motorsport.

**Qué tradeoff introduce:** `session_id` y `lap_number` como tags aumentan cardinalidad.
Con muchas sesiones, esto puede convertirse en un problema operacional en InfluxDB. La
alternativa es calcular el análisis en Python usando el cliente de InfluxDB, en lugar de
en queries directas. Evaluar cuál escala mejor antes de comprometerse con el schema.

**Qué NO hacer:** no construir un dashboard React para visualizar el análisis de vuelta.
Grafana tiene plugins de análisis temporal. El valor está en el análisis, no en la
visualización.

---

### FASE H — Multi-site industrial
**Prioridad: DIFERENCIADOR enterprise IIoT.**

**Límite que resuelve:** el sistema actual gestiona un único origen de datos OPC UA (una
planta, o un simulador). En IIoT industrial real, un gateway centralizado suele agregar
datos de múltiples plantas, líneas o células de producción simultáneamente. Sin esta
capacidad, el sistema no puede ser presentado como una arquitectura de telemetría
industrial a escala de empresa.

**Motivación arquitectónica:** la configuración actual asume implícitamente una única
fuente OPC UA. El `sensors.yaml` no tiene noción de planta — todos los sensores comparten
el mismo namespace. Ampliar a múltiples plantas requiere un modelo de configuración
multi-tenant y aislamiento lógico de señales en el schema de InfluxDB.

**Qué construir:**

*Múltiples servidores OPC UA en la misma instancia del gateway:*
El gateway abre y mantiene suscripciones simultáneas a múltiples endpoints OPC UA. Cada
servidor tiene su propio archivo de configuración:

```
config/
  sensors_planta1.yaml    →  opc.tcp://opc-sim-planta1:4840/
  sensors_planta2.yaml    →  opc.tcp://opc-sim-planta2:4840/
```

La reconexión exponencial opera de forma independiente por servidor — un fallo de OPC UA
en planta2 no afecta a la suscripción de planta1.

*Aislamiento lógico en el naming:*
El `sensor_id` ya usa jerarquía ISA-95 (`planta1.maquina1.temperatura`). Con multi-site,
el primer componente de la jerarquía discrimina la planta. No hay cambio de schema en
InfluxDB — el aislamiento es semántico, en el `sensor_id`.

*Variables dinámicas en Grafana:*
Dashboard parametrizado por `$planta` como variable dinámica. La query `SHOW TAG VALUES
FROM sensor_readings WITH KEY = "sensor_id"` permite que Grafana descubra dinámicamente
qué plantas tiene el sistema, sin hardcoding en el dashboard.

*Impacto en cardinalidad:*
Multiplicar el número de plantas multiplica el número de series activas en InfluxDB. Con
2 plantas de 50 sensores: 2 × 150 = ~300 series. Con 5 plantas: ~750 series. Dentro del
margen operacional del hardware de laboratorio (~100.000 series). Pero hay que calcularlo
antes de desplegarlo.

**Qué añade técnicamente:** introduce el concepto de multi-tenancy en el gateway. El
gateway pasa de ser "el gateway de la planta" a ser "la plataforma de telemetría de la
empresa". Este es el salto conceptual más importante entre un proyecto de laboratorio y
una arquitectura enterprise.

**Qué riesgo evita:** evita que el sistema sea percibido como un prototipo de sensor
único. Multi-site es la demostración de que la arquitectura escala en número de orígenes,
no solo en número de señales por origen.

**Qué tradeoff introduce:** cada servidor OPC UA adicional añade una tarea asyncio y
consume recursos del event loop. Con 2–3 plantas en un nodo de laboratorio, el margen es
amplio (~87x en throughput MQTT). Con 10–20 plantas, el modelo de single-process puede
empezar a mostrar presión. Ese límite debe medirse, no anticiparse.

La configuración multi-fichero requiere un mecanismo de recarga. Si el operador añade
una planta nueva, ¿requiere reiniciar el gateway? Sí, en esta fase. Recarga en caliente
es complejidad que no está justificada todavía.

**Qué NO hacer:** no implementar microservicios con un gateway por planta. No añadir un
service mesh. No añadir descubrimiento dinámico de servidores OPC UA. No implementar
autenticación por planta todavía. El aislamiento lógico por `sensor_id` es suficiente
para esta fase — aislamiento físico (redes separadas, credentials separadas) es
infraestructura enterprise que requiere un entorno de producción real.

**Valor profesional:** multi-site es el escenario habitual en IIoT enterprise. Las
plataformas de telemetría industrial (Azure IoT Hub, AWS IoT Core, AVEVA PI System) están
diseñadas para este escenario. Demostrar que la arquitectura lo soporta — aunque sea con
dos servidores simulados — demuestra comprensión de los requisitos reales del mercado.

---

### FASE I — Edge vs Cloud
**Prioridad: AVANZADO. Posicionamiento arquitectónico.**

**Límite que resuelve:** el sistema actual tiene un único destino (broker MQTT local →
InfluxDB local). En arquitecturas IIoT modernas, el edge y el cloud tienen
responsabilidades distintas y complementarias. El edge procesa y filtra en tiempo real;
el cloud agrega y analiza a escala. Sin esta separación, el sistema no puede articular
por qué existe el edge si hay cloud disponible.

**Motivación arquitectónica:** "edge computing" como concepto carece de valor si no se
puede demostrar qué hace el edge que el cloud no puede hacer, y vice versa. El edge tiene
ventajas en latencia, resiliencia ante conectividad intermitente, y reducción de volumen
de datos. El cloud tiene ventajas en capacidad de cómputo histórico, almacenamiento a
largo plazo y correlación cross-site. La arquitectura correcta usa ambos, con
responsabilidades distintas y explícitas.

**Qué construir:**

*Segundo destino en el Strategy Pattern:*
El `BaseDestination` ABC ya permite múltiples implementaciones. Esta fase añade
`CloudDestination` como segundo destino. El worker puede configurarse para enviar a
destinos distintos según criterios configurables:

```
Destino MQTT local:   todos los datos raw → InfluxDB edge
Destino cloud:        agregados por ventana temporal → endpoint HTTP mock
```

La separación no requiere duplicar el pipeline — requiere añadir lógica de routing en
el worker.

*Agregación de ventana temporal:*
En lugar de enviar cada dato al cloud, el gateway calcula estadísticas por ventana
(p.ej. cada 60 segundos): media, min, max, count, p95 por señal. Solo esos agregados
van al cloud. Esto reduce el volumen de datos en ~99% para señales a alta frecuencia.

*Mock cloud endpoint:*
Un servidor HTTP simple (puede ser el propio gateway en un puerto alternativo, o un
contenedor Python con FastAPI) que acepta POSTs de agregados y los loguea. No hace falta
un cloud real — hace falta demostrar que el gateway sabe separar lo que va al edge de lo
que va al cloud.

*Separación semántica documentada:*
```
Edge:  datos raw, latencia ~ms, retención días/semanas, análisis online (Fase F)
Cloud: agregados, latencia minutos, retención meses/años, análisis histórico cross-site
```

Esta separación debe estar en el README y en el código — no solo en la documentación.

**Qué añade técnicamente:** introduce la noción de responsabilidades diferenciadas en la
arquitectura de destinos. El gateway pasa de tener un único destino a tener una política
de routing por tipo de dato. Esto refleja arquitecturas IIoT modernas como AWS IoT
Greengrass (edge processing + cloud sync) o Azure IoT Edge.

**Qué riesgo evita:** evita que el sistema sea percibido como "un gateway MQTT con
InfluxDB" — que es un stack commodity. La separación edge/cloud articulada y demostrable
posiciona el sistema como arquitectura IIoT, no como integración de herramientas.

**Qué tradeoff introduce:** dos destinos implican dos políticas de reliability. ¿Qué pasa
si el cloud endpoint no está disponible? ¿Se almacenan los agregados en SQLite? ¿Se
descartan? La política tiene que ser explícita. La recomendación para esta fase es discard
on cloud failure — los agregados se pueden recalcular desde InfluxDB edge si hace falta.
Eso simplifica el sistema y documenta la asimetría: el edge es la fuente de verdad.

**Qué NO hacer:** no usar AWS IoT Core, Azure IoT Hub, ni ningún servicio cloud real —
añaden costes, autenticación, y dependencias externas que oscurecen la arquitectura. El
mock es suficiente para demostrar el concepto. No usar Kubernetes para el cloud endpoint.
No implementar sincronización bidireccional (cloud → edge) — eso requiere un modelo de
conflictos que no está en scope.

**Valor profesional:** la capacidad de articular y demostrar la separación edge/cloud es
una de las competencias más valoradas en arquitectura IIoT. No porque edge/cloud sea
complicado de implementar, sino porque requiere entender las responsabilidades de cada
capa — algo que muchos engineers de software generalistas no tienen claro.



Las siguientes ideas no tienen cabida en este proyecto. No porque sean malas ideas en
abstracto, sino porque no resuelven ningún límite conocido del sistema y romperían su
identidad.

**Dashboard React + MQTT.js:**
Grafana ya hace el trabajo. Añadir un frontend consume tiempo, añade superficie de
mantenimiento, y desvía el proyecto de su identidad como plataforma de telemetría edge.
Si la motivación es "visualización en tiempo real", el problema real es la arquitectura del
pipeline que alimenta la visualización — no el framework de UI.

**Microservicios:**
El sistema cabe en un proceso asyncio y tiene margen de ~87x antes de necesitar más
capacidad de proceso. Dividir en servicios multiplica la complejidad operacional (red entre
servicios, orquestación, descubrimiento) sin añadir nada al throughput para la carga actual.

**Kafka / Redis como sustituto de MQTT:**
MQTT aguanta 8.300 msg/s con broker local. La carga actual es ~95 msg/s. No hay problema
a resolver. Kafka añadiría un componente con curva de operación significativa para comprar
throughput que no se necesita.

**Cloud deployment (AWS IoT Core, Azure IoT Hub):**
Edge gateway significa que vive en el edge. Cloud como destino primario del pipeline
contradice la identidad del proyecto. Si se quiere explorar cloud, que sea como backend de
análisis histórico (leer de InfluxDB, procesar en cloud), no como reemplazo del pipeline
de transporte.

**OpenTelemetry / Jaeger / Prometheus:**
Los dos endpoints HTTP actuales son suficientes para el estado del sistema. OpenTelemetry
añadiría una dependencia significativa para reemplazar lo que ya funciona. Cuando el
sistema tenga múltiples instancias del gateway o un backend de análisis distribuido,
OpenTelemetry tendrá sentido.

**Múltiples workers paralelos:**
No hay justificación con 87x de margen. Si el cuello de botella del sistema llegara a ser
el worker (los benchmarks lo demostrarían), la solución primero es optimizar el worker
existente, no añadir más.

---

## CHECKLIST DE INGENIERÍA

Este checklist no es decorativo. Si no puedes responder una pregunta sin mirar el código,
esa pregunta es lo que tienes que resolver antes de continuar.

### Asyncio y event loop

- [x] Explica qué hace el event loop en términos de cuándo cede el control entre corutinas
- [x] Sabes cuándo usar `create_task()` vs `await corutina()` y las consecuencias de cada decisión
- [x] Sabes cuándo un `await` puede bloquear el event loop y cuándo no
- [x] Sabes la diferencia entre `put()` y `put_nowait()` y cuándo elegir cada uno
- [x] Sabes implementar shutdown limpio con `CancelledError` sin perder mensajes
- [x] Sabes por qué `datagram_received` es síncrono y qué implica para `queue.put()`
- [x] Sabes qué hace `TaskGroup` cuando una tarea lanza excepción
- [ ] El shutdown del sistema está bajo control explícito, no dependiendo del comportamiento implícito de `TaskGroup` *(deuda activa)*

### Contrato de datos

- [x] Explica por qué un timestamp naive es un problema en un sistema distribuido
- [x] Sabes qué pasa con NaN en una query de promedio en InfluxDB
- [x] Sabes en qué punto del pipeline ocurre la validación y por qué ahí y no en otro sitio
- [x] Sabes la diferencia entre `ValidationError` (dato rechazado) y `quality=BAD` (dato marcado pero válido)
- [x] Sabes por qué `opc_node_id=None` es correcto para señales AC, no un campo de error
- [ ] Las señales AC tienen validación de rango en `from_ac_packet()` *(pendiente Fase B)*

### Diseño de infraestructura

- [x] Sabes qué es un tag vs un field en InfluxDB y las consecuencias de diseño de cada uno
- [x] Sabes calcular la cardinalidad de series de un schema antes de desplegarlo
- [x] Sabes por qué el gateway no escribe directamente a InfluxDB en esta arquitectura
- [x] Sabes qué controla el gateway y qué no controla en el pipeline hasta InfluxDB
- [x] Sabes por qué UDP y no OPC UA para telemetría AC de alta frecuencia
- [x] Sabes por qué `_pack_=4` en el ctypes struct y qué pasa si no está

### Observabilidad

- [x] Distingues entre "el proceso está vivo" y "el sistema está funcionando"
- [x] Sabes qué métricas tienen valor operativo y cuáles son decorativas
- [x] Puedes detectar en Grafana si hubo un período de Store & Forward activo
- [x] Sabes implementar un servidor HTTP en asyncio sin bloquear el event loop
- [x] Sabes la diferencia entre lo que mide `/health` y lo que mide `/metrics`
- [ ] La latencia `received_at - timestamp` está medida y logueada *(deuda activa)*
- [ ] Los contadores de `/metrics` distinguen entre fuente OPC UA y AC *(deuda activa)*

### Reliability

- [x] Sabes cuándo SQLite deja de ser viable y qué lo reemplazaría
- [x] Tienes documentado el throughput máximo con números medidos
- [x] Sabes qué pasa con cada parte del sistema si cae cualquier otro componente
- [x] Sabes implementar reconexión automática MQTT sin reiniciar el proceso
- [ ] El benchmark del worker es reproducible sin infraestructura externa *(deuda activa)*
- [ ] Existe al menos un test de integración del pipeline E2E *(deuda activa — crítica)*

### Escalado

- [x] Añadir un sensor OPC UA requiere solo cambiar `sensors.yaml` + reiniciar
- [x] El schema de InfluxDB está diseñado para el número real de sensores
- [x] Sabes dónde está el cuello de botella del sistema con N sensores
- [x] El mismo pipeline maneja datos industriales y de motorsport sin modificación
- [ ] Existe política de retención por criticidad de señal AC *(pendiente Fase B)*

### Motorsport & IIoT

- [x] Sabes leer Shared Memory de un proceso Windows con ctypes + mmap
- [x] Sabes implementar un emisor UDP con detección de cambios y umbrales por señal
- [x] Sabes por qué UDP es el protocolo correcto para telemetría time-driven de alta frecuencia
- [x] Sabes aplicar naming convention ISA-95 a dos dominios diferentes
- [x] Sabes por qué la frecuencia efectiva de emisión difiere de la frecuencia de polling

---

## STRUCT PHYSICS — REFERENCIA TÉCNICA

```python
class PhysicsData(ctypes.Structure):
    _pack_ = 4
    _fields_ = [
        ("packet_id", ctypes.c_int),
        ("gas", ctypes.c_float),
        ("brake", ctypes.c_float),
        ("fuel", ctypes.c_float),
        ("gear", ctypes.c_int),
        ("rpms", ctypes.c_int),
        ("steer_angle", ctypes.c_float),
        ("speed_kmh", ctypes.c_float),
        ("velocity", ctypes.c_float * 3),
        ("acc_g", ctypes.c_float * 3),
        ("wheel_slip", ctypes.c_float * 4),
        ("wheel_load", ctypes.c_float * 4),
        ("wheels_pressure", ctypes.c_float * 4),
        ("wheel_angular_speed", ctypes.c_float * 4),
        ("tyre_wear", ctypes.c_float * 4),
        ("tyre_dirty_level", ctypes.c_float * 4),
        ("tyre_core_temperature", ctypes.c_float * 4),  # FL, FR, RL, RR
    ]
```

El orden de `_fields_` es crítico. Define el offset de cada campo en memoria. Un campo
fuera de orden produce valores numéricos plausibles pero correspondientes al campo
incorrecto — sin error observable.

---

## ROADMAP TEMPORAL

### Ahora (meses 1-2) — Fase A completa

- Tests de integración E2E con MockDestination
- Shutdown bajo control explícito
- Benchmark reproducible sin broker
- Latencia medida en `/metrics`
- Borrar `main()` huérfano
- Docker Compose funcional (Fase D puede hacerse en paralelo)

### Después (meses 3-4) — Fase B

- Señales AC completas (20 canales)
- `config/ac_signals.yaml` con criticidad y umbrales
- Política de retención CRITICAL / BEST_EFFORT
- Validación de rango en `from_ac_packet()`

### Más adelante (meses 5-8) — Fase C

- `GraphicsData` struct para eventos de vuelta
- `session_id` y `lap_number` en el schema
- Análisis de delta entre vueltas
- Detección de anomalías por umbral estadístico

### Probablemente nunca

- Dashboard React
- Kafka
- Kubernetes
- Múltiples workers
- OpenTelemetry
- Microservicios
- Cloud deployment como destino primario

---

*v4.0 — mayo 2026*
*Reestructuración completa orientada a madurez técnica y evolución arquitectónica.*
*Basada en auditoría del sistema real hasta Fase 11.*
*Roadmap justificado por límites medidos, no por aspiraciones.*
