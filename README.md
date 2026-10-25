# Pruebas de Integración en Arquitecturas Basadas en Mensajes y Eventos

Laboratorio del taller de Aseguramiento de la Calidad: sistema de pedidos implementado
dos veces, una con **RabbitMQ** (mensajes) y otra con **Apache Kafka** (eventos).
Cada componente corre en su propio contenedor; productor y consumidores solo se
comunican a través del broker.

```
rabbitmq/                      kafka/
├── docker-compose.yml         ├── docker-compose.yml
├── orders-api/   (productor)  ├── orders-api/            (productor OrderCreated)
└── notification-worker/       ├── inventory-consumer/    (inventory-service-group)
                               ├── billing-consumer/      (billing-service-group)
                               └── notification-consumer/ (notification-service-group)
scripts/
├── send-orders.sh             # envía N pedidos con curl y mide tiempo de respuesta
└── postman_collection.json    # requests listos para importar en Postman
evidencias/rabbitmq, evidencias/kafka   # carpeta para guardar las capturas
```

| Arquitectura | Servicios | Puertos en el host |
|---|---|---|
| RabbitMQ | rabbitmq, orders-api, notification-worker | 3000 (API), 5672 (AMQP), 15672 (consola) |
| Kafka | kafka, kafka-init, kafka-ui, orders-api, 3 consumidores | 3000 (API), 29092 (Kafka externo), 8080 (Kafka UI) |

> **Importante:** ambas arquitecturas usan el puerto 3000 y el contenedor `orders-api`.
> Ejecute **una a la vez**: `docker compose down` en una antes de levantar la otra.

Requisitos: Docker + Docker Compose v2, `curl`. Si su Docker requiere `sudo`,
anteponga `sudo` a cada comando `docker`.

---

## Parte A — RabbitMQ

```bash
cd rabbitmq
docker compose up -d --build
docker compose ps                     # evidencia: contenedores y puertos
docker compose logs rabbitmq orders-api notification-worker   # logs de arranque
```

Consola: <http://localhost:15672> (admin / admin) → pestaña **Queues** → `pedidos`.

| Caso | Pasos | Evidencia a capturar |
|---|---|---|
| **CP-01** Creación de pedido | Postman `POST http://localhost:3000/orders` con el body del anexo, o `../scripts/send-orders.sh rabbitmq 1` | Respuesta `202 accepted` con `orderId` |
| **CP-02** Mensaje en la cola | `docker compose stop notification-worker` → enviar 3 pedidos → abrir cola `pedidos` | Contador **Ready = 3**; en *Get messages* el JSON del mensaje |
| **CP-03** Consumo | `docker compose start notification-worker` → `docker compose logs -f notification-worker` | Logs "Mensaje recibido…", "ACK enviado…"; Ready vuelve a 0. Mientras procesa se ve **Unacked = 1** |
| **CP-06** Trazabilidad | `curl -s -X POST localhost:3000/orders -H 'Content-Type: application/json' -H 'X-Correlation-Id: corr-traza-01' -d '{"customerName":"Pedro","product":"Libro","quantity":1}'` → `docker compose logs orders-api notification-worker \| grep corr-traza-01` | Mismo `orderId`/`correlationId` en API, en el mensaje (consola: propiedad `correlation_id`) y en el worker |
| **CP-07** Responsive | `../scripts/send-orders.sh rabbitmq 5` | `tiempo total` en milisegundos, aunque el worker tarda 2–5 s por mensaje; log `respuestaEn=` de la API |
| **CP-08** Message-driven | `docker compose ps` y revisar `docker-compose.yml` / código | El worker **no tiene puertos**; la API solo usa `amqp://…@rabbitmq:5672`; consola → *Connections* muestra API y worker conectados al broker |
| **CP-09** Resilient | `docker compose stop notification-worker` → enviar pedidos | La API sigue respondiendo `202`; mensajes se acumulan en Ready |
| **CP-10** Elastic | `docker compose up -d --scale notification-worker=3` → `../scripts/send-orders.sh rabbitmq 9` → `docker compose logs -f notification-worker` | Cada réplica (hostname distinto) procesa mensajes distintos; consola → *Consumers = 3* |
| **CP-11** Desacoplamiento temporal | Worker apagado → enviar 5 pedidos (captura Ready=5) → `docker compose start notification-worker` | Los 5 se procesan al iniciar; Ready → 0 |
| **CP-12** Recuperación | **(a) Consumidor:** con 1 worker, enviar 3 pedidos y durante el procesamiento `docker compose kill notification-worker` (Unacked → Ready); luego `start` → log `(REENTREGADO)`. **(b) Broker:** worker apagado, enviar 3 pedidos, `docker compose restart rabbitmq`, verificar Ready=3 (cola durable + mensajes persistentes), arrancar worker | Capturas antes/después de los contadores y logs |

Pruebas obligatorias de la sección 8: *consumidor apagado* = CP-09/CP-11,
*múltiples consumidores* = CP-10, *independencia productor-consumidor* = CP-07/CP-08.

Volver a 1 worker: `docker compose up -d --scale notification-worker=1`.
Apagar todo: `docker compose down` (añada `-v` para borrar los mensajes persistidos).

---

## Parte B — Kafka

```bash
cd kafka
docker compose up -d --build
docker compose ps -a                  # kafka-init aparece "Exited (0)": creó el tópico
docker compose logs kafka-init        # tópico orders.events con 3 particiones
docker compose logs orders-api inventory-consumer billing-consumer notification-consumer
```

Kafka UI: <http://localhost:8080> → cluster `local` → **Topics** / **Consumers**.

| Caso | Pasos | Evidencia a capturar |
|---|---|---|
| **CP-01** Creación de pedido | Postman `POST http://localhost:3000/orders` (con `unitPrice`) o `../scripts/send-orders.sh kafka 1` | `202 accepted` con `orderId` |
| **CP-04** Publicación OrderCreated | `docker compose logs orders-api` y Kafka UI → Topics → `orders.events` → *Messages* | Log con `partition=` `offset=`; JSON con `eventType: OrderCreated`, `eventId`, `correlationId`, `data.orderId`; pestaña de particiones (3) |
| **CP-05** Consumo | `docker compose logs inventory-consumer`, luego billing y notification (capturas separadas) | Los tres reciben **el mismo** `orderId`/`eventId`; Kafka UI → *Consumers*: 3 grupos distintos con lag 0 |
| **CP-06** Trazabilidad | Enviar con `-H 'X-Correlation-Id: corr-traza-01'` → `docker compose logs \| grep corr-traza-01` | Mismo `correlationId` en API, en el mensaje (Kafka UI, también en *headers*) y en los 3 consumidores |
| **CP-07** Responsive | `../scripts/send-orders.sh kafka 5` | Respuesta en ms; la API solo espera el ack del broker, no a los consumidores |
| **CP-08** Message-driven | `docker compose ps` / código | Consumidores sin puertos; API solo conoce `kafka:9092` |
| **CP-09** Resilient | `docker compose stop billing-consumer` → enviar pedidos | API sigue en `202`; inventory y notification siguen procesando; en Kafka UI el grupo `billing-service-group` muestra **lag > 0** |
| **CP-10** Elastic | `docker compose up -d --scale inventory-consumer=3` → `../scripts/send-orders.sh kafka 9` → `docker compose logs -f inventory-consumer` | Log "particiones asignadas" distintas por instancia; Kafka UI → grupo con 3 miembros, una partición cada uno |
| **CP-11** Desacoplamiento / consumidor posterior | (a) Con `billing-consumer` detenido publicar 5 eventos y luego `docker compose start billing-consumer`: retoma desde su offset y lag → 0. (b) **Consumidor posterior:** `docker compose --profile late up -d audit-consumer` → `docker compose logs audit-consumer` | (b) Un grupo nuevo (`audit-service-group`) lee **todo el histórico** desde el offset 0: el log de Kafka persiste los eventos |
| **CP-12** Recuperación | **(a) Consumidor:** enviar pedidos y `docker compose kill notification-consumer` durante el procesamiento → `start` → procesa desde el último offset confirmado. **(b) Broker:** detener un consumidor, publicar eventos, `docker compose restart kafka`, esperar healthy, arrancar consumidor | Offsets/lag antes y después; ningún `orderId` perdido |

Pruebas obligatorias de la sección 9: *consumidor posterior* = CP-11 (b),
*múltiples consumidores* = CP-10 (mismo grupo) y CP-05 (grupos diferentes),
*desacoplamiento total* = CP-08/CP-09.

Apagar todo: `docker compose --profile late down` (añada `-v` para borrar el log de eventos).

---

## Notas de diseño (para el análisis técnico)

- **Reintentos de conexión:** `depends_on` + healthchecks ordenan el arranque, y además
  cada servicio reintenta la conexión al broker cada 3 s.
- **RabbitMQ:** cola `pedidos` durable, mensajes `persistent`, `prefetch(1)` y ack manual;
  el worker tarda 2–5 s para poder observar *Unacked*. La API usa un *confirm channel*.
- **Kafka:** el tópico se crea con 3 particiones (`kafka-init`) para que el escalado en el
  mismo grupo reparta carga. La key es el `orderId`. Los consumidores confirman offsets
  tras procesar cada lote; con `docker compose stop` se desconectan limpiamente, con
  `kill` los eventos no confirmados se vuelven a leer (entrega *at-least-once*).
- **Evento vs comando:** RabbitMQ transporta el comando `ProcessOrder`; Kafka publica el
  hecho `OrderCreated` con `eventId`, `eventVersion`, `occurredAt`, `source`, `correlationId`.
