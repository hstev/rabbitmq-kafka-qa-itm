const { Kafka, logLevel } = require('kafkajs');
const os = require('os');

const SERVICE_NAME = process.env.SERVICE_NAME || 'notification-consumer';
const SERVICE_ACTION = process.env.SERVICE_ACTION || 'Simulando envio de notificacion al cliente';
const BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC = process.env.KAFKA_TOPIC || 'orders.events';
const GROUP_ID = process.env.KAFKA_GROUP_ID || 'notification-service-group';
const MIN_MS = Number(process.env.PROCESSING_MIN_MS || 1000);
const MAX_MS = Number(process.env.PROCESSING_MAX_MS || 2000);
// Identifica la instancia cuando se escala con --scale
const INSTANCE = os.hostname();

const log = (...args) => console.log(new Date().toISOString(), `[${SERVICE_NAME} ${INSTANCE}]`, ...args);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const kafka = new Kafka({ clientId: `${SERVICE_NAME}-${INSTANCE}`, brokers: [BROKER], logLevel: logLevel.WARN });
const consumer = kafka.consumer({ groupId: GROUP_ID });
let processed = 0;

consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
  const assigned = JSON.stringify(payload.memberAssignment);
  log(`Unido al consumer group "${GROUP_ID}" particiones asignadas=${assigned}`);
});

// Si kafkajs no puede reiniciar el consumidor, se termina el proceso y Docker lo reinicia.
consumer.on(consumer.events.CRASH, ({ payload }) => {
  log(`Consumidor caido: ${payload.error.message} (reinicio interno=${payload.restart})`);
  if (!payload.restart) process.exit(1);
});

async function start() {
  for (let attempt = 1; ; attempt++) {
    try {
      await consumer.connect();
      // fromBeginning solo aplica si el grupo aun no tiene offsets confirmados.
      await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
      log(`Conectado a Kafka (${BROKER}), suscrito a "${TOPIC}" con groupId="${GROUP_ID}"`);
      break;
    } catch (err) {
      log(`Intento ${attempt}: Kafka no disponible (${err.message}). Reintentando en 3s...`);
      await sleep(3000);
    }
  }

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());
      if (event.eventType !== 'OrderCreated') return;

      log(`Evento recibido: ${event.eventType} orderId=${event.data.orderId} eventId=${event.eventId} correlationId=${event.correlationId} topic=${topic} partition=${partition} offset=${message.offset}`);
      log('Contenido:', JSON.stringify(event));

      const delay = MIN_MS + Math.floor(Math.random() * (MAX_MS - MIN_MS + 1));
      log(`${SERVICE_ACTION} para ${event.data.product} x${event.data.quantity} (${delay}ms)...`);
      await sleep(delay);

      processed++;
      log(`Procesado orderId=${event.data.orderId} partition=${partition} offset=${message.offset} totalProcesadosPorEstaInstancia=${processed}`);
    },
  });
}

async function shutdown(signal) {
  log(`${signal} recibido: confirmando offsets y saliendo del grupo`);
  await consumer.disconnect().catch(() => {});
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

start().catch((err) => {
  log('Error fatal:', err.message);
  process.exit(1);
});
