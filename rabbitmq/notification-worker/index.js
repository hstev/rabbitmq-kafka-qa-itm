const amqp = require('amqplib');
const os = require('os');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE = process.env.RABBITMQ_QUEUE || 'pedidos';
const MIN_MS = Number(process.env.PROCESSING_MIN_MS || 2000);
const MAX_MS = Number(process.env.PROCESSING_MAX_MS || 5000);
// Identifica la instancia cuando se escala con --scale
const WORKER_ID = os.hostname();

const log = (...args) => console.log(new Date().toISOString(), `[notification-worker ${WORKER_ID}]`, ...args);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let processed = 0;

async function start() {
  for (let attempt = 1; ; attempt++) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      connection.on('close', () => {
        log('Conexion con RabbitMQ cerrada, reconectando...');
        setTimeout(start, 2000);
      });
      connection.on('error', (err) => log('Error de conexion RabbitMQ:', err.message));

      const channel = await connection.createChannel();
      await channel.assertQueue(QUEUE, { durable: true });
      // Un mensaje a la vez por worker: reparte la carga entre instancias.
      await channel.prefetch(1);
      log(`Conectado a RabbitMQ. Escuchando cola "${QUEUE}" (prefetch=1, ack manual)`);

      await channel.consume(QUEUE, async (msg) => {
        if (!msg) return;
        const data = JSON.parse(msg.content.toString());
        const redelivered = msg.fields.redelivered ? ' (REENTREGADO)' : '';
        log(`Mensaje recibido${redelivered} orderId=${data.orderId} correlationId=${data.correlationId}`, JSON.stringify(data));

        const delay = MIN_MS + Math.floor(Math.random() * (MAX_MS - MIN_MS + 1));
        log(`Simulando envio de notificacion a ${data.customerName} (${delay}ms)...`);
        await sleep(delay);

        channel.ack(msg);
        processed++;
        log(`ACK enviado orderId=${data.orderId} correlationId=${data.correlationId} totalProcesadosPorEstaInstancia=${processed}`);
      }, { noAck: false });
      return;
    } catch (err) {
      log(`Intento ${attempt}: RabbitMQ no disponible (${err.message}). Reintentando en 3s...`);
      await sleep(3000);
    }
  }
}

process.on('SIGTERM', () => {
  log('SIGTERM recibido: deteniendo worker (mensajes sin ACK vuelven a Ready)');
  process.exit(0);
});

start();
