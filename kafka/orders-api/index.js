const express = require('express');
const { Kafka, logLevel } = require('kafkajs');
const { randomUUID } = require('crypto');

const PORT = process.env.PORT || 3000;
const BROKER = process.env.KAFKA_BROKER || 'kafka:9092';
const TOPIC = process.env.KAFKA_TOPIC || 'orders.events';

const log = (...args) => console.log(new Date().toISOString(), '[orders-api]', ...args);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const kafka = new Kafka({ clientId: 'orders-api', brokers: [BROKER], logLevel: logLevel.WARN });
const producer = kafka.producer();
let connected = false;
let orderSeq = 1000;

producer.on(producer.events.DISCONNECT, () => {
  connected = false;
  log('Productor desconectado de Kafka');
});

// depends_on no garantiza que Kafka acepte conexiones: se reintenta.
async function connectKafka() {
  for (let attempt = 1; ; attempt++) {
    try {
      await producer.connect();
      connected = true;
      log(`Productor conectado a Kafka (${BROKER}), topico "${TOPIC}"`);
      return;
    } catch (err) {
      log(`Intento ${attempt}: Kafka no disponible (${err.message}). Reintentando en 3s...`);
      await sleep(3000);
    }
  }
}

function validateOrder(body) {
  const errors = [];
  if (!body || typeof body !== 'object') return ['El cuerpo debe ser un JSON'];
  if (typeof body.customerName !== 'string' || !body.customerName.trim()) errors.push('customerName es obligatorio');
  if (typeof body.product !== 'string' || !body.product.trim()) errors.push('product es obligatorio');
  if (!Number.isInteger(body.quantity) || body.quantity <= 0) errors.push('quantity debe ser un entero positivo');
  if (typeof body.unitPrice !== 'number' || body.unitPrice < 0) errors.push('unitPrice debe ser un numero >= 0');
  return errors;
}

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.json({ status: connected ? 'up' : 'degraded', broker: connected ? 'connected' : 'disconnected' });
});

app.post('/orders', async (req, res) => {
  const start = process.hrtime.bigint();
  const errors = validateOrder(req.body);
  if (errors.length) {
    log('Pedido rechazado por validacion:', errors.join('; '));
    return res.status(400).json({ status: 'rejected', errors });
  }

  const orderId = `ORD-${++orderSeq}-${Date.now().toString(36).toUpperCase()}`;
  const correlationId = req.get('X-Correlation-Id') || `corr-${randomUUID()}`;
  const { customerName, product, quantity, unitPrice } = req.body;
  const event = {
    eventId: `evt-${randomUUID()}`,
    eventType: 'OrderCreated',
    eventVersion: '1.0',
    occurredAt: new Date().toISOString(),
    source: 'orders-api',
    correlationId,
    data: { orderId, customerName, product, quantity, unitPrice, total: quantity * unitPrice },
  };

  try {
    // Se espera solo la confirmacion del broker (acks), nunca a los consumidores.
    const [meta] = await producer.send({
      topic: TOPIC,
      messages: [{
        key: orderId,
        value: JSON.stringify(event),
        headers: { eventType: event.eventType, correlationId },
      }],
    });
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
    log(`Evento OrderCreated publicado topic=${TOPIC} partition=${meta.partition} offset=${meta.baseOffset} orderId=${orderId} eventId=${event.eventId} correlationId=${correlationId} respuestaEn=${elapsedMs.toFixed(2)}ms`);

    res.status(202).set('X-Correlation-Id', correlationId).json({
      status: 'accepted',
      message: 'Pedido recibido y evento OrderCreated publicado en Kafka',
      orderId,
    });
  } catch (err) {
    log(`Error publicando evento orderId=${orderId}: ${err.message}`);
    res.status(503).json({ status: 'unavailable', message: 'Broker Kafka no disponible' });
  }
});

app.listen(PORT, () => log(`Escuchando HTTP en puerto ${PORT}`));
connectKafka();

process.on('SIGTERM', async () => {
  await producer.disconnect().catch(() => {});
  process.exit(0);
});
