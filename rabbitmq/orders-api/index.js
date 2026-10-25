const express = require('express');
const amqp = require('amqplib');
const { randomUUID } = require('crypto');

const PORT = process.env.PORT || 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://admin:admin@rabbitmq:5672';
const QUEUE = process.env.RABBITMQ_QUEUE || 'pedidos';

let channel = null;
let orderSeq = 1000;

const log = (...args) => console.log(new Date().toISOString(), '[orders-api]', ...args);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// depends_on no garantiza que RabbitMQ acepte conexiones: se reintenta.
async function connectRabbit() {
  for (let attempt = 1; ; attempt++) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      connection.on('close', () => {
        log('Conexion con RabbitMQ cerrada, reconectando...');
        channel = null;
        setTimeout(connectRabbit, 2000);
      });
      connection.on('error', (err) => log('Error de conexion RabbitMQ:', err.message));

      const ch = await connection.createConfirmChannel();
      await ch.assertQueue(QUEUE, { durable: true });
      channel = ch;
      log(`Conectado a RabbitMQ (${RABBITMQ_URL.replace(/\/\/.*@/, '//***@')}), cola "${QUEUE}" declarada (durable)`);
      return;
    } catch (err) {
      log(`Intento ${attempt}: RabbitMQ no disponible (${err.message}). Reintentando en 3s...`);
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
  return errors;
}

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.json({ status: channel ? 'up' : 'degraded', broker: channel ? 'connected' : 'disconnected' });
});

app.post('/orders', (req, res) => {
  const start = process.hrtime.bigint();
  const errors = validateOrder(req.body);
  if (errors.length) {
    log('Pedido rechazado por validacion:', errors.join('; '));
    return res.status(400).json({ status: 'rejected', errors });
  }
  if (!channel) {
    return res.status(503).json({ status: 'unavailable', message: 'Broker RabbitMQ no disponible' });
  }

  const orderId = `ORD-${++orderSeq}-${Date.now().toString(36).toUpperCase()}`;
  const correlationId = req.get('X-Correlation-Id') || `corr-${randomUUID()}`;
  const message = {
    type: 'ProcessOrder',
    orderId,
    correlationId,
    customerName: req.body.customerName,
    product: req.body.product,
    quantity: req.body.quantity,
    createdAt: new Date().toISOString(),
  };

  // Publicacion "fire and forget" respecto al consumidor: no se espera al worker.
  channel.sendToQueue(QUEUE, Buffer.from(JSON.stringify(message)), {
    persistent: true,
    contentType: 'application/json',
    messageId: orderId,
    correlationId,
    type: message.type,
  });

  const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
  log(`Mensaje publicado en cola "${QUEUE}" orderId=${orderId} correlationId=${correlationId} respuestaEn=${elapsedMs.toFixed(2)}ms`);

  res.status(202).set('X-Correlation-Id', correlationId).json({
    status: 'accepted',
    message: 'Pedido recibido y enviado a RabbitMQ',
    orderId,
  });
});

app.listen(PORT, () => log(`Escuchando HTTP en puerto ${PORT}`));
connectRabbit();
