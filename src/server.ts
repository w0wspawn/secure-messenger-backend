import { WebSocketServer, WebSocket } from 'ws';

const PORT = process.env.PORT ? parseInt(process.env.PORT) : 8888;
const wss = new WebSocketServer({ port: PORT });

// Маппинг: Cryptographic ID -> Активный сокет пользователя
const clients = new Map<string, WebSocket>();

// Очередь сообщений для офлайн-пользователей: Receiver ID -> Массив сообщений
interface QueuedMessage {
    payload: string;
    timestamp: number;
}
const offlineQueue = new Map<string, QueuedMessage[]>();
const TTL_24H = 24 * 60 * 60 * 1000; // Время жизни сообщения в очереди (24 часа)

console.log(`WebSocket Relay Server started on port ${PORT}`);

wss.on('connection', (ws: WebSocket) => {
    let clientCryptoId: string | null = null;
    (ws as any).isAlive = true;

    ws.on('pong', () => {
        (ws as any).isAlive = true;
    });

    ws.on('message', (message: string) => {
        try {
            const data = JSON.parse(message);

            // 1. Рукопожатие/Регистрация клиента на сервере
            if (data.type === 'register' && data.id) {
                clientCryptoId = data.id;
                clients.set(clientCryptoId!, ws);
                console.log(`User registered: ${clientCryptoId}`);

                // Доставка офлайн-сообщений, если они есть
                const pendingMessages = offlineQueue.get(clientCryptoId!);
                if (pendingMessages) {
                    console.log(`Delivering ${pendingMessages.length} pending messages to ${clientCryptoId}`);
                    pendingMessages.forEach(msg => {
                        ws.send(msg.payload);
                    });
                    offlineQueue.delete(clientCryptoId!); // Очищаем очередь после доставки
                }
                return;
            }

            // 2. Маршрутизация сообщений (E2EE Chat или Gossip Sync)
            if (data.receiverId && data.payload) {
                const targetSocket = clients.get(data.receiverId);

                if (targetSocket && targetSocket.readyState === WebSocket.OPEN) {
                    // Получатель в сети — мгновенно перенаправляем сообщение
                    targetSocket.send(JSON.stringify({
                        senderId: clientCryptoId,
                        payload: data.payload
                    }));
                } else {
                    // Получатель офлайн — сохраняем сообщение в память на 24 часа
                    console.log(`User ${data.receiverId} is offline. Queueing message.`);
                    const queue = offlineQueue.get(data.receiverId) || [];
                    queue.push({
                        payload: JSON.stringify({ senderId: clientCryptoId, payload: data.payload }),
                        timestamp: Date.now()
                    });
                    offlineQueue.set(data.receiverId, queue);
                }
            }
        } catch (e) {
            console.error('Failed to parse message:', e);
        }
    });

    ws.on('close', () => {
        if (clientCryptoId) {
            clients.delete(clientCryptoId);
            console.log(`User disconnected: ${clientCryptoId}`);
        }
    });
});

// Очистка мертвых соединений (Ping/Pong) каждые 30 секунд
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if ((ws as any).isAlive === false) {
            return ws.terminate();
        }
        (ws as any).isAlive = false;
        ws.ping();
    });
}, 30000);

// Очистка устаревших офлайн-сообщений (старше 24 часов) каждый час
setInterval(() => {
    const now = Date.now();
    offlineQueue.forEach((messages, receiverId) => {
        const freshMessages = messages.filter(msg => now - msg.timestamp < TTL_24H);
        if (freshMessages.length === 0) {
            offlineQueue.delete(receiverId);
        } else {
            offlineQueue.set(receiverId, freshMessages);
        }
    });
}, 3600000);

wss.on('close', () => {
    clearInterval(interval);
});
