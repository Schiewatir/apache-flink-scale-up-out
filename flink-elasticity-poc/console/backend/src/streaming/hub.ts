import type { WebSocket } from 'ws';
import type { StreamMessage } from '@flink-console/shared';

type StreamTopic = StreamMessage['type'];

interface Client {
  socket: WebSocket;
  /** Latest coalesced message per topic waiting to flush to a slow client. */
  pending: Map<StreamTopic, StreamMessage>;
  flushScheduled: boolean;
}

/**
 * Fans out typed stream messages to all connected clients. Under client
 * backpressure it coalesces to the latest message per topic rather than
 * buffering without bound, so a slow client can never exhaust server memory and
 * never delays other clients.
 */
export class StreamHub {
  private readonly clients = new Set<Client>();
  private readonly lastByTopic = new Map<StreamTopic, StreamMessage>();

  add(socket: WebSocket): void {
    const client: Client = { socket, pending: new Map(), flushScheduled: false };
    this.clients.add(client);

    // Replay the latest of each topic so a new client renders immediately.
    for (const message of this.lastByTopic.values()) {
      this.enqueue(client, message);
    }

    socket.on('close', () => this.clients.delete(client));
    socket.on('error', () => this.clients.delete(client));
  }

  broadcast(message: StreamMessage): void {
    this.lastByTopic.set(message.type, message);
    for (const client of this.clients) {
      this.enqueue(client, message);
    }
  }

  private enqueue(client: Client, message: StreamMessage): void {
    // Coalesce: only the most recent message per topic is retained.
    client.pending.set(message.type, message);
    if (!client.flushScheduled) {
      client.flushScheduled = true;
      setImmediate(() => this.flush(client));
    }
  }

  private flush(client: Client): void {
    client.flushScheduled = false;
    if (client.socket.readyState !== client.socket.OPEN) {
      client.pending.clear();
      return;
    }
    // Skip clients whose send buffer is backed up; keep only the latest per topic.
    if (client.socket.bufferedAmount > 1_000_000) {
      client.flushScheduled = true;
      setTimeout(() => this.flush(client), 50);
      return;
    }
    const messages = [...client.pending.values()];
    client.pending.clear();
    for (const message of messages) {
      try {
        client.socket.send(JSON.stringify(message));
      } catch {
        this.clients.delete(client);
        return;
      }
    }
  }

  get clientCount(): number {
    return this.clients.size;
  }
}
