import { Kafka, logLevel, type Producer, type Admin, type EachMessagePayload } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import type pg from 'pg';
import { eventSchema, envelope } from './events.js';
export const topic = process.env.KAFKA_TOPIC ?? 'taskforge.lifecycle.v1';
export function kafka(brokers = (process.env.KAFKA_BROKERS ?? 'kafka:9092').split(',')) {
  return new Kafka({
    clientId: 'taskforge-events',
    brokers,
    logLevel: logLevel.NOTHING,
    connectionTimeout: 1500,
    requestTimeout: 3000,
    retry: { retries: 2, initialRetryTime: 200, maxRetryTime: 1500 },
  });
}
export async function ensureTopic(admin: Admin, name = topic) {
  await admin.createTopics({
    waitForLeaders: true,
    topics: [
      {
        topic: name,
        numPartitions: 3,
        replicationFactor: 1,
        configEntries: [{ name: 'retention.ms', value: '604800000' }],
      },
    ],
  });
}
export async function health(
  db: pg.Pool,
  component: string,
  available: boolean,
  lag: number | null = null,
) {
  await db.query(
    `INSERT INTO pipeline_health(component,available,lag,error_count) VALUES($1,$2,$3,CASE WHEN $2 THEN 0 ELSE 1 END) ON CONFLICT(component) DO UPDATE SET heartbeat_at=clock_timestamp(),available=EXCLUDED.available,lag=EXCLUDED.lag,error_count=pipeline_health.error_count+CASE WHEN EXCLUDED.available THEN 0 ELSE 1 END`,
    [component, available, lag],
  );
}
export async function publishBatch(
  db: pg.Pool,
  producer: Producer,
  name = topic,
  afterAck?: () => Promise<void>,
) {
  const token = randomUUID();
  const claimed = await db.query(
    `UPDATE outbox_events o SET claim_token=$1,lease_until=clock_timestamp()+interval '30 seconds',attempts=o.attempts+1 FROM (SELECT event_id FROM outbox_events WHERE delivered_at IS NULL AND next_attempt_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<clock_timestamp()) ORDER BY sequence_id FOR UPDATE SKIP LOCKED LIMIT 100) c WHERE o.event_id=c.event_id RETURNING o.*`,
    [token],
  );
  if (!claimed.rowCount) return { claimed: 0, delivered: 0 };
  try {
    const messages = claimed.rows.map((row) => ({
      key: row.aggregate_type + ':' + row.aggregate_id,
      value: JSON.stringify(envelope(row)),
    }));
    await producer.send({ topic: name, acks: -1, messages });
    if (afterAck) await afterAck(); // Fault-injection boundary: acknowledged by Kafka, not yet acknowledged in PostgreSQL.
    const result = await db.query(
      'UPDATE outbox_events SET delivered_at=clock_timestamp(),claim_token=NULL,lease_until=NULL,last_error=NULL WHERE claim_token=$1 AND delivered_at IS NULL',
      [token],
    );
    await health(db, 'publisher', true);
    return { claimed: claimed.rowCount!, delivered: result.rowCount! };
  } catch (error) {
    await db.query(
      `UPDATE outbox_events SET claim_token=NULL,lease_until=NULL,next_attempt_at=clock_timestamp()+LEAST(60,power(2,LEAST(attempts,6))) * interval '1 second',last_error='publication failed; delivery may be duplicated' WHERE claim_token=$1 AND delivered_at IS NULL`,
      [token],
    );
    await health(db, 'publisher', false);
    throw error;
  }
}
export async function applyMessage(
  db: pg.Pool,
  namespace: string,
  input: Pick<EachMessagePayload, 'topic' | 'partition' | 'message'>,
) {
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    let event;
    try {
      event = eventSchema.parse(JSON.parse(input.message.value?.toString() ?? ''));
    } catch {
      // Keep the original Kafka record for replay; persist a non-sensitive poison-record marker before committing its offset.
      await client.query(
        `INSERT INTO consumer_failures(namespace,topic,partition,message_offset,reason) VALUES($1,$2,$3,$4,'invalid lifecycle schema') ON CONFLICT DO NOTHING`,
        [namespace, input.topic, input.partition, input.message.offset],
      );
    }
    if (event) {
      const inserted = await client.query(
        'INSERT INTO processed_events(namespace,event_id) VALUES($1,$2) ON CONFLICT DO NOTHING RETURNING event_id',
        [namespace, event.event_id],
      );
      if (inserted.rowCount && event.project_id) {
        await client.query(
          `INSERT INTO analytics_projection(namespace,project_id,aggregate_type,aggregate_id,state_version,status,body,event_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(namespace,aggregate_type,aggregate_id) DO UPDATE SET state_version=EXCLUDED.state_version,status=EXCLUDED.status,body=EXCLUDED.body,event_id=EXCLUDED.event_id,updated_at=clock_timestamp() WHERE analytics_projection.state_version<EXCLUDED.state_version`,
          [
            namespace,
            event.project_id,
            event.aggregate_type,
            event.aggregate_id,
            event.state_version,
            event.body.status,
            JSON.stringify(event.body),
            event.event_id,
          ],
        );
        await client.query(
          `INSERT INTO analytics_event_counts(namespace,project_id,event_type,total) VALUES($1,$2,$3,1) ON CONFLICT(namespace,project_id,event_type) DO UPDATE SET total=analytics_event_counts.total+1`,
          [namespace, event.project_id, event.event_type],
        );
      }
    }
    await client.query(
      `INSERT INTO consumer_positions(namespace,topic,partition,last_offset) VALUES($1,$2,$3,$4) ON CONFLICT(namespace,topic,partition) DO UPDATE SET last_offset=GREATEST(consumer_positions.last_offset,EXCLUDED.last_offset),updated_at=clock_timestamp()`,
      [namespace, input.topic, input.partition, input.message.offset],
    );
    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}
export async function observedLag(db: pg.Pool, admin: Admin, namespace: string, name = topic) {
  const ends = await admin.fetchTopicOffsets(name);
  const positions = await db.query(
    'SELECT partition,last_offset::text FROM consumer_positions WHERE namespace=$1 AND topic=$2',
    [namespace, name],
  );
  const offsets = new Map(positions.rows.map((r) => [r.partition, BigInt(r.last_offset) + 1n]));
  return ends.reduce(
    (sum, p) =>
      sum +
      Number(
        BigInt(p.high) > (offsets.get(p.partition) ?? BigInt(p.low))
          ? BigInt(p.high) - (offsets.get(p.partition) ?? BigInt(p.low))
          : 0n,
      ),
    0,
  );
}
export async function startConsumer(db: pg.Pool, namespace: string, groupId: string, name = topic) {
  if (!/^[a-zA-Z0-9_-]{1,64}$/.test(namespace)) throw new Error('Invalid projection namespace');
  const client = kafka(),
    admin = client.admin(),
    consumer = client.consumer({
      groupId,
      sessionTimeout: 15000,
      heartbeatInterval: 1500,
      retry: { retries: 5 },
      maxBytesPerPartition: 1048576,
    });
  let running = false,
    closing = false,
    lastHeartbeat = 0;
  const active = () => {
    if (!closing) {
      running = true;
      lastHeartbeat = Date.now();
    }
  };
  consumer.on(consumer.events.GROUP_JOIN, active);
  consumer.on(consumer.events.HEARTBEAT, active);
  for (const event of [consumer.events.STOP, consumer.events.DISCONNECT, consumer.events.CRASH])
    consumer.on(event, () => {
      running = false;
    });
  try {
    await admin.connect();
    await ensureTopic(admin, name);
    await consumer.connect();
    await consumer.subscribe({ topic: name, fromBeginning: true });
    await consumer.run({
      autoCommit: true,
      autoCommitInterval: 1000,
      partitionsConsumedConcurrently: 3,
      eachMessage: async (input) => {
        await applyMessage(db, namespace, input);
      },
    });
  } catch (error) {
    closing = true;
    await consumer.disconnect().catch(() => {});
    await admin.disconnect().catch(() => {});
    throw error;
  }
  let busy = false,
    pending: Promise<void> | undefined;
  const update = async () => {
    if (busy || closing) return;
    busy = true;
    try {
      const lag = await observedLag(db, admin, namespace, name);
      if (!closing)
        await health(
          db,
          'consumer:' + namespace,
          running && Date.now() - lastHeartbeat < 15000,
          lag,
        );
    } catch {
      if (!closing) await health(db, 'consumer:' + namespace, false).catch(() => {});
    } finally {
      busy = false;
    }
  };
  await update();
  const timer = setInterval(() => {
    if (!busy && !closing) pending = update();
  }, 2000);
  timer.unref();
  return {
    consumer,
    admin,
    close: async () => {
      closing = true;
      running = false;
      clearInterval(timer);
      await pending;
      await consumer.stop();
      await consumer.disconnect();
      await admin.disconnect();
      await health(db, 'consumer:' + namespace, false).catch(() => {});
    },
  };
}
