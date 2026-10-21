import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import pg from 'pg';
import {
  kafka,
  ensureTopic,
  publishBatch,
  startConsumer,
  observedLag,
  applyMessage,
} from '../../apps/events/src/pipeline.js';
import { envelope } from '../../apps/events/src/events.js';
import { testDatabase } from '../support/config.js';
import { startEngine, stopProcess, until } from '../support/processes.js';

test(
  'real Kafka outbox rollback, acknowledgement loss, restart, duplicates, poison events and isolated replay',
  { timeout: 90000 },
  async () => {
    const db = new pg.Pool({ connectionString: testDatabase });
    const publisherDb = new pg.Pool({
      connectionString: testDatabase,
      options: '-c role=taskforge_publisher',
    });
    const consumerDb = new pg.Pool({
      connectionString: testDatabase,
      options: '-c role=taskforge_consumer',
    });
    const name = 'taskforge-test-' + randomUUID(),
      namespace = 'test_' + randomUUID().replaceAll('-', ''),
      replay = 'replay_' + randomUUID().replaceAll('-', '');
    const client = kafka(),
      admin = client.admin();
    let producer = client.producer({ idempotent: true, maxInFlightRequests: 1 });
    let live: Awaited<ReturnType<typeof startConsumer>> | undefined, rebuilt: typeof live;
    const engines: ReturnType<typeof startEngine>[] = [];
    try {
      process.env.ADMIN_DATABASE_URL = testDatabase;
      await import('../../scripts/migrate.js');
      await db.query(
        'TRUNCATE projects,workers,outbox_events,processed_events,consumer_positions,consumer_failures,pipeline_health CASCADE',
      );
      await admin.connect();
      await ensureTopic(admin, name);
      await producer.connect();
      const p = (
        await db.query(
          "INSERT INTO projects(name,token_hash) VALUES('events',md5(random()::text)||md5(random()::text)) RETURNING id",
        )
      ).rows[0];
      const q = (
        await db.query("INSERT INTO queues(project_id,name) VALUES($1,'events') RETURNING id", [
          p.id,
        ])
      ).rows[0];
      const connection = await db.connect();
      let rolledBack: string;
      try {
        await connection.query('BEGIN');
        rolledBack = (
          await connection.query(
            "INSERT INTO jobs(project_id,queue_id,type) VALUES($1,$2,'noop') RETURNING id",
            [p.id, q.id],
          )
        ).rows[0].id;
        await connection.query('ROLLBACK');
      } finally {
        connection.release();
      }
      assert.equal(
        (
          await db.query('SELECT count(*)::integer AS n FROM outbox_events WHERE aggregate_id=$1', [
            rolledBack!,
          ])
        ).rows[0].n,
        0,
        'rolled-back job cannot publish',
      );
      const job = (
        await db.query(
          "INSERT INTO jobs(project_id,queue_id,type) VALUES($1,$2,'noop') RETURNING id",
          [p.id, q.id],
        )
      ).rows[0];
      const failure = (
        await db.query(
          "INSERT INTO jobs(project_id,queue_id,type,max_attempts) VALUES($1,$2,'fail',2) RETURNING id",
          [p.id, q.id],
        )
      ).rows[0];
      const cancelled = (
        await db.query(
          "INSERT INTO jobs(project_id,queue_id,type) VALUES($1,$2,'noop') RETURNING id",
          [p.id, q.id],
        )
      ).rows[0];
      await db.query('SELECT cancel_job($1,$2)', [p.id, cancelled.id]);
      await db.query('SELECT retry_job($1,$2)', [p.id, cancelled.id]);
      engines.push(startEngine('worker'), startEngine('scheduler'), startEngine('scheduler'));
      await until(
        async () =>
          (
            await db.query(
              "SELECT count(*)::integer AS n FROM jobs WHERE id=ANY($1::uuid[]) AND status IN ('COMPLETED','DEAD_LETTERED')",
              [[job.id, failure.id, cancelled.id]],
            )
          ).rows[0].n === 3,
        'real lifecycle completion',
      );
      await Promise.all(engines.map((e) => stopProcess(e.child)));
      engines.length = 0;
      const wf = (
        await db.query(
          "INSERT INTO workflows(project_id,name) VALUES($1,'event-workflow') RETURNING id",
          [p.id],
        )
      ).rows[0];
      await db.query("UPDATE workflows SET status='RUNNING' WHERE id=$1", [wf.id]);
      await db.query("UPDATE workflows SET status='COMPLETED' WHERE id=$1", [wf.id]);
      await db.query(
        "INSERT INTO workers(id,name,capacity,heartbeat_at) VALUES(gen_random_uuid(),'offline-test',1,now()-interval '20 seconds')",
      );
      assert.ok((await publisherDb.query('SELECT report_offline_workers() AS n')).rows[0].n > 0);
      assert.equal(
        (await publisherDb.query('SELECT report_offline_workers() AS n')).rows[0].n,
        0,
        'offline edge is emitted once',
      );
      await assert.rejects(
        publisherDb.query('UPDATE jobs SET priority=1 WHERE id=$1', [job.id]),
        /permission denied/,
      );
      await assert.rejects(
        consumerDb.query('UPDATE jobs SET priority=1 WHERE id=$1', [job.id]),
        /permission denied/,
      );
      const rows = (await db.query('SELECT * FROM outbox_events ORDER BY sequence_id')).rows;
      const kinds = new Set(rows.map((r) => r.event_type));
      for (const kind of [
        'JOB_CREATED',
        'JOB_READY',
        'JOB_STARTED',
        'JOB_COMPLETED',
        'JOB_FAILED',
        'JOB_RETRIED',
        'JOB_CANCELLED',
        'WORKER_OFFLINE',
        'WORKFLOW_COMPLETED',
      ])
        assert.ok(kinds.has(kind), kind);
      assert.ok(
        rows.every(
          (r) => !['payload', 'result', 'error', 'token_hash'].some((key) => key in r.body),
        ),
      );
      await assert.rejects(
        publishBatch(publisherDb, producer, name, async () => {
          throw new Error('simulated publisher crash after Kafka acknowledgement');
        }),
      );
      assert.equal(
        (
          await db.query(
            'SELECT count(*)::integer AS n FROM outbox_events WHERE delivered_at IS NOT NULL',
          )
        ).rows[0].n,
        0,
        'must never mark delivered before durable acknowledgement step',
      );
      await producer.disconnect();
      producer = client.producer({ idempotent: true, maxInFlightRequests: 1 });
      await producer.connect();
      await db.query('UPDATE outbox_events SET next_attempt_at=now()');
      const published = await publishBatch(publisherDb, producer, name);
      assert.equal(published.delivered, rows.length);
      // A disconnected real producer is a broker-path failure, not a mocked successful send.
      await producer.disconnect();
      await db.query("INSERT INTO jobs(project_id,queue_id,type) VALUES($1,$2,'noop')", [
        p.id,
        q.id,
      ]);
      await assert.rejects(publishBatch(publisherDb, producer, name));
      assert.equal(
        (
          await db.query(
            'SELECT count(*)::integer AS n FROM outbox_events WHERE delivered_at IS NULL',
          )
        ).rows[0].n,
        2,
      );
      producer = client.producer({ idempotent: true, maxInFlightRequests: 1 });
      await producer.connect();
      await db.query('UPDATE outbox_events SET next_attempt_at=now()');
      await publishBatch(publisherDb, producer, name);
      await producer.send({
        topic: name,
        acks: -1,
        messages: [
          { key: 'poison', value: '{"schema_version":999}' },
          {
            key: 'oversized-version',
            value: JSON.stringify({ ...envelope(rows[0]!), state_version: '9223372036854775808' }),
          },
        ],
      });
      live = await startConsumer(consumerDb, namespace, 'group-' + namespace, name);
      await until(
        async () => (await observedLag(db, admin, namespace, name)) === 0,
        'live consumer catches up',
        30000,
      );
      const unique = (await db.query('SELECT count(*)::integer AS n FROM outbox_events')).rows[0].n;
      assert.equal(
        (
          await db.query('SELECT count(*)::integer AS n FROM processed_events WHERE namespace=$1', [
            namespace,
          ])
        ).rows[0].n,
        unique,
        'duplicate publications have one durable effect',
      );
      assert.equal(
        (
          await db.query(
            'SELECT status FROM analytics_projection WHERE namespace=$1 AND aggregate_id=$2',
            [namespace, job.id],
          )
        ).rows[0].status,
        'COMPLETED',
      );
      assert.equal(
        (
          await db.query(
            'SELECT count(*)::integer AS n FROM consumer_failures WHERE namespace=$1',
            [namespace],
          )
        ).rows[0].n,
        2,
      );
      // Deliver an earlier real record after the completed projection to exercise monotonic fencing.
      const old = rows.find((r) => r.aggregate_id === job.id && r.event_type === 'JOB_CREATED')!;
      await applyMessage(consumerDb, namespace, {
        topic: name,
        partition: 0,
        message: {
          offset: '0',
          key: null,
          value: Buffer.from(JSON.stringify(envelope(old))),
          timestamp: '0',
          attributes: 0,
          headers: {},
        },
      });
      assert.equal(
        (
          await db.query(
            'SELECT status FROM analytics_projection WHERE namespace=$1 AND aggregate_id=$2',
            [namespace, job.id],
          )
        ).rows[0].status,
        'COMPLETED',
      );
      const ordered = namespace + '_order';
      const latest = rows
        .filter((r) => r.aggregate_id === job.id && r.event_type === 'JOB_COMPLETED')
        .at(-1)!;
      for (const record of [latest, old])
        await applyMessage(consumerDb, ordered, {
          topic: name,
          partition: 0,
          message: {
            offset: '0',
            key: null,
            value: Buffer.from(JSON.stringify(envelope(record))),
            timestamp: '0',
            attributes: 0,
            headers: {},
          },
        });
      assert.equal(
        (
          await db.query(
            'SELECT status FROM analytics_projection WHERE namespace=$1 AND aggregate_id=$2',
            [ordered, job.id],
          )
        ).rows[0].status,
        'COMPLETED',
        'unique late records cannot regress projection',
      );
      const bad = envelope(old);
      bad.event_id = randomUUID();
      bad.project_id = randomUUID();
      bad.body.project_id = bad.project_id;
      await assert.rejects(
        applyMessage(consumerDb, namespace, {
          topic: name,
          partition: 0,
          message: {
            offset: '0',
            key: null,
            value: Buffer.from(JSON.stringify(bad)),
            timestamp: '0',
            attributes: 0,
            headers: {},
          },
        }),
        /foreign key/,
      );
      assert.equal(
        (
          await db.query(
            'SELECT count(*)::integer AS n FROM processed_events WHERE namespace=$1 AND event_id=$2',
            [namespace, bad.event_id],
          )
        ).rows[0].n,
        0,
        'projection failure rolls back inbox and position',
      );
      await live.consumer.stop();
      await until(
        async () =>
          (
            await db.query('SELECT available FROM pipeline_health WHERE component=$1', [
              'consumer:' + namespace,
            ])
          ).rows[0]?.available === false,
        'stopped consumer is unavailable despite reachable broker',
      );
      await live.close();
      live = undefined;
      live = await startConsumer(consumerDb, namespace, 'group-' + namespace, name);
      await until(
        async () => (await observedLag(db, admin, namespace, name)) === 0,
        'consumer restart resumes durable positions',
      );
      rebuilt = await startConsumer(consumerDb, replay, 'group-' + replay, name);
      await until(
        async () => (await observedLag(db, admin, replay, name)) === 0,
        'namespaced Kafka replay',
        30000,
      );
      const projection = async (ns: string) =>
        (
          await db.query(
            'SELECT aggregate_type,aggregate_id,state_version::text,status FROM analytics_projection WHERE namespace=$1 ORDER BY aggregate_type,aggregate_id',
            [ns],
          )
        ).rows;
      assert.deepEqual(await projection(namespace), await projection(replay));
      const counts = async (ns: string) =>
        (
          await db.query(
            'SELECT project_id,event_type,total::text FROM analytics_event_counts WHERE namespace=$1 ORDER BY project_id,event_type',
            [ns],
          )
        ).rows;
      assert.deepEqual(await counts(namespace), await counts(replay));
      assert.equal(await observedLag(db, admin, replay, name), 0);
    } finally {
      await Promise.all(engines.map((e) => stopProcess(e.child)));
      await live?.close();
      await rebuilt?.close();
      await producer.disconnect().catch(() => {});
      await admin.disconnect().catch(() => {});
      await Promise.all([db.end(), publisherDb.end(), consumerDb.end()]);
    }
  },
);
