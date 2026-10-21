# Lifecycle events and replay

PostgreSQL and the custom C++ scheduler remain responsible for durable job assignment. Kafka carries lifecycle facts to derived analytics. Losing Kafka connectivity delays analytics; it does not block a job transaction after the PostgreSQL outbox insert succeeds.

## Capture and delivery

Migration 005 installs database triggers, so API changes and C++ procedure changes use the same event contract. A rollback removes both the state change and its outbox events. Existing jobs/workflows receive current-state snapshot events during migration. Payloads, results, token hashes and raw error messages are excluded from envelopes. Events have UUID identity, schema_version 1, project/aggregate identity, monotonic aggregate version, timestamp, type and a compact body.

Captured facts include JOB_CREATED, JOB_READY, JOB_ASSIGNED, JOB_STARTED, JOB_COMPLETED, JOB_FAILED, JOB_RETRIED, JOB_CANCEL_REQUESTED, JOB_CANCELLED, workflow transitions and WORKER_OFFLINE. Failed attempts are represented by JOB_FAILED when the job enters retry wait or dead letter. Worker offline detection records a heartbeat threshold crossing once; a subsequent heartbeat permits the next crossing to be reported. Detection runs in the publisher and can be delayed while that process is stopped.

The independently restartable publisher claims up to 100 rows with SKIP LOCKED and a random claim token, committing a 30-second lease before contacting Kafka. It sends aggregate-type/UUID keys to a three-partition topic with acks=all. Only a successful broker acknowledgement permits a conditional PostgreSQL delivery update with that claim token. Failures clear the claim and back off up to 60 seconds. An abandoned claim becomes reclaimable after expiry. An acknowledgement lost between Kafka and PostgreSQL can duplicate publication. Kafka's idempotent producer does not remove that cross-system crash window.

Keys keep an aggregate on one Kafka partition. Multiple publishers, batch return order and retries can still reorder lifecycle versions. Consumers must accept duplicate and out-of-order records; strict lifecycle arrival order is not promised.

## Consumer effects

`apps/events/src/pipeline.ts` maintains a PostgreSQL inbox keyed by namespace/event UUID. Inbox insertion, project-scoped analytics upsert, event counts and consumed offset commit in one database transaction. A projection changes only when the incoming state_version is greater than the stored version. Kafka offsets are committed after the handler succeeds. A crash between the SQL commit and Kafka commit repeats delivery safely. This protects these derived database effects; it does not provide exactly-once external job effects.

Schema-invalid records, including versions outside PostgreSQL bigint, get durable consumer_failures markers before their offsets advance. Original bytes remain in Kafka during retention. Database availability/transaction failures throw and are retried; they do not silently acknowledge an unapplied projection. Fix incompatible schemas and replay into a new namespace to reprocess quarantined records. Alert on failures, lag and outbox age.

`consumer:live` availability depends on consumer lifecycle/heartbeat state as well as successful broker/SQL observation. A reachable admin client does not prove a consumer is running. Observed lag compares Kafka end offsets with durable projection offsets; it can differ briefly from Kafka's group-committed lag. Health expires after 15 seconds. Missing/unavailable lag is null, not zero. Broker lag is shared-deployment metadata; outbox and analytics counts are scoped to the authenticated project. Pipeline heartbeat/error counts are operational shared data.

## Rebuild derived state

For replay within the topic's seven-day retention, choose a fresh namespace and group:

```sh
docker compose run --rm --no-deps -e PROJECTION_NAMESPACE=rebuild_20261021 -e KAFKA_GROUP=taskforge-rebuild-20261021 consumer
```

This command stays running. It rebuilds a separate namespace from the beginning of retained Kafka records. It does not replace the live namespace or change jobs. A reused group resumes its offsets; choose a new group for a fresh rebuild.

For history outside Kafka retention, republish the retained PostgreSQL outbox to a fresh topic:

```sh
docker compose run --rm --no-deps publisher node dist/replay-events.mjs taskforge.replay.audit20261021
docker compose run --rm --no-deps -e KAFKA_TOPIC=taskforge.replay.audit20261021 -e PROJECTION_NAMESPACE=retained_20261021 -e KAFKA_GROUP=taskforge-retained-20261021 consumer
```

The replay tool uses a read-only repeatable-read snapshot, preserves event IDs and leaves delivery markers alone. It refuses an existing replay topic. This produces a consistent retained-event snapshot, not a live subscription to subsequent changes. Long snapshots can delay vacuum; use a maintenance window for large archives. Keep live traffic and replay namespaces separate. Historical projections may retain terminal jobs after the job-retention CLI removes their source rows.

## Deployment boundaries

Compose uses the official Apache Kafka 3.9.1 JVM image in single-node KRaft mode, a persistent volume and no published Kafka port. A one-shot volume initializer assigns its directory to Kafka's non-root UID. Publisher and consumer have separate PostgreSQL credentials and cannot change jobs. Topic retention is seven days; outbox/inbox/projections currently have no automatic retention. Monitor disk growth and design coordinated retention before long-running deployment.

The internal Kafka network uses PLAINTEXT without SASL/ACLs and assumes trusted service hosts. One broker and replication factor one provide persistence across process restart, not broker/disk fault tolerance. Production requires authenticated encrypted transport, broker ACLs, replication, backups and capacity planning. The image and client are pinned by version ranges/tags, not immutable digests.

Tests exercise real broker acknowledgements, transaction rollback, publication failure/restart, acknowledgement loss, duplicate effects, out-of-order unique events, poison schemas, stopped-consumer status, consumer restart, retained outbox replay and independent projection rebuild. See TESTING.md and benchmarks/results for executed evidence.
