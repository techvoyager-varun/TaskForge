import { z } from 'zod';
export const eventSchema = z
  .object({
    event_id: z.uuid(),
    schema_version: z.literal(1),
    event_type: z.enum([
      'JOB_CREATED',
      'JOB_SNAPSHOT',
      'JOB_READY',
      'JOB_ASSIGNED',
      'JOB_STARTED',
      'JOB_COMPLETED',
      'JOB_FAILED',
      'JOB_RETRIED',
      'JOB_CANCEL_REQUESTED',
      'JOB_CANCELLED',
      'WORKER_OFFLINE',
      'WORKFLOW_CREATED',
      'WORKFLOW_SNAPSHOT',
      'WORKFLOW_RUNNING',
      'WORKFLOW_COMPLETED',
      'WORKFLOW_FAILED',
      'WORKFLOW_CANCELLED',
    ]),
    aggregate_type: z.enum(['job', 'workflow', 'worker']),
    aggregate_id: z.uuid(),
    project_id: z.uuid().nullable(),
    state_version: z
      .string()
      .max(19)
      .regex(/^[1-9][0-9]*$/)
      .refine(
        (value) => BigInt(value) <= 9223372036854775807n,
        'Version exceeds PostgreSQL bigint',
      ),
    occurred_at: z.string().datetime({ offset: true }),
    body: z
      .object({ id: z.uuid(), project_id: z.uuid().optional(), status: z.string().optional() })
      .catchall(z.unknown()),
  })
  .strict()
  .superRefine((event, ctx) => {
    if (event.body.id !== event.aggregate_id)
      ctx.addIssue({ code: 'custom', message: 'aggregate ID mismatch' });
    if (
      event.aggregate_type !== 'worker' &&
      (!event.project_id || event.project_id !== event.body.project_id || !event.body.status)
    )
      ctx.addIssue({ code: 'custom', message: 'project/status mismatch' });
  });
export type LifecycleEvent = z.infer<typeof eventSchema>;
export function envelope(row: Record<string, any>): LifecycleEvent {
  return eventSchema.parse({
    event_id: row.event_id,
    schema_version: row.schema_version,
    event_type: row.event_type,
    aggregate_type: row.aggregate_type,
    aggregate_id: row.aggregate_id,
    project_id: row.project_id,
    state_version: String(row.state_version),
    occurred_at: new Date(row.occurred_at).toISOString(),
    body: row.body,
  });
}
