import test from 'node:test';
import assert from 'node:assert/strict';
import { topologicalOrder, jobSchema, canonicalJson } from '../../packages/contracts/src/index.js';
test('Kahn ordering validates diamond, cycles, duplicate keys/edges and unknown references', () => {
  assert.deepEqual(
    topologicalOrder(
      ['A', 'B', 'C', 'D'],
      [
        { from: 'A', to: 'B' },
        { from: 'A', to: 'C' },
        { from: 'B', to: 'D' },
        { from: 'C', to: 'D' },
      ],
    ),
    ['A', 'B', 'C', 'D'],
  );
  for (const [nodes, edges] of [
    [
      ['A', 'B'],
      [
        { from: 'A', to: 'B' },
        { from: 'B', to: 'A' },
      ],
    ],
    [['A', 'A'], []],
    [['A'], [{ from: 'A', to: 'missing' }]],
    [
      ['A', 'B'],
      [
        { from: 'A', to: 'B' },
        { from: 'A', to: 'B' },
      ],
    ],
  ] as [string[], { from: string; to: string }[]][])
    assert.throws(() => topologicalOrder(nodes, edges));
});
test('handler contracts reject arbitrary execution, malformed numbers, oversized arrays and unknown fields', () => {
  const input = {
    queue_id: 'e22e6657-0ea4-4d4c-b971-09d0134846df',
    type: 'sum',
    payload: { numbers: [2, 4] },
  };
  assert.equal(jobSchema.parse(input).timeout_ms, 30000);
  for (const body of [
    { ...input, type: 'shell' },
    { ...input, payload: { numbers: ['4'] } },
    { ...input, payload: { numbers: [Infinity] } },
    { ...input, payload: { numbers: Array(10001).fill(1) } },
    { ...input, unrecognized: true },
  ])
    assert.equal(jobSchema.safeParse(body).success, false);
});
test('request canonicalization ignores property order while preserving changed values', () => {
  assert.equal(
    canonicalJson({ b: 1, a: { d: 3, c: 2 } }),
    canonicalJson({ a: { c: 2, d: 3 }, b: 1 }),
  );
  assert.notEqual(canonicalJson({ a: 1 }), canonicalJson({ a: 2 }));
});
