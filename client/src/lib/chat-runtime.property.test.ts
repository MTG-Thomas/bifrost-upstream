import fc from "fast-check";
import { describe, expect, it } from "vitest";
import type { ChatStreamChunk } from "@/services/websocket";
import {
	applyChatStreamEnvelope,
	applyChatStreamEnvelopes,
	type ChatProjection,
	type ChatStreamEnvelope,
} from "./chat-runtime";

/**
 * Property tests for the chat stream projection reducer.
 *
 * `applyChatStreamEnvelopes` is documented as a performance optimisation: it
 * applies one render frame's events against a single clone instead of one
 * clone per event. That makes it observationally equivalent to folding
 * `applyChatStreamEnvelope` over the same input — a contract no example test
 * can cover, because the interesting inputs are rejected and out-of-order
 * envelopes arriving in the same frame as accepted ones.
 *
 * The seed is fixed so the PR run is reproducible and a failure is a real
 * counterexample rather than a flake. A nightly job is expected to rotate the
 * seed and print it; see docs/research/property-based-testing.md.
 */
const SEED = 20261117;
const NUM_RUNS = 100;

const CONVERSATIONS = ["conv-1", "conv-2"] as const;
const RUNS = ["run-0", "run-1", "run-2"] as const;
const EVENT_IDS = ["evt-1", "evt-2", "evt-3", "evt-4", "evt-5"] as const;

/**
 * A conversation-scoped projection with one run already at sequence 3, so a
 * generated envelope can be genuinely stale rather than merely early.
 */
function seedProjection(): ChatProjection {
	return {
		conversation_id: "conv-1",
		messages: [],
		system_events: [],
		runs: {
			"run-0": {
				run_id: "run-0",
				conversation_id: "conv-1",
				status: "streaming",
				last_sequence: 3,
				last_event_id: "evt-3",
				applied_event_ids: ["evt-1", "evt-2", "evt-3"],
				user_message_id: "user-0",
				assistant_message_id: null,
				streaming_message_id: null,
			},
		},
		run_order: ["run-0"],
		active_run_id: null,
		last_sequence: 3,
		context_usage: null,
		context_usage_sequence: 0,
	};
}

const chunkArb: fc.Arbitrary<ChatStreamChunk> = fc.oneof(
	fc.record({
		type: fc.constant("delta" as const),
		content: fc.string({ maxLength: 4 }),
	}),
	fc.record({
		type: fc.constant("message_start" as const),
		user_message_id: fc.constant("user-1"),
		assistant_message_id: fc.constant("assistant-1"),
	}),
	fc.record({
		type: fc.constant("run_status" as const),
		run_status: fc.constantFrom("streaming", "succeeded", "failed"),
	}),
	fc.record({
		type: fc.constant("done" as const),
		run_status: fc.constantFrom("succeeded", "failed"),
	}),
	fc.record({
		type: fc.constant("error" as const),
		error: fc.string({ maxLength: 3 }),
	}),
	fc.record({
		type: fc.constant("title_update" as const),
		title: fc.constant("t"),
	}),
);

const envelopeArb: fc.Arbitrary<ChatStreamEnvelope> = fc.record({
	event_id: fc.option(fc.constantFrom(...EVENT_IDS), { nil: null }),
	sequence: fc.option(fc.integer({ min: -1, max: 6 }), { nil: null }),
	conversation_id: fc.option(fc.constantFrom(...CONVERSATIONS), { nil: null }),
	run_id: fc.option(fc.constantFrom(...RUNS), { nil: null }),
	occurred_at: fc.constant("2026-09-02T00:00:00Z"),
	payload: fc.option(chunkArb, { nil: null }),
});

describe("chat-runtime batch/sequencing properties", () => {
	it("applying a frame in one batch equals folding the same envelopes", () => {
		fc.assert(
			fc.property(
				fc.array(envelopeArb, { maxLength: 6 }),
				(envelopes) => {
					const folded = envelopes.reduce(
						applyChatStreamEnvelope,
						seedProjection(),
					);
					const batched = applyChatStreamEnvelopes(
						seedProjection(),
						envelopes,
					);
					expect(batched).toEqual(folded);
				},
			),
			{ seed: SEED, numRuns: NUM_RUNS },
		);
	});
});
