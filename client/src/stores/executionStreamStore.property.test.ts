import fc from "fast-check";
import { beforeEach, describe, expect, it } from "vitest";
import {
	useExecutionStreamStore,
	type StreamingLog,
} from "./executionStreamStore";

/**
 * Property tests for the execution log reorder buffer.
 *
 * The server publishes its first log for an execution with sequence 0:
 * `log_and_broadcast` in api/bifrost/_logging.py counts from 0, and
 * persistence allocates from `coalesce(max(sequence), -1) + 1`. The store is
 * the only participant that assumes a 1-based origin, so these properties are
 * anchored at the origin the server actually uses.
 *
 * The oracle is independent of the reducer: the store must display exactly the
 * contiguous run of delivered sequences starting at the first sequence, in
 * ascending order, with no duplicates — whatever order and batching the
 * websocket delivered them in.
 */
const SEED = 20261118;
const NUM_RUNS = 100;
const FIRST_SEQUENCE = 0;
const EXECUTION = "execution-property";

function log(sequence: number): StreamingLog {
	return {
		level: "INFO",
		message: `line ${sequence}`,
		timestamp: "2026-11-18T00:00:00Z",
		sequence,
	};
}

function resetStream(): void {
	useExecutionStreamStore.setState({ streams: {} });
	useExecutionStreamStore.getState().startStreaming(EXECUTION);
}

function visibleSequences(): number[] {
	return (
		useExecutionStreamStore.getState().streams[EXECUTION]?.streamingLogs ?? []
	).map((entry) => entry.sequence as number);
}

function appendAll(sequences: readonly number[]): void {
	const store = useExecutionStreamStore.getState();
	for (const sequence of sequences) {
		store.appendLogs(EXECUTION, [log(sequence)]);
	}
}

/** The contiguous run of delivered sequences starting at the server's origin. */
function contiguousPrefix(delivered: readonly number[]): number[] {
	const seen = new Set(delivered);
	const prefix: number[] = [];
	for (
		let sequence = FIRST_SEQUENCE;
		seen.has(sequence);
		sequence += 1
	) {
		prefix.push(sequence);
	}
	return prefix;
}

const deliveryArb = fc.array(fc.integer({ min: 0, max: 12 }), {
	maxLength: 24,
});

describe("execution stream reorder properties", () => {
	beforeEach(() => {
		useExecutionStreamStore.setState({ streams: {} });
	});

	it("shows the contiguous prefix of delivered sequences in ascending order", () => {
		fc.assert(
			fc.property(deliveryArb, (delivery) => {
				resetStream();
				appendAll(delivery);
				expect(visibleSequences()).toEqual(contiguousPrefix(delivery));
			}),
			{ seed: SEED, numRuns: NUM_RUNS },
		);
	});

	it("ignores retransmissions of logs it already displayed", () => {
		fc.assert(
			fc.property(deliveryArb, (delivery) => {
				resetStream();
				appendAll(delivery);
				const once = visibleSequences();

				appendAll(delivery);
				expect(visibleSequences()).toEqual(once);
			}),
			{ seed: SEED, numRuns: NUM_RUNS },
		);
	});

	it("displays the same prefix regardless of batch boundaries", () => {
		fc.assert(
			fc.property(
				deliveryArb,
				fc.integer({ min: 1, max: 5 }),
				(delivery, batchSize) => {
					resetStream();
					appendAll(delivery);
					const oneAtATime = visibleSequences();

					resetStream();
					const store = useExecutionStreamStore.getState();
					for (let index = 0; index < delivery.length; index += batchSize) {
						store.appendLogs(
							EXECUTION,
							delivery.slice(index, index + batchSize).map(log),
						);
					}

					expect(visibleSequences()).toEqual(oneAtATime);
				},
			),
			{ seed: SEED, numRuns: NUM_RUNS },
		);
	});

	it("flushes every delivered log in sequence order on completion", () => {
		fc.assert(
			fc.property(deliveryArb, (delivery) => {
				resetStream();
				appendAll(delivery);
				useExecutionStreamStore.getState().completeExecution(EXECUTION);

				const expected = [...new Set(delivery)].sort(
					(left, right) => left - right,
				);
				expect(visibleSequences()).toEqual(expected);
			}),
			{ seed: SEED, numRuns: NUM_RUNS },
		);
	});
});
