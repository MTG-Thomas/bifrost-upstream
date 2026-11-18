import { Data, Effect, Schema } from "effect";

/**
 * A minimal Effect client for the Bifrost App wire contract: the same
 * requests the React SDK's `BifrostProvider` makes, with no React. The host
 * passes the connection to `mount()`; the bearer token is read from the
 * platform auth bridge first so a token the host has rotated is used.
 */
export const Connection = Schema.Struct({
	baseUrl: Schema.String,
	token: Schema.String,
	maybeOrgScope: Schema.OptionFromNullOr(Schema.String),
	maybeAppId: Schema.OptionFromNullOr(Schema.String),
});
export type Connection = typeof Connection.Type;

export class RequestFailed extends Data.TaggedError("RequestFailed")<{
	readonly status: number;
	readonly detail: string;
}> {}

type PlatformAuthBridge = { getAccessToken: () => string | null };

const currentToken = (connection: Connection): string =>
	(
		globalThis as { __BIFROST_PLATFORM_AUTH_V1__?: PlatformAuthBridge }
	).__BIFROST_PLATFORM_AUTH_V1__?.getAccessToken() ?? connection.token;

const headers = (connection: Connection): Record<string, string> => ({
	"Content-Type": "application/json",
	Authorization: `Bearer ${currentToken(connection)}`,
	...(connection.maybeOrgScope._tag === "Some"
		? { "X-Bifrost-Org": connection.maybeOrgScope.value }
		: {}),
	...(connection.maybeAppId._tag === "Some"
		? { "X-Bifrost-App": connection.maybeAppId.value }
		: {}),
});

const requestJson = <A>(
	connection: Connection,
	path: string,
	schema: Schema.Codec<A, unknown>,
	init: RequestInit = {},
): Effect.Effect<A, RequestFailed> =>
	Effect.tryPromise({
		try: async () => {
			const response = await fetch(
				`${connection.baseUrl.replace(/\/$/, "")}${path}`,
				{ ...init, headers: headers(connection) },
			);
			const body: unknown = await response.json().catch(() => null);
			if (!response.ok) {
				throw new RequestFailed({
					status: response.status,
					detail: JSON.stringify(body ?? response.statusText),
				});
			}
			return body;
		},
		catch: (error) =>
			error instanceof RequestFailed
				? error
				: new RequestFailed({ status: 0, detail: String(error) }),
	}).pipe(
		Effect.flatMap((body) =>
			Schema.decodeUnknownEffect(schema)(body).pipe(
				Effect.mapError(
					(issue) =>
						new RequestFailed({ status: 0, detail: String(issue) }),
				),
			),
		),
	);

// USER

export const CurrentUser = Schema.Struct({
	email: Schema.String,
	name: Schema.optional(Schema.NullOr(Schema.String)),
});
export type CurrentUser = typeof CurrentUser.Type;

export const fetchCurrentUser = (connection: Connection) =>
	requestJson(connection, "/auth/me", CurrentUser);

// WORKFLOWS

export const TERMINAL_STATUSES = [
	"Success",
	"Failed",
	"CompletedWithErrors",
	"Timeout",
	"Cancelled",
];

export const Execution = Schema.Struct({
	execution_id: Schema.String,
	status: Schema.String,
	result: Schema.optional(Schema.Unknown),
	error: Schema.optional(Schema.NullOr(Schema.String)),
	duration_ms: Schema.optional(Schema.NullOr(Schema.Number)),
});
export type Execution = typeof Execution.Type;

const ExecutionDetail = Schema.Struct({
	execution_id: Schema.String,
	status: Schema.String,
	result: Schema.optional(Schema.Unknown),
	error_message: Schema.optional(Schema.NullOr(Schema.String)),
	duration_ms: Schema.optional(Schema.NullOr(Schema.Number)),
});

/** Starts a workflow by id, name, or `path::function` reference. A
 *  `path::function` reference resolves to this App's install. */
export const startWorkflow = (
	connection: Connection,
	workflowRef: string,
	input: Record<string, unknown>,
) =>
	requestJson(connection, "/api/workflows/execute", Execution, {
		method: "POST",
		body: JSON.stringify({
			workflow_id: workflowRef,
			input_data: input,
			...(connection.maybeAppId._tag === "Some"
				? { app_id: connection.maybeAppId.value }
				: {}),
		}),
	});

export const fetchExecution = (connection: Connection, executionId: string) =>
	requestJson(
		connection,
		`/api/executions/${encodeURIComponent(executionId)}`,
		ExecutionDetail,
	).pipe(
		Effect.map(
			(detail): Execution => ({
				execution_id: detail.execution_id,
				status: detail.status,
				result: detail.result,
				error: detail.error_message ?? null,
				duration_ms: detail.duration_ms ?? null,
			}),
		),
	);
