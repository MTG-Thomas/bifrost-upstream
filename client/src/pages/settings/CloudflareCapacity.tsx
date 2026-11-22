import { useEffect, useRef, useState, type FormEvent } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link } from "react-router-dom";

import {
	AlertDialog,
	AlertDialogAction,
	AlertDialogCancel,
	AlertDialogContent,
	AlertDialogDescription,
	AlertDialogFooter,
	AlertDialogHeader,
	AlertDialogTitle,
	AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
	Dialog,
	DialogContent,
	DialogDescription,
	DialogFooter,
	DialogHeader,
	DialogTitle,
	DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ApiError, getErrorMessage } from "@/lib/api-error";
import { GLOBAL_TARGET } from "@/lib/authorization";
import { useAuthorization } from "@/services/authorization";
import {
	configureCloudflareCapacity,
	getCloudflareCapacity,
	rotateCloudflareAccountToken,
	startCloudflareAction,
	type CloudflareAction,
	type CloudflareCapacityStatus,
	type CloudflareConfiguration,
} from "@/services/cloudflareCapacity";
import { observePlatformJob, type PlatformJob } from "@/services/platformJobs";
import { webSocketService } from "@/services/websocket";

const terminal = new Set([
	"succeeded",
	"failed",
	"cancelled",
	"requires_action",
]);

const statusKey = ["cloudflare-capacity"];
const fieldNames: Record<string, string> = {
	worker_ingress_origin: "origin",
	slots_per_launcher: "slots",
	slots_per_tenant: "tenant_slots",
	max_lifetime_seconds: "lifetime",
	linger_seconds: "linger",
	queue_prewarm_per_tenant: "queue_prewarm",
	monthly_limit_usd: "monthly_limit",
	shared_managed_worker_cap: "shared_cap",
	work_kinds: "workflows",
};

type FieldErrors = Record<string, string>;

function validationFields(error: unknown, form: HTMLFormElement): FieldErrors {
	const fields: FieldErrors = {};
	if (!(error instanceof ApiError)) return fields;

	function addField(path: string, message: string) {
		const field = path.split(".").filter((part) => !/^\d+$/.test(part)).at(-1);
		if (!field) return;
		const name = fieldNames[field] ?? field;
		if (form.elements.namedItem(name)) fields[name] = message;
	}

	const reported = error.details?.fields;
	if (reported && typeof reported === "object") {
		for (const [path, message] of Object.entries(reported)) {
			if (typeof message === "string") addField(path, message);
		}
	}
	const issues = error.details?.validation_errors;
	if (Array.isArray(issues)) {
		for (const issue of issues) {
			if (Array.isArray(issue?.loc) && typeof issue.msg === "string") {
				addField(issue.loc.join("."), issue.msg);
			}
		}
	}
	if (error.errorCode === "validation_error" && !reported) {
		// The request-validation handler returns "field.path: message" entries.
		for (const entry of error.message.split(/; (?=[\w.]+: )/)) {
			const [prefix, path] = entry.match(/^([\w.]+): /) ?? [];
			if (prefix) addField(path, entry.slice(prefix.length));
		}
	}
	return fields;
}

function ErrorDetails({ error }: { error: unknown }) {
	if (!(error instanceof ApiError)) return null;
	const { code, reason, remedy } = error.details ?? {};
	const displayCode = typeof code === "string" ? code : error.errorCode;
	return (
		<>
			{!["RequestError", "UnknownError"].includes(displayCode) && <p>Error Code <code>{displayCode}</code></p>}
			{typeof reason === "string" && <p>{reason}</p>}
			{typeof remedy === "string" && <p>{remedy}</p>}
		</>
	);
}

function CloudflareConfigurationForm({ status, onSaved, onCancel, onRefresh, busy }: {
	status: CloudflareCapacityStatus;
	onSaved: (status: CloudflareCapacityStatus) => void;
	onCancel: () => void;
	onRefresh: () => void;
	busy: boolean;
}) {
	const installation = status.installation;
	const settings = installation?.policy;
	const [saving, setSaving] = useState(false);
	const [failure, setFailure] = useState<{ message: string; fields: FieldErrors; cause?: unknown }>();
	const formRef = useRef<HTMLFormElement>(null);
	const summaryRef = useRef<HTMLDivElement>(null);

	useEffect(() => {
		formRef.current?.querySelector("input")?.focus();
	}, []);

	useEffect(() => {
		if (failure) summaryRef.current?.focus();
	}, [failure]);

	function fieldProps(name: string, help?: string) {
		const error = failure?.fields[name];
		return {
			"aria-invalid": error ? true : undefined,
			"aria-describedby": [help, error && `cf-error-${name}`].filter(Boolean).join(" ") || undefined,
		};
	}

	function fieldError(name: string) {
		const error = failure?.fields[name];
		return error && <p className="text-sm text-destructive" id={`cf-error-${name}`}>{error}</p>;
	}
	const [managedTunnel, setManagedTunnel] = useState(false);

	async function save(event: FormEvent<HTMLFormElement>) {
		event.preventDefault();
		if (saving || busy) return;
		const form = event.currentTarget;
		const fields = new FormData(form);
		const invalid: FieldErrors = {};
		for (const field of form.elements) {
			if (field instanceof HTMLInputElement && !field.validity.valid) {
				invalid[field.name] = field.name === "consent"
					? "Accept the Cloudflare terms to connect."
					: field.validity.patternMismatch
						? "Enter a 32-character lowercase hexadecimal ID."
						: field.validationMessage;
			}
		}
		if (Object.keys(invalid).length) {
			setFailure({ message: "Review the highlighted fields.", fields: invalid });
			return;
		}
		const sharedCap = String(fields.get("shared_cap") ?? "");
		const policy = {
			version: 1 as const,
			workers: {
				launcher_cap: Number(fields.get("launcher_cap")),
				slots_per_launcher: Number(fields.get("slots")),
				slots_per_tenant: Number(fields.get("tenant_slots")),
				installation_slots: Number(fields.get("installation_slots")),
				max_lifetime_seconds: Number(fields.get("lifetime")),
				linger_seconds: Number(fields.get("linger")),
				queue_prewarm_per_tenant: Number(fields.get("queue_prewarm")),
				work_kinds: fields.get("workflows") === "on" ? ["workflow" as const] : [],
			},
			monthly_limit_usd: String(fields.get("monthly_limit")),
			shared_managed_worker_cap: sharedCap ? Number(sharedCap) : null,
		};
		const request: CloudflareConfiguration = installation ? {
			action: "policy",
			installation_id: installation.installation_id,
			expected_revision: installation.desired_revision,
			policy,
		} : {
			action: "connect",
			account_id: String(fields.get("account_id")),
			worker_ingress_origin: String(fields.get("origin")),
			reachability_mode: managedTunnel ? "managed_tunnel" : "existing_https_endpoint",
			...(managedTunnel ? { zone_id: String(fields.get("zone_id")) } : {}),
			api_token: String(fields.get("api_token")),
			consent: true,
			consent_version: status.consent_text_version,
			policy,
		};
		const secretInput = form.elements.namedItem("api_token");
		if (secretInput instanceof HTMLInputElement) secretInput.value = "";
		setSaving(true);
		setFailure(undefined);
		try {
			onSaved(await configureCloudflareCapacity(request));
		} catch (cause) {
			const message = getErrorMessage(cause, "Could not save Cloudflare capacity.");
			const rejected = validationFields(cause, form);
			if (message.startsWith("The estimated fleet usage is USD ")) {
				rejected.monthly_limit = `${message}. The submitted limit was ${policy.monthly_limit_usd} USD/month. Raise the limit or reduce the fleet size or lifetime.`;
			}
			setFailure({ message: Object.keys(rejected).length ? "Review the highlighted fields." : message, fields: rejected, cause });
		} finally {
			setSaving(false);
		}
	}

	return (
		<form ref={formRef} className="space-y-4" noValidate onSubmit={save}>
			{failure && (
				<div ref={summaryRef} tabIndex={-1} role="alert" className="space-y-2 rounded-md border border-destructive p-3">
					<h3 className="font-semibold">Cloudflare Settings Error</h3>
					<p>{failure.message}</p>
					<ErrorDetails error={failure.cause} />
					{Object.keys(failure.fields).length > 0 && (
						<ul className="list-disc pl-5">
							{Object.entries(failure.fields).map(([name, message]) => (
								<li key={name}>
									<button className="text-left underline" type="button" onClick={() => {
										const field = formRef.current?.elements.namedItem(name);
										if (field instanceof HTMLElement) field.focus();
									}}>{message}</button>
								</li>
							))}
						</ul>
					)}
					{failure.cause instanceof ApiError && failure.cause.statusCode === 409 && (
						<Button type="button" variant="outline" onClick={onRefresh}>Refresh and Review Settings</Button>
					)}
				</div>
			)}
			{!installation && (
				<fieldset className="min-w-0 space-y-3">
					<legend className="pb-2 font-semibold">Connection</legend>
					<Label htmlFor="cf-account-id">Cloudflare Account ID</Label>
					<Input id="cf-account-id" name="account_id" pattern="[a-f0-9]{32}" required {...fieldProps("account_id")} />{fieldError("account_id")}
					{status.managed_tunnel_available && (
						<div className="grid gap-2">
							<Label htmlFor="cf-reachability-mode">Reachability</Label>
							<select id="cf-reachability-mode" name="reachability_mode" {...fieldProps("reachability_mode")} className="rounded-md border bg-background p-2"
								value={managedTunnel ? "managed_tunnel" : "existing_https_endpoint"}
								onChange={(event) => setManagedTunnel(event.target.value === "managed_tunnel")}
							>
								<option value="existing_https_endpoint">Existing HTTPS Endpoint</option>
								<option value="managed_tunnel">Managed Tunnel</option>
							</select>
							{fieldError("reachability_mode")}
						</div>
					)}
					{managedTunnel && (
						<>
							<Label htmlFor="cf-zone-id">Cloudflare DNS Zone ID</Label>
							<Input id="cf-zone-id" name="zone_id" pattern="[a-f0-9]{32}" required {...fieldProps("zone_id", "cf-tunnel-help")} />{fieldError("zone_id")}
							<p id="cf-tunnel-help" className="text-sm text-muted-foreground">Grant Cloudflare Tunnel Write on the account and Zone Read and DNS Write on this zone.</p>
						</>
					)}
					<Label htmlFor="cf-ingress-origin">{managedTunnel ? "Dedicated Tunnel HTTPS Origin" : "Existing HTTPS Endpoint"}</Label>
					<Input id="cf-ingress-origin" name="origin" type="url" required {...fieldProps("origin")} />{fieldError("origin")}
					<Label htmlFor="cf-connect-token">Cloudflare API Token</Label>
					<Input id="cf-connect-token" name="api_token" type="password" autoComplete="new-password" required {...fieldProps("api_token", managedTunnel ? "cf-tunnel-help" : undefined)} />{fieldError("api_token")}
					<p id="cf-consent-help" className="text-sm text-muted-foreground">{status.consent_text}</p>
					<label className="flex items-center gap-2 text-sm">
						<input type="checkbox" name="consent" required {...fieldProps("consent", "cf-consent-help")} />
						Accept Cloudflare Terms
					</label>
					{fieldError("consent")}
				</fieldset>
			)}
			<fieldset className="min-w-0">
				<legend className="pb-2 font-semibold">Spending</legend>
				<div className="space-y-1">
					<Label htmlFor="cf-monthly">Monthly Estimated Usage Limit</Label>
					<Input id="cf-monthly" name="monthly_limit" type="number" min="0.01" step="0.01" defaultValue={settings?.monthly_limit_usd ?? "30.00"} required {...fieldProps("monthly_limit", "cf-monthly-unit")} />{fieldError("monthly_limit")}
					<p id="cf-monthly-unit" className="text-sm text-muted-foreground">USD/month</p>
				</div>
			</fieldset>
			<fieldset className="min-w-0 space-y-3">
				<legend className="pb-2 font-semibold">Advanced Policy</legend>
				<div className="grid gap-3 sm:grid-cols-2">
					<div><Label htmlFor="cf-launcher-cap">Workers Launcher Limit</Label><Input id="cf-launcher-cap" name="launcher_cap" type="number" min={1} max={1000} defaultValue={settings?.workers.launcher_cap ?? 1} required {...fieldProps("launcher_cap")} />{fieldError("launcher_cap")}</div>
					<div><Label htmlFor="cf-slots">Slots per Launcher</Label><Input id="cf-slots" name="slots" type="number" min={1} max={4} defaultValue={settings?.workers.slots_per_launcher ?? 2} required {...fieldProps("slots")} />{fieldError("slots")}</div>
					<div><Label htmlFor="cf-tenant-slots">Slots per Tenant</Label><Input id="cf-tenant-slots" name="tenant_slots" type="number" min={1} max={32} defaultValue={settings?.workers.slots_per_tenant ?? 4} required {...fieldProps("tenant_slots")} />{fieldError("tenant_slots")}</div>
					<div><Label htmlFor="cf-installation-slots">Installation Slot Limit</Label><Input id="cf-installation-slots" name="installation_slots" type="number" min={2} max={256} defaultValue={settings?.workers.installation_slots ?? 9} required {...fieldProps("installation_slots")} />{fieldError("installation_slots")}</div>
					<div><Label htmlFor="cf-lifetime">Maximum Lifetime in Seconds</Label><Input id="cf-lifetime" name="lifetime" type="number" min={60} max={604800} defaultValue={settings?.workers.max_lifetime_seconds ?? 3600} required {...fieldProps("lifetime")} />{fieldError("lifetime")}</div>
					<div><Label htmlFor="cf-linger">Tenant Engine Linger in Seconds</Label><Input id="cf-linger" name="linger" type="number" min={0} max={300} defaultValue={settings?.workers.linger_seconds ?? 90} required {...fieldProps("linger")} />{fieldError("linger")}</div>
					<div><Label htmlFor="cf-queue-prewarm">Max Queue Pre-Warms per Tenant</Label><Input id="cf-queue-prewarm" name="queue_prewarm" type="number" min={0} max={32} defaultValue={settings?.workers.queue_prewarm_per_tenant ?? 0} required {...fieldProps("queue_prewarm", "cf-prewarm-help")} />{fieldError("queue_prewarm")}<p id="cf-prewarm-help" className="text-sm text-muted-foreground">Set to 0 to disable. Requires a positive engine linger.</p></div>
					<div><Label htmlFor="cf-shared-cap">Shared Managed Worker Limit</Label><Input id="cf-shared-cap" name="shared_cap" type="number" min={1} max={1000} defaultValue={settings?.shared_managed_worker_cap ?? undefined} {...fieldProps("shared_cap")} />{fieldError("shared_cap")}</div>
				</div>
				<label className="flex items-center gap-2 text-sm">
					<input type="checkbox" name="workflows" defaultChecked={settings?.workers.work_kinds.includes("workflow") ?? true} {...fieldProps("workflows")} />
					Allow Approved Workflows
				</label>
				{fieldError("workflows")}
			</fieldset>
			<div className="flex flex-wrap gap-2">
				<Button type="submit" disabled={saving || busy}>{installation ? "Save Policy" : "Connect Cloudflare"}</Button>
				<Button type="button" variant="outline" disabled={saving} onClick={onCancel}>Cancel</Button>
			</div>
		</form>
	);
}

export function CloudflareCapacity() {
	const authorization = useAuthorization();
	const canRead = authorization.canAt("platform.read", GLOBAL_TARGET);
	const canWrite = authorization.canAt("platform.readwrite", GLOBAL_TARGET);
	const queryClient = useQueryClient();
	const { data, error, isLoading, isFetching, refetch } = useQuery({
		queryKey: statusKey,
		retry: false,
		queryFn: getCloudflareCapacity,
		enabled: canRead,
	});
	const [acceptedJobId, setAcceptedJobId] = useState<string>();
	const [observedJob, setObservedJob] = useState<PlatformJob>();
	const [localAction, setLocalAction] = useState<{ id: string; action: CloudflareAction }>();
	const [readingJob, setReadingJob] = useState(false);
	const [readbackFailure, setReadbackFailure] = useState<string>();
	const readbackRef = useRef<(replace?: boolean) => void>(null);
	const [connecting, setConnecting] = useState(false);
	const [focusTarget, setFocusTarget] = useState<"setup" | "enable" | "policy">();
	const setupRef = useRef<HTMLButtonElement>(null);
	const enableRef = useRef<HTMLButtonElement>(null);
	const policyRef = useRef<HTMLButtonElement>(null);
	const jobStatusRef = useRef<HTMLDivElement>(null);
	const actionErrorRef = useRef<HTMLDivElement>(null);
	const [rotateOpen, setRotateOpen] = useState(false);
	const [rotating, setRotating] = useState(false);
	const [rotateError, setRotateError] = useState<unknown>();
	const [editing, setEditing] = useState(false);
	const installation = data?.installation;
	const jobId = acceptedJobId ?? data?.latest_job_id;
	const job = observedJob?.id === jobId ? observedJob : undefined;
	const instructions = job?.result?.instructions;
	const legacyWorkers = job?.result?.workers;
	const mutation = useMutation({
		mutationFn: (action: CloudflareAction) => {
			if (!installation || !canWrite)
				throw new Error("Cloudflare capacity actions are unavailable.");
			return startCloudflareAction(action, {
				installation_id: installation.installation_id,
				expected_revision: installation.desired_revision,
			});
		},
		onSuccess: (accepted, action) => {
			setLocalAction({ id: accepted.job_id, action });
			setAcceptedJobId(accepted.job_id);
			void refetch();
		},
	});

	useEffect(() => {
		if (!canRead) return;
		const unsubscribeJobs = webSocketService.onAnyPlatformJobUpdate((next) => {
			if (next.resource_type === "cloudflare_installation" && next.id !== jobId) void refetch();
		});
		const unsubscribeConnection = webSocketService.onConnectionStatusChange((connected) => {
			if (!connected) return;
			void refetch();
			readbackRef.current?.(true);
		});
		return () => {
			unsubscribeJobs();
			unsubscribeConnection();
		};
	}, [canRead, jobId, refetch]);

	useEffect(() => {
		if (!canRead || !jobId) return;
		let current: PlatformJob | undefined;
		let disposed = false;
		const update = (next: PlatformJob) => {
			if (disposed || (current && current.revision >= next.revision)) return;
			current = next;
			setObservedJob((previous) =>
				previous?.id === next.id && previous.revision >= next.revision ? previous : next,
			);
			setReadbackFailure(undefined);
			if (terminal.has(next.status)) {
				void refetch().then(({ data: status, isSuccess }) => {
					if (!disposed && isSuccess && status?.latest_job_id) {
						setAcceptedJobId((accepted) => accepted === next.id ? undefined : accepted);
					}
				});
			}
		};
		const unsubscribe = webSocketService.onPlatformJobUpdate(jobId, update);
		let observation: ReturnType<typeof observePlatformJob> | undefined;
		function readback(replace = false) {
			if (!jobId || (observation && !replace) || (current && terminal.has(current.status))) return;
			observation?.cancel();
			setReadingJob(true);
			setReadbackFailure(undefined);
			const snapshot = observePlatformJob(jobId, update);
			observation = snapshot;
			void snapshot.promise.catch(() => {
				if (!disposed && observation === snapshot && (!current || !terminal.has(current.status))) {
					setReadbackFailure(jobId);
				}
			}).finally(() => {
				if (disposed || observation !== snapshot) return;
				observation = undefined;
				setReadingJob(false);
			});
		}
		readbackRef.current = readback;
		readback();
		return () => {
			disposed = true;
			unsubscribe();
			observation?.cancel();
			readbackRef.current = null;
		};
	}, [canRead, jobId, refetch]);

	useEffect(() => {
		if (!focusTarget) return;
		const target = { setup: setupRef, enable: enableRef, policy: policyRef }[focusTarget].current;
		if (target && !target.disabled) {
			target.focus();
			setFocusTarget(undefined);
		}
	}, [focusTarget, installation]);

	useEffect(() => {
		if (mutation.error) actionErrorRef.current?.focus();
	}, [mutation.error]);

	const ownJob = localAction?.id === jobId ? localAction : undefined;
	const jobRefused = job?.status === "failed" || job?.status === "requires_action";
	const disconnected = ownJob?.action === "disconnect" && job?.status === "succeeded" && data?.installation === null;

	useEffect(() => {
		if (disconnected) setupRef.current?.focus();
		else if (ownJob) jobStatusRef.current?.focus();
	}, [disconnected, ownJob, jobRefused]);

	function finishConfiguration(next: CloudflareCapacityStatus) {
		if (!installation) {
			setAcceptedJobId(undefined);
			setLocalAction(undefined);
		}
		setConnecting(false);
		setEditing(false);
		queryClient.setQueryData(statusKey, next);
		setFocusTarget(installation ? "policy" : "enable");
	}

	function cancelConfiguration() {
		setConnecting(false);
		setEditing(false);
		setFocusTarget(installation ? "policy" : "setup");
	}

	async function submitRotation(event: FormEvent<HTMLFormElement>) {
		event.preventDefault();
		if (!installation || !canWrite || rotating) return;
		const form = event.currentTarget;
		const candidate = new FormData(form).get("api_token");
		form.reset();
		if (typeof candidate !== "string" || !candidate) return;
		setRotating(true);
		setRotateError(undefined);
		try {
			const accepted = await rotateCloudflareAccountToken({
				installation_id: installation.installation_id,
				expected_revision: installation.desired_revision,
				api_token: candidate,
			});
			setAcceptedJobId(accepted.job_id);
			setRotateOpen(false);
			void refetch();
		} catch (error) {
			setRotateError(error);
		} finally {
			setRotating(false);
		}
	}

	const jobPending = Boolean(canRead && jobId && (!job || !terminal.has(job.status)));
	const busy = mutation.isPending || rotating || jobPending;
	const readyToEnable = installation?.deployed_revision == null && !jobId;
	const connectionStatus =
		installation?.desired_state === "disconnected"
			? "Disconnecting"
			: installation?.desired_state === "scaled_in"
				? (readyToEnable ? "Ready to Enable" : "Scaled In")
				: installation?.upgrade_pending
					? "Upgrade Pending"
					: installation?.admission_open
						? "Enabled"
						: "Admission Closed";

	return (
		<Card>
			<CardHeader>
				<h2 className="text-base font-medium">Scale Out with Cloudflare</h2>
			</CardHeader>
			<CardContent className="space-y-4">
				{(authorization.isLoading || isLoading) && (
					<p role="status">Loading Cloudflare capacity…</p>
				)}
				{!authorization.isLoading && !canRead && (
					<p>
						Platform permission is required to view Cloudflare
						capacity.
					</p>
				)}
				{error && (
					<div className="space-y-2">
						<div role="alert">
							<p>{getErrorMessage(error, "Could not load Cloudflare capacity.")}</p>
							<ErrorDetails error={error} />
						</div>
						<Button variant="outline" disabled={isFetching} onClick={() => void refetch()}>Retry Loading</Button>
					</div>
				)}
				{canRead && data && !canWrite && (
					<p className="text-muted-foreground">
						Ask a Platform Admin or a user with Read and Write Platform Operations permission at Global scope to manage Cloudflare capacity.
					</p>
				)}
				{canRead && data && !installation && (
					<section className="space-y-2">
						<h3 className="font-semibold">Cloudflare Connection</h3>
						<p>Use a Cloudflare account with Workers Paid, an account API token, and a reachable HTTPS endpoint or a managed tunnel.</p>
						<p>Connecting authorizes Bifrost to manage Cloudflare resources for approved workflows.</p>
						{canWrite && !connecting && (
							<Button ref={setupRef} disabled={busy} onClick={() => setConnecting(true)}>Set Up Cloudflare</Button>
						)}
					</section>
				)}
				{canRead && data && (
					<p>
						Only approved workflows can use Cloudflare capacity.{" "}
						<Link className="underline" to="/workflows">Review Workflows</Link>
					</p>
				)}
				{canRead && data && canWrite && (installation ? editing : connecting) && (
					<CloudflareConfigurationForm
						key={installation?.desired_revision ?? "connect"}
						status={data}
						onSaved={finishConfiguration}
						onCancel={cancelConfiguration}
						onRefresh={() => void refetch()}
						busy={busy}
					/>
				)}
				{canRead && installation && (
					<>
						<dl className="grid grid-cols-1 gap-4 text-sm sm:grid-cols-2">
							<div>
								<dt className="text-muted-foreground">
									Status
								</dt>
								<dd>{connectionStatus}</dd>
							</div>
							{installation.deployed_revision != null && (
								<div>
									<dt className="text-muted-foreground">Deployed Revision</dt>
									<dd>{installation.deployed_revision}</dd>
								</div>
							)}
							<div>
								<dt className="text-muted-foreground">
									{installation.reachability_mode === "managed_tunnel" ? "Managed Tunnel" : "Existing HTTPS Endpoint"}
								</dt>
								<dd className="break-all">
									{installation.worker_ingress_origin}
								</dd>
							</div>
							<div>
								<dt className="text-muted-foreground">
									Workers Launcher Limit
								</dt>
								<dd>
									{installation.policy.workers.launcher_cap}
								</dd>
							</div>
							<div>
								<dt className="text-muted-foreground">
									Slots per Launcher
								</dt>
								<dd>
									{
										installation.policy.workers
											.slots_per_launcher
									}
								</dd>
							</div>
							<div>
								<dt className="text-muted-foreground">
									Monthly Estimated Usage Limit
								</dt>
								<dd>
									{installation.policy.monthly_limit_usd} USD/month
								</dd>
							</div>
							<div>
								<dt className="text-muted-foreground">
									Estimated Fleet Usage
								</dt>
								<dd>{installation.fleet_envelope_usd} USD</dd>
							</div>
							{installation.spend && (
								<>
									<div>
										<dt className="text-muted-foreground">Estimated Bifrost Usage This UTC Month</dt>
										<dd>{installation.spend.estimated_usd === null ? "Spend Ledger Unavailable" : `${installation.spend.estimated_usd} USD/month`}</dd>
										{installation.spend.state === "budget_reached" && <dd>Estimated usage reached the monthly limit.</dd>}
									</div>
									<div>
										<dt className="text-muted-foreground">Workers Paid Subscription (Billed by Cloudflare)</dt>
										<dd>{installation.spend.workers_paid_subscription_usd} USD/month</dd>
									</div>
									<div>
										<dt className="text-muted-foreground">Cloudflare Analytics Advisory</dt>
										<dd>{installation.spend.analytics?.request_difference != null
											? `${installation.spend.analytics.worker_requests} sampled Worker requests, ${installation.spend.analytics.request_difference} difference from the Bifrost request estimate`
											: "No Attributable Sample"}</dd>
											{installation.spend.analytics?.request_difference == null && (
												<dd className="text-muted-foreground">Cloudflare Analytics samples are advisory. Bifrost tracks estimated usage in a separate ledger.</dd>
											)}
									</div>
									<div>
										<dt className="text-muted-foreground">Usage Estimate Coverage</dt>
										<dd>
											{Boolean(installation.spend.coverage?.length) && (
												<ul aria-label="Usage Estimate Coverage" tabIndex={0} className="max-h-[min(16rem,40vh)] space-y-1 overflow-auto">
													{installation.spend.coverage?.map((line) => <li key={line}>{line}</li>)}
												</ul>
											)}
										</dd>
									</div>
								</>
							)}
						</dl>
						{canWrite && (
							<div className="flex flex-wrap gap-2">
								<Button ref={policyRef} variant="outline" disabled={busy || editing || installation.desired_state === "disconnected"}
									onClick={() => setEditing(true)}
								>
									Edit Policy
								</Button>
								<Button
									ref={enableRef}
									disabled={busy || installation.admission_open || installation.upgrade_pending || installation.desired_state === "disconnected"}
									onClick={() => mutation.mutate("enable")}
								>
									Enable
								</Button>
								<Button variant="outline" disabled={busy || installation.desired_state !== "enabled"}
									onClick={() => mutation.mutate("reconcile")}
								>
									Reconcile Cloudflare
								</Button>
								<Button variant="outline" disabled={busy || !installation.upgrade_available}
									onClick={() => mutation.mutate("upgrade")}
								>
									Upgrade Cloudflare
								</Button>
								{installation.desired_state === "scaled_in" && (
									<>
										<Button variant="outline" disabled={busy} onClick={() => mutation.mutate("rotate-root")}>Rotate Control Root</Button>
										{installation.connector_token_version != null && (
											<Button variant="outline" disabled={busy} onClick={() => mutation.mutate("rotate-connector")}>
												Rotate Connector Token
											</Button>
										)}
										<AlertDialog>
											<AlertDialogTrigger asChild>
												<Button variant="outline" disabled={busy}>Rotate Scope Bearer</Button>
											</AlertDialogTrigger>
											<AlertDialogContent>
												<AlertDialogHeader>
													<AlertDialogTitle>Rotate Cloudflare Scope Bearer</AlertDialogTitle>
													<AlertDialogDescription>
														Bifrost will delete the previous Workers deployment before staging the new bearer. Enable will deploy the Workers again.
													</AlertDialogDescription>
												</AlertDialogHeader>
												<AlertDialogFooter>
													<AlertDialogCancel>Cancel</AlertDialogCancel>
													<AlertDialogAction onClick={() => mutation.mutate("rotate-bearer")}>Rotate Scope Bearer</AlertDialogAction>
												</AlertDialogFooter>
											</AlertDialogContent>
										</AlertDialog>
									</>
								)}
								<Dialog open={rotateOpen} onOpenChange={setRotateOpen}>
									<DialogTrigger asChild>
										<Button variant="outline" disabled={busy}>Rotate API Token</Button>
									</DialogTrigger>
									<DialogContent>
										<DialogHeader>
											<DialogTitle>Rotate Cloudflare API Token</DialogTitle>
											<DialogDescription>
												Enter a replacement API token for the connected account.
											</DialogDescription>
										</DialogHeader>
										<form className="space-y-4" onSubmit={submitRotation}>
											<div className="space-y-2">
												<Label htmlFor="cloudflare-api-token">Replacement API Token</Label>
												<Input id="cloudflare-api-token" name="api_token" type="password" autoComplete="new-password" required />
											</div>
											{rotateError != null && (
											<div role="alert">
												<p>{getErrorMessage(rotateError, "Could not queue Cloudflare API token rotation.")}</p>
												<ErrorDetails error={rotateError} />
												{rotateError instanceof ApiError && rotateError.statusCode === 409 && (
													<Button type="button" variant="outline" onClick={() => void refetch()}>Refresh Settings</Button>
												)}
											</div>
										)}
											<DialogFooter>
												<Button type="submit" disabled={rotating}>Rotate API Token</Button>
											</DialogFooter>
										</form>
									</DialogContent>
								</Dialog>
								<Button
									variant="outline"
									disabled={
										busy ||
										installation.desired_state !== "enabled"
									}
									onClick={() => mutation.mutate("scale-in")}
								>
									Scale In
								</Button>
								<AlertDialog>
									<AlertDialogTrigger asChild>
										<Button
											variant="outline"
											disabled={busy}
										>
											Disconnect
										</Button>
									</AlertDialogTrigger>
									<AlertDialogContent>
										<AlertDialogHeader>
											<AlertDialogTitle>
												Disconnect Cloudflare
											</AlertDialogTitle>
											<AlertDialogDescription>
												Bifrost will drain Cloudflare
												workers and remove the resources
												it owns before erasing their
												credentials.
											</AlertDialogDescription>
										</AlertDialogHeader>
										<AlertDialogFooter>
											<AlertDialogCancel>
												Cancel
											</AlertDialogCancel>
											<AlertDialogAction
												onClick={() =>
													mutation.mutate(
														"disconnect",
													)
												}
											>
												Disconnect
											</AlertDialogAction>
										</AlertDialogFooter>
									</AlertDialogContent>
								</AlertDialog>
							</div>
						)}
					</>
				)}
				{mutation.error && (
					<div ref={actionErrorRef} tabIndex={-1} role="alert" className="space-y-2">
						<h3 className="font-semibold">Cloudflare Action Error</h3>
						<p>{getErrorMessage(mutation.error, "Could not start the Cloudflare job.")}</p>
						<ErrorDetails error={mutation.error} />
						{mutation.error instanceof ApiError && mutation.error.statusCode === 409 && (
							<Button variant="outline" disabled={isFetching} onClick={() => void refetch()}>Refresh and Review Settings</Button>
						)}
					</div>
				)}
				{canRead && jobId && (
					<section ref={jobStatusRef} tabIndex={-1} className="space-y-2" aria-labelledby="cf-job-status-heading">
						<h3 id="cf-job-status-heading" className="font-semibold">Cloudflare Job Status</h3>
						<p role="status">
							{disconnected ? "Cloudflare was disconnected."
								: readbackFailure === jobId ? "Could not read the Cloudflare job status."
									: job?.error?.message ?? job?.progress.phase
										?? (job?.status === "queued" ? "The Cloudflare job is queued." : "Waiting for Cloudflare job status.")}
						</p>
						{jobPending && (
							<Button variant="outline" disabled={readingJob} onClick={() => {
								void refetch();
								readbackRef.current?.();
							}}>Check Job Status</Button>
						)}
						{(job?.error || typeof job?.result?.code === "string") && (
							<p>Error Code <code>{job.error?.code ?? String(job.result?.code)}</code></p>
						)}
						{typeof job?.result?.reason === "string" && <p>{job.result.reason}</p>}
						{typeof job?.result?.remedy === "string" && <p>{job.result.remedy}</p>}
						{Array.isArray(instructions) && (
							<ol aria-label="Recovery Instructions" tabIndex={0} className="max-h-[min(16rem,40vh)] list-decimal space-y-1 overflow-auto pl-5 text-sm">
								{instructions.filter((instruction): instruction is string => typeof instruction === "string")
									.map((instruction) => <li key={instruction}>{instruction}</li>)}
							</ol>
						)}
						{Array.isArray(legacyWorkers) && (
							<ul aria-label="Workers Requiring Review" tabIndex={0} className="max-h-[min(16rem,40vh)] space-y-1 overflow-auto text-sm">
								{legacyWorkers.map((worker) => (
									<li className="break-all" key={String(worker.worker_id)}>
										Worker {worker.worker_id} · Scope {worker.scope} · Resource UID {worker.resource_uid ?? "Unavailable"}
									</li>
								))}
							</ul>
						)}
					</section>
				)}
			</CardContent>
		</Card>
	);
}
