import { useEffect, useRef, useState, type FormEvent, type ReactNode } from "react";
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
import { CloudflareWorkflowApprovals } from "@/components/workflows/CloudflareWorkflowApprovals";
import { ApiError, getErrorMessage } from "@/lib/api-error";
import { GLOBAL_TARGET } from "@/lib/authorization";
import type { components } from "@/lib/v1";
import { useAuthorization } from "@/services/authorization";
import {
	activateCloudflareCapacity,
	configureCloudflareCapacity,
	getCloudflareCapacity,
	preflightCloudflareCapacity,
	rotateCloudflareAccountToken,
	startCloudflareAction,
	type CloudflareAction,
	type CloudflareCapacityStatus,
	type CloudflareJobAccepted,
	type CloudflarePreflight,
	type CloudflarePreflightRequest,
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
const utcDate = new Intl.DateTimeFormat(undefined, {
	dateStyle: "medium", timeStyle: "medium", timeZone: "UTC",
});

function timestamp(value?: string | null) {
	return value ? `${utcDate.format(new Date(value))} UTC` : "Unobserved";
}

function amount(value?: string | number | null) {
	return value == null ? "Unavailable" : `${value} USD`;
}
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

function validationMessages(error: unknown, form: HTMLFormElement) {
	const fields: FieldErrors = {};
	const messages: string[] = [];
	if (!(error instanceof ApiError)) return { fields, messages };

	function addField(path: string, message: string) {
		const field = path.split(".").filter((part) => !/^\d+$/.test(part)).at(-1);
		const name = field && (fieldNames[field] ?? field);
		if (name && form.elements.namedItem(name)) fields[name] = message;
		else messages.push(message);
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
	if (error.errorCode === "validation_error" && !reported && !Array.isArray(issues)) {
		// The request-validation handler returns "field.path: message" entries.
		for (const entry of error.message.split(/; (?=[\w.]+: )/)) {
			const [prefix, path] = entry.match(/^([\w.]+): /) ?? [];
			if (prefix) addField(path, entry.slice(prefix.length));
			else messages.push(entry);
		}
	}
	return { fields, messages };
}

function ErrorDetails({ error }: { error: unknown }) {
	if (!(error instanceof ApiError)) return null;
	const { code, reason, remedy } = error.details ?? {};
	const displayCode = typeof code === "string" ? code : error.errorCode;
	return (
		<>
			{!["RequestError", "UnknownError", "ValidationError", "validation_error"].includes(displayCode) && <p>Error Code <code>{displayCode}</code></p>}
			{typeof reason === "string" && <p>{reason}</p>}
			{typeof remedy === "string" && <p>{remedy}</p>}
		</>
	);
}

function BoundedList({ as: List = "ul", children, label, className }: {
	as?: "ul" | "ol";
	children: ReactNode;
	label?: string;
	className?: string;
}) {
	const ref = useRef<HTMLUListElement & HTMLOListElement>(null);
	const [overflows, setOverflows] = useState(false);

	useEffect(() => {
		const list = ref.current;
		if (!list) return;
		const measure = () => setOverflows(list.scrollHeight > list.clientHeight);
		measure();
		if (typeof ResizeObserver === "undefined") return;
		const observer = new ResizeObserver(measure);
		observer.observe(list);
		for (const item of list.children) observer.observe(item);
		return () => observer.disconnect();
	}, [children]);

	return (
		<List ref={ref} aria-label={label} tabIndex={overflows ? 0 : undefined}
			className={`max-h-[min(16rem,40vh)] space-y-1 overflow-auto ${className ?? ""}`}>
			{children}
		</List>
	);
}

type CapacityPolicy = components["schemas"]["CloudflareCapacityPolicy-Input"];

function readPolicy(fields: FormData): CapacityPolicy {
	const sharedCap = String(fields.get("shared_cap") ?? "");
		return {
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
}

function PolicyFields({ settings, fieldProps = () => ({}), fieldError = () => null }: {
	settings: CapacityPolicy;
	fieldProps?: (name: string, help?: string) => Record<string, unknown>;
	fieldError?: (name: string) => ReactNode;
}) {
	return (
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
	);
}

function CloudflareSetupForm({ status, onAccepted, onCancel }: {
	status: CloudflareCapacityStatus;
	onAccepted: (accepted: CloudflareJobAccepted) => void;
	onCancel: () => void;
}) {
	const [preflight, setPreflight] = useState<CloudflarePreflight>();
	const [stale, setStale] = useState(false);
	const [policyEdited, setPolicyEdited] = useState(false);
	const [limit, setLimit] = useState(String(status.suggested_policy?.monthly_limit_usd ?? ""));
	const [consent, setConsent] = useState(false);
	const [limitConfirmed, setLimitConfirmed] = useState(false);
	const [saving, setSaving] = useState(false);
	const [error, setError] = useState<unknown>();
	const submitLock = useRef(false);
	const tokenRef = useRef<HTMLInputElement>(null);
	const mode = preflight?.reachability_mode;
	const [reachability, setReachability] = useState<CloudflarePreflightRequest["reachability_mode"]>();
	const selectedMode = reachability ?? mode;
	const receipt = stale ? undefined : preflight?.preflight_receipt;
	const preview = preflight?.preview;
	const showZones = selectedMode === "managed_tunnel" && preflight?.connector_ready && preflight.workers_paid === "verified";
	useEffect(() => { tokenRef.current?.focus(); }, []);

	async function submit(event: FormEvent<HTMLFormElement>) {
		event.preventDefault();
		if (submitLock.current) return;
		const form = event.currentTarget;
		const fields = new FormData(form);
		const value = (name: string) => String(fields.get(name) ?? "") || undefined;
		const api_token = value("api_token");
		submitLock.current = true;
		setSaving(true);
		setError(undefined);
		try {
			if (receipt && preflight) {
				if (!consent || !limitConfirmed || Number(limit) < Number(preview?.fleet_envelope_usd)) return;
				const accepted = await activateCloudflareCapacity({
					api_token, preflight_receipt: receipt, consent: true,
					consent_version: preflight.consent_version, monthly_limit_usd: limit,
				});
				form.reset();
				onAccepted(accepted);
			} else {
				const next = await preflightCloudflareCapacity({
					api_token,
					account_id: value("account_id") ?? preflight?.account_id ?? undefined,
					zone_id: selectedMode === "managed_tunnel" ? value("zone_id") ?? preflight?.zone_id ?? undefined : undefined,
					reachability_mode: selectedMode,
					worker_ingress_origin: value("worker_ingress_origin"),
					policy: policyEdited ? readPolicy(fields) : undefined,
				});
				setPreflight(next);
				setStale(false);
				setConsent(false);
				setLimitConfirmed(false);
			}
		} catch (cause) {
			setError(cause);
			if (receipt && cause instanceof ApiError && cause.statusCode === 409) setStale(true);
		} finally {
			setSaving(false);
			submitLock.current = false;
		}
	}

	function invalidate() {
		setStale(true);
		setConsent(false);
		setLimitConfirmed(false);
	}

	return (
		<form aria-label="Cloudflare Setup" className="space-y-4" onSubmit={submit}>
			{Boolean(error) && (
				<div role="alert" className="rounded-md border border-destructive p-3">
					<p>{getErrorMessage(error, "Could not complete Cloudflare setup.")}</p>
					<ErrorDetails error={error} />
				</div>
			)}
			<fieldset disabled={saving} className="min-w-0 space-y-4">
				<div className="space-y-1">
					<Label htmlFor="cf-connect-token">Cloudflare API Token</Label>
						<Input ref={tokenRef} id="cf-connect-token" name="api_token" type="password" autoComplete="new-password" required minLength={16} maxLength={512}
						onChange={() => { setPreflight(undefined); setReachability(undefined); invalidate(); }} />
				</div>
				{preflight && (
					<div className="space-y-3">
						<dl className="grid gap-2 text-sm sm:grid-cols-2">
							<div><dt>Primary Capacity</dt><dd>{{ static: "Local Workers", job: "Kubernetes Jobs", external: "External Worker Provider", unknown: "Unconfirmed" }[preflight.primary]}</dd></div>
							<div><dt>Managed Tunnel Channel</dt><dd>{preflight.connector_ready ? (preflight.connector_channel === "compose" ? "Compose Ready" : "Kustomize Ready") : "Unavailable"}</dd></div>
							<div><dt>Workers Paid</dt><dd>{preflight.workers_paid === "verified" ? "Verified" : "Unverified"}</dd></div>
							<div><dt>Placement</dt><dd>Unrestricted for Approved Workflows</dd></div>
						</dl>
						{preflight.workers_paid === "denied" && <p>Grant Account Settings Read for the selected account and check it again.</p>}
						{preflight.workers_paid === "unverified" && <p>Verify Workers Paid and active Workers and Containers entitlements in Cloudflare.</p>}
						<div className="space-y-1">
							{preflight.accounts === null && !preflight.account_id ? (
								<><Label htmlFor="cf-account-id">Cloudflare Account ID</Label><Input id="cf-account-id" name="account_id" required pattern="[a-f0-9]{32}" onChange={invalidate} /></>
							) : (preflight.accounts?.length ?? 0) > 1 ? (
								<><Label htmlFor="cf-account-id">Cloudflare Account</Label>
									<select id="cf-account-id" name="account_id" className="block max-w-full rounded-md border bg-background p-2" defaultValue={preflight.account_id ?? ""} required onChange={invalidate}>
										<option value="">Select an Account</option>
										{preflight.accounts?.map(({ id, name }) => <option key={id} value={id}>{name}</option>)}
									</select></>
							) : preflight.account_id ? (
								<p>Cloudflare Account: {preflight.accounts?.find(({ id }) => id === preflight.account_id)?.name ?? preflight.account_id}</p>
							) : <p>No Cloudflare accounts are available to this token.</p>}
						</div>
						<div className="space-y-1">
							<Label htmlFor="cf-reachability-mode">Reachability</Label>
							<select id="cf-reachability-mode" name="reachability_mode" className="block max-w-full rounded-md border bg-background p-2"
								value={selectedMode ?? undefined} onChange={(event) => {
									setReachability(event.target.value === "managed_tunnel" ? "managed_tunnel" : "existing_https_endpoint");
									invalidate();
								}}>
								<option value="existing_https_endpoint">Existing HTTPS Endpoint</option>
								{preflight.connector_ready && <option value="managed_tunnel">Managed Tunnel</option>}
							</select>
							{!preflight.connector_ready && <p className="text-sm text-muted-foreground">Managed Tunnel requires the Compose connector or the Kustomize component.</p>}
						</div>
						{showZones && (
							<div className="space-y-1">
								{preflight.zones == null && !preflight.zone_id ? (
									<><Label htmlFor="cf-zone-id">Cloudflare DNS Zone ID</Label><Input id="cf-zone-id" name="zone_id" required pattern="[a-f0-9]{32}" onChange={invalidate} /></>
								) : (preflight.zones?.length ?? 0) > 1 ? (
									<><Label htmlFor="cf-zone-id">Cloudflare DNS Zone</Label>
										<select id="cf-zone-id" name="zone_id" className="block max-w-full rounded-md border bg-background p-2" defaultValue={preflight.zone_id ?? ""} required onChange={invalidate}>
											<option value="">Select a DNS Zone</option>
											{preflight.zones?.map(({ id, name }) => <option key={id} value={id}>{name}</option>)}
										</select></>
								) : preflight.zone_name ? <p>Cloudflare DNS Zone: {preflight.zone_name}</p> : <p>No active zones are available to this token.</p>}
								{mode === "managed_tunnel" && preflight.worker_ingress_origin && <p className="break-all">Dedicated Hostname: {preflight.worker_ingress_origin}</p>}
								{preflight.hostname_collision && <p role="alert">The proposed hostname is already in use. Choose another zone or use an existing HTTPS endpoint.</p>}
							</div>
						)}
						{selectedMode === "existing_https_endpoint" && (
							<div className="space-y-1">
								<Label htmlFor="cf-ingress-origin">Existing HTTPS Endpoint</Label>
								<Input id="cf-ingress-origin" name="worker_ingress_origin" type="url" required defaultValue={mode === selectedMode ? preflight.worker_ingress_origin ?? "" : ""}
									onChange={invalidate} />
							</div>
						)}
					</div>
				)}
				{preview && (
					<div className="space-y-2">
						<h3 className="font-semibold">Workers Capacity Preview</h3>
						<p>{preview.policy.workers.launcher_cap} launchers × {preview.policy.workers.slots_per_launcher} slots, {preview.policy.workers.installation_slots} installation slots, and a {preview.policy.workers.max_lifetime_seconds.toLocaleString()}-second lifetime.</p>
						<dl className="grid gap-2 text-sm sm:grid-cols-2">
							<div><dt>Estimated Overflow Usage</dt><dd>About ${Number(preview.estimated_usage_min_usd)}–${Number(preview.estimated_usage_max_usd)}/month</dd></div>
							<div><dt>Workers Paid Subscription</dt><dd>{amount(preview.workers_paid_subscription_usd)}/month, billed separately</dd></div>
							<div><dt>Minimum Limit for This Fleet</dt><dd>{amount(preview.fleet_envelope_usd)}/month</dd></div>
						</dl>
						<p className="text-sm text-muted-foreground">The moderate forecast assumes 40 workflows per day in bursts, 35% cold starts, and about 0.077% Workers duty with nine slots. At the usage limit, new work stays on the primary. Overflow can pause earlier if usage or the next reservation cannot be verified.</p>
					</div>
				)}
				{preflight && (
					<div className="space-y-2">
						<Label htmlFor="cf-activation-limit">Monthly Estimated Usage Limit</Label>
						<Input id="cf-activation-limit" name="monthly_limit" type="number" required min={receipt ? Number(preview?.fleet_envelope_usd) : 0.01}
							step="0.01" value={limit} onChange={(event) => { setLimit(event.target.value); setLimitConfirmed(false); }} />
						{status.suggested_policy && (
							<details onChange={() => { setPolicyEdited(true); invalidate(); }}>
								<summary className="cursor-pointer font-medium">Edit Policy</summary>
								<PolicyFields settings={status.suggested_policy} />
							</details>
						)}
					</div>
				)}
				{receipt && (
					<div className="space-y-2">
						<label className="flex items-center gap-2"><input type="checkbox" required checked={limitConfirmed} onChange={(event) => setLimitConfirmed(event.target.checked)} /> Confirm Monthly Usage Limit</label>
						<p id="cf-activation-consent" className="text-sm">{preflight?.consent_text}</p>
						<label className="flex items-center gap-2"><input type="checkbox" required checked={consent} aria-describedby="cf-activation-consent" onChange={(event) => setConsent(event.target.checked)} /> Accept Cloudflare Terms</label>
					</div>
				)}
				<div className="flex flex-wrap gap-2">
					<Button type="submit">{receipt ? "Scale Out with Cloudflare" : preflight ? "Check Cloudflare Setup" : "Run Preflight"}</Button>
					<Button type="button" variant="outline" onClick={onCancel}>Cancel</Button>
				</div>
			</fieldset>
		</form>
	);
}

function CloudflareConfigurationForm({ installation, onSaved, onCancel, onRefresh, busy }: {
	installation: NonNullable<CloudflareCapacityStatus["installation"]>;
	onSaved: (status: CloudflareCapacityStatus) => void;
	onCancel: () => void;
	onRefresh: () => void;
	busy: boolean;
}) {
	const settings = installation.policy;
	const [saving, setSaving] = useState(false);
	const [failure, setFailure] = useState<{
		message: string;
		fields: FieldErrors;
		labels: Record<string, string>;
		messages?: string[];
		cause?: unknown;
	}>();
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

	async function save(event: FormEvent<HTMLFormElement>) {
		event.preventDefault();
		if (saving || busy) return;
		const form = event.currentTarget;
		const fields = new FormData(form);
		const labels: Record<string, string> = {};
		const invalid: FieldErrors = {};
		for (const field of form.elements) {
			if (!(field instanceof HTMLInputElement || field instanceof HTMLSelectElement)) continue;
			labels[field.name] = field.labels?.[0]?.textContent?.trim() || field.name;
			if (!field.validity.valid) {
				invalid[field.name] = field.name === "consent"
					? "Accept the Cloudflare terms to connect."
					: field.validity.patternMismatch
						? "Enter a 32-character lowercase hexadecimal ID."
						: field.validationMessage;
			}
		}
		if (Object.keys(invalid).length) {
			setFailure({ message: "Review the highlighted fields.", fields: invalid, labels });
			return;
		}
		const request = {
			action: "policy" as const,
			installation_id: installation.installation_id,
			expected_revision: installation.desired_revision,
			policy: readPolicy(fields),
		};
		setSaving(true);
		setFailure(undefined);
		try {
			onSaved(await configureCloudflareCapacity(request));
		} catch (cause) {
			const message = getErrorMessage(cause, "Could not save Cloudflare capacity.");
			const { fields: rejected, messages } = validationMessages(cause, form);
			const summary = Object.keys(rejected).length ? "Review the highlighted fields."
				: messages.length ? "Review the settings below." : message;
			setFailure({ message: summary, fields: rejected, labels, messages, cause });
		} finally {
			setSaving(false);
		}
	}

	return (
		<form ref={formRef} className="space-y-4" noValidate onSubmit={save}>
			{failure && (
				<div ref={summaryRef} tabIndex={-1} role="region" aria-labelledby="cf-settings-error" className="space-y-2 rounded-md border border-destructive p-3">
					<h3 id="cf-settings-error" className="font-semibold">Cloudflare Settings Error</h3>
					<p>{failure.message}</p>
					<ErrorDetails error={failure.cause} />
					{(Object.keys(failure.fields).length > 0 || Boolean(failure.messages?.length)) && (
						<ul className="list-disc pl-5">
							{[...new Set(failure.messages)].map((message) => <li key={message}>{message}</li>)}
							{Object.entries(failure.fields).map(([name, message]) => (
								<li key={name}>
									<button className="text-left underline" type="button" onClick={() => {
										const field = formRef.current?.elements.namedItem(name);
										if (field instanceof HTMLElement) field.focus();
									}}>{failure.labels[name]}: {message}</button>
								</li>
							))}
						</ul>
					)}
					{failure.cause instanceof ApiError && failure.cause.statusCode === 409 && (
						<Button type="button" variant="outline" onClick={onRefresh}>Refresh and Review Settings</Button>
					)}
				</div>
			)}
			<fieldset className="min-w-0">
				<legend className="pb-2 font-semibold">Spending</legend>
				<div className="space-y-1">
					<Label htmlFor="cf-monthly">Monthly Estimated Usage Limit</Label>
					<Input id="cf-monthly" name="monthly_limit" type="number" min="0.01" step="0.01" defaultValue={settings.monthly_limit_usd} required {...fieldProps("monthly_limit", "cf-monthly-unit")} />{fieldError("monthly_limit")}
					<p id="cf-monthly-unit" className="text-sm text-muted-foreground">USD/month</p>
				</div>
			</fieldset>
			<PolicyFields settings={settings} fieldProps={fieldProps} fieldError={fieldError} />
			<div className="flex flex-wrap gap-2">
				<Button type="submit" disabled={saving || busy}>Save Policy</Button>
				<Button type="button" variant="outline" disabled={saving} onClick={onCancel}>Cancel</Button>
			</div>
		</form>
	);
}

function PlacementReasons({ reasons }: { reasons: components["schemas"]["CloudflarePlacementReason"][] }) {
	return (
		<BoundedList label="Placement Reasons" className="space-y-2">
			{reasons.map((reason) => (
				<li key={reason.code}>
					<p className="font-medium">{reason.label}</p>
					<p>{reason.remedy}</p>
					{reason.next_eligible_at && (
						<p>{reason.code === "cloudflare_capacity_unavailable" ? "Cooldown Until" : "Next Eligible Check"} {timestamp(reason.next_eligible_at)}</p>
					)}
				</li>
			))}
		</BoundedList>
	);
}

function RecoveryDetails({ recovery }: { recovery: components["schemas"]["CloudflareRecoveryDetails"] }) {
	return (
		<div className="space-y-1">
			<p>{recovery.remedy}</p>
			{recovery.check && (
				<dl className="grid gap-2 text-sm sm:grid-cols-2">
					<div><dt className="text-muted-foreground">Resource Class</dt><dd><code>{recovery.resource_class}</code></dd></div>
					<div><dt className="text-muted-foreground">Verification Check</dt><dd><code>{recovery.check}</code></dd></div>
					<div><dt className="text-muted-foreground">Observed Category</dt><dd><code>{recovery.observed_category}</code></dd></div>
					<div><dt className="text-muted-foreground">Expected Category</dt><dd><code>{recovery.expected_category}</code></dd></div>
				</dl>
			)}
		</div>
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
	const [localAction, setLocalAction] = useState<{ id: string; action: CloudflareAction | "rotate-api-token" }>();
	const [readingJob, setReadingJob] = useState(false);
	const [readbackFailure, setReadbackFailure] = useState<string>();
	const readbackRef = useRef<(replace?: boolean) => void>(null);
	const [connecting, setConnecting] = useState(false);
	const [focusTarget, setFocusTarget] = useState<"setup" | "policy">();
	const setupRef = useRef<HTMLButtonElement>(null);
	const enableRef = useRef<HTMLButtonElement>(null);
	const policyRef = useRef<HTMLButtonElement>(null);
	const jobStatusRef = useRef<HTMLDivElement>(null);
	const actionErrorRef = useRef<HTMLDivElement>(null);
	const rotationAccepted = useRef(false);
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
					if (!disposed && isSuccess && (status?.latest_job_id || status?.disconnected_at)) {
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
		const target = { setup: setupRef, policy: policyRef }[focusTarget].current;
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
	const disconnected = !installation && (Boolean(data?.disconnected_at)
		|| (ownJob?.action === "disconnect" && job?.status === "succeeded" && data?.installation === null));

	useEffect(() => {
		if (disconnected && localAction?.action === "disconnect") setupRef.current?.focus();
		else if (ownJob) jobStatusRef.current?.focus();
	}, [disconnected, localAction, ownJob, jobRefused]);

	const jobAnnouncements: Partial<Record<PlatformJob["status"], string>> = {
		queued: "The Cloudflare job is queued.",
		succeeded: "The Cloudflare job completed.",
		failed: "The Cloudflare job failed.",
		cancelled: "The Cloudflare job was cancelled.",
		requires_action: "The Cloudflare job needs your attention.",
	};
	const jobAnnouncement = readbackFailure && readbackFailure === jobId ? "Could not read the Cloudflare job status."
			: job && ((terminal.has(job.status) ? job.error?.message : undefined) ?? jobAnnouncements[job.status]);

	function finishConfiguration(next: CloudflareCapacityStatus) {
		setEditing(false);
		queryClient.setQueryData(statusKey, next);
		setFocusTarget("policy");
	}

	function cancelConfiguration() {
		setConnecting(false);
		setEditing(false);
		setFocusTarget(installation ? "policy" : "setup");
	}

	function finishActivation(accepted: CloudflareJobAccepted) {
		setLocalAction({ id: accepted.job_id, action: "enable" });
		setAcceptedJobId(accepted.job_id);
		setConnecting(false);
		void refetch();
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
			rotationAccepted.current = true;
			setLocalAction({ id: accepted.job_id, action: "rotate-api-token" });
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
	const enableDisabled = busy || installation?.admission_open || installation?.upgrade_pending || installation?.desired_state === "disconnected";
	const readyToEnable = !enableDisabled && installation?.deployed_revision == null && !jobId
		&& installation?.spend?.state !== "budget_reached";
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
				{canRead && <CloudflareWorkflowApprovals />}
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
						{disconnected && <p role="status">Cloudflare was disconnected. Bifrost verified removal of its owned resources.{data.disconnected_at && ` ${timestamp(data.disconnected_at)}.`}</p>}
						<p>Use a dedicated Cloudflare account with Workers Paid where possible. A shared account is supported.</p>
						<p>Grant Account Settings Read and Workers Scripts Write. For Managed Tunnel, also grant Cloudflare Tunnel Write, Zone Read, and DNS Write on the selected zone.</p>
						{!data.suggested_policy && <p>Install a qualified Workers release with lifetime meters to set up Cloudflare capacity.</p>}
						<PlacementReasons reasons={data.placement_reasons ?? []} />
						{canWrite && !connecting && (
							<Button ref={setupRef} disabled={busy || !data.suggested_policy} onClick={() => setConnecting(true)}>Set Up Cloudflare</Button>
						)}
					</section>
				)}
				{canRead && data && (
					<p>
						<span>{data.approved_workflow_count == null ? "Approved workflow count is unavailable." :
							`${data.approved_workflow_count} approved ${data.approved_workflow_count === 1 ? "workflow" : "workflows"}.`}</span>{" "}
						Only approved workflows can use Cloudflare capacity.{" "}
						<Link className="underline" to="/workflows">Review Workflows</Link>
					</p>
				)}
				{canRead && data && canWrite && !installation && connecting && (
					<CloudflareSetupForm status={data} onAccepted={finishActivation} onCancel={cancelConfiguration} />
				)}
				{canRead && data && canWrite && installation && editing && (
					<CloudflareConfigurationForm
						key={installation?.desired_revision ?? "connect"}
						installation={installation}
						onSaved={finishConfiguration}
						onCancel={cancelConfiguration}
						onRefresh={() => void refetch()}
						busy={busy}
					/>
				)}
				{canRead && installation && (
					<section className="space-y-4" aria-labelledby="cf-installation-heading">
						<h3 id="cf-installation-heading" className="font-semibold">Cloudflare Installation</h3>
						<dl className="grid grid-cols-1 gap-4 text-sm sm:grid-cols-2">
							<div>
								<dt className="text-muted-foreground">
									Status
								</dt>
								<dd>{connectionStatus}</dd>
							</div>
							<div><dt className="text-muted-foreground">Desired Revision</dt><dd>{installation.desired_revision}</dd></div>
							<div><dt className="text-muted-foreground">Observed Deployment Revision</dt><dd>{installation.deployed_revision ?? "Unobserved"}</dd></div>
							<div>
								<dt className="text-muted-foreground">Reachability</dt>
								<dd>{installation.reachability_mode === "managed_tunnel" ? "Managed Tunnel" : "Existing HTTPS Endpoint"}</dd>
							</div>
							{data?.deployment && (
								<>
									<div><dt className="text-muted-foreground">Last Verified Ingress Probe</dt><dd>{timestamp(data.deployment.ingress_verified_at)}</dd></div>
									<div><dt className="text-muted-foreground">Last Verified DNS</dt><dd>{installation.reachability_mode === "managed_tunnel" ? timestamp(data.deployment.dns_verified_at) : "Not Separately Verified"}</dd></div>
									<div><dt className="text-muted-foreground">Last Signed Canary</dt><dd>{timestamp(data.deployment.signed_canary_at)}</dd></div>
									{data.deployment.revision != null && <div><dt className="text-muted-foreground">Ingress and Canary Evidence Revision</dt><dd>{data.deployment.revision}</dd></div>}
									{installation.reachability_mode === "managed_tunnel" && (
										<>
											<div><dt className="text-muted-foreground">Last Observed Connector Session</dt><dd>{({
												connected: "Connected", disconnected: "Disconnected", unobserved: "Unobserved",
												waiting_for_projection: "Waiting for Token Projection",
											})[data.deployment.connector_session ?? "unobserved"]}</dd></div>
											<div><dt className="text-muted-foreground">Connector Session Checked</dt><dd>{timestamp(data.deployment.connector_checked_at)}</dd></div>
											<div><dt className="text-muted-foreground">Connector Last Ready</dt><dd>{timestamp(data.deployment.connector_last_ready_at)}</dd></div>
										</>
									)}
								</>
							)}
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
						</dl>
						<section aria-labelledby="cf-spend-heading" className="space-y-2">
							<h4 id="cf-spend-heading" className="font-semibold">Spending</h4>
							<dl className="grid grid-cols-1 gap-4 text-sm sm:grid-cols-2">
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
									Fleet Envelope Backstop
								</dt>
								<dd>{installation.fleet_envelope_usd} USD/month</dd>
							</div>
							{installation.spend && (
								<>
									<div>
										<dt className="text-muted-foreground">Estimated Bifrost Usage This UTC Month</dt>
										<dd>{installation.spend.estimated_usd === null ? "Spend Ledger Unavailable" : `${installation.spend.estimated_usd} USD`}</dd>
										{installation.spend.state === "budget_reached" && <dd>Estimated usage reached the monthly limit.</dd>}
									</div>
									<div><dt className="text-muted-foreground">Estimated Usage Outside Reservations</dt><dd>{amount(installation.spend.settled_usd)}</dd><dd className="text-muted-foreground">Workflow activity charges are estimated and cannot yet be split into settled and unsettled amounts.</dd></div>
									<div><dt className="text-muted-foreground">Outstanding Reservations</dt><dd>{amount(installation.spend.reserved_usd)}</dd></div>
									<div><dt className="text-muted-foreground">Remaining Monthly Headroom</dt><dd>{amount(installation.spend.remaining_headroom_usd)}</dd></div>
									<div><dt className="text-muted-foreground">Monthly Limit Resets</dt><dd>{timestamp(installation.spend.period_end)}</dd></div>
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
									{installation.spend.coverage?.length ? (
										<div>
											<dt className="text-muted-foreground">Usage Estimate Coverage</dt>
											<dd>
												<BoundedList>
													{installation.spend.coverage.map((line) => <li key={line}>{line}</li>)}
												</BoundedList>
											</dd>
										</div>
									) : null}
								</>
							)}
							</dl>
						</section>
						{Boolean(data?.lanes?.length) && (
							<section className="space-y-3" aria-labelledby="cf-placement-heading">
								<h4 id="cf-placement-heading" className="font-semibold">Overflow Placement</h4>
								<p>Observed {timestamp(data?.observed_at)}</p>
								<Button variant="outline" disabled={isFetching} onClick={() => void refetch()}>Refresh Capacity Status</Button>
								{data?.lanes?.map((lane) => (
									<section key={lane.lane} className="space-y-2 text-sm">
										<h5 className="font-semibold">{lane.lane === "workers" ? "Workers" : "Containers"}</h5>
										<dl className="grid gap-2 sm:grid-cols-2">
											<div><dt className="text-muted-foreground">Eligible Queued</dt><dd>{lane.eligible_queued}</dd></div>
											<div><dt className="text-muted-foreground">Claimed</dt><dd>{lane.claimed}</dd></div>
											<div><dt className="text-muted-foreground">Running</dt><dd>{lane.running}</dd></div>
											<div><dt className="text-muted-foreground">Queued Work Fitting the Primary</dt><dd>{lane.primary_fit ?? "Unobserved"}</dd></div>
											<div><dt className="text-muted-foreground">Primary Saturation</dt><dd>{lane.primary_saturated == null ? "Unobserved" : lane.primary_saturated ? "At Capacity Ceiling" : "Ceiling Not Confirmed"}</dd></div>
											<div><dt className="text-muted-foreground">Oldest Eligible Queue Age / Launch Gate</dt><dd>{lane.oldest_eligible_queue_age_seconds == null
												? "Queue Empty" : `${lane.oldest_eligible_queue_age_seconds.toFixed(2)} s`} / {lane.launch_gate_seconds} s</dd></div>
										</dl>
										<PlacementReasons reasons={lane.reasons} />
									</section>
								))}
							</section>
						)}
						{data?.recovery && !job && (
							<section aria-label="Cloudflare Recovery">
								<p>Error Code <code>{data.recovery.code}</code></p>
								<RecoveryDetails recovery={data.recovery} />
							</section>
						)}
						{canWrite && (
							<div className="flex flex-wrap gap-2">
								<Button ref={policyRef} variant="outline" disabled={busy || editing || installation.desired_state === "disconnected"}
									onClick={() => setEditing(true)}
								>
									Edit Policy
								</Button>
								<Button
									ref={enableRef}
									disabled={enableDisabled}
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
								<Dialog open={rotateOpen} onOpenChange={(open) => {
									if (open) rotationAccepted.current = false;
									setRotateOpen(open);
								}}>
									<DialogTrigger asChild>
										<Button variant="outline" disabled={busy}>Rotate API Token</Button>
									</DialogTrigger>
									<DialogContent onCloseAutoFocus={(event) => {
										if (!rotationAccepted.current) return;
										rotationAccepted.current = false;
										event.preventDefault();
										jobStatusRef.current?.focus();
									}}>
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
													<Button type="button" variant="outline" onClick={() => void refetch()}>Refresh and Review Settings</Button>
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
					</section>
				)}
				{mutation.error && (
					<div ref={actionErrorRef} tabIndex={-1} role="region" aria-labelledby="cf-action-error" className="space-y-2">
						<h3 id="cf-action-error" className="font-semibold">Cloudflare Action Error</h3>
						<p>{getErrorMessage(mutation.error, "Could not start the Cloudflare job.")}</p>
						<ErrorDetails error={mutation.error} />
						{mutation.error instanceof ApiError && mutation.error.statusCode === 409 && (
							<Button variant="outline" disabled={isFetching} onClick={() => {
								mutation.reset();
								void refetch();
							}}>Refresh and Review Settings</Button>
						)}
					</div>
				)}
				{canRead && jobId && !disconnected && (
					<section ref={jobStatusRef} tabIndex={-1} className="space-y-2" aria-labelledby="cf-job-status-heading">
						<h3 id="cf-job-status-heading" className="font-semibold">Cloudflare Job Status</h3>
						<p role="status" className={jobAnnouncement ? undefined : "sr-only"}>{jobAnnouncement}</p>
						{!jobAnnouncement && !job?.progress.phase && <p>Waiting for Cloudflare job status.</p>}
						{job?.progress.phase && job.progress.phase !== jobAnnouncement && <p>{job.progress.phase}</p>}
						{jobPending && (
							<Button variant="outline" disabled={readingJob} onClick={() => {
								void refetch();
								readbackRef.current?.();
							}}>Check Job Status</Button>
						)}
						{jobRefused && (job?.error || typeof job?.result?.code === "string") && (
							<p>Error Code <code>{job.error?.code ?? String(job.result?.code)}</code></p>
						)}
						{jobRefused && typeof job?.result?.reason === "string" && <p>{job.result.reason}</p>}
						{jobRefused && !data?.recovery && typeof job?.result?.remedy === "string" && <p>{job.result.remedy}</p>}
						{jobRefused && data?.recovery && job?.id === data.latest_job_id && <RecoveryDetails recovery={data.recovery} />}
						{jobRefused && Array.isArray(instructions) && (
							<BoundedList as="ol" label="Recovery Instructions" className="list-decimal pl-5 text-sm">
								{instructions.filter((instruction): instruction is string => typeof instruction === "string")
									.map((instruction) => <li key={instruction}>{instruction}</li>)}
							</BoundedList>
						)}
						{jobRefused && Array.isArray(legacyWorkers) && (
							<BoundedList label="Workers Requiring Review" className="text-sm">
								{legacyWorkers.map((worker) => (
									<li className="break-all" key={String(worker.worker_id)}>
										Worker {worker.worker_id} · Scope {worker.scope} · Resource UID {worker.resource_uid ?? "Unavailable"}
									</li>
								))}
							</BoundedList>
						)}
					</section>
				)}
			</CardContent>
		</Card>
	);
}
