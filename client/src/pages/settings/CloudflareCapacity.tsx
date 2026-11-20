import { useEffect, useState, type FormEvent } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";

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
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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
import { getErrorMessage } from "@/lib/api-error";
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

function CloudflareConfigurationForm({ status, onSaved }: {
	status: CloudflareCapacityStatus;
	onSaved: () => void;
}) {
	const installation = status.installation;
	const settings = installation?.policy;
	const [saving, setSaving] = useState(false);
	const [failure, setFailure] = useState<string>();

	async function save(event: FormEvent<HTMLFormElement>) {
		event.preventDefault();
		if (saving) return;
		const form = event.currentTarget;
		const fields = new FormData(form);
		if (!installation && fields.get("consent") !== "on") {
			setFailure("Accept the Cloudflare terms to connect.");
			return;
		}
		const sharedCap = String(fields.get("shared_cap") ?? "");
		const policy = {
			version: 1 as const,
			workers: {
				launcher_cap: Number(fields.get("launcher_cap")),
				slots_per_launcher: Number(fields.get("slots")),
				max_lifetime_seconds: Number(fields.get("lifetime")),
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
			await configureCloudflareCapacity(request);
			onSaved();
		} catch (error) {
			setFailure(getErrorMessage(error, "Could not save Cloudflare capacity."));
		} finally {
			setSaving(false);
		}
	}

	return (
		<form className="space-y-4" onSubmit={save}>
			{!installation && (
				<div className="space-y-3">
					<Label htmlFor="cf-account-id">Cloudflare Account ID</Label>
					<Input id="cf-account-id" name="account_id" pattern="[a-f0-9]{32}" required />
					<Label htmlFor="cf-ingress-origin">Existing HTTPS Endpoint</Label>
					<Input id="cf-ingress-origin" name="origin" type="url" required />
					<Label htmlFor="cf-connect-token">Cloudflare API Token</Label>
					<Input id="cf-connect-token" name="api_token" type="password" autoComplete="new-password" required />
					<p className="text-sm text-muted-foreground">{status.consent_text}</p>
					<label className="flex items-center gap-2 text-sm">
						<input type="checkbox" name="consent" required />
						I Accept the Cloudflare Terms Above
					</label>
				</div>
			)}
			<div className="grid gap-3 sm:grid-cols-2">
				<div><Label htmlFor="cf-launcher-cap">Workers Launcher Limit</Label><Input id="cf-launcher-cap" name="launcher_cap" type="number" min={1} max={1000} defaultValue={settings?.workers.launcher_cap ?? 1} required /></div>
				<div><Label htmlFor="cf-slots">Slots Per Launcher</Label><Input id="cf-slots" name="slots" type="number" min={1} max={4} defaultValue={settings?.workers.slots_per_launcher ?? 2} required /></div>
				<div><Label htmlFor="cf-lifetime">Maximum Lifetime in Seconds</Label><Input id="cf-lifetime" name="lifetime" type="number" min={60} max={604800} defaultValue={settings?.workers.max_lifetime_seconds ?? 3600} required /></div>
				<div><Label htmlFor="cf-monthly">Monthly Usage Limit in USD</Label><Input id="cf-monthly" name="monthly_limit" type="number" min="0.01" step="0.01" defaultValue={settings?.monthly_limit_usd ?? "30.00"} required /></div>
				<div><Label htmlFor="cf-shared-cap">Shared Managed Worker Limit</Label><Input id="cf-shared-cap" name="shared_cap" type="number" min={1} max={1000} defaultValue={settings?.shared_managed_worker_cap ?? undefined} /></div>
			</div>
			<label className="flex items-center gap-2 text-sm">
				<input type="checkbox" name="workflows" defaultChecked={settings?.workers.work_kinds.includes("workflow") ?? true} />
				Allow Approved Workflows
			</label>
			{failure && <p role="alert">{failure}</p>}
			<Button type="submit" disabled={saving}>{installation ? "Save Policy" : "Connect Cloudflare"}</Button>
		</form>
	);
}

export function CloudflareCapacity() {
	const authorization = useAuthorization();
	const canRead = authorization.canAt("platform.read", GLOBAL_TARGET);
	const canWrite = authorization.canAt("platform.readwrite", GLOBAL_TARGET);
	const { data, error, isLoading, refetch } = useQuery({
		queryKey: ["cloudflare-capacity"],
		queryFn: getCloudflareCapacity,
		enabled: canRead,
	});
	const [acceptedJobId, setAcceptedJobId] = useState<string>();
	const [observedJob, setObservedJob] = useState<PlatformJob>();
	const [rotateOpen, setRotateOpen] = useState(false);
	const [rotating, setRotating] = useState(false);
	const [rotateError, setRotateError] = useState<string>();
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
		onSuccess: (accepted) => {
			setAcceptedJobId(accepted.job_id);
			void refetch();
		},
	});

	useEffect(() => {
		if (!jobId) return;
		const update = (next: PlatformJob) => {
			setObservedJob((current) =>
				current?.id === next.id && current.revision >= next.revision ? current : next,
			);
			if (terminal.has(next.status)) void refetch();
		};
		const unsubscribe = webSocketService.onPlatformJobUpdate(jobId, update);
		const observation = observePlatformJob(jobId, update);
		void observation.promise.catch(() => undefined);
		return () => {
			unsubscribe();
			observation.cancel();
		};
	}, [jobId, refetch]);

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
			setRotateError(getErrorMessage(error, "Could not queue Cloudflare API token rotation."));
		} finally {
			setRotating(false);
		}
	}

	const busy =
		mutation.isPending ||
		rotating ||
		Boolean(jobId && (!job || !terminal.has(job.status)));
	const connectionStatus =
		installation?.desired_state === "disconnected"
			? "Disconnecting"
			: installation?.desired_state === "scaled_in"
				? "Scaled In"
				: installation?.admission_open
					? "Enabled"
					: "Admission Closed";

	return (
		<Card>
			<CardHeader>
				<CardTitle>Scale Out With Cloudflare</CardTitle>
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
					<p role="alert">
						{getErrorMessage(
							error,
							"Could not load Cloudflare capacity.",
						)}
					</p>
				)}
				{data && !installation && !canWrite && (
					<p>No Cloudflare connection is configured.</p>
				)}
				{data && canWrite && (!installation || editing) && (
					<CloudflareConfigurationForm status={data} onSaved={() => {
						setEditing(false);
						void refetch();
					}} />
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
							<div>
								<dt className="text-muted-foreground">
									Existing HTTPS Endpoint
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
									Slots Per Launcher
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
									Monthly Usage Limit
								</dt>
								<dd>
									{installation.policy.monthly_limit_usd} USD
								</dd>
							</div>
							<div>
								<dt className="text-muted-foreground">
									Fleet Usage Envelope
								</dt>
								<dd>{installation.fleet_envelope_usd} USD</dd>
							</div>
							{installation.spend && (
								<>
									<div>
										<dt className="text-muted-foreground">Estimated Bifrost Usage This UTC Month</dt>
										<dd>{installation.spend.estimated_usd === null ? "Spend Ledger Unavailable" : `${installation.spend.estimated_usd} USD`}</dd>
										{installation.spend.state === "budget_reached" && <dd>Budget Reached</dd>}
									</div>
									<div>
										<dt className="text-muted-foreground">Workers Paid Subscription (Billed by Cloudflare)</dt>
										<dd>{installation.spend.workers_paid_subscription_usd} USD per Month</dd>
									</div>
									<div>
										<dt className="text-muted-foreground">Cloudflare Analytics Advisory</dt>
										<dd>{installation.spend.analytics?.request_difference != null
											? `${installation.spend.analytics.worker_requests} sampled Worker requests, ${installation.spend.analytics.request_difference} difference from the Bifrost request estimate`
											: "No Attributable Sample"}</dd>
									</div>
									<div>
										<dt className="text-muted-foreground">Usage Estimate Coverage</dt>
										<dd>
											{installation.spend.coverage?.map((line) => <div key={line}>{line}</div>)}
										</dd>
									</div>
								</>
							)}
						</dl>
						{canWrite && (
							<div className="flex flex-wrap gap-2">
								<Button variant="outline" disabled={busy || installation.desired_state === "disconnected"}
									onClick={() => setEditing((open) => !open)}
								>
									{editing ? "Cancel Policy Edit" : "Edit Policy"}
								</Button>
								<Button
									disabled={busy || installation.admission_open || installation.desired_state === "disconnected"}
									onClick={() => mutation.mutate("enable")}
								>
									Enable
								</Button>
								<Button variant="outline" disabled={busy || installation.desired_state !== "enabled"}
									onClick={() => mutation.mutate("reconcile")}
								>
									Reconcile
								</Button>
								{installation.desired_state === "scaled_in" && (
									<>
										<Button variant="outline" disabled={busy} onClick={() => mutation.mutate("rotate-root")}>Rotate Control Root</Button>
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
											{rotateError && <p role="alert">{rotateError}</p>}
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
					<p role="alert">
						{getErrorMessage(
							mutation.error,
							"Could not start the Cloudflare job.",
						)}
					</p>
				)}
				{jobId && (
					<p role="status" aria-live="polite">
						{job?.error?.message ??
							job?.progress.phase ??
							"Cloudflare Job Queued"}
					</p>
				)}
				{Array.isArray(instructions) && (
					<ol className="list-decimal space-y-1 pl-5 text-sm">
						{instructions.filter((instruction): instruction is string => typeof instruction === "string")
							.map((instruction) => <li key={instruction}>{instruction}</li>)}
					</ol>
				)}
				{Array.isArray(legacyWorkers) && (
					<ul className="space-y-1 text-sm">
						{legacyWorkers.map((worker) => (
							<li className="break-all" key={String(worker.worker_id)}>
								Worker {worker.worker_id} · Scope {worker.scope} · Resource UID {worker.resource_uid ?? "Unavailable"}
							</li>
						))}
					</ul>
				)}
			</CardContent>
		</Card>
	);
}
