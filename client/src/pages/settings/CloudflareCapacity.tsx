import { useEffect, useState } from "react";
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
import { getErrorMessage } from "@/lib/api-error";
import { GLOBAL_TARGET } from "@/lib/authorization";
import { useAuthorization } from "@/services/authorization";
import {
	getCloudflareCapacity,
	startCloudflareAction,
	type CloudflareAction,
} from "@/services/cloudflareCapacity";
import { observePlatformJob, type PlatformJob } from "@/services/platformJobs";
import { webSocketService } from "@/services/websocket";

const terminal = new Set([
	"succeeded",
	"failed",
	"cancelled",
	"requires_action",
]);

export function CloudflareCapacity() {
	const authorization = useAuthorization();
	const canRead = authorization.canAt("platform.read", GLOBAL_TARGET);
	const canWrite = authorization.canAt("platform.readwrite", GLOBAL_TARGET);
	const { data, error, isLoading, refetch } = useQuery({
		queryKey: ["cloudflare-capacity"],
		queryFn: getCloudflareCapacity,
		enabled: canRead,
	});
	const [jobId, setJobId] = useState<string>();
	const [job, setJob] = useState<PlatformJob>();
	const installation = data?.installation;
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
			setJob(undefined);
			setJobId(accepted.job_id);
			void refetch();
		},
	});

	useEffect(() => {
		if (!jobId) return;
		const update = (next: PlatformJob) => {
			setJob((current) =>
				current && current.revision >= next.revision ? current : next,
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

	const busy =
		mutation.isPending ||
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
				{data && !installation && (
					<p>No Cloudflare connection is configured.</p>
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
									Monthly Spend Limit
								</dt>
								<dd>
									{installation.policy.monthly_limit_usd} USD
								</dd>
							</div>
							<div>
								<dt className="text-muted-foreground">
									Fleet Spend Envelope
								</dt>
								<dd>{installation.fleet_envelope_usd} USD</dd>
							</div>
						</dl>
						{canWrite && (
							<div className="flex flex-wrap gap-2">
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
			</CardContent>
		</Card>
	);
}
