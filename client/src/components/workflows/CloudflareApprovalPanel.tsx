import { useId, useState } from "react";
import { ApiError } from "@/lib/api-client";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
	ApprovalReviewError,
	acceptCloudflareApproval,
	defaultApprovalOptions,
	getCloudflareApproval,
	proposeCloudflareApproval,
	revokeCloudflareApproval,
	type ApprovalOptions,
	type ApprovalState,
} from "@/services/cloudflare-approval";

export const approvalStatuses: Record<ApprovalState["status"], string> = {
	approved: "Approved",
	pending_review: "Pending Review",
	revision_changed: "Revision Changed",
	ineligible: "Ineligible",
	temporarily_unavailable: "Temporarily Unavailable",
};

export function CloudflareApprovalPanel({ workflowId, onChange }: {
	workflowId: string;
	onChange?: () => void;
}) {
	const id = useId();
	const queryClient = useQueryClient();
	const queryKey = ["cloudflare-approval", workflowId];
	const status = useQuery({
		queryKey, queryFn: () => getCloudflareApproval(workflowId), retry: false,
	});
	const [optionsEdited, setOptionsEdited] = useState(false);
	const [busy, setBusy] = useState(false);
	const [error, setError] = useState("");
	const [confirmRevoke, setConfirmRevoke] = useState(false);
	const accessDenied = status.error instanceof ApiError
		&& (status.error.statusCode === 403 || status.error.statusCode === 404);
	const state = accessDenied ? undefined : status.data;
	const proposal = optionsEdited ? undefined : state?.proposal;
	const declaration = proposal ?? state?.current_declaration;

	async function request(action: () => Promise<void>) {
		setBusy(true);
		setError("");
		try {
			await action();
		} catch (cause) {
			setOptionsEdited(true);
			setError(cause instanceof Error ? cause.message : "The approval request failed.");
			if (cause instanceof ApprovalReviewError) {
				queryClient.setQueryData(queryKey, cause.state);
			} else {
				await status.refetch();
			}
		} finally {
			setBusy(false);
		}
	}

	function saveState(next: ApprovalState) {
		setOptionsEdited(false);
		queryClient.setQueryData(queryKey, next);
		onChange?.();
	}

	return (
		<section aria-label="Cloudflare Approval" className="space-y-4">
			<Badge variant="outline" role="status">
				{accessDenied ? "Unavailable" : state ? approvalStatuses[state.status] : status.isPending ? "Loading" : "Temporarily Unavailable"}
			</Badge>
			{(error || status.isError) && (
				<p className="text-sm text-destructive" role="alert">
					{error || status.error?.message || "Approval status could not be loaded."}
				</p>
			)}
			{!state && status.isError && !accessDenied && (
				<Button type="button" variant="outline" onClick={() => void status.refetch()}>Refresh Status</Button>
			)}
			{state?.can_manage && !accessDenied && (
				<form
					onChange={() => setOptionsEdited(true)}
					onSubmit={(event) => {
						event.preventDefault();
						const data = new FormData(event.currentTarget);
						const options: ApprovalOptions = {
							subrequest_allowance: Number(data.get("subrequests")),
							cpu_budget_ms: Number(data.get("cpu")),
							https_hosts: String(data.get("hosts") ?? "").split(/[\s,]+/).filter(Boolean),
						};
						void request(async () => saveState(await proposeCloudflareApproval(workflowId, options)));
					}}
					className="space-y-3"
				>
					<fieldset disabled={busy} className="grid gap-3 sm:grid-cols-2">
						<div className="space-y-1.5">
							<Label htmlFor={`${id}-subrequests`}>Subrequest Allowance</Label>
							<Input id={`${id}-subrequests`} name="subrequests" type="number" min={0} max={50} required
								defaultValue={(state.proposal ?? state.current_declaration)?.subrequest_allowance ?? defaultApprovalOptions.subrequest_allowance} />
						</div>
						<div className="space-y-1.5">
							<Label htmlFor={`${id}-cpu`}>CPU Budget (ms)</Label>
							<Input id={`${id}-cpu`} name="cpu" type="number" min={1} required
								defaultValue={(state.proposal ?? state.current_declaration)?.cpu_budget_ms ?? defaultApprovalOptions.cpu_budget_ms} />
						</div>
						{state.can_manage && (
							<div className="space-y-1.5 sm:col-span-2">
								<Label htmlFor={`${id}-hosts`}>Additional HTTPS Hosts</Label>
								<Input id={`${id}-hosts`} name="hosts" placeholder="api.example.com, *.example.com"
									defaultValue={(state.proposal ?? state.current_declaration)?.https_hosts.filter(
										(host) => !(state.proposal ?? state.current_declaration)?.required_https_hosts.includes(host),
									).join(", ")} />
							</div>
						)}
					</fieldset>
					<Button type="submit" variant="outline" disabled={busy}>Propose Approval</Button>
				</form>
			)}
			{state?.diagnostics?.map((diagnostic) => (
				<div className="rounded-md border p-3 text-sm" key={diagnostic.rule_id}>
					<code>{diagnostic.rule_id}</code>
					<p>{diagnostic.message}</p>
					<p className="text-muted-foreground">{diagnostic.hint}</p>
				</div>
			))}
			{declaration && (
				<div className="space-y-3 rounded-md border p-3 text-sm">
					<h4 className="font-semibold">{proposal ? "Approval Proposal" : "Current Approval"}</h4>
					<dl className="space-y-2">
						<div><dt>Packages</dt><dd>{declaration.packages.join(", ") || "None"}</dd></div>
						<div><dt>HTTPS Allowlist</dt><dd>{declaration.https_hosts.join(", ") || "None"}</dd></div>
						<div><dt>Budgets</dt><dd>{declaration.subrequest_allowance} Subrequests · {declaration.cpu_budget_ms} ms CPU</dd></div>
						<div><dt>Reviewed Digest</dt><dd className="break-all font-mono">{proposal?.review_sha256 ?? state?.current_revision}</dd></div>
					</dl>
					<details>
						<summary className="cursor-pointer">Review Source Pins and Scope</summary>
						<pre className="mt-2 max-h-64 overflow-auto text-xs">{JSON.stringify(declaration, null, 2)}</pre>
					</details>
					{proposal && proposal.wildcard_https_hosts.length > 0 && (
						<p className="rounded-md border border-destructive p-3" role="alert">
							<strong>Wildcard HTTPS Hosts</strong>
							{" allow requests to any matching single-label subdomain: "}
							{proposal.wildcard_https_hosts.join(", ")}
						</p>
					)}
					{proposal && state?.proposal_sources && (
						<details>
							<summary className="cursor-pointer">Review Full Source</summary>
							{Object.entries(state.proposal_sources).map(([path, source]) => (
								<div key={path} className="mt-2">
									<p className="break-all font-mono">{path}</p>
									<pre className="max-h-64 overflow-auto text-xs">{source}</pre>
								</div>
							))}
						</details>
					)}
					{proposal && state?.can_manage && (
						<>
							{proposal.organization_id === null && (
								<p>Approving this Global workflow allows it to run on Cloudflare for every tenant.</p>
							)}
							<Button type="button" disabled={busy} onClick={() => {
								void request(async () => {
									const next = await acceptCloudflareApproval(workflowId, {
										subrequest_allowance: proposal.subrequest_allowance,
										cpu_budget_ms: proposal.cpu_budget_ms,
										https_hosts: proposal.https_hosts.filter((host) => !proposal.required_https_hosts.includes(host)),
										review_sha256: proposal.review_sha256,
									});
									saveState({ ...next, proposal: null, proposal_sources: null });
								});
							}}>Accept Reviewed Digest</Button>
						</>
					)}
				</div>
			)}
			{state?.can_manage && state.current_revision && (
				confirmRevoke ? (
					<div className="flex flex-wrap gap-2">
						<Button type="button" variant="destructive" disabled={busy} onClick={() => {
							void request(async () => {
								saveState(await revokeCloudflareApproval(workflowId));
								setConfirmRevoke(false);
							});
						}}>Confirm Revocation</Button>
						<Button type="button" variant="outline" disabled={busy} onClick={() => setConfirmRevoke(false)}>Cancel</Button>
					</div>
				) : (
					<Button type="button" variant="outline" disabled={busy} onClick={() => setConfirmRevoke(true)}>Revoke Approval</Button>
				)
			)}
		</section>
	);
}
