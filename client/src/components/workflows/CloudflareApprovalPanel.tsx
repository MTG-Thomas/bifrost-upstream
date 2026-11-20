import { useId, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
	acceptCloudflareApproval,
	getCloudflareApproval,
	proposeCloudflareApproval,
	revokeCloudflareApproval,
	type ApprovalOptions,
	type ApprovalState,
} from "@/services/cloudflare-approval";

const statuses: Record<ApprovalState["status"], string> = {
	approved: "Approved",
	pending_review: "Pending Review",
	revision_changed: "Revision Changed",
	ineligible: "Ineligible",
	temporarily_unavailable: "Temporarily Unavailable",
};

export function CloudflareApprovalPanel({ workflowId }: { workflowId: string }) {
	const id = useId();
	const queryClient = useQueryClient();
	const queryKey = ["cloudflare-approval", workflowId];
	const status = useQuery({
		queryKey, queryFn: () => getCloudflareApproval(workflowId), retry: false,
	});
	const [review, setReview] = useState<{ state: ApprovalState; options: ApprovalOptions }>();
	const [busy, setBusy] = useState(false);
	const [error, setError] = useState("");
	const [confirmRevoke, setConfirmRevoke] = useState(false);
	const state = review?.state ?? status.data;
	const proposal = review?.state.proposal;
	const declaration = proposal ?? state?.current_declaration;

	async function request(action: () => Promise<void>) {
		setBusy(true);
		setError("");
		try {
			await action();
		} catch (cause) {
			setReview(undefined);
			setError(cause instanceof Error ? cause.message : "The approval request failed.");
			await status.refetch();
		} finally {
			setBusy(false);
		}
	}

	function saveState(next: ApprovalState) {
		setReview(undefined);
		queryClient.setQueryData(queryKey, next);
	}

	return (
		<section aria-label="Cloudflare Approval" className="space-y-4">
			<Badge variant="outline" role="status">
				{state ? statuses[state.status] : status.isPending ? "Loading" : "Temporarily Unavailable"}
			</Badge>
			{(error || status.isError) && (
				<p className="text-sm text-destructive" role="alert">
					{error || "Approval status could not be loaded."}
				</p>
			)}
			{!state && status.isError && (
				<Button type="button" variant="outline" onClick={() => void status.refetch()}>Refresh Status</Button>
			)}
			{state && (
				<form
					onChange={() => setReview(undefined)}
					onSubmit={(event) => {
						event.preventDefault();
						const data = new FormData(event.currentTarget);
						const options: ApprovalOptions = {
							subrequest_allowance: Number(data.get("subrequests")),
							cpu_budget_ms: Number(data.get("cpu")),
							https_hosts: String(data.get("hosts") ?? "").split(/[\s,]+/).filter(Boolean),
						};
						void request(async () => {
							const next = await proposeCloudflareApproval(workflowId, options);
							setReview({ state: next, options });
						});
					}}
					className="space-y-3"
				>
					<fieldset disabled={busy} className="grid gap-3 sm:grid-cols-2">
						<div className="space-y-1.5">
							<Label htmlFor={`${id}-subrequests`}>Subrequest Allowance</Label>
							<Input id={`${id}-subrequests`} name="subrequests" type="number" min={0} max={50} required
								defaultValue={state.current_declaration?.subrequest_allowance ?? 10} />
						</div>
						<div className="space-y-1.5">
							<Label htmlFor={`${id}-cpu`}>CPU Budget (ms)</Label>
							<Input id={`${id}-cpu`} name="cpu" type="number" min={1} required
								defaultValue={state.current_declaration?.cpu_budget_ms ?? 1000} />
						</div>
						{state.can_manage && (
							<div className="space-y-1.5 sm:col-span-2">
								<Label htmlFor={`${id}-hosts`}>Additional HTTPS Hosts</Label>
								<Input id={`${id}-hosts`} name="hosts" placeholder="api.example.com, *.example.com"
									defaultValue={state.current_declaration?.https_hosts.filter(
										(host) => !state.current_declaration?.required_https_hosts.includes(host),
									).join(", ")} />
							</div>
						)}
					</fieldset>
					<Button type="submit" variant="outline" disabled={busy}>Propose Approval</Button>
				</form>
			)}
			{state?.diagnostics.map((diagnostic) => (
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
						<pre className="mt-2 overflow-auto text-xs">{JSON.stringify(declaration, null, 2)}</pre>
					</details>
					{proposal && review && state?.can_manage && (
						<Button type="button" disabled={busy} onClick={() => {
							void request(async () => saveState(await acceptCloudflareApproval(workflowId, {
								...review.options, review_sha256: proposal.review_sha256,
							})));
						}}>Accept Reviewed Digest</Button>
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
