import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { defaultApprovalOptions, listCloudflareApprovals, proposeCloudflareApproval, type ApprovalList } from "@/services/cloudflare-approval";
import { approvalStatuses, CloudflareApprovalPanel } from "./CloudflareApprovalPanel";

const queryKey = ["cloudflare-approval-list"];
const scopes = {
	provider: "Provider Organization",
	global: "Global",
	organization: "Customer Organization",
};

type Workflow = ApprovalList["workflows"][number];

export function CloudflareWorkflowApprovals() {
	const queryClient = useQueryClient();
	const { data, isPending, isError } = useQuery({ queryKey, queryFn: listCloudflareApprovals, retry: false });
	const [showWorkflows, setShowWorkflows] = useState(false);
	const [selected, setSelected] = useState<Workflow>();
	const refresh = () => void queryClient.invalidateQueries({ queryKey });
	const generate = useMutation({
		mutationFn: (workflow: Workflow) => proposeCloudflareApproval(workflow.workflow_id, defaultApprovalOptions),
		onSuccess: (state, workflow) => {
			queryClient.setQueryData(["cloudflare-approval", workflow.workflow_id], state);
			refresh();
			setSelected(workflow);
		},
	});

	return (
		<section aria-label="Workflow Approvals" className="space-y-3 rounded-md border p-4">
			<h3 className="font-semibold">Workflow Approvals</h3>
			{isPending && <p role="status">Loading workflow approvals…</p>}
			{isError && <p role="alert">Could not load workflow approvals.</p>}
			{data && (
				<>
					<dl className="flex flex-wrap gap-4 text-sm">
						{(Object.keys(approvalStatuses) as (keyof typeof approvalStatuses)[]).map((status) => (
							<div key={status}>
								<dt className="text-muted-foreground">{approvalStatuses[status]}</dt>
								<dd>{data.counts[status]}</dd>
							</div>
						))}
					</dl>
					<Button variant="outline" aria-expanded={showWorkflows} onClick={() => setShowWorkflows(!showWorkflows)}>
						{showWorkflows ? "Hide Workflow Approvals" : "Review Eligible Workflows"}
					</Button>
					{showWorkflows && (
						<ul className="max-h-96 divide-y overflow-auto">
							{data.workflows.map((workflow) => {
								const offerDefaults = data.can_propose && !workflow.has_proposal
									&& (workflow.status === "pending_review" || workflow.status === "revision_changed");
								return (
									<li key={workflow.workflow_id} className="flex flex-wrap items-center justify-between gap-2 py-2">
										<div>
											<p className="font-medium">{workflow.name}</p>
											<p className="text-sm text-muted-foreground">
												{scopes[workflow.scope]} · {approvalStatuses[workflow.status]}
											</p>
										</div>
										<Button variant="outline" size="sm" disabled={generate.isPending}
											aria-label={offerDefaults ? `Generate Proposal for ${workflow.name}` : `Review Approval for ${workflow.name}`}
											onClick={() => {
												generate.reset();
												if (offerDefaults) generate.mutate(workflow);
												else setSelected(workflow);
											}}>
											{offerDefaults ? "Generate Proposal" : "Review Approval"}
										</Button>
									</li>
								);
							})}
							{data.workflows.length === 0 && <li className="py-2">No workflows in your authorized scope.</li>}
						</ul>
					)}
					{generate.isPending && <p role="status">Analyzing workflow source…</p>}
					{generate.isError && <p role="alert">Could not generate the workflow proposal.</p>}
					<Dialog open={selected !== undefined} onOpenChange={(open) => { if (!open) setSelected(undefined); }}>
						<DialogContent className="max-h-[90vh] overflow-auto sm:max-w-2xl">
							<DialogHeader>
								<DialogTitle>{selected?.name}</DialogTitle>
								<DialogDescription>Cloudflare Workers approval for this workflow.</DialogDescription>
							</DialogHeader>
							{selected && <CloudflareApprovalPanel workflowId={selected.workflow_id} onChange={refresh} />}
						</DialogContent>
					</Dialog>
				</>
			)}
		</section>
	);
}
