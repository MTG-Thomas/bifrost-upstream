const sha = /^[a-f0-9]{40}$/;
const knownStates = new Set(['active', 'disabled_manually', 'disabled_inactivity', 'disabled_fork']);

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') return Object.fromEntries(
    Object.keys(value).sort().map(key => [key, canonical(value[key])]));
  return value;
}

function equal(actual, expected, name) {
  if (JSON.stringify(canonical(actual)) !== JSON.stringify(canonical(expected))) {
    throw new Error(`${name} changed or is unreviewed; hold.`);
  }
}

export function definitionBlobs(snapshot) {
  if (snapshot.tree?.truncated !== false || !Array.isArray(snapshot.tree.tree) ||
      !sha.test(snapshot.targetTree) || snapshot.tree.sha !== snapshot.targetTree ||
      snapshot.targetCommitMetadata?.sha !== snapshot.targetCommit ||
      snapshot.targetCommitMetadata?.tree?.sha !== snapshot.targetTree) {
    throw new Error('Complete commit-bound recursive tree is required; hold.');
  }
  const paths = new Set(), definitions = [];
  for (const entry of snapshot.tree.tree) {
    if (typeof entry.path !== 'string' || entry.path.includes('\\') ||
        entry.path.split('/').some(part => !part || part === '.' || part === '..') || paths.has(entry.path)) {
      throw new Error('Malformed or duplicate tree path; hold.');
    }
    paths.add(entry.path);
    if (!entry.path.startsWith('.github/workflows/') && !entry.path.startsWith('.github/actions/')) continue;
    if (entry.type === 'tree' && entry.mode === '040000') continue;
    if (entry.type !== 'blob' || !['100644', '100755'].includes(entry.mode) || !sha.test(entry.sha)) {
      throw new Error('Unreviewable workflow/action entry; hold.');
    }
    definitions.push([entry.path, { sha: entry.sha, mode: entry.mode }]);
  }
  if (!definitions.some(([path]) => path.startsWith('.github/workflows/'))) {
    throw new Error('Missing workflow definitions is not inactivity evidence; hold.');
  }
  return Object.fromEntries(definitions.sort(([a], [b]) => a.localeCompare(b)));
}

export function verifyTriggerGate(candidate, snapshot, policy, now = Date.now()) {
  if (policy?.status !== 'approved' || typeof policy.approvalReference !== 'string' ||
      !policy.approvalReference.trim() || policy.safety?.decision !== 'qualified' ||
      !policy.safety?.evidenceReference || policy.safety?.noProductionConsumersConfirmed !== true) {
    throw new Error('Trigger approval and consumer/safety evidence are pending; hold.');
  }
  const captured = Date.parse(snapshot.capturedAt);
  if (!Number.isFinite(captured) || captured > now || now - captured > 300000) {
    throw new Error('Snapshot is stale or from the future; hold.');
  }
  if (snapshot.complete !== true || !sha.test(candidate.after) || snapshot.targetCommit !== candidate.after ||
      snapshot.fork?.full_name !== 'MTG-Thomas/bifrost-upstream' || snapshot.fork.id !== 1441017913 ||
      snapshot.fork.fork !== true ||
      snapshot.fork.default_branch !== 'main' || snapshot.fork.parent?.full_name !== 'gobifrost/bifrost' ||
      snapshot.fork.parent?.id !== 1074603568 || snapshot.upstream?.full_name !== 'gobifrost/bifrost' ||
      snapshot.upstream.id !== 1074603568 || snapshot.upstream.default_branch !== 'main') {
    throw new Error('Snapshot target/repository identity is incomplete or changed; hold.');
  }
  const inventory = snapshot.registeredWorkflows;
  if (inventory?.complete !== true || !Array.isArray(inventory.workflows) ||
      inventory.total_count !== inventory.workflows.length ||
      new Set(inventory.workflows.map(item => item.id)).size !== inventory.workflows.length ||
      inventory.workflows.some(item => !Number.isSafeInteger(item.id) || !knownStates.has(item.state) || typeof item.path !== 'string')) {
    throw new Error('Incomplete/unknown workflow inventory; 404 is not inactivity evidence.');
  }
  if (typeof snapshot.actions?.enabled !== 'boolean' || typeof snapshot.actions.allowed_actions !== 'string' ||
      typeof snapshot.workflowPermissions?.can_approve_pull_request_reviews !== 'boolean' ||
      !['read', 'write'].includes(snapshot.workflowPermissions.default_workflow_permissions) ||
      !Array.isArray(snapshot.rulesets) || !snapshot.protection) {
    throw new Error('Actions/rules/protection evidence missing; hold.');
  }
  equal(definitionBlobs(snapshot), policy.definitionBlobs, 'Workflow/local-action definitions');
  equal(snapshot.actions, policy.actions, 'Actions settings');
  equal(snapshot.workflowPermissions, policy.workflowPermissions, 'Workflow permissions');
  equal([...inventory.workflows].sort((a, b) => a.id - b.id), policy.registeredWorkflows, 'Registered workflows');
  equal(snapshot.rulesets, policy.rulesets, 'Repository rules');
  equal(snapshot.protection, policy.protection, 'Main protection');
  // This direct-ref writer has no review/queue adapter. Refuse newly protected
  // histories, even if a proposed policy happens to include those controls.
  if (snapshot.rulesets.length || snapshot.protection.httpStatus !== 404 ||
      snapshot.protection.reason !== 'Branch not protected') {
    throw new Error('Normal protection/review adapter required; direct-ref candidate cannot proceed.');
  }
  return { state: 'offline-trigger-plan-matches', target: candidate.after,
    approvalReference: policy.approvalReference };
}
