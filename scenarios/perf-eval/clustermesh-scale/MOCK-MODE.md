# ClusterMesh Scale — MOCK mode (KWOK + mock-cilium-agent)

This scenario can run in **mock mode**, where each cluster's real workload nodes
are replaced by **KWOK virtual nodes + a forked mock-cilium-agent** (real Cilium
control plane, DryMode/fake datapath). This reduces the per-node cost from a whole
VM (~4 vCPU) to a free API object + a tiny Pod (~9m CPU / ~56Mi, measured), giving
roughly a **10× vCPU reduction** at the 10k-node target while keeping the entire
AKS + ACNS product surface (kube-apiserver, clustermesh-apiserver, kvstoremesh,
cilium-operator) **real** — those remain the System Under Test.

The mock framework itself (the agent fork, image build, and per-cluster deployer
`provision-kwok-layer.sh`) lives in the companion `mock-clustermesh/` tree. This
doc covers only the **telescope-side integration**.

## Architecture

```
Real (today)                          Mock mode
------------                          ---------
20 x D4s_v5 workload nodes/cluster    2 x D8s_v5 thin worker pool/cluster
  each = a real VM + kubelet            hosts ONLY the mock-cilium-agent Pods
  + real cilium-agent (DaemonSet)     100 KWOK virtual nodes/cluster (API objects)
  + real workload Pods                  each served by 1 mock-cilium-agent Pod
                                        (real watches/identities/policy/clustermesh
                                         consume; datapath faked)
```

The real AKS-managed cilium-agent still runs on the thin worker pool (it is the
harness agent); the **mock** agents are what represent the simulated nodes.

## What is integrated here (validated 2026-06-23)

| Piece | File | Notes |
|-------|------|-------|
| Thin-worker-pool tfvars | `terraform-inputs/azure-2-mock.tfvars`, `azure-2-mock-shared-dsv3.tfvars`, `azure-2-mock-shared-dsv4.tfvars` | All use the n=100 shared-VNet/no-peering layout. The DSv3/DSv4 variants support quota-qualified non-Canary stages; the older East US 2 EUAP variant is retained for reproducibility. |
| CL2 mock gating | `modules/.../config/config.yaml`, `modules/scale-test*.yaml`, `modules/clustermesh.yaml` | `CL2_MOCK_MODE=true` → workload Pods get `nodeSelector type=kwok` + the `kwok.x-k8s.io/node` toleration, and a PodMonitor for `app=mock-cilium-agent:9962` is added so Prometheus scrapes the mock agents. Default `false` → real runs unchanged. |
| Mock-agent PodMonitor | `modules/clustermesh/podmonitor-mock-agent.yaml` | Scrapes the mock agents on :9962 in the `mock-clustermesh` namespace. |
| Real-node kubelet/cAdvisor monitor | `config/prometheus-additional-monitors/real-node-kubelet.yaml` | Loaded before CL2's Prometheus readiness gate via `--prometheus-additional-monitors-path`. Uses the real Cilium DaemonSet as a one-pod-per-real-node discovery anchor, then scrapes each host's kubelet `/metrics` and `/metrics/cadvisor` on :10250. KWOK nodes never become targets. |
| KWOK synthetic resource usage | `config/prometheus-additional-monitors/00-kwok-resource-usage.yaml`, `02-kwok-resource-scrape-secret.yaml` | Applies KWOK `ResourceUsage`/`Metric` CRDs and a node-discovery scrape job. Workload annotations configure synthetic container/pod/node CPU-memory; values are explicitly simulation data. |
| API server backend resources | `modules/apiserver-backend-exporter/` | Fingerprints hidden API server HA replicas by `process_start_time_seconds` and exposes stable per-backend CPU counters/RSS to the native snapshot. |
| AKS prometheus storage fix | `modules/python/clusterloader2/utils.py`, `clustermesh-scale/scale.py` | Passes `--prometheus-pvc-storage-class=managed-csi` for `provider=aks`. CL2's default `ssd`/`kubernetes.io/gce-pd` class does NOT provision on AKS → prometheus-k8s stays Pending → "no endpoints". |
| `CL2_MOCK_MODE` wiring | `clustermesh-scale/scale.py` (`--mock-mode`), engine `execute.yml` (re-export) | Matrix var `mock_mode` → `MOCK_MODE` → `CL2_MOCK_MODE` → overrides → templates. |
| Mock topology | `steps/topology/clustermesh-scale-mock/` | Base validation runs on real thin-pool nodes. Execute configures managed telemetry first, then deploys the mock layer, then starts CL2; this prevents `az aks update` from destabilizing fresh KWOK Nodes. |
| Vendored deploy scripts | `scenarios/perf-eval/clustermesh-scale/mock/` | `provision-kwok-layer.sh` + `attrition-check.sh`, vendored from `mock-clustermesh/deploy/`. |
| Dedicated node-churn pool | Region-specific n=2/n=100 mock tfvars | Only mesh-1 gets a tainted `churnpool`. Node replacement targets this real pool; mock agents remain on the stable default pool and are recreated by their Parallel StatefulSet if a Pod is evicted. |

## How the mock layer is deployed (the `clustermesh-scale-mock` topology)

After terraform provisions the clusters (Fleet + ACNS + thin worker pool) and
before the CL2 engine runs, a topology step must deploy the KWOK + mock-agent
layer on **each** cluster. This is exactly what `mock-clustermesh/deploy/provision-kwok-layer.sh`
does (validated standalone). Per cluster:

```bash
KUBECONFIG_FILE=<cluster-kubeconfig> \
  NODE_COUNT=100 \
  ACR_HOST=<registry>.azurecr.io \
  AGENT_TAG=<mock-agent-image-tag> \
  CONSUME_CLUSTERMESH=true \
  mock-clustermesh/deploy/provision-kwok-layer.sh
```

This is now wired as the **`clustermesh-scale-mock` topology** (see below).

The topology (`steps/topology/clustermesh-scale-mock/`) reuses the base
`clustermesh-scale` validation (Fleet/ACNS/clustermesh-apiserver readiness +
cross-cluster smoke on the real thin pool — mock-compatible because it only asserts
nodes Ready and runs before the mock layer is added), then runs `deploy-mock-layer.yml`
which loops every cluster and invokes the vendored `mock/provision-kwok-layer.sh`.
The CL2 execute/collect steps delegate to the base scenario unchanged.

Preserved debug resumes can already contain synthetic Nodes from an earlier
attempt. Those resumes validate only real workers (`type!=kwok`) in the base
gate, repair stale live Fleet peers, and then rerun `deploy-mock-layer.yml`
before CL2. The deploy step migrates legacy naked agents to StatefulSet
ownership and restores the exact synthetic layer before any scenario starts.

The full `CL2_MOCK_MODE` flow: a matrix var `mock_mode: true` auto-exports as
`MOCK_MODE` → engine `execute.yml` re-exports `CL2_MOCK_MODE` → `scale.py configure
--mock-mode` writes `CL2_MOCK_MODE: true` into the overrides → the config templates
gate kwok-targeting + the mock PodMonitor.

### Finishing an already prepared worker replacement

`prepared_worker_retirement.py` completes one deliberately prepared n100
maintenance operation: remove a UID-pinned, drained `default` worker and return
the pool from four workers to three. It does not choose a worker, drain it,
restart Pods, change disruption budgets, or increase the generic recovery limits.
The source must already have its explicit CNI repair-hold record, and contain
only verified `kube-system` DaemonSet Pods.

The helper requires the preserved subscription, region, tfvars fingerprint,
unexpired parent/node-group leases, exact 100-member Fleet identities, healthy
VMSS state, all 100 owned mock agents and KWOK Nodes Ready, and strict real-agent
peer proof. It submits at most one retirement request, preserves failure
diagnostics, and requires the exact final count and unchanged workload identities.
An authorization failure is not retried through another identity or API.

The local CLI is read-only unless `--execute` is supplied:

```bash
python3 modules/python/clusterloader2/clustermesh-scale/prepared_worker_retirement.py \
  --resource-group "$RUN_ID" --confirm-resource-group "$RUN_ID" \
  --expected-subscription "$SUBSCRIPTION_ID" --expected-region "$REGION" \
  --expected-tfvars-sha "$(sha256sum "$TFVARS_PATH" | awk '{print $1}')" \
  --role "$ROLE" --node-name "$NODE_NAME" --node-uid "$NODE_UID" \
  --summary-file "$OUTPUT_DIR/retirement.json"
```

For the `new-pipeline-test.yml` preserved n100 resume stage, set
`scaleDebugPreparedRetirementRole`, `scaleDebugPreparedRetirementNode`, and
`scaleDebugPreparedRetirementUid` to the explicit prepared plan. The same helper
then runs through the existing service connection before ordinary resume gates.
All three parameters default to empty, which disables retirement.

Set `scaleDebugPreparedRetirementOnly=true` to run only this maintenance job,
without Fleet/pool reconciliation, CL2, or scenario telemetry setup. This is
useful when local permissions prevent finishing an already drained replacement.
It does not count as a workload run. Diagnostics are published as
`n100-prepared-worker-retirement-<build>-<attempt>`.

For read-only diagnostics, also set `scaleDebugPreparedRetirementObserveOnly=true`
and `scaleDebugRunWorkload=false`. This runs the same helper without `--execute`,
with a 600-second observer budget and no retirement, recovery, lease renewal,
CL2, or resource cleanup. The job uses private temporary credentials through the
normal service connection; failed, structurally owned Fleet members retain the
bounded diagnostic capture described below. The normal workload job is disabled
and other maintenance jobs are excluded in this mode. Conflicting mode flags
fail before observation. A failed health observation still fails the diagnostic job;
read-only mode does not turn unhealthy infrastructure into a successful proof.

Prepared retirement preserves the initial Fleet member payload and names any
unhealthy members. Structurally valid `PartialConnectivity` is observed read-only
through the existing bounded Fleet observer, but every member must actually
return to Connected with unchanged authoritative identities before retirement.
Other health errors, foreign identities and exhausted observations remain fatal.
For other health errors with fully validated ownership, the helper first collects
read-only diagnostics for at most five unhealthy members, bounded to five minutes:
API readiness, Nodes/Pods, system controllers/endpoints, UID-associated events,
NNC state, exact all-agent Cilium peer status, and bounded API-server/Cilium/CNS
container logs. Credentials remain in a private temporary directory outside the
published artifact and are deleted on exit. Diagnostic failures are recorded;
even healthy Cilium results do not waive the Fleet Connected gate or allow writes.
Transient read timeouts have bounded retries; deletion requests and authorization
failures are never retried automatically.

### Recovering an unreachable monitoring worker

`scaleDebugUnreachableWorkerRecoveryOnly=true` selects an isolated recovery job
for one explicitly pinned, unreachable `prompool` worker. Set
`scaleDebugRunWorkload=false`, leave other maintenance-only modes disabled, and
provide `scaleDebugUnreachableWorkerPlanJson`. The bounded JSON plan carries the
worker/provider identity, the complete mock/KWOK UID inventory, the failed
ClusterMesh API-server Pod/controller identities, and any explicitly selected
never-started framework replacements.

The job calls `unreachable_prom_worker_recovery.py` first without `--execute`.
It requires a read-only plan receipt and an unchanged input-plan hash before
calling the executable path once; task-level retries are disabled. Read-only
observation takes precedence over this recovery mode, and ordinary workload,
ARM-repair, and CNI-maintenance jobs cannot run alongside it in the same stage.
The job contains no CL2, lease-renewal, or generic resource-cleanup templates.

This phase is limited to the qualified unhealthy monitoring host and selected
failed framework Pods. It is not a substitute for the separate bounded CNI
worker-maintenance gates or the full workload handoff. Evidence is published as
`n100-unreachable-worker-recovery-<build>-<attempt>`, including the original plan,
read-only assessment, and execution summary; credentials remain private.
If the owned prompool VMSS is Failed, planning captures its aggregate status and
the pinned VM's status/extension codes before refusing mutation. Failed models
are never accepted as restart-ready, and private model or extension settings
are not included in this diagnostic capture.

`scaleDebugUnreachableWorkerReimageFailedOs=true` selects a separate, explicit
OS-reimage action for the captured mesh-96 VM ID and
`OSProvisioningClientError` only. It never restarts a Failed VM as though it
were healthy. The failure must be stably terminal, the target host must still
have no Ready or PVC-backed Pods, and all existing identity, controller,
healthy-default, and KWOK protections remain required. Only instance `0` of
the pinned prompool scale set is reimaged once; no OS-model, data-disk, pool
count, or default-worker changes are requested. Owned in-flight provisioning
states can be observed after that accepted action, but success still requires
fully Succeeded/Running models, a new boot, Ready Pods, and strict peer/Fleet
postproof. A new provisioning failure or ambiguous request retains the marker
and fails without another reimage.

If an accepted reimage outlives its observer, set
`scaleDebugUnreachableWorkerObserveBuildId` to the build that recorded acceptance,
with the same original plan and OS-reimage option. This downloads that run's
receipt and runs `--observe-accepted-action` without `--execute`. The observer
requires matching acceptance, VM ID, plan hash, Node UID, and live marker, and
waits only for the already submitted action. It never recreates markers,
reimages again, moves Pods, or clears the marker. A Ready host observation is
not a full framework/CNI recovery or workload-ready result.

For the captured terminal `OSProvisioningInternalError` **after** that accepted
reimage, `scaleDebugUnreachableWorkerReplaceFailedHostBuildId` supplies the
original acceptance receipt to a separate native replacement path. Observation
and reimage must both be disabled. The original identity plan can be loaded from
that same artifact by leaving `scaleDebugUnreachableWorkerPlanJson` empty; it
must still match the accepted receipt and live marker, and is never regenerated
from later live state. Planning requires the exact live action marker,
failed VM identity, original Node/Pod identities, and a PVC-free, entirely
Pod-Unready target. The live monitoring pool must be a fixed-count **User** pool
with one instance; the two healthy default workers must remain in the unchanged
System pool. A System monitoring pool or ambiguous state stops without mutation.

The operator submits one exact `az aks nodepool delete-machines` request, then
waits for native removal of the old VM, Node, Pod references, and network
container and a quiescent zero-count User pool. Only then may it submit one
native scale back to one. It does not delete/recreate the pool, retry a host
operation, manipulate the default pool, force-delete old Kubernetes objects,
or treat a failed model as healthy. A replacement receives a separately recorded
VM/Node/network-container identity; the original input manifest is never
rewritten. Framework moves still require actual capacity/IP and strict peer/Fleet
proof. Failed or ambiguous operations retain their receipts for diagnosis,
without automatic rollback or resubmission.

If capacity restoration is rejected for quota after native removal, set
`scaleDebugUnreachableWorkerQuotaObserveOnly=true` and
`scaleDebugUnreachableWorkerResumeReplacementBuildId` to the native-operation
build, retaining the original accepted-action build parameter. This selects
a separate **read-only** step instead of the recovery executable. It captures
current regional/family quota, the monitoring pool and VM inventory, the latest
AKS operation, and the previously audited accidental n2 resource group in the
same subscription. It never retries restoration, raises quota, or deletes
resources. Its completed observation is not host recovery or workload readiness.

### Planned single-worker CNI maintenance

`cni_worker_maintenance.py` provides the local operator path for one explicitly
identified default worker with UID-matched Azure CNI exhaustion evidence. Its
default mode only validates a plan; add `--execute` to perform maintenance.
Supply the preserved scope and tfvars fingerprint, the role, source Node name
and UID, exact provider ID and network-container ID, and a private kubeconfig:

```bash
python3 modules/python/clusterloader2/clustermesh-scale/cni_worker_maintenance.py \
  --resource-group "$RUN_ID" --confirm-resource-group "$RUN_ID" \
  --expected-subscription "$SUBSCRIPTION_ID" --expected-region "$REGION" \
  --expected-tfvars-sha "$TFVARS_SHA256" \
  --role "$ROLE" --node-name "$NODE_NAME" --node-uid "$NODE_UID" \
  --source-provider-id "$PROVIDER_ID" --source-network-container-id "$NC_ID" \
  --kubeconfig "$KUBECONFIG_FILE" --context "$CONTEXT" \
  --summary-file "$OUTPUT_DIR/cni-worker-maintenance.json"
```

The initial default pool must be quiescent at two or three workers, with exact
preserved ownership and full real-agent 99-peer proof. The helper submits one
temporary scale to four workers, proves fresh IP-batch growth on every new
worker, and moves only the source's proven Pending agents one at a time. Healthy
source evacuation is limited to 25 agents, with a 99-Ready floor and a
100-Ready barrier between moves. Per-move capacity, actual memory, destination
identity, controller ownership and Pod UID checks remain mandatory.

Only known, live-controller-owned, PVC-free system/framework Pods may be drained;
PDBs are honored. The helper then reuses prepared retirement to return to exactly
three healthy workers. Probe and scheduling cleanup are UID/ownership scoped,
and incomplete operations retain their failure evidence instead of reporting
success. It refuses multiple broken sources, image/operation drift, unknown
workloads and larger healthy-source evacuations. It does not raise the generic
mock-reconciliation guard or automatic capacity-repair limit.

After ARM accepts the surge and reports four/Succeeded/Running, the helper
observes expected new-worker registration and readiness for at most 300 seconds
(less if needed to preserve cleanup, retirement and final-proof budgets). Only
expected fresh-worker not-Ready observations are retried; original UID/provider,
pool configuration/image, unexpected Node/taint, or VMSS/count drift fails
immediately. Applicable fresh system DaemonSets must become Ready before strict
99-peer proof on **every** real Cilium agent. This is read-only convergence, not
another scale request or automatic rollback.

#### Continuing an accepted, pre-probe surge

Continuation is explicitly opt-in. Append **all three** arguments to the same
local command above, still read-only unless `--execute` is included:

```bash
  --resume-build-id "$ORIGINAL_BUILD_ID" \
  --resume-summary "$INPUT_DIR/maintenance.json" \
  --resume-manifest "$INPUT_DIR/resume-manifest.json"
```

`--resume-build-id` is a positive integer (default `0`); both paths default to
empty. Keep the original summary and manifest immutable and use a **different**
`--summary-file` for this invocation. The caller must obtain the original
`maintenance.json` artifact from the **same project, pipeline definition and
specified build**; the helper makes no ADO calls and cannot attest artifact
origin itself. The supplemental operator manifest must be pinned to genuine
pre-operation snapshots, not reconstructed from today's workloads:

```json
{
  "schema_version": 1,
  "source_build_id": 79797,
  "resource_group": "78751-f36f3d5a",
  "role": "mesh-89",
  "source_worker": "<original source Node name>",
  "source_worker_uid": "<original source Node UID>",
  "original_real_node_uids": {"<each original real Node, including prompool>": "<UID>"},
  "original_kwok_node_uids": {"<each of the exact 100 kwok-node-N names>": "<original Node UID>"},
  "agent_uids": {"<each of the exact 100 kwok-node-N names>": "<original Pod UID>"},
  "controller_uid": "<original kwok-node StatefulSet UID>",
  "fresh_node_uids": {"<each explicitly identified new default Node>": "<UID>"},
  "fresh_network_container_ids": {"<same new Node names>": "<network-container ID>"}
}
```

The map placeholders above must be expanded completely. Fresh Nodes number two
after an original count of two, or one after an original count of three.
New regular summaries persist original real/KWOK/agent/controller identities;
manifests must agree with those records. Legacy summaries require the same full
manifest; missing original identities are never inferred or silently accepted.

Only a failed, executed `waiting-for-surge` summary with an accepted scale and
the exact source quarantine is eligible. Any prior probe/growth or Pod-move
intent/completion, retirement, or unclean cleanup/exclusion evidence is refused.
Fresh validation repeats full 100-cluster ownership, authoritative Fleet
identities and lease checks, node-resource-group ownership, actual fixed
four-worker ARM/VMSS health, original configuration and instance sets, the exact
original-plus-new Node UID union, original 100 agent/KWOK/controller/template
identities, unchanged Pending/healthy source sets with UID-matched CNI events,
new Node-owned network containers and the unchanged drain allowlist.

A resumed plan leaves the existing hold untouched and reports
`mutation_started=false` for this invocation. Resumed execution never re-adds
the hold or submits scale/update: it requalifies system DaemonSets and all-agent
99-peer identity proof, demands genuine **new** IP-batch growth on the pinned
workers, then uses the unchanged bounded evacuation, memory/PDB/drain, single
prepared retirement to three, final proof and cleanup flow. Provenance includes
the source build, input hashes and original failure/quarantine evidence. A
failed continuation retains its hold and failure evidence; it is not permission
to resume partially moved workloads or perform arbitrary rollback.

An additional, explicit recovery path handles one empty newly added worker that
failed IP qualification. Supply `--recover-empty-fresh-node` and
`--recover-empty-fresh-uid` together with the three resume arguments. This accepts
only a `proving-fresh-ip-growth` checkpoint with persisted original identities,
zero workload moves, completed probe cleanup, and exactly one unqualified fresh
worker. The target must contain only current kube-system DaemonSet Pods, with no
mock agents, other workloads or PVCs. The read-only plan performs no host action.

Execution cordons that exact target, submits one single-VM Compute **redeploy**
request (not a pool update, scale, deletion or reimage), and observes the original
Node UID with a changed boot ID and successful/running instance state for at most
15 minutes. It preserves all original mock-agent UIDs and never retries the host
request. Normal scheduling is restored before fresh qualification; a failed
qualification retains an explicit target cordon. Host recovery is not successful
until real IP growth passes. IP qualification is bounded to five minutes, with
retirement/finalization time reserved, rather than consuming the full operation
budget on a permanently unprogrammed IP batch.

When local Azure permissions are unavailable, the same helper can run through
the existing service connection in the preserved n100 resume stage. Select
`debugMode=resume-existing`, `scaleDebugCniWorkerMaintenanceOnly=true`, and
`scaleDebugRunWorkload=false`; leave the other maintenance-only modes disabled.
Supply `scaleDebugCniWorkerRole`, `scaleDebugCniWorkerNode`,
`scaleDebugCniWorkerUid`, `scaleDebugCniWorkerProviderId`, and
`scaleDebugCniWorkerNetworkContainerId` from the explicit source plan.

For the narrowly scoped pre-probe continuation above, also set
`scaleDebugCniWorkerResumeBuildId` to the original positive build ID and
`scaleDebugCniWorkerResumeManifestJson` to the pinned schema-1 manifest JSON
(at most 32,768 bytes). Both are required together; defaults `0` and empty leave
normal maintenance unchanged. The job downloads
`n100-cni-worker-maintenance-<build>-1` from that exact build in the same project
and pipeline definition. It preserves the input summary and manifest in the new
diagnostics and passes the same three resume CLI arguments to both the read-only
plan and the explicit execution.

For the explicit empty-host recovery above, additionally set
`scaleDebugCniWorkerRecoverEmptyFreshNode` and
`scaleDebugCniWorkerRecoverEmptyFreshUid`. Both default to empty and require the
original checkpoint and manifest. They cannot select an original worker or a
fresh worker whose prior IP qualification succeeded. All ordinary capacity,
healthy-source evacuation, ownership and cleanup limits remain unchanged.

If that exact empty worker remains IP-unqualified after a completed redeploy,
`--replace-empty-fresh` (pipeline
`scaleDebugCniWorkerReplaceEmptyFresh=true`) selects one explicitly recorded
replacement, not another redeploy. It requires the failed post-redeploy
checkpoint, its original UID manifest and the matching retained quarantine.
The target must still contain only owned kube-system DaemonSet Pods and no mock
agents or PVCs. Original and already qualified workers remain protected.

Replacement deletes only the named failed machine, observes its VM, Node, Pod
references and network container disappear at count three, then submits one
restoration to four. It never requests five or uses an arbitrary scale-down.
Exactly one distinct new instance, Node UID and network-container ID must appear;
the derived manifest and identity transition are persisted without rewriting the
input evidence. Real IP qualification and the original source evacuation and
retirement still follow. The pipeline uses a bounded 60-minute helper budget for
this explicit path, retaining the existing per-phase and cleanup limits. A
failed or ambiguous request is recorded rather than retried automatically.

New replacement NNC objects may briefly exist before their status is published.
Only the explicitly new, Node-UID-pinned object's absent/empty network-container
status is observed within the replacement deadline; malformed retained objects
or incorrect owners still fail immediately. A failed
`restoring-owned-replacement-capacity` checkpoint with both exact deletion and
restoration already accepted can be adopted read-only after the new worker and
network container are fully initialized. That continuation validates the old
identities are gone, derives the new pinned manifest, and repeats **neither**
deletion nor scaling. Plan-only adoption never adds a quarantine or changes Pods.

The maintenance-only job obtains private, job-local credentials, runs the
read-only plan, then invokes the same helper with `--execute` and fresh proof.
Credentials are removed on exit and are never included in the
`n100-cni-worker-maintenance-<build>-<attempt>` diagnostic artifact. No CL2,
Fleet rejoin, or separate failed-pool reconciliation runs in this mode.
Completion still requires the helper's exact final count, workload, peer, and
cleanup gates; it is not scenario success or permission to skip normal resume
preflight.

## Running via the telescope pipeline

Add a stage to `pipelines/perf-eval/Network Benchmark/clustermesh-scale.yml` that
points at the mock topology + tfvars and sets the mock variables. The
`mock-cilium-agent` image must be pullable by the clusters (push to a
pipeline-accessible ACR; see `mock/README.md`).

```yaml
  - stage: azure_mock_n2
    dependsOn: []
    variables:
      MOCK_ACR_HOST: <registry>.azurecr.io   # hosts mock-cilium-agent:<tag>
      MOCK_AGENT_TAG: v26
      MOCK_NODE_COUNT: 100
      MOCK_CONSUME_CLUSTERMESH: true
    jobs:
      - template: /jobs/competitive-test.yml
        parameters:
          cloud: azure
          regions: [eastus2euap]
          engine: clusterloader2
          engine_input:
            image: "ghcr.io/azure/clusterloader2:v20250513"
            install: false
            operation_timeout: 15m
          topology: clustermesh-scale-mock
          terraform_input_file_mapping:
            - eastus2euap: "scenarios/perf-eval/clustermesh-scale/terraform-inputs/azure-2-mock.tfvars"
          matrix:
            n2_mock:
              cluster_count: 2
              mesh_size: 2
              cl2_config_file: config.yaml      # or pod-churn-combined.yaml for a real window
              test_type: mock-default
              namespaces: 1
              deployments_per_namespace: 2
              replicas_per_deployment: 5
              mock_mode: true                   # → CL2_MOCK_MODE
              hold_duration: 30s
              warmup_duration: 10s
              restart_count: 0
              api_server_calls_per_second: 5
              trigger_reason: ${{ variables['Build.Reason'] }}
```

For real measurements use a scenario with a steady-state window (e.g.
`pod-churn-combined.yaml`) — see the measurement-window note below. The n=20 tier
is the same stage with `azure-20-mock.tfvars` and `cluster_count: 20`.

## How to run CL2 in mock mode (validated recipe, local docker)

Set `CL2_MOCK_MODE: true` in the CL2 overrides (scale.py writes the overrides
file; add it there for the mock variant). The storage-class flags are applied
automatically for `provider=aks`. Locally-validated docker invocation:

```bash
docker run --rm --network host \
  -v <admin-kubeconfig>:/root/.kube/config \
  -v <config-dir>:/root/perf-tests/clusterloader2/config \
  -v <results-dir>:/root/perf-tests/clusterloader2/results \
  ghcr.io/azure/clusterloader2:v20250513 \
  --provider=aks --enable-prometheus-server=true \
  --prometheus-scrape-kubelets=false \
  --prometheus-additional-monitors-path=/root/perf-tests/clusterloader2/config/prometheus-additional-monitors \
  --prometheus-pvc-storage-class=managed-csi \
  --prometheus-storage-class-provisioner=disk.csi.azure.com \
  --kubeconfig /root/.kube/config \
  --testconfig /root/perf-tests/clusterloader2/config/config.yaml \
  --testoverrides=/root/perf-tests/clusterloader2/config/overrides.yaml \
  --report-dir /root/perf-tests/clusterloader2/results
```

(Use an **admin** (cert-based) kubeconfig so the CL2 container can auth without an
exec plugin.)

## Validation results (mockmesh3-1, 100 KWOK nodes + 100 mock agents)

- A full CL2 run (`config.yaml`, `CL2_MOCK_MODE=true`) returns **Status: Success**;
  the kwok-targeted workload deploys (KWOK acks Pods Running, `WaitForControlledPodsRunning`
  passes) and Prometheus scrapes all 100 mock-agent targets.
- With an adequate steady-state window, the `cilium.yaml` measurement reads the
  **mock** agents (Cilium Avg CPU Perc50 ≈ 0.008 cpu ≈ 8m, matching `kubectl top`),
  and `clustermesh-metrics.yaml` reads Identity Count / Remote Clusters Connected.
- The real-node kubelet/cAdvisor monitor is wired for the next mock canary. Each
  run now writes `telemetry/telemetry-audit-self-hosted.{json,md}` before
  snapshot teardown so missing `kubelet_*` / `container_*` families or down
  real-node targets are explicit rather than discovered later from an empty
  offline query.
- KWOK synthetic usage was validated locally through Prometheus and
  metrics-server: configured 25m/64Mi and 40m/96Mi workloads appeared per pod,
  per container, and per virtual node, and `kubectl top` returned the simulated
  values.

## Known consideration: measurement window

CL2's Prometheus measurements need the target scraped for **≥ ~2 scrape intervals
(≥30s)** during the start→gather window. The trivial **Phase-1** `config.yaml`
deploys a few Pods and gathers almost immediately (~7s window < the 15s scrape
interval), so *no* Prometheus metric — mock **or** apiserver — populates reliably.
Real scenarios (`pod-churn-combined`, `event-throughput`, soak) run for minutes and
do not have this issue. For short-window runs, apply the mock PodMonitor at
prometheus-init via `--prometheus-additional-monitors-path` so the mock agents are
scraped from the start (validated working).
