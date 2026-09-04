# ClusterMesh scale-testing program history

Last updated: 2026-09-04

Status cutoff: 2026-09-04 10:30 PDT / 17:30 UTC. Build 79006 had a failed
handoff task and was still `InProgress` while finalization continued.

This document records the engineering history of the ClusterMesh scale-testing
program from the first Telescope vertical slice through the preserved n=100
workload attempt in build 79006.

It is a chronological engineering record: why the framework was created, how
the architecture evolved, which builds proved each capability, which failures
changed the design, and what remains unresolved. It complements, but does not
replace, the
[quantitative test report](clustermesh-scale-testing-report.md) or the
[metric reference](MESH-METRICS.md).

## 1. Scope and evidence conventions

The history was reconstructed from:

- the branch history after the April 28, 2026 merge base with `origin/main`;
- 423 program commits through `3746878`;
- Azure DevOps definition 23 build records and retained artifacts;
- the original and consolidated session handoffs;
- the complete checkpoint history for the lifecycle, telemetry, preservation,
  and n=100 campaigns;
- live Azure and Kubernetes checks made while builds were active.

Evidence labels used below:

- **LIVE-PROVEN** - observed in a real AKS/ADO run, artifact, snapshot, or live
  query.
- **CODE/PREVIEW** - implemented, locally tested, or ADO-preview compiled, but
  not yet exercised at the relevant scale.
- **SESSION-DERIVED** - recorded in contemporaneous handoffs/checkpoints where
  the original ADO record is no longer retained.
- **SUPERSEDED** - a former design or conclusion replaced by later evidence.

The active development surface is:

- repository checkout: `telescope-upstream`;
- branch: `skosuri/clustermesh-scale-2`;
- pipeline: `pipelines/system/new-pipeline-test.yml`;
- Azure DevOps definition: 23, `New Pipeline Test`.

## 2. Executive summary

The program evolved through five broad transitions:

1. Build a real multi-cluster Telescope scenario and prove the Fleet,
   ClusterMesh, Cilium, CL2, and aggregation path.
2. Expand from one event-throughput scenario to a complete scale and failure
   suite, while hardening Azure provisioning and Fleet formation.
3. Replace expensive real workload nodes with KWOK Nodes and lightweight mock
   Cilium agents while retaining the real AKS/Fleet/ClusterMesh control plane.
4. Add complete native and managed telemetry, scenario evidence, cleanup
   barriers, time budgets, and durable mock-agent ownership.
5. Treat a 100-cluster Fleet as a long-lived customer environment: preserve it,
   repair it in place, prove exact cross-run identity survival, and gate the
   full workload suite on a verified handoff.

The primary live-proven result is:

- A real AKS/Fleet/ClusterMesh control plane can sustain a
  **100-cluster x 100-KWOK-node = 10,000-node mesh**.
- At n=100, every agent can observe 99 remotes and roughly 10.3k mesh nodes.
- In the clean headline run, ClusterMesh steady-state load remained modest:
  kvstoremesh p95 was approximately 20 ms, API-server inflight was generally
  2-6, and APF rejects were zero.
- The dominant reliability wall has been **Fleet formation and long-lived
  lifecycle correctness**, not steady-state mesh capacity.
- A single managed AKS API server was operationally clean around 5,000
  simulated nodes under churn; 7,000-10,000 entered an unstable region.
  Sharding protects each Kubernetes API server but re-globalizes Cilium state
  through ClusterMesh.

The primary remaining gap is:

- The complete eight-scenario n=100 suite has still not started on the final
  preserved environment.
- Builds 78903 and 78916 failed safely before scenario one. In build 79006, the
  handoff task failed before scenario one while the overall build remained
  `InProgress` while finalizing evidence.
- Build 79006 proved the final telemetry recovery, but its handoff rejected a
  real-node-drain recreation of one controller-owned mock-agent Pod with a new
  UID.

## 3. Program timeline at a glance

| Period | Main objective | Outcome |
|---|---|---|
| Apr 28-May 4 | First real ClusterMesh vertical slice and measurements | Multi-cluster CL2 fan-out, Fleet/Cilium validation, event-throughput scenario |
| May 5-May 21 | Scale tiers, scenario suite, shared VNet, failure handling | n=2/5/10/20, churn/failure/isolation/node/upper-bound scenarios, first n=100 real-scale data |
| Jun 2-Jun 24 | Direct behavior probes, policy, soak, snapshots | Direct propagation, CNP rollout, detach/rejoin, failover, restart survival, Blob TSDB snapshots |
| Jun 11-Jul 10 | Low-cost mock architecture and 10k characterization | Kubemark replaced by KWOK, mock Cilium publish/consume, clean n=100 10k result |
| Jul 14-Jul 20 | Full telemetry program | Native Prometheus, AMW managed Prometheus, ACNS, platform metrics, scenario windows |
| Jul 20-Jul 27 | Complete lifecycle and regional hardening | Full eight-scenario n=2 lifecycle, cleanup/recovery/time budgets, Canada quota wall |
| Jul 31-Aug 11 | Alternate subscription, staged Fleet, n=99 boundary | Cilium policy drift discovered, staged enrollment, n=99 full convergence |
| Aug 4-Aug 20 | Preserved Fleet lifecycle and durable ownership | Existing-Fleet resume, one-AMW-per-cluster, StatefulSet agents, definitive n=2 suite |
| Aug 22-Sep 1 | Preservation proof and cleanup hardening | Cross-process UID proof at n=2, guarded preserve/resume/cleanup |
| Sep 2-Sep 4 | Exact n=100 preservation and workload handoff | Fresh preserved n=100, exact 20k UID proof, and three progressively later pre-workload stops |

## 4. Detailed chronological history

### 4.1 April 28: first Telescope vertical slice

Commit `2000574` added the first Cilium ClusterMesh scale-test scenario.

The first architecture established the pattern still used today:

1. Provision multiple AKS clusters and a Fleet ClusterMesh.
2. Discover all clusters into a role-indexed inventory.
3. Validate ClusterMesh on every cluster.
4. Fan one CL2 worker out per cluster.
5. Aggregate results with explicit cluster attribution.

The first workload was intentionally small. It proved wiring, not scale.

Immediate corrections established the first durable invariant:

- `44d106d` used `cilium-dbg status` inside Cilium Pods and added runner-side
  Cilium CLI diagnostics.
- `54e581b` added periodic Pod, Cilium, and Fleet dumps during convergence.
- `76c1ae5` exposed Fleet connection state rather than trusting provisioning
  state alone.
- `08c6d98` marked the smoke namespace
  `clustermesh.cilium.io/global=true`, because AKS-managed Cilium gates
  synchronization at namespace scope.

**Lesson:** Fleet and ARM success do not prove a working mesh. Kubernetes
resources, every Cilium view, and real cross-cluster traffic must agree.

### 4.2 April 29-May 10: measurement foundation and scale tiers

The next phase built reusable measurements and the first real scenario.

Key commits:

- `ea51dea` - Cilium, control-plane, and ClusterMesh measurements.
- `879a6e9` - plumb `mesh_size` end to end.
- `84d98e2` - add cross-cluster event-throughput.
- `aa43ffb` - test configure/collect and multi-cluster aggregation.
- `be79cce` - correct kvstoremesh PromQL.

The event-throughput scenario created Pods and same-named global Services at a
controlled API rate, performed rollout activity, waited for drain, and gathered
Cilium, kvstoremesh, control-plane, etcd, and filesystem signals.

The scale matrix expanded:

- n=5 in `506d195`;
- n=10 in `3a9af93`;
- n=20 in `55c8a40`;
- bounded CL2 fan-out in `b5fe281`;
- lower AKS provisioning parallelism after resource-provider throttling.

By May 9, n=20 used:

- Terraform parallelism tuned for AKS;
- CL2 parallelism eight;
- a 480-minute job budget;
- a 30-minute mesh convergence budget.

### 4.3 May 11-May 21: full scenario family and formation hardening

The framework expanded from event throughput into a reusable suite.

#### Pod churn

Commits `d80105a`, `a021e02`, and related wiring added:

- workload scale-up and scale-down cycles;
- random Pod deletion;
- combined scale and kill behavior against one workload;
- post-scale and post-kill convergence evidence.

The current headline suite uses `pod-churn-combined`.

#### Failure and isolation

The following scenarios were added:

- ClusterMesh API-server failure and recovery;
- target-only isolation while all peers observe;
- HA replica configuration;
- host-side AKS nodepool scale and VMSS replacement.

Node churn had to run on the host because the CL2 container did not have Azure
CLI. CL2 owns the observation window; `node-churner.sh` performs Azure
operations and restores the original pool size.

Build [67185](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=67185)
proved isolated n=2 node churn with 17 operations across six operation types.
Build 67155, referenced by the later commit history, proved node replacement
end to end. Node names, not private IPs, became the authoritative replacement
signal because Azure can reuse IPs.

#### Upper bound

Commit `a8df66a` added a versioned saturation classifier using:

- latency;
- kvstore queue size;
- API-server CPU;
- mesh failure rate;
- etcd tail latency.

Raw values and thresholds are retained so a result can be reclassified without
rerunning.

The upper-bound workload changed several times:

1. Large restart bursts caused Prometheus OOM.
2. Host-driven label flips were capped by `kubectl` throughput.
3. The current implementation uses bounded CL2-native restart rungs, with an
   optional label-churn path.

#### Formation and Azure recovery

Fleet formation quickly became the dominant source of failures:

- large-mesh API-server formation increased from 30 to 90, then 120 minutes;
- profile reapply was added because some members were silently dropped;
- diagnostics captured Fleet members, secrets, ConfigMaps, peer names,
  Deployments, Services, and Cilium status;
- Fleet-assigned `cluster-id=0` became a fail-fast error;
- ClusterMesh API-server Deployment and LoadBalancer readiness became
  prerequisites for Cilium validation.

Partial Terraform state was preserved after apply failure. This avoided
scorched-earth cleanup racing Azure's asynchronous deletion tail and producing
`AlreadyExists` or `AnotherOperationInProgress` loops.

JUnit also changed meaning. A workload SLI miss or transient API error could be
valuable scale evidence, so the pipeline preserved telemetry instead of
discarding the run. Cleanup, infrastructure, and target-stimulus failures
remained hard gates.

#### Shared VNet and `%global`

Shared-VNet support avoided O(n^2) explicit peering at large n, but exposed
Azure's serialization of subnet PUTs.

Builds
[67954](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=67954)
and
[67959](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=67959)
validated the `%global` matrix. Later n=100 runs used 0%, 20%, 60%, and 100%
global namespace density.

Notable early large-scale builds:

- [67579](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=67579):
  n=20 upper-bound, all five rungs, 35,623 result rows.
- [67839](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=67839):
  n=100 shared-VNet pod churn; all 100 clusters completed. Four clusters crossed
  the three-minute Pod-startup p99 threshold.
- [68171](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=68171):
  n=100 `%global` matrix, including event throughput, pod churn, and isolation.
- [68700](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=68700):
  final rerun produced 100/100 coverage after shared-subnet outbound and
  extension failures in earlier attempts.

### 4.4 June 2-June 24: behavior probes, policies, soak, and snapshots

The framework moved beyond steady-state counts into direct behavior.

Key commits:

- `4c1c54f` - direct propagation probe and failure catalog.
- `aa90d3e` - detach/rejoin probe.
- `35ced14` - policy-scale and expanded Cilium/Hubble metrics.
- `32367f8` - cross-cluster CNP propagation.
- `def3fd8` - identity GC, failover, and restart-survival probes.
- `fa197c0` - OAuth-only Prometheus snapshot upload.
- `a3f1116` - snapshot support for the six-hour soak.

Important corrections:

- Pause Pods could not prove application traffic; direct propagation required a
  real HTTP server and client.
- Global backend discovery required the same Service name in participating
  clusters.
- Point-in-time CL2 gathers could not show six-hour memory, BPF, or etcd drift.
  Full TSDB snapshots preserved the 15-second history.
- Missing Hubble targets and metric-name drift were treated as telemetry
  defects, not workload success.

The failure catalog in
[`clustermesh-scale-failure-modes.md`](clustermesh-scale-failure-modes.md)
records the observed Azure, Fleet, Cilium, mock-layer, snapshot, cleanup, and
pipeline failure signatures. Some old coverage statements in that document
predate later policy, soak, telemetry, and recovery work.

### 4.5 June 11-July 10: low-cost mock architecture and 10k testing

Running 10,000 real AKS worker nodes was economically impractical. A parallel
framework effort preserved the real system under test while virtualizing the
workload layer.

#### Kubemark prototype

The first mock used kubemark hollow kubelets and a forked Cilium agent in
`DryMode`. It retained:

- Kubernetes watches;
- identity allocation;
- CiliumNode and CiliumEndpoint publication;
- kvstore synchronization;
- ClusterMesh consumption;
- metrics.

It bypassed BPF, cgroup, root, map, and load-balancer operations.

**SESSION-DERIVED / LIVE-PROVEN:** the June 12-16 prototype used manual Helm,
certificate/secret exchange, and explicitly supplied peer endpoints rather
than Azure Fleet. It proved that AKS accepted the hollow-node layer, that the
forked agent could coexist with real Cilium, and that both node-level and
automated Pod-level state propagated through the real ClusterMesh data model.
It did **not** prove Fleet integration. The later Telescope/KWOK stages replaced
that manual management plane with real Fleet-managed ClusterMesh.

Kubemark was rejected because its fake CRI returned the same Pod IP for every
Pod.

#### KWOK pivot

KWOK v0.7.0 replaced kubemark because it could assign Pod IPs from virtual
node PodCIDRs and keep Pod, EndpointSlice, and CiliumEndpoint addresses
consistent.

Durable design decisions:

- unique PodCIDRs and InternalIPs for every virtual node;
- synthetic address space outside the real Azure `10.0.0.0/8` network;
- `hostNetwork=false` for mock agents to avoid metric-port collisions;
- real Fleet-managed ClusterMesh rather than manual certificates;
- mock agents both publish local state and consume remote state;
- the mock fork remains platform-agnostic, while provisioning is AKS-specific.

The Telescope integration began with:

- `0ec395d` - KWOK plus mock-Cilium topology;
- `c665bd6` - n=2 mock smoke in definition 23;
- `afd9d22` - run pod churn on KWOK Nodes;
- `8367099` - n=20 mock;
- `2a4ba53` - n=100 shared-VNet mock;
- `ab17321` - single-cluster 10k baseline;
- `b4c3397` - n=2 x 2500 and n=5 x 1000 comparisons;
- `ea8f47a` - add `providerID` so AKS cloud-node lifecycle does not delete KWOK
  Nodes;
- `5bb8daa` - dedicated APF priority for the KWOK controller.

Build
[71645](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=71645)
was the first clean n=2 non-hollow proof:

- churn Pods scheduled on KWOK Nodes;
- mock agents were scraped;
- measurements populated;
- the workload did not accidentally run only on real nodes.

Build
[71650](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=71650)
formed an n=20 Fleet and completed on 19/20 clusters; one cluster hit a transient
API-server error during Prometheus setup.

#### First n=100 and threshold findings

Early n=100 attempts exposed:

- real-node readiness failures;
- Fleet projection plateaus around 30-35 ClusterMesh API servers;
- cloud-node-lifecycle deletion of KWOK Nodes without provider IDs;
- Prometheus cardinality and memory limits;
- mock-agent placement and KWOK-controller APF starvation.

Build
[72210](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=72210)
was the first full 100-cluster x 100-node proof. It used 20% global density and
therefore proved the system architecture, not the final headline workload.

A single-cluster 10,000-node control then separated per-API-server limits from
mesh fan-out:

- 10,000 virtual Nodes could be created and held.
- Around 5,000 agents, realistic churn was clean.
- Around 7,000-10,000, watch and heartbeat traffic began shedding or flapping.
- The conclusion was approximately 5,000 clean and 7,000 at the operational
  edge, not a hard failure at node 5,001.

The equal-total-node comparison used:

- build [72937](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=72937):
  clean n=1 x 5,000;
- build [72973](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=72973):
  n=2 x 2,500, noisy because mesh-2 stalled;
- build [73002](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=73002):
  clean n=5 x 1,000.

The tiers initially inherited 20% global density. Commit `38785f4` corrected
n=100 to 100% global density.

Build
[73076](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=73076)
became the clean headline 10k dataset:

- 100 clusters x 100 KWOK Nodes;
- 100% global density;
- approximately 11 hours end to end;
- Fleet 100/100 in approximately 41 minutes;
- 99 remotes per agent;
- approximately 10.3k mesh Nodes per agent;
- ten merged global Services;
- kvstoremesh p95 approximately 20 ms;
- API-server inflight generally 2-6;
- zero APF rejects.

Two soft transient cluster errors made the pipeline partially successful, but
the workload and steady-state data were accepted as the clean dataset.

### 4.6 July 14-July 20: telemetry completeness

The program added three complementary telemetry planes.

#### Native CL2 Prometheus

Native snapshots eventually covered:

- Kubernetes API server and APF;
- kube-state-metrics;
- real-node kubelet and cAdvisor;
- real Cilium;
- mock agents and KWOK controller;
- ClusterMesh API server, kvstoremesh, and embedded etcd;
- API-server backend CPU/RSS;
- synthetic KWOK CPU/memory;
- Hubble and ACNS signals;
- scenario and cluster identity.

Build
[73451](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=73451)
proved 3/3 real kubelet and 3/3 real cAdvisor targets per cluster.

#### Managed Prometheus

Managed Prometheus added hidden AKS control-plane metrics:

- API server;
- etcd;
- scheduler;
- controller manager;
- collector health;
- API-backend fingerprinting.

**SESSION-DERIVED / LIVE-PROVEN:** a disposable AKS/AMW experiment observed
approximately 1,678 metric names, 235k series, and 5.59M samples. It validated
the query/export mechanics before the managed telemetry path was integrated
into the reusable pipeline.

AMW does not expose a native TSDB snapshot, federation, or `remote_read`.
A PromQL-to-OpenMetrics-to-promtool reconstruction path was built, but it is
query-equivalent rather than lossless. It cannot preserve stale markers,
exemplars, native histograms, HELP/TYPE metadata, or arbitrarily fast samples.
Large n=100 reconstruction was therefore moved out of the critical pipeline.

#### ACNS, Azure metrics, and logs

The final telemetry design also collected:

- Hubble DNS and flow evidence from real nodes;
- ContainerNetworkLog archives;
- Azure platform CPU/memory where available;
- AKS diagnostic settings into a persistent Log Analytics workspace;
- exact per-scenario start/end/result windows.

Key implementation commits included:

- `1461c77` - full telemetry collection;
- `f82a411` - split configure/wait/audit/reconstruct/upload;
- `b60832a` - scale collection and add ACNS capture;
- `d47b54d` - exact scenario windows;
- `7b103d6` - correct DNS selectors and ADO logging-command quoting.

#### Pipeline and subscription corrections

Build 73551 used the wrong subscription. Build 73558 proved that AzureCLI tasks
could revert to the service connection's default subscription.

The resulting permanent rule is:

`ADO UI selection -> $(AZURE_SUBSCRIPTION_ID) -> explicit az account set and verification in every Azure task`

Other live telemetry failures included:

- Go-template parsing of embedded Python `{{...}}`;
- transient provider-registration resets;
- successful-looking zero-sample AMW reconstruction;
- persistent AMW active-series residue;
- bad Hubble DNS selectors;
- xtrace corrupting an ADO logging-command value.

Build
[74100](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=74100)
proved most of the telemetry plane but still had the DNS and metadata defects.
Build
[74112](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=74112)
correctly blocked before CL2 because the persistent AMWs were above the 40%
active-series headroom gate.

### 4.7 July 20-July 27: full eight-scenario lifecycle

The intended customer-like n=100 lifecycle was fixed as:

1. `propagation-probe`
2. `event-throughput`
3. `policy-scale`
4. `pod-churn-combined`
5. `apiserver-failure`
6. `isolation`
7. `node-churn-combined`
8. `upper-bound`

Customer-like means reusing and repairing one long-lived Fleet. It does not
mean weakening stressors or deliberately carrying benchmark contamination.

HA replica testing was removed from the headline suite because ENO restored the
replica count within approximately 23-38 seconds, invalidating the intended A/B
measurement.

Build
[74128](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=74128)
exposed a destructive node-churn design: deleting both real default-pool nodes
left desired count at two but actual capacity at zero.

The resulting lifecycle hardening added:

- a dedicated real `churnpool`;
- refusal to remove every pool node;
- desired/actual VMSS reconciliation;
- a `desired+1` nudge and restoration;
- Ready, schedulable, and Cilium health checks;
- scenario-specific evidence contracts;
- cleanup barriers;
- disk, time, and finalization reserves;
- per-scenario artifact preservation.

Successive n=2 builds fixed:

- ACNS probe placement;
- propagation readiness;
- mock reconciliation performance;
- AMW rotation;
- credential retries;
- asynchronous Fleet state;
- orphaned VNets;
- operations completing after helper timeouts;
- lease expiry;
- snapshot and target-reload races;
- ADO `ARG_MAX`;
- metric renames.

Build
[74774](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=74774)
was the first fully valid eight-scenario n=2 lifecycle in this hardening
sequence.

East US 2 EUAP failures were temporarily contaminated by a regional AKS
incident and disaster-drill activity. The headline gate moved to Canada
Central. Canada validated the harness but exposed a separate live quota:

Build
[74819](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=74819)
created 93 n=100 clusters and then reached the region's 99-ManagedClusters
limit. Cleanup removed all 201 Terraform resources.

### 4.8 July 31-August 11: Cilium policy drift, staged Fleet, and n=99

The next candidate subscription had enough ManagedClusters and DSv3 quota.
A local n=2 smoke proved AKS 1.35, Fleet, internal LBs, and 1/1 Cilium peers.

Build
[75328](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=75328)
ran all eight workloads but showed that managed-monitoring reconciliation could
roll Cilium during the workload and leave `enable-policy=never`.

Build
[75365](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=75365)
validated the extension/Cilium convergence barrier and all eight workloads, but
found retired-target and Azure platform-metric audit defects.

Attempts to reassert policy through supported AKS update did not reliably
restore it. The program therefore:

- kept `policy-scale` scientifically strict;
- allowed an explicit and recorded DNS/L7 telemetry gap for non-policy
  scenarios;
- kept native and managed Prometheus as hard gates;
- treated Azure platform metrics as supplementary.

#### Staged Fleet enrollment

Build
[75513](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=75513)
created all 100 AKS clusters, but Fleet projected 0/100 ClusterMesh API servers.
Post-hoc batched repair could not recover a completely failed initial
projection.

The architecture changed:

- members begin as `mesh=staged`;
- the profile selector remains `mesh=true`;
- members are enrolled in bounded batches;
- each batch must reach Deployment, LB, Fleet, and Cilium convergence before
  the next batch.

Repeated runs tuned this system:

- 75554 reached 90/100, but repeated profile applies destabilized healthy
  members;
- 75606 reached 38/40;
- 75709 reached 43/60;
- 75734 reached 69/70;
- 75838 achieved n=75 full peer convergence;
- 75870 achieved n=90 Fleet convergence, with one AKS CNI node failure.

**SESSION-DERIVED:** Fleet engineering reported a backend store-pagination
defect around more than 99 members. The clean n=99 results and repeated n=100
projection failures were consistent with that reported boundary.

Build
[75916](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=75916)
achieved 99/99 API servers/LBs and 98/98 peers on every cluster after recovery.

Build
[76401](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=76401)
became the conclusive n=99 proof:

- surgical rejoin moved the Fleet to 99/99;
- every cluster passed 98/98 peers;
- cleanup succeeded.

### 4.9 August 4-August 18: preserved n=100 lifecycle and AMW redesign

Repeatedly paying the 100-cluster Fleet-formation risk was wasteful. The
program introduced guarded lifecycle modes:

- fresh provision and preserve;
- resume an existing Fleet without Terraform;
- surgical repair of only unhealthy members;
- explicit confirmed cleanup;
- overlay reset only as an emergency path.

Existing-Fleet resume became the preferred model because reset/recreate exposed
stale certificates, root CAs, asynchronous operations, and asymmetric peer
state.

Build 76071 and 76082 proved the model at n=2 with stable AKS IDs and clean
workload create/scale/delete behavior.

Build
[76575](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=76575)
created the first preserved n=100 environment:

- 100 AKS clusters;
- 201 pools;
- Fleet 89/100;
- resource group `76575-f36f3d5a` retained.

Build
[76629](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=76629)
surgically repaired the remaining 11 members:

- Fleet 100/100 Connected;
- every cluster 99/99 peers;
- cross-cluster data path passed.

#### One AMW per cluster

The first n=100 telemetry design used 50 AMWs with two clusters each. Live data
disproved it:

- all 50 workspaces dropped events;
- median event rate was approximately 889k/min;
- maximum exceeded 1.3M/min.

The final design used 100 deterministic one-cluster shards.

Build 76685 then proved that `az aks update --enable-azure-monitor-metrics`
did not migrate existing DCR destinations. The implementation moved each
existing `MSProm-*` DCR explicitly and required:

- the expected alias in the expected workspace;
- complete ActiveTimeSeries and EventsPerMinute samples;
- no `LimitThrottling` drops.

Build
[76954](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=76954)
proved:

- 100 DCR destinations;
- 100 distinct workspaces;
- 100/100 alias routing;
- approximately 24-31% active-series use;
- approximately 53-79% event-rate use;
- zero throttling drops.

#### Resume and finalization defects

Builds 76751, 76897, and 76924 exposed:

- unbounded `kubectl exec`;
- unbounded runner-side Cilium diagnostics;
- `kubectl wait nodes --all` opening many watches and timing out despite Ready
  Nodes;
- `always()` stages continuing after cancellation.

These became bounded commands, failure-only diagnostics, a single JSON Node
snapshot, and cancellation-safe conditions.

Build
[77107](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=77107)
was the first deep preserved n=100 lifecycle attempt:

- preserved validation passed;
- Fleet was 100/100;
- AMW mapping passed;
- mock deployment passed;
- propagation ran on all 100 clusters;
- 99/100 snapshots, about 11.3GB, were uploaded.

It also exposed the central lifecycle defect:

- 207 bare mock-agent Pods disappeared or became unhealthy during long
  preservation and diagnostics;
- one-shot recovery regressed;
- the health gate stopped scenarios 2-8;
- finalization was too serial and one upload was not overwrite-safe.

Preservation amplified exposure time; it did not create the bare-Pod ownership
defect.

### 4.10 August 18-August 20: durable ownership and definitive n=2

The earlier decision that bare Pods were sufficient was reversed.

Commits `be03f40`, `94bd8ac`, and `539e9f1` introduced:

- a Parallel StatefulSet with one stable ordinal per KWOK Node;
- a selectorless headless Service;
- desired-state schema v3;
- fail-closed migration from naked Pods;
- logical telemetry identity based on ordinal;
- worker-local and parent reconciliation;
- port-forward recreation;
- concurrent finalization;
- idempotent artifact publication.

The selectorless Service avoided creating 100 EndpointSlice backends per
cluster and contaminating the mesh.

Build
[77179](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=77179)
ran all eight n=2 scenarios and proved StatefulSet self-healing, but found:

- the installed kubectl did not support `api-resources -o json`;
- AKS had Cilium dataplane without explicit `network-policy=cilium`, leaving
  policy disabled.

Build
[77205](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=77205)
showed fresh East US 2 EUAP Fleet formation still failing:

- both members moved `Connecting -> Failed`;
- no ClusterMesh API-server resources appeared;
- recovery returned `ResourceNotFinalState`.

Build
[77227](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=77227)
was the definitive standalone n=2 result:

- all eight scenarios passed;
- `overall_rc=0`;
- explicit Cilium network policy;
- DNS/ACNS telemetry;
- API-server replacement UID observed in seven seconds;
- pod churn passed;
- eleven node-churn operations;
- cleanup passed after every scenario;
- managed telemetry had no AMW limit drops;
- all 17 Terraform resources were destroyed.

### 4.11 August 19-August 20: preserved n=100 preflight repair

Build
[77293](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=77293)
failed before workloads even though Fleet reported 100/100 Connected:

- four observers saw only 98/99 peers;
- all missed Cilium identity `mesh-9191`, corresponding to `mesh-91`;
- one cluster exposed stale NotReady KWOK Nodes and no real workers;
- sequential waits consumed nearly three hours;
- recovery was ordered after the failing gate.

This proved:

- Fleet `Connected` is not authoritative;
- every Cilium agent must be inspected;
- Cilium identity names cannot be parsed heuristically;
- real-worker recovery must precede generic validation.

Commit `e37f8d3` added:

- structured `cilium-dbg status -o json`;
- canonical Cilium-name-to-Fleet-role mapping;
- bounded AKS nodepool/VMSS recovery;
- forced surgical repair even for a Fleet-Connected member;
- `type!=kwok` real-node filtering;
- desired-count rollback;
- Fleet selector-label cleanup traps;
- one repair pass followed by authoritative reprobe.

Build
[77314](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=77314)
then failed closed because the managed node resource group for `mesh-1` no
longer existed. The parent lease had been extended, but the AKS-managed child RG
leases had not.

Commit `e123017` protected and validated managed node-RG leases. The old
preserved environment was already irrecoverable.

### 4.12 August 22-September 1: preservation proof at n=2

Preservation received a dedicated proof sequence.

Key commits:

- `2ef7d98` - add KWOK preservation proof;
- `4a80562` - compact the proof stage;
- `406dad5` - run the proof job;
- `523e3a6` - fix fault injection;
- `61c0e04` - fix expected UID accounting;
- `68ae3c2` - generalize the lifecycle;
- `c53d08d` - bound no-workload preserve.

Build sequence:

- 77642 proved 200/200 Node and agent UIDs survived a new process, but the
  fault wait was wrong.
- 77669 passed preservation and recovery, but the final expected-UID assertion
  was wrong.
- [77671](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=77671)
  was the final green n=2 preservation/recovery proof.
- 77858 preserved two clusters but Fleet formation failed.
- 77913 refused unsafe repair after both AKS resources became ARM `Failed`.
- 77915 performed guarded cleanup.

Local Azure authorization also became unreliable for cleanup. Service-connection
cleanup runs removed residual resources.

### 4.13 September 2-September 4: exact n=100 preservation

#### Fresh preserved infrastructure

Build
[78751](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=78751)
created the final preserved environment:

- resource group `78751-f36f3d5a`;
- 100 AKS clusters;
- 201 pools;
- 100/100 Fleet members;
- ClusterMesh API-server/LB validation;
- all-agent Cilium validation;
- cross-cluster data-path smoke;
- workloads disabled.

Six clusters retained stale ARM `Failed` scars despite healthy live state.

Build
[78801](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=78801)
performed structured surgical ARM repair:

- 100/100 AKS `Succeeded`;
- 201/201 pools `Succeeded`;
- Fleet 100/100;
- Cilium and data path passed.

#### Exact baseline capture

Commit `0bbece8` added baseline capture.

Build
[78812](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=78812)
captured:

- exactly 100 roles;
- exact AKS resource IDs;
- 10,000 KWOK Node name-to-UID mappings;
- 10,000 mock-agent Pod name-to-UID mappings;
- 800 desired-state files and their digests;
- exact Cilium identities and peer health.

Artifact:

`n100-kwok-preservation-78812`

The later workload run downloaded 802 artifact files totaling about 63.2MB.
No CL2 scenario ran during capture.

#### Cross-run verification

Commit `d99d092` added a separate verification run.

Build
[78851](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=78851)
proved all 20,000 object UIDs survived the pipeline boundary before mutation.

It then injected bounded loss on five spread-out clusters:

- 25 KWOK Nodes;
- 50 mock-agent Pods.

The reconciler restored all 100 clusters. Exactly the deliberately removed
25 Node UIDs and 50 agent UIDs changed; **19,925 identities remained
unchanged**. Post-recovery traffic passed.

Artifact:

`n100-kwok-verification-78851`

This is the definitive n=100 cross-run preservation proof.

### 4.14 September 3-September 4: three workload attempts

#### Build 78903: repair-selection failure

Build
[78903](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=78903)
failed safely before telemetry, handoff, or workloads.

One observer, `mesh-16`, had different partial peer failures across its three
Cilium agents. The repair selector expanded that local observer problem into 79
remote Fleet-member repairs and exceeded the 20-member safety cap.

The fixes:

- exact cap-bounded vertex-cover repair selection;
- local-star drift selects the observer;
- remote-star drift selects the shared remote;
- no mutation if no cover of 20 or fewer roles exists;
- every Cilium agent, not one arbitrary DaemonSet Pod;
- exact expected remote names;
- authoritative smoke namespace deletion.

Commits:

- `d01839f` - fix repair selection;
- `a6bbb03` - harden workload rerun;
- `943a3d6` - harden live validation.

#### Build 78916: managed telemetry failure

Build
[78916](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=78916)
passed:

- proof downloads;
- exact AKS/Fleet prevalidation;
- real-worker checks;
- all-agent Cilium validation;
- initial data-path smoke.

Managed telemetry reached 99/100:

- `mesh-86` hit an ARM 503 during DCR association access;
- the script continued with invalid empty values;
- the convergence gate rejected `enable-policy=never` even though the
  non-policy suite explicitly accepted that known telemetry gap.

No handoff, mock redeployment, or scenario ran.

Commit `3746878` hardened:

- conditional and evidenced policy-gap acceptance;
- bounded ARM, AKS, DCR, diagnostic, extension, and kubectl calls;
- timeout exit statuses 124 and 137;
- ambiguous AKS update polling;
- exact DCR destination verification;
- extension revalidation before evidence;
- exact Fleet and AKS IDs in resume manifests;
- incomplete and malformed inventory handling;
- definitive named-Fleet absence checks for reset mode.

Before launch it passed:

- 132 directly affected tests;
- 270 surrounding tests;
- Pylint 10/10;
- YAML, Bash, Python compilation, and final review;
- exact-SHA ADO preview, run ID `-1`.

#### Build 79006: telemetry success, handoff rejection

Build
[79006](https://dev.azure.com/akstelescope/telescope/_build/results?buildId=79006)
was pinned to `37468780ada3c900a055c0dfa303e636119529db`.

At this document's status cutoff, the handoff task had failed but the overall
ADO build was still `InProgress` while telemetry and lifecycle finalization
continued.

Live gates that passed:

- baseline and verification artifacts downloaded;
- 100/100 AKS ARM states healthy;
- 100/100 real-worker checks;
- 100/100 Fleet members Connected;
- all 100 ClusterMesh API-server Deployments and LBs ready;
- live overlay healthy in one observation round;
- no Fleet repair or profile reapply;
- every Cilium agent reported exact 99/99 peers;
- initial cross-cluster curl passed on attempt one;
- both smoke namespaces were confirmed absent;
- managed telemetry completed for 100/100 clusters;
- accepted policy-gap evidence was recorded;
- `mesh-86` exercised the new ambiguous-update recovery:
  - `az aks update` timed out after 120 seconds;
  - live state was polled through `Updating -> Succeeded`;
  - the exact DCR destination was verified;
  - Cilium changed `default -> never`;
  - the extension and all three Cilium agents were stable for 120 seconds;
- the verified handoff restored the persisted desired state;
- mock reconciliation reported 100/100 healthy clusters;
- reconciliation recreated zero KWOK Nodes and zero mock-agent Pods;
- no broad mock redeployment occurred.

The strict live handoff then passed 99/100 clusters and failed on:

`mesh-99 / kwok-node-28`

Kubernetes evidence:

- the Pod was evicted at `2026-09-04T16:22:12Z` during a real-node drain;
- scheduling initially failed because one real node lacked CPU and Azure CNS
  temporarily had no IPv4 allocation available;
- it was scheduled at `16:27:42Z`;
- its container started at `16:28:14Z`;
- the handoff gate failed at `16:27:37Z`, 37 seconds before recovery;
- the StatefulSet returned to 100/100 ready;
- the replacement Pod had a new UID.

The environment recovered functionally, but the exact preservation identity
contract was broken. The handoff artifact was published and all eight workload
scenarios were suppressed.

This failure was not:

- a Fleet failure;
- a managed telemetry failure;
- a broad mock-layer drift;
- a mock redeployment.

It was a real-node drain plus scheduling/IP-capacity event that caused one
controller-owned Pod identity to change during the handoff.

## 5. Key build ledger

`Not retained` means the exact terminal ADO record was unavailable at the
document cutoff; the engineering outcome is reconstructed from contemporaneous
session, artifact, or snapshot evidence.

### 5.1 Early scenario and scale builds

| Build | Scale/mode | Reported ADO result | Campaign/scientific outcome |
|---|---|---|---|
| 67185 | n=2 node churn | Succeeded | 17 node operations across six operation types |
| 67578 | n=2 scenario gate | Succeeded | Seven original scenarios substantiated across the gate sequence |
| 67579 | n=20 upper bound | Succeeded | Five saturation rungs; etcd commit-tail was the dominant signal |
| 67747 | n=2 shared VNet | Succeeded | Shared-VNet topology proved end to end |
| 67839 | n=100 pod churn | Succeeded | 100 clusters completed; four crossed Pod-startup p99 |
| 68171 | n=100 `%global` matrix | Mixed across jobs/reruns | g0/g20/g60/g100 data completed for event, churn, and isolation |
| 68700 | n=100 g60 rerun | Mixed across attempts | Final attempt produced 100/100 coverage after shared-subnet outbound failures |

### 5.2 Mock and 10k characterization

| Build | Scale/mode | Reported ADO result | Campaign/scientific outcome |
|---|---|---|---|
| 71645 | n=2 mock | Succeeded | First non-hollow CL2 proof |
| 71650 | n=20 mock | Not retained | Fleet formed; 19/20 clusters completed and one hit a transient Prometheus/API failure |
| 72129 | n=100 mock | Failed | Fleet projected only about 30-35 API servers |
| 72210 | n=100, 10k | Succeeded | First full 100 x 100 proof, at 20% global density |
| 72539 | n=1, 10k | Not retained | KWOK 10k held; operational/Prometheus wall remained |
| 72937 | n=1 x 5000 | Not retained | Clean consolidated comparison endpoint |
| 72973 | n=2 x 2500 | Not retained | Noisy: mesh-2 stalled; not a clean comparison |
| 73002 | n=5 x 1000 | Not retained | Clean sharded comparison endpoint |
| 73076 | n=100 x 100 | PartiallySucceeded | Measurement accepted as the clean headline 100%-density 10k dataset |

### 5.3 Telemetry and lifecycle builds

| Build | Scale/mode | Reported ADO result | Campaign/scientific outcome |
|---|---|---|---|
| 73451 | n=2 telemetry | Succeeded | Real kubelet/cAdvisor targets proved |
| 73551/73558 | telemetry | Failed | Wrong subscription and AzureCLI reset discovered |
| 73635 | n=2 telemetry | Succeeded | Native plus reconstructed managed telemetry |
| 74100 | n=2 telemetry | PartiallySucceeded | Most telemetry passed; DNS selector/metadata defects |
| 74128 | n=2 lifecycle | PartiallySucceeded | Default-pool node replacement defect |
| 74774 | n=2 Canada | Succeeded | First clean eight-scenario lifecycle in the hardening series |
| 74819 | n=100 Canada | Failed | 93 clusters, then regional ManagedClusters quota |
| 75328 | n=2 candidate | PartiallySucceeded | All workloads; managed monitoring changed policy |
| 75365 | n=2 candidate | Failed after workloads | Eight scenarios passed; telemetry audit defects |
| 77227 | n=2 standalone | Succeeded | Definitive current eight-scenario n=2 proof |

### 5.4 Fleet scale and preservation builds

| Build | Scale/mode | ADO result | Engineering/scientific outcome |
|---|---|---|---|
| 75513 | fresh n=100 | Failed | 100 AKS, 0/100 ClusterMesh API servers |
| 75734 | staged n=100 | Failed | Reached 69/70; terminal member failure |
| 75838 | n=75 | Succeeded | Full peer convergence |
| 75870 | n=90 | Failed | Fleet converged; one AKS CNI node failed |
| 75916 | n=99 | Succeeded | Recovery reached 99/99 API servers/LBs and 98/98 peers |
| 76401 | n=99 | Succeeded | Conclusive surgical-rejoin n=99 proof |
| 76575 | preserved n=100 | Failed | 100 AKS, 201 pools, Fleet 89/100; RG preserved |
| 76629 | resume/repair | Failed | Repair objective passed: 100/100 Fleet, peers, and data path |
| 76954 | n=100 telemetry | Canceled | Telemetry objective passed before cancellation: one-AMW-per-cluster routing and no-drop capacity |
| 77107 | n=100 resume | Failed | Propagation ran; bare-Pod attrition stopped the suite |
| 77293 | n=100 resume | Failed | Fleet state disagreed with live Cilium/worker health |
| 77314 | n=100 resume | Failed | Managed node RGs had expired |
| 77671 | n=2 preservation | Succeeded | Cross-process UID preservation and bounded recovery |

### 5.5 Final n=100 campaign

| Build | Mode | ADO result | Workloads | Engineering/scientific outcome |
|---|---|---|---|---|
| 78751 | fresh-preserve | Succeeded | No | Final 100-cluster preserved environment |
| 78801 | ARM repair | Succeeded | No | 100 AKS, 201 pools, Fleet 100/100 |
| 78812 | baseline capture | Succeeded | No | Exact 10k Node + 10k agent baseline |
| 78851 | cross-run verify | Succeeded | No | Exact 20k UID proof and bounded fault recovery |
| 78903 | workload resume | Failed | No | Failed before handoff; unsafe 79-role repair expansion rejected |
| 78916 | workload resume | Failed | No | Failed in telemetry; 99/100 monitoring and policy-gap defect |
| 79006 | workload resume | InProgress at cutoff | No | Handoff task failed; telemetry 100/100, one drain-driven Pod UID mutation, finalization continuing |

## 6. Current architecture

### 6.1 System under test

The framework keeps these components real:

- AKS managed control planes;
- Azure Fleet and ClusterMeshProfile;
- ClusterMesh API server and kvstoremesh;
- Cilium operators and real Cilium agents on real nodes;
- Azure networking, load balancers, and managed monitoring;
- CL2 orchestration and Prometheus.

The simulation boundary is the workload-node layer:

- KWOK represents Kubernetes Nodes and Pod status;
- one lightweight mock Cilium agent represents each virtual node;
- synthetic Pods do not execute processes, cgroups, or kernel datapath;
- KWOK CPU/memory is explicitly synthetic;
- actual framework overhead is measured from real cAdvisor targets.

See:

- [`scenarios/perf-eval/clustermesh-scale/MOCK-MODE.md`](../scenarios/perf-eval/clustermesh-scale/MOCK-MODE.md)
- [`mock/provision-kwok-layer.sh`](../scenarios/perf-eval/clustermesh-scale/mock/provision-kwok-layer.sh)
- [`mock_layer_reconcile.py`](../modules/python/clusterloader2/clustermesh-scale/mock_layer_reconcile.py)

### 6.2 Fleet formation and live health

Large-n formation uses:

- staged member enrollment;
- ten-member batches;
- bounded recovery;
- surgical member rejoin;
- no broad ClusterMeshProfile delete/recreate at n=100.

Health requires:

- exact AKS inventory and state;
- ClusterMesh API-server Deployment availability;
- internal LoadBalancer endpoint;
- unique nonzero Cilium cluster identity;
- every named Cilium agent reporting exactly n-1 ready peers;
- cross-cluster data-path smoke.

Fleet `Connected` alone is not sufficient.

Primary files:

- [`staged-fleet-enrollment.sh`](../steps/topology/clustermesh-scale/staged-fleet-enrollment.sh)
- [`validate-resources.yml`](../steps/topology/clustermesh-scale/validate-resources.yml)
- [`preserved_live_overlay.py`](../modules/python/clusterloader2/clustermesh-scale/preserved_live_overlay.py)
- [`cilium_agent_health.py`](../modules/python/clusterloader2/clustermesh-scale/cilium_agent_health.py)
- [`cross-cluster-smoke.sh`](../steps/topology/clustermesh-scale/cross-cluster-smoke.sh)

### 6.3 Headline scenario suite and budgets

| Order | Scenario | n=100 outer budget |
|---:|---|---:|
| 1 | `propagation-probe` | 2h |
| 2 | `event-throughput` | 2h |
| 3 | `policy-scale` | 3h |
| 4 | `pod-churn-combined` | 7h |
| 5 | `apiserver-failure` | 1.5h |
| 6 | `isolation` | 2h |
| 7 | `node-churn-combined` | 3h |
| 8 | `upper-bound` | 2h |

The current envelope is:

- scenario suite: 44h;
- finalization reserve: 3h;
- job buffer: 6h;
- ADO job cap: 50h;
- cancellation tail: 2h;
- resource lease sized beyond the workload and cleanup envelope.

The engine proves that enough time remains for the scenario, recovery,
artifacts, and finalization before starting another scenario.

Primary files:

- [`execute.yml`](../steps/engine/clusterloader2/clustermesh-scale/execute.yml)
- [`run-cl2-on-cluster.sh`](../steps/engine/clusterloader2/clustermesh-scale/run-cl2-on-cluster.sh)
- [`scale.py`](../modules/python/clusterloader2/clustermesh-scale/scale.py)
- [`scenario_policy.py`](../modules/python/clusterloader2/clustermesh-scale/scenario_policy.py)

### 6.4 Measurement and continuation policy

The framework separates:

- **measurement validity** - whether the scenario produced trustworthy
  stimulus and evidence;
- **suite continuation** - whether cleanup, infrastructure, artifacts, and
  mock reconciliation leave the shared Fleet safe for the next scenario.

Current principles:

- Fleet formation is 100/100.
- Propagation tolerates zero worker failures.
- Ordinary scenarios have a small explicit worker-failure allowance.
- Upper-bound has a larger measurement allowance because failure can be the
  saturation signal.
- A target-scoped failure target must succeed.
- Cleanup, infrastructure recovery, required artifacts, and preservation
  identity remain hard gates.

Primary files:

- [`scenario_evidence.py`](../modules/python/clusterloader2/clustermesh-scale/scenario_evidence.py)
- [`scenario_policy.py`](../modules/python/clusterloader2/clustermesh-scale/scenario_policy.py)
- [`scenario_cleanup_reconcile.py`](../modules/python/clusterloader2/clustermesh-scale/scenario_cleanup_reconcile.py)

### 6.5 Telemetry planes

#### Native Prometheus

Native per-cluster Prometheus is the authoritative workload and Cilium signal.
It captures:

- Cilium and ClusterMesh metrics;
- kvstoremesh and mesh-etcd;
- Kubernetes control-plane metrics;
- real kubelet/cAdvisor;
- mock-agent and KWOK-controller resource use;
- synthetic KWOK usage;
- ACNS/Hubble evidence;
- scenario identity and timing.

Large snapshots go to Blob, not ADO PipelineArtifact.

Metric reference:

- [`MESH-METRICS.md`](MESH-METRICS.md)

#### Managed Prometheus

The final n=100 architecture uses one persistent AMW per cluster.

Safety includes:

- capacity preflight;
- regional workspace-count guard;
- exact run-unique aliases;
- exact DCR destination;
- extension and Cilium convergence;
- explicit policy-gap evidence;
- real sample and no-throttling checks.

Primary scripts:

- [`configure-managed-prometheus.sh`](../scenarios/perf-eval/clustermesh-scale/telemetry/configure-managed-prometheus.sh)
- [`wait-managed-prometheus.sh`](../scenarios/perf-eval/clustermesh-scale/telemetry/wait-managed-prometheus.sh)
- [`audit-managed-prometheus.sh`](../scenarios/perf-eval/clustermesh-scale/telemetry/audit-managed-prometheus.sh)

Raw AMW remains authoritative. Portable TSDB reconstruction is expensive and
lossy and is intentionally not in the n=100 critical path.

#### Azure and ACNS

The pipeline also captures:

- Azure platform metrics where the provider emits useful samples;
- diagnostic settings into a persistent LAW;
- ACNS DNS/flow evidence and ContainerNetworkLogs;
- exact scenario windows for later bounded queries.

### 6.6 Preservation and verified handoff

The preserved lifecycle has three separate contracts:

1. **Infrastructure identity:** exact subscription, region, run ID, AKS IDs,
   pool inventory, Fleet identity, tfvars digest, and lease.
2. **Desired state:** exact persisted role directories, manifests, and SHA-256
   digests.
3. **Live identity:** exact KWOK Node and mock-agent name-to-UID maps plus live
   Cilium health.

The sequence is:

1. Capture the baseline.
2. Verify it in an independent run.
3. Inject bounded loss and prove exact recovery accounting.
4. Before workloads, restore the exact desired state.
5. Reconcile all clusters.
6. Verify the proof chain, platform state, exact live identities, all Cilium
   agents, and cross-cluster traffic.
7. Only then start CL2.

Primary files:

- [`preserved_mock_capture.py`](../modules/python/clusterloader2/clustermesh-scale/preserved_mock_capture.py)
- [`preserved_mock_verify.py`](../modules/python/clusterloader2/clustermesh-scale/preserved_mock_verify.py)
- [`preserved_mock_handoff.py`](../modules/python/clusterloader2/clustermesh-scale/preserved_mock_handoff.py)
- [`verified-workload-handoff.yml`](../steps/topology/clustermesh-scale-mock/verified-workload-handoff.yml)
- [`write-resume-manifest.sh`](../steps/topology/clustermesh-scale/reuse/write-resume-manifest.sh)

## 7. Durable live findings

### 7.1 Scale and performance

- n=100 with 10,000 virtual Nodes is viable when workload Nodes are sharded
  across real managed control planes.
- Fleet formation is much less reliable than steady-state mesh operation.
- Each n=100 agent can hold 99 remote-cluster relationships and roughly 10.3k
  mesh Nodes.
- In the clean headline run, API-server and APF signals were modest.
- **LIVE-PROVEN:** end-to-end apply-to-peer-ipcache propagation was
  approximately:
  - 33-38s at 2 x 50 idle;
  - approximately 43s at 2 x 300 idle;
  - 56-69s at 2 x 300 under load.
- The single-cluster operational knee was around 5,000 simulated agents under
  realistic churn.
- Builds 72973 and 73040 both observed a noisy n=2 x 2,500 midpoint; a clean
  midpoint remains outstanding. n=1 x 5,000 and n=5 x 1,000 are the cleaner
  consolidated/sharded comparison.

### 7.2 Fleet and Azure

- A broad n=100 profile apply can project no ClusterMesh API servers.
- Staged enrollment and surgical rejoin are safer than broad reset/recreate.
- Fleet member state can say Connected while live Cilium peer state is stale.
- Shared VNet avoids peering explosion but creates subnet PUT serialization and
  shared-resource operation conflicts.
- ManagedClusters regional quota must be checked before n=100 provisioning.
- Parent and AKS-managed child resource-group leases must both be protected.

### 7.3 Mock lifecycle

- Bare mock-agent Pods are not adequate for 20+ hour preserved runs.
- StatefulSet ownership restores functional service, but necessarily changes a
  Pod UID after eviction.
- Reconciliation must distinguish:
  - missing desired objects;
  - unhealthy desired objects;
  - unknown extra objects;
  - systemic mass drift.
- Broad destructive repair is unsafe.

### 7.4 Telemetry

- Native and managed Prometheus answer different questions and both are needed.
- Two clusters sharing one 1M-events/min AMW is insufficient at n=100.
- Event-rate utilization above a nominal threshold is not data loss; actual
  `LimitThrottling` drops are the completeness signal.
- Managed monitoring can reconcile Cilium policy to `never`.
- That gap may be explicitly accepted for non-policy telemetry, but
  `policy-scale` must remain strict.
- AzureCLI subscription context cannot be assumed across tasks.

## 8. Important mistakes and corrections

These are retained because they materially changed the workflow.

### 8.1 Workflow corrections

**SESSION-DERIVED:** these operating rules came from explicit corrections made
during the program. They are process history, not conclusions inferred from a
test result.

| Mistake or false assumption | Correction |
|---|---|
| Work in the stale `telescope-main` checkout | Use `telescope-upstream` |
| Iterate in another pipeline | Use definition 23 and `new-pipeline-test.yml` |
| Assume the service connection's default subscription | Select and verify the UI-provided subscription in every Azure task |
| Push through the enterprise-managed GitHub identity | Push through `skosuri1` |
| Use long commit messages or generated trailers | Keep commits short and omit coauthor/session trailers |

### 8.2 Technical corrections

| Mistake or false assumption | Correction |
|---|---|
| Treat Fleet `Connected` as live health | Inspect every Cilium agent and run traffic |
| Treat one arbitrary `ds/cilium` Pod as representative | Enumerate every named Cilium Pod |
| Parse concatenated Cilium names heuristically | Build canonical identity maps from `cilium-config` |
| Recreate the full Fleet to repair a few members | Use bounded surgical rejoin |
| Use kubemark despite duplicate Pod IPs | Replace it with KWOK |
| Let bare mock-agent Pods survive long runs | Use a durable StatefulSet |
| Treat nominal AMW utilization as data loss | Require actual complete samples and zero throttling drops |
| Reuse a two-cluster AMW shard at n=100 | Use one AMW per cluster |
| Trust a successful `az aks update` response | Poll live state and verify the exact DCR route |
| Continue after failed cleanup or identity validation | Stop the shared suite and preserve evidence |
| Assume a green/orange pipeline means complete data | Inspect artifacts, scenario windows, and telemetry audits |
| Claim a scenario was never run without querying history | Check Kusto/build evidence; isolation did run in earlier n=100 builds |

## 9. Current unresolved decisions

### 9.1 What should preservation identity mean?

The current contract requires every unplanned baseline UID to remain unchanged.
Build 79006 proved that a real-node drain can legitimately trigger StatefulSet
recovery while changing a Pod UID.

The next design decision must explicitly choose one of:

1. Keep exact UID immutability and require a new baseline after any unplanned
   recreation.
2. Permit controller-driven mock-agent UID changes when:
   - the KWOK Node UID is unchanged;
   - the StatefulSet ordinal and logical identity are unchanged;
   - desired-state hashes are unchanged;
   - the replacement cause is captured;
   - all Cilium, telemetry, and traffic checks pass.
3. Treat any unplanned recreation as contamination and abandon the preserved
   environment for headline testing.

This contract must not be silently weakened.

### 9.2 Full n=100 suite

Still not live-proven:

- scenario-one start after the final handoff;
- full eight-scenario continuation;
- n=100 node churn on the current preserved Fleet;
- n=100 upper bound;
- all cleanup barriers under cumulative runtime;
- complete finalization after a 40+ hour scenario suite.

### 9.3 Regional and platform risks

- Fresh Fleet formation in East US 2 EUAP remains unreliable.
- Cilium policy can still drift to `never`.
- Real-node drain and CNS IP availability can affect controller-owned mock
  agents.
- The final n=100 duration may approach the 44-hour suite budget plus reserves.
- A truly resumable multi-job scenario engine remains deferred.

### 9.4 Remaining research and documentation

- clean n=2 x 2,500 comparison midpoint;
- formal real-versus-mock calibration matrix;
- longer soak and beyond-10k exploration;
- selected AKSInfra export;
- final quantitative report updates after a completed n=100 suite.

## 10. Selected milestone commits

| Commit | Date | Purpose |
|---|---|---|
| `2000574` | 2026-04-28 | First ClusterMesh scale-test vertical slice |
| `ea51dea` | 2026-04-29 | Measurement modules |
| `84d98e2` | 2026-04-29 | Event-throughput scenario |
| `55c8a40` | 2026-05-08 | n=20 tier |
| `a021e02` | 2026-05-11 | Combined pod churn |
| `a8df66a` | 2026-05-14 | Upper-bound classifier |
| `4339893` | 2026-05-17 | Preserve partial Terraform state |
| `df54d53` | 2026-05-19 | Shared-VNet support |
| `4c1c54f` | 2026-06-02 | Direct propagation probe |
| `35ced14` | 2026-06-04 | Policy-scale and expanded metrics |
| `fa197c0` | 2026-06-12 | Blob TSDB snapshots |
| `0ec395d` | 2026-06-25 | KWOK/mock topology |
| `ea8f47a` | 2026-07-04 | KWOK provider IDs |
| `5bb8daa` | 2026-07-08 | KWOK-controller APF priority |
| `38785f4` | 2026-07-08 | n=100 100% global density |
| `1461c77` | 2026-07-14 | Full telemetry |
| `f82a411` | 2026-07-14 | Split managed telemetry phases |
| `7b103d6` | 2026-07-19 | Telemetry smoke corrections |
| `2475611` | 2026-07-20 | n=100 lifecycle |
| `35b2258` | 2026-07-26 | Harden n=100 launch |
| `c6bd145` | 2026-08-02 | Staged Fleet enrollment |
| `6d8888b` | 2026-08-04 | Reusable n=100 lifecycle |
| `df174d4` | 2026-08-10 | n=99 Fleet rejoin |
| `23b28e8` | 2026-08-12 | Existing-Fleet repair resume |
| `03f721a` | 2026-08-13 | Rebalance n=100 telemetry shards |
| `bebdfbb` | 2026-08-14 | Exact telemetry reassignment |
| `be03f40` | 2026-08-18 | Durable mock-agent ownership |
| `e37f8d3` | 2026-08-19 | Preserved worker/live-overlay recovery |
| `e123017` | 2026-08-20 | Protect managed node-RG leases |
| `2ef7d98` | 2026-08-22 | KWOK preservation proof |
| `68ae3c2` | 2026-08-24 | General preserved lifecycle |
| `0bbece8` | 2026-09-02 | n=100 exact baseline capture |
| `d99d092` | 2026-09-03 | n=100 cross-run verification |
| `28e8638` | 2026-09-03 | Verified workload handoff |
| `d01839f` | 2026-09-03 | Minimum bounded repair selection |
| `a6bbb03` | 2026-09-03 | Workload rerun hardening |
| `943a3d6` | 2026-09-03 | Every-agent live validation |
| `3746878` | 2026-09-04 | Managed telemetry resume hardening |

## 11. Source map

| Area | Primary repository sources |
|---|---|
| Pipeline parameters and active stages | [`pipelines/system/new-pipeline-test.yml`](../pipelines/system/new-pipeline-test.yml) |
| Standard provision/execute/finalize job | [`jobs/competitive-test.yml`](../jobs/competitive-test.yml) |
| Preserved n=100 resume job | [`jobs/clustermesh-debug-resume.yml`](../jobs/clustermesh-debug-resume.yml) |
| Topology formation and validation | [`steps/topology/clustermesh-scale/`](../steps/topology/clustermesh-scale/) |
| Mock topology and handoff | [`steps/topology/clustermesh-scale-mock/`](../steps/topology/clustermesh-scale-mock/) |
| CL2 shared-suite engine | [`steps/engine/clusterloader2/clustermesh-scale/`](../steps/engine/clusterloader2/clustermesh-scale/) |
| Scenario definitions and measurements | [`modules/python/clusterloader2/clustermesh-scale/config/`](../modules/python/clusterloader2/clustermesh-scale/config/) |
| Scale orchestration and aggregation | [`scale.py`](../modules/python/clusterloader2/clustermesh-scale/scale.py) |
| Scenario evidence | [`scenario_evidence.py`](../modules/python/clusterloader2/clustermesh-scale/scenario_evidence.py) |
| Measurement/continuation policy | [`scenario_policy.py`](../modules/python/clusterloader2/clustermesh-scale/scenario_policy.py) |
| Scenario cleanup reconciliation | [`scenario_cleanup_reconcile.py`](../modules/python/clusterloader2/clustermesh-scale/scenario_cleanup_reconcile.py) |
| Mock-layer reconciliation | [`mock_layer_reconcile.py`](../modules/python/clusterloader2/clustermesh-scale/mock_layer_reconcile.py) |
| Preserved ARM/worker/overlay repair | [`preserved_aks_arm_reconcile.py`](../modules/python/clusterloader2/clustermesh-scale/preserved_aks_arm_reconcile.py), [`preserved_worker_reconcile.py`](../modules/python/clusterloader2/clustermesh-scale/preserved_worker_reconcile.py), [`preserved_live_overlay.py`](../modules/python/clusterloader2/clustermesh-scale/preserved_live_overlay.py) |
| Exact preservation and handoff | [`preserved_mock_capture.py`](../modules/python/clusterloader2/clustermesh-scale/preserved_mock_capture.py), [`preserved_mock_verify.py`](../modules/python/clusterloader2/clustermesh-scale/preserved_mock_verify.py), [`preserved_mock_handoff.py`](../modules/python/clusterloader2/clustermesh-scale/preserved_mock_handoff.py) |
| Managed telemetry | [`scenarios/perf-eval/clustermesh-scale/telemetry/`](../scenarios/perf-eval/clustermesh-scale/telemetry/) |
| Failure catalog | [`clustermesh-scale-failure-modes.md`](clustermesh-scale-failure-modes.md) |
| Metric reference | [`MESH-METRICS.md`](MESH-METRICS.md) |

## 12. Maintenance guidance

Update this history when a run changes one of these boundaries:

- a new scale tier becomes live-proven;
- a scenario runs at n=100 for the first time;
- a lifecycle gate changes the meaning of success;
- a preservation contract changes;
- a new failure mode changes architecture or operating procedure;
- the full n=100 suite completes.

For each update, record:

1. build ID and source SHA;
2. exact scale, mode, region, and preserved/fresh lifecycle;
3. last successful gate;
4. failure or scientific result;
5. whether workloads ran;
6. artifacts and snapshots;
7. whether the result is live-proven or only code/preview validated.
