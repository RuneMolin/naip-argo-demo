# naip-argo

A simplified, "lift-and-shift" version of the app-of-apps pattern used for Kafka
in the `solution-wrapper` / `solution-helm-chart` / `kafka-helm-chart` repos.

Three Helm charts, three Argo CD levels:

```mermaid
flowchart TD
    Z["bootstrap/&lt;env&gt;.yaml<br/>apply once, pins targetRevision"] -->|"Application: naip-{cluster}-{env}"| A["wrapper/<br/>root Application, one per cluster/env"]
    A --> B["solution/<br/>decides WHICH components to deploy"]
    B -->|"one Application per kafka.sites[] entry"| C1["kafka/<br/>site: standalone (sandbox)"]
    B -->|"one Application per kafka.sites[] entry"| C2["kafka/<br/>site: active (staging/production)"]
    B -->|"one Application per kafka.sites[] entry"| C3["kafka/<br/>site: passive (staging/production)"]
    C1 --> D["Kafka CR, KafkaNodePool,<br/>explicit KafkaTopics, KafkaUsers,<br/>Apicurio Registry"]
    C2 --> D
    C3 -->|"MirrorMaker2 pulls from active"| D
```

## Key simplifications vs. the original repos

| Original | This version |
|---|---|
| Topics auto-discovered by scanning every Spring Boot microservice's values | Topics are declared **explicitly** as a plain list in the `kafka` values block |
| Vault-backed secrets, dual/broker node-pool split, Cruise Control, rack awareness | Single `KafkaNodePool`, plain Kubernetes `Secret`s (swap for Vault/ESO later) |
| No cross-cluster replication | `KafkaMirrorMaker2` wired for an **active/passive** (DR) topology |
| No schema registry | **Apicurio Registry** deployment included |

## Layout

```
bootstrap/  - one plain Argo CD Application manifest per environment, applied once by hand/pipeline
wrapper/    - root Argo CD Application chart, one release per cluster/environment
solution/   - per-environment chart; renders the child "kafka" Argo CD Application(s)
kafka/      - leaf chart; renders Strimzi Kafka, topics, users, MM2, Apicurio
```

## Branching strategy: one branch, pin by tag

All environments live on a single trunk branch. Environment differences are
data (values files + `kafka.sites`), not code, so there is nothing to branch —
promoting a change means bumping a `targetRevision` (git tag/SHA), never
merging one environment's branch into another's:

- **sandbox** always tracks `main` (see [bootstrap/sandbox.yaml](bootstrap/sandbox.yaml)) — no promotion gate.
- **staging** and **production** each pin a release tag in their own
  `bootstrap/<env>.yaml` + `wrapper/values/<env>/values.yaml` +
  `solution/values/<env>/values.yaml`. Promotion = bump the tag in staging's
  files, verify, then bump the same tag in production's files.
- `solution/values/<env>/values.yaml`'s `kafkaChart.targetRevision` can pin
  the leaf `kafka` chart to a different ref than the rest of the tree, if you
  ever need to roll Kafka forward/back independently.

## Environments and Kafka topology

Three example environments live under `solution/values/<env>/values.yaml`:
sandbox, staging, production. Each environment's `kafka.sites` list controls
whether it gets one plain Kafka cluster or a full active/passive DR pair — the
`solution` chart renders **one child Argo CD Application per site**, so the
number of Kafka clusters an environment gets is purely a function of how many
entries are in `kafka.sites`:

| Environment | `kafka.sites` | Result |
|---|---|---|
| [sandbox](solution/values/sandbox/values.yaml) | one entry, `role: standalone` | a single Kafka cluster, no MirrorMaker2 |
| [staging](solution/values/staging/values.yaml) | two entries, `role: active` + `role: passive` | two Kafka clusters wired by MirrorMaker2 |
| [production](solution/values/production/values.yaml) | same shape as staging | same, but sized up and pointed at two distinct registered clusters (`destinationServer`) for real DR |

### Active/passive MirrorMaker2 model

- The **active** site is where applications produce/consume normally. It does
  **not** run MirrorMaker2 (no `mirrorMaker2` block).
- The **passive** (DR/secondary) site runs `MirrorMaker2` and continuously
  replicates topics *from* active *to* passive using
  `IdentityReplicationPolicy`, so topic names are **not** prefixed with the
  source cluster alias — this is what makes true active/passive failover
  possible (apps can point at either cluster using the same topic names).
- On failover you repoint applications at the passive cluster. To fail back,
  you flip the roles in values (remove `mirrorMaker2` from the new active,
  add it to the new passive pointing the other direction).
- Each site gets its own namespace (e.g. `staging-active`/`staging-passive`),
  created automatically via `CreateNamespace=true` on its Application.

## Apicurio Registry

Deployed per-cluster, backed by in-memory storage by default
(`apicurio/templates/apicurio-registry.yaml` in the `kafka` chart). This is
intentionally the simplest possible storage backend so the example always
works out of the box — swap `apicurio.storage` for a real backend (PostgreSQL
or Kafka-based `kafkasql`) before using this in production, and double check
the exact env var names against the Apicurio version you deploy (they changed
between the 2.x and 3.x major releases).

## How to adapt this for a new project

1. Rename `naip-argo-*` in the three `Chart.yaml` files and update `repoURL`
   values in [wrapper/values.yaml](wrapper/values.yaml), [solution/values.yaml](solution/values.yaml),
   and every file under [bootstrap/](bootstrap).
2. Add one `wrapper/values/<env>/values.yaml` and one `solution/values/<env>/values.yaml`
   per environment, plus a matching `bootstrap/<env>.yaml`.
3. List your real topics under `kafka.topics` in each env's solution values file.
4. Decide the topology per environment: one `kafka.sites` entry
   (`role: standalone`) for a simple environment, or two entries
   (`role: active` / `role: passive`) for full DR — the passive entry's
   `mirrorMaker2.source.bootstrapServers` must point at the active entry's
   Kafka bootstrap address.
5. Pin each non-sandbox environment's `targetRevision` (in its `bootstrap/`,
   `wrapper/values/`, and `solution/values/` files) to a release tag instead
   of `main`, and promote by bumping that tag.
