# GCP Deployment Guide — Fraud Detection Pipeline

> **Audience**: Developers new to Google Cloud Platform.  
> This guide walks you through every step: creating a GCP account, provisioning resources, deploying all 15 services, running the pipeline, and monitoring it.

---

## Table of Contents

1. [Architecture on GCP](#1-architecture-on-gcp)
2. [GCP Compatibility Fixes Applied](#2-gcp-compatibility-fixes-applied)
3. [Prerequisites](#3-prerequisites)
4. [GCP Account & Project Setup](#4-gcp-account--project-setup)
5. [Option A — Compute Engine VM (Recommended for Beginners)](#5-option-a--compute-engine-vm-recommended-for-beginners)
   - [5.1 Create GCP Resources (Automated)](#51-create-gcp-resources-automated)
   - [5.2 SSH into the VM](#52-ssh-into-the-vm)
   - [5.3 Install Docker on the VM](#53-install-docker-on-the-vm)
   - [5.4 Clone and Configure the Repository](#54-clone-and-configure-the-repository)
   - [5.5 Build and Start the Full Stack](#55-build-and-start-the-full-stack)
   - [5.6 Submit Flink Jobs](#56-submit-flink-jobs)
   - [5.7 Register the Debezium CDC Connector](#57-register-the-debezium-cdc-connector)
   - [5.8 Start the Load Generator](#58-start-the-load-generator)
6. [Option B — GKE (Advanced / Production)](#6-option-b--gke-advanced--production)
7. [Accessing the UIs](#7-accessing-the-uis)
8. [Testing the Pipeline](#8-testing-the-pipeline)
9. [Monitoring](#9-monitoring)
10. [Troubleshooting](#10-troubleshooting)
11. [Cost Estimates](#11-cost-estimates)
12. [Cleanup](#12-cleanup)

---

## 1. Architecture on GCP

The stack runs on a single **Compute Engine VM** for simplicity, with **Google Cloud Storage (GCS)** for Flink checkpoints so state survives container restarts.

```
┌─────────────────────────── Compute Engine VM (n2-standard-8) ───────────────────────────┐
│                                                                                           │
│  ┌──────────────┐   CDC    ┌──────────────────┐  raw txns  ┌───────────────────────┐   │
│  │  PostgreSQL  │─────────▶│  Kafka Connect   │───────────▶│  Apache Kafka (KRaft) │   │
│  │  (port 5432) │          │  + Debezium      │            │  (port 9092)           │   │
│  └──────────────┘          └──────────────────┘            └───────────┬───────────┘   │
│         ▲                                                               │                │
│         │ inserts                                           scored/     │                │
│  ┌──────┴──────┐                                           anomalies   ▼                │
│  │   Load Gen  │                                        ┌──────────────────────────┐   │
│  │  (port 8080)│                                        │   Apache Flink 1.18.1    │   │
│  └─────────────┘                                        │   EnrichScore + Anomaly  │   │
│                                                          │   + SpendAnalytics Jobs  │   │
│  ┌──────────────┐           ┌──────────────┐            └──────────────┬───────────┘   │
│  │   AI Agent   │◀──────────│   Next.js UI │                           │ writes        │
│  │  FastAPI +   │  /api/*   │   (port 3001)│            checkpoints    ▼               │
│  │  Claude API  │           └──────────────┘          ┌─────────────────────────────┐  │
│  │  (port 8000) │                                      │  Google Cloud Storage (GCS) │  │
│  └──────────────┘                                      │  gs://bucket/flink-ckpt/    │  │
│                                                         └─────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────┐                              │
│  │  Observability Stack                                   │                              │
│  │  Prometheus (9090) • Grafana (3000) • Jaeger (16686)  │                              │
│  │  OTel Collector • Kafka Exporter • Postgres Exporter  │                              │
│  └───────────────────────────────────────────────────────┘                              │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

**GCP Services Used:**

| GCP Service | Purpose | Estimated Monthly Cost |
|---|---|---|
| Compute Engine n2-standard-8 | Runs all containers | ~$200 |
| Cloud Storage (GCS) | Flink checkpoints | < $1 |
| Cloud Firewall | Port access rules | Free |
| IAM Service Account | GCS + logging access | Free |

---

## 2. GCP Compatibility Fixes Applied

The following issues were identified and fixed before this guide was written:

### Issue 1 — Flink checkpoints on local filesystem (CRITICAL for GKE)

**Problem:** `state.checkpoints.dir=file:///flink-checkpoints` hard-codes a local path. In Kubernetes, the JobManager and TaskManager run on *different nodes* and cannot share a local volume, causing checkpoint failures and job crashes.

**Fix:** The Flink Dockerfile now installs the `flink-gs-fs-hadoop` GCS plugin, and the checkpoint directory is driven by the `FLINK_CHECKPOINT_DIR` environment variable (defaults to the local path, set to `gs://` for GCP).

**File changed:** `infra/flink/Dockerfile`, `infra/docker-compose.yml`

---

### Issue 2 — Kafka external listener hardcoded to `localhost` (CRITICAL)

**Problem:** `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,EXTERNAL://localhost:9092` — external clients on a GCP VM cannot reach Kafka via `localhost`.

**Fix:** Parameterized to `EXTERNAL://${KAFKA_EXTERNAL_HOST:-localhost}:9092`. Set `KAFKA_EXTERNAL_HOST` to the VM's external IP in `.env`.

**File changed:** `infra/docker-compose.yml`

---

### Issue 3 — Hardcoded credentials (SECURITY)

**Problem:** Passwords (`fraudpass`, `admin`) and the Debezium connector config were hardcoded throughout docker-compose.

**Fix:** All credentials are now driven by environment variables with safe defaults. Set real passwords in `.env` for production.

**Files changed:** `infra/docker-compose.yml`, `infra/kafka/connector-config.json`, `Makefile`

---

### Issue 4 — AI Agent CORS allows all origins (SECURITY)

**Problem:** `allow_origins=["*"]` lets any website make authenticated requests to the API.

**Fix:** CORS origins are now read from the `CORS_ORIGINS` environment variable (comma-separated list). Default is `*` for local dev; set to the UI URL for GCP.

**File changed:** `services/ai-agent/src/main.py`

---

### Issue 5 — Iceberg warehouse path hardcoded to S3

**Problem:** `warehouse-path = "s3://fraud-warehouse"` in `application.conf` would fail on GCP.

**Status:** Already has `${?ICEBERG_WAREHOUSE_PATH}` override. Set `ICEBERG_WAREHOUSE_PATH=gs://your-bucket/iceberg` in `.env` if/when Iceberg is activated.

---

## 3. Prerequisites

### On your local machine:

- **Google Cloud account** with billing enabled
- **gcloud CLI** — [Install guide](https://cloud.google.com/sdk/docs/install)
- **Git** — to clone the repository

### Verify gcloud is installed:

```bash
gcloud version
# Should print: Google Cloud SDK 460.0.0 or later
```

### Authenticate gcloud:

```bash
gcloud auth login
# Opens a browser — log in with your Google account

gcloud auth application-default login
# Needed for API calls from local scripts
```

---

## 4. GCP Account & Project Setup

### Step 4.1 — Create a GCP Project

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Click the project dropdown at the top → **New Project**
3. Give it a name like `fraud-detection-pipeline`
4. Note the **Project ID** (auto-generated, e.g. `fraud-detection-pipeline-123456`)
5. Click **Create**

### Step 4.2 — Enable Billing

1. In the GCP Console, go to **Billing**
2. Link a billing account to your project
3. (New accounts get $300 free credit — this pipeline fits comfortably within that)

### Step 4.3 — Set your project in gcloud

```bash
# Replace with your actual Project ID
export GCP_PROJECT_ID="fraud-detection-pipeline-123456"

gcloud config set project $GCP_PROJECT_ID
```

---

## 5. Option A — Compute Engine VM (Recommended for Beginners)

This option runs the entire pipeline on a single VM using Docker Compose — the simplest path.

### 5.1 Create GCP Resources (Automated)

The script `gcp/setup-gcp-resources.sh` creates everything you need with one command:

```bash
# Clone the repo locally first
git clone https://github.com/YOUR_ORG/fraud-detection-pipeline.git
cd fraud-detection-pipeline

# Set your project ID
export GCP_PROJECT_ID="fraud-detection-pipeline-123456"

# (Optional) customize region/zone
export GCP_REGION="us-central1"
export GCP_ZONE="us-central1-a"

# Run the setup script
chmod +x gcp/setup-gcp-resources.sh
./gcp/setup-gcp-resources.sh
```

**What the script does:**
1. Enables required GCP APIs (Compute, Storage, Secret Manager, etc.)
2. Creates a service account with GCS + logging permissions
3. Creates a GCS bucket for Flink checkpoints (with 30-day lifecycle)
4. Opens firewall ports for the pipeline UIs
5. Creates a Compute Engine VM (n2-standard-8, 100 GB SSD, Ubuntu 22.04)
6. Installs Docker and tools via startup script

> **Expected output:** The script prints the VM's external IP and all next steps at the end. Save the external IP.

---

### Manual alternative (if you prefer the GCP Console)

<details>
<summary>Click to expand manual steps</summary>

**Create VM:**
1. Go to **Compute Engine → VM Instances → Create Instance**
2. Name: `fraud-pipeline-vm`
3. Region: `us-central1` / Zone: `us-central1-a`
4. Machine type: `n2-standard-8` (8 vCPU, 32 GB)
5. Boot disk: Ubuntu 22.04 LTS, 100 GB SSD
6. Service account: Use the one created by the script (or Compute Engine default)
7. Access scopes: **Allow full access to all Cloud APIs**
8. Click **Create**

**Create firewall rule:**
1. Go to **VPC Network → Firewall → Create Firewall Rule**
2. Name: `fraud-pipeline-allow-web`
3. Targets: Specified target tags → `fraud-pipeline`
4. Source IP ranges: `0.0.0.0/0`
5. Protocols/ports: TCP `3000,3001,8080,8082,8083,9090,9080,16686`

**Create GCS bucket:**
1. Go to **Cloud Storage → Create Bucket**
2. Name: `fraud-flink-ckpt-YOUR_PROJECT_ID` (must be globally unique)
3. Region: `us-central1` (same as VM)
4. Click **Create**

</details>

---

### 5.2 SSH into the VM

```bash
# Replace with your zone and project
gcloud compute ssh fraud-pipeline-vm \
  --zone=us-central1-a \
  --project=$GCP_PROJECT_ID
```

> **First time?** gcloud generates SSH keys automatically and uploads them to the VM.

Once inside the VM, wait ~2 minutes for the startup script to finish installing Docker:

```bash
# Check if Docker is ready
docker --version
# Expected: Docker version 26.x.x
```

If Docker isn't installed yet, wait 60 seconds and try again.

---

### 5.3 Install Docker on the VM

> **Skip this step if Docker is already installed** (the startup script handles it).

```bash
# If startup script didn't run, install manually
curl -fsSL https://get.docker.com | sudo bash
sudo usermod -aG docker $USER
sudo apt-get install -y docker-compose-plugin git gettext-base make
newgrp docker   # apply group change without logout
```

Verify:
```bash
docker compose version
# Expected: Docker Compose version v2.x.x
```

---

### 5.4 Clone and Configure the Repository

```bash
# Clone the repository on the VM
git clone https://github.com/YOUR_ORG/fraud-detection-pipeline.git
cd fraud-detection-pipeline

# Create your .env from the GCP template
cp .env.gcp.example .env
```

Now edit `.env` and fill in your values:

```bash
nano .env
```

**Required values to set:**

```bash
# 1. Your Anthropic API key (get from console.anthropic.com)
ANTHROPIC_API_KEY=sk-ant-api03-...

# 2. Change the Postgres password from the default
POSTGRES_PASSWORD=MyStr0ngP@ssword2024

# 3. Flink checkpoint GCS path (use your bucket name from Step 5.1)
FLINK_CHECKPOINT_DIR=gs://fraud-flink-ckpt-YOUR_PROJECT_ID/flink-checkpoints

# 4. VM external IP (shown in the setup script output, or run:)
#    curl -s ifconfig.me
KAFKA_EXTERNAL_HOST=34.123.45.67   # replace with your VM's external IP

# 5. Restrict CORS to your UI URL
CORS_ORIGINS=http://34.123.45.67:3001

# 6. Grafana password
GRAFANA_ADMIN_PASSWORD=MyGrafana2024
```

Save and exit (`Ctrl+X`, `Y`, `Enter` in nano).

**Verify your .env:**
```bash
grep -E "^(ANTHROPIC|POSTGRES_PASSWORD|FLINK_CHECKPOINT|KAFKA_EXTERNAL|CORS)" .env
```

Expected (with your actual values):
```
ANTHROPIC_API_KEY=sk-ant-api03-...
POSTGRES_PASSWORD=MyStr0ngP@ssword2024
FLINK_CHECKPOINT_DIR=gs://fraud-flink-ckpt-my-project/flink-checkpoints
KAFKA_EXTERNAL_HOST=34.123.45.67
CORS_ORIGINS=http://34.123.45.67:3001
```

---

### 5.5 Build and Start the Full Stack

Build the custom Docker images and start all services:

```bash
# This builds: Flink image (with GCS plugin), AI Agent, UI, Load Generator
# Then starts all 15 services
make up
```

> **Expected time:** 5-10 minutes for the first build (downloads base images + builds Scala fat jar takes the longest). Subsequent starts are < 2 minutes.

**What `make up` does:**
1. Builds the Flink image (includes GCS plugin)
2. Starts all 15 containers via Docker Compose
3. Waits for Kafka Connect to be healthy (~60 seconds)
4. Registers the Debezium CDC connector

**Monitor the startup:**
```bash
# In a second terminal window:
make logs
```

**Check all containers are healthy:**
```bash
docker compose -f infra/docker-compose.yml ps
```

Expected — all services should show `healthy` or `running`:
```
NAME               STATUS          PORTS
kafka              healthy         0.0.0.0:9092->9092/tcp
schema-registry    healthy         0.0.0.0:8081->8081/tcp
kafka-connect      healthy         0.0.0.0:8083->8083/tcp
postgres           healthy         0.0.0.0:5432->5432/tcp
jobmanager         healthy         0.0.0.0:8082->8081/tcp
taskmanager        running
prometheus         healthy         0.0.0.0:9090->9090/tcp
grafana            healthy         0.0.0.0:3000->3000/tcp
jaeger             healthy         0.0.0.0:16686->16686/tcp
otel-collector     running
ai-agent           healthy         0.0.0.0:8000->8000/tcp
ui                 healthy         0.0.0.0:3001->3000/tcp
kafka-ui           running         0.0.0.0:9080->8080/tcp
kafka-exporter     running         0.0.0.0:9308->9308/tcp
postgres-exporter  running         0.0.0.0:9187->9187/tcp
```

---

### 5.6 Submit Flink Jobs

The Flink cluster is running but has no jobs yet. Submit the three pipeline jobs:

```bash
# Build the Scala fat jar and submit all 3 Flink jobs
make submit-jobs
```

> **Expected time:** The `sbt assembly` build takes 3-5 minutes on first run (downloads Scala/Flink dependencies).

**What gets submitted:**
1. **EnrichScoreJob** — reads raw transactions from Kafka, enriches with account/merchant data, computes risk scores, writes to `transactions.scored`
2. **AnomalyDetectionJob** — reads scored transactions, detects velocity bursts / geo-travel anomalies / z-score spikes, writes to `transactions.anomalies` and Postgres
3. **SpendAnalyticsJob** — aggregates spend by account/merchant/category in tumbling windows

**Verify jobs are running:**
```bash
curl -s http://localhost:8082/jobs | python3 -m json.tool
```

Expected:
```json
{
  "jobs": [
    {"id": "abc123", "status": "RUNNING"},
    {"id": "def456", "status": "RUNNING"},
    {"id": "ghi789", "status": "RUNNING"}
  ]
}
```

You can also see them in the Flink Web UI at `http://YOUR_VM_IP:8082`.

---

### 5.7 Register the Debezium CDC Connector

> **Note:** `make up` already attempts to register the connector. Run this manually only if it failed.

```bash
# Check connector status
make connector-status
```

If it shows an error, register it manually:

```bash
# Uses envsubst to substitute your .env passwords into the connector config
source .env
envsubst < infra/kafka/connector-config.json | \
  curl -sf -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d @-
```

Expected response:
```json
{
  "name": "transactions-cdc",
  "config": {...},
  "tasks": [...],
  "type": "source"
}
```

**Verify CDC is working:**
```bash
make connector-status
# Look for: "state": "RUNNING"
```

---

### 5.8 Start the Load Generator

The load generator inserts synthetic transactions into Postgres. Debezium picks them up → Kafka → Flink → anomaly detection → AI analysis.

```bash
# Start the load generator (100 txn/s, 5% fraud, runs continuously)
docker compose -f infra/docker-compose.yml --profile demo up -d load-generator

# Verify it's inserting:
docker logs load-generator --tail=20
```

Expected log output:
```
2024-01-15 10:23:01 INFO     Starting load generator: rate=100 tps, fraud_pct=5.0%, duration=0s
2024-01-15 10:23:01 INFO     Postgres connection pool established.
2024-01-15 10:23:01 INFO     Prometheus metrics → http://0.0.0.0:8000/metrics
```

**For a burst demo (500 tps for 5 minutes):**
```bash
docker compose -f infra/docker-compose.yml run --rm load-generator \
  python src/generator.py --rate 500 --fraud-pct 5 --duration 300
```

---

## 6. Option B — GKE (Advanced / Production)

> This section gives an overview. Full K8s manifests are in `gcp/k8s/` (to be added).

GKE deployment is recommended when you need:
- High availability (multiple replicas)
- Horizontal scaling of Flink TaskManagers
- Rolling deployments without downtime
- Integration with Google Cloud Monitoring

### Overview of changes vs. VM deployment:

| Component | VM (Docker Compose) | GKE |
|---|---|---|
| Kafka | Single-broker container | Strimzi Operator or Confluent Cloud |
| PostgreSQL | Container with local volume | Cloud SQL (managed) |
| Flink | 2 containers | Flink Kubernetes Operator |
| Observability | Containers | GKE Managed Prometheus + Cloud Trace |
| Secrets | `.env` file | Kubernetes Secrets + GCP Secret Manager |
| Ingress | Direct port exposure | GKE Ingress / Cloud Load Balancer |

### Quick GKE cluster setup:

```bash
# Create a GKE cluster
gcloud container clusters create fraud-pipeline-cluster \
  --project=$GCP_PROJECT_ID \
  --zone=us-central1-a \
  --num-nodes=3 \
  --machine-type=n2-standard-4 \
  --workload-pool=$GCP_PROJECT_ID.svc.id.goog

# Get credentials
gcloud container clusters get-credentials fraud-pipeline-cluster \
  --zone=us-central1-a \
  --project=$GCP_PROJECT_ID

# Verify
kubectl get nodes
```

### Critical GKE-specific configuration:

1. **Flink checkpoints MUST use GCS** (local volumes don't work across nodes):
   ```
   FLINK_CHECKPOINT_DIR=gs://your-bucket/flink-checkpoints
   ```

2. **Workload Identity** for Flink → GCS access (no key file needed):
   ```bash
   gcloud iam service-accounts add-iam-policy-binding \
     fraud-pipeline-sa@$PROJECT_ID.iam.gserviceaccount.com \
     --role=roles/iam.workloadIdentityUser \
     --member="serviceAccount:$PROJECT_ID.svc.id.goog[fraud/flink-sa]"
   ```

3. **Kubernetes Secrets** for credentials:
   ```bash
   kubectl create secret generic fraud-secrets \
     --from-literal=postgres-password=YOUR_PASSWORD \
     --from-literal=anthropic-api-key=sk-ant-...
   ```

---

## 7. Accessing the UIs

Replace `YOUR_VM_EXTERNAL_IP` with your VM's external IP in all URLs.

| Service | URL | Credentials |
|---|---|---|
| **Fraud Dashboard** (Next.js UI) | `http://YOUR_VM_EXTERNAL_IP:3001` | None |
| **Grafana** | `http://YOUR_VM_EXTERNAL_IP:3000` | admin / (your GRAFANA_ADMIN_PASSWORD) |
| **Flink Web UI** | `http://YOUR_VM_EXTERNAL_IP:8082` | None |
| **Prometheus** | `http://YOUR_VM_EXTERNAL_IP:9090` | None |
| **Kafka UI** | `http://YOUR_VM_EXTERNAL_IP:9080` | None |
| **Kafka Connect** | `http://YOUR_VM_EXTERNAL_IP:8083` | None |
| **Jaeger Tracing** | `http://YOUR_VM_EXTERNAL_IP:16686` | None |
| **AI Agent API** | `http://YOUR_VM_EXTERNAL_IP:8000/docs` | None (Swagger UI) |

> **Find your VM IP at any time:**
> ```bash
> gcloud compute instances describe fraud-pipeline-vm \
>   --zone=us-central1-a \
>   --format="get(networkInterfaces[0].accessConfigs[0].natIP)"
> ```

---

## 8. Testing the Pipeline

### 8.1 Verify end-to-end data flow

After starting the load generator, wait 60 seconds for transactions to flow through, then:

**Check transactions are being inserted:**
```bash
docker exec postgres psql -U frauduser -d frauddb -c \
  "SELECT COUNT(*) FROM transactions;"
```

**Check Flink is scoring transactions:**
```bash
docker exec postgres psql -U frauduser -d frauddb -c \
  "SELECT COUNT(*), AVG(risk_score) FROM scored_transactions;"
```

**Check anomalies are being detected:**
```bash
docker exec postgres psql -U frauduser -d frauddb -c \
  "SELECT reason_code, COUNT(*) FROM anomaly_events
   GROUP BY reason_code ORDER BY COUNT(*) DESC;"
```

Expected output:
```
     reason_code      | count
----------------------+-------
 HIGH_VELOCITY        |    45
 AMOUNT_SPIKE         |    23
 GEO_TRAVEL_ANOMALY   |    12
```

**Check Claude is analyzing fraud events:**
```bash
docker exec postgres psql -U frauduser -d frauddb -c \
  "SELECT classification, COUNT(*) FROM ai_fraud_analysis
   GROUP BY classification;"
```

### 8.2 Test via the REST API

```bash
# Health check
curl http://localhost:8000/health
# Expected: {"status":"ok"}

# Get dashboard stats
curl http://localhost:8000/api/stats | python3 -m json.tool

# List recent fraud events
curl "http://localhost:8000/api/fraud-events?limit=5" | python3 -m json.tool

# Trigger manual analysis on a specific event
EVENT_ID=$(curl -s "http://localhost:8000/api/fraud-events?limit=1" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)[0]['id'])")
curl -X POST "http://localhost:8000/api/fraud-events/$EVENT_ID/analyze" | \
  python3 -m json.tool
```

### 8.3 Test Kafka topic flow

```bash
# List topics
make topic-list

# Watch raw transactions flowing in (Ctrl+C to stop)
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 \
  --topic transactions.raw \
  --from-beginning \
  --max-messages 5
```

### 8.4 Verify GCS checkpoints are being written

After Flink jobs have been running for 30+ seconds:

```bash
gsutil ls -r gs://YOUR_BUCKET/flink-checkpoints/
```

Expected: checkpoint directories per job appearing in GCS.

---

## 9. Monitoring

### 9.1 Grafana Dashboards

1. Open `http://YOUR_VM_EXTERNAL_IP:3000`
2. Log in with `admin` / your `GRAFANA_ADMIN_PASSWORD`
3. Go to **Dashboards** — three pre-built dashboards are provisioned:
   - **Fraud Monitoring** — real-time anomaly rates, AI classifications, risk score distributions
   - **Real-time Traffic** — transaction throughput, Kafka lag, Flink processing latency
   - **Spend Analytics** — spend aggregation by merchant/account/category

### 9.2 Key Prometheus Metrics

Open `http://YOUR_VM_EXTERNAL_IP:9090` and try these queries:

```promql
# Transaction throughput from load generator
rate(load_gen_transactions_inserted_total[1m])

# Kafka consumer lag per Flink job
kafka_consumer_group_lag{group="flink-enrich-score-cg"}

# Flink job uptime
flink_jobmanager_job_uptime

# Flink checkpoint duration
flink_jobmanager_job_lastCheckpointDuration
```

### 9.3 Jaeger Distributed Tracing

1. Open `http://YOUR_VM_EXTERNAL_IP:16686`
2. Select **Service** → `fraud-pipeline`
3. Click **Find Traces** to see end-to-end traces for transaction processing

### 9.4 GCP Cloud Monitoring (Optional)

The VM's service account has `roles/monitoring.metricWriter` — Docker container metrics are automatically exported to Cloud Monitoring.

```bash
# View logs in GCP Cloud Logging
gcloud logging read "resource.type=gce_instance" \
  --project=$GCP_PROJECT_ID \
  --limit=50
```

---

## 10. Troubleshooting

### Containers won't start

```bash
# Check logs for a specific service
docker compose -f infra/docker-compose.yml logs kafka --tail=50
docker compose -f infra/docker-compose.yml logs jobmanager --tail=50
docker compose -f infra/docker-compose.yml logs ai-agent --tail=50
```

### Kafka isn't healthy after 5 minutes

```bash
# Kafka KRaft needs the cluster ID to be initialized
# If it shows errors about missing meta.properties, clean volumes and restart:
docker compose -f infra/docker-compose.yml down -v
docker compose -f infra/docker-compose.yml up -d kafka
```

### Flink jobs fail with GCS permission error

```bash
# Verify the VM has GCS access
curl -H "Metadata-Flavor: Google" \
  "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/scopes"
# Should include: https://www.googleapis.com/auth/devstorage.full_control

# Check if the GCS plugin is loaded
docker logs jobmanager 2>&1 | grep -i "gs-fs\|gcs\|google"
```

If not working:
```bash
# Verify bucket exists and is accessible from the VM
gsutil ls gs://YOUR_BUCKET/
```

### Debezium connector shows "FAILED" status

```bash
# Check connector logs
docker logs kafka-connect --tail=100 | grep -i error

# Common cause: Postgres logical replication not enabled
# Verify replication slot exists:
docker exec postgres psql -U frauduser -d frauddb -c \
  "SELECT slot_name, active FROM pg_replication_slots;"
```

### Flink jobs not processing (stuck)

```bash
# Check if Kafka topics have data
docker exec kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list kafka:29092 --topic transactions.raw --time -1

# Check Flink task manager logs
docker compose -f infra/docker-compose.yml logs taskmanager --tail=100
```

### AI Agent not analyzing events

```bash
# Check if ANTHROPIC_API_KEY is set
docker exec ai-agent env | grep ANTHROPIC

# Check agent logs
docker logs ai-agent --tail=50

# Manually trigger analysis
curl -X POST "http://localhost:8000/api/fraud-events/EVENT_ID_HERE/analyze"
```

### UI shows blank page or API errors

```bash
# Check if ai-agent is reachable from ui container
docker exec fraud-ui wget -qO- http://ai-agent:8000/health

# Check Next.js logs
docker logs fraud-ui --tail=50

# Verify API_URL is set correctly
docker exec fraud-ui env | grep API_URL
```

### Port not accessible from browser

```bash
# Verify firewall rule allows the port
gcloud compute firewall-rules describe fraud-pipeline-allow-web \
  --project=$GCP_PROJECT_ID

# Verify container is actually listening
docker ps --format "table {{.Names}}\t{{.Ports}}"
```

### Out of disk space on VM

```bash
# Check disk usage
df -h

# Remove old Docker images/containers
docker system prune -af --volumes
```

---

## 11. Cost Estimates

### Compute Engine VM (n2-standard-8)

| Item | Estimate |
|---|---|
| n2-standard-8 VM (24/7) | ~$200/month |
| 100 GB SSD boot disk | ~$17/month |
| Egress traffic (light) | < $5/month |
| **VM Total** | **~$222/month** |

### Cheaper alternatives:

- **e2-standard-8** (spot/preemptible): ~$50/month (pipeline resumes from GCS checkpoints after preemption)
- **e2-standard-4** (4 vCPU, 16 GB): ~$100/month (reduce Flink parallelism to 2)

### GCS (Flink checkpoints)

| Item | Estimate |
|---|---|
| Storage (< 1 GB of checkpoint data) | < $0.03/month |
| API operations | < $0.01/month |
| **GCS Total** | **< $1/month** |

### Tips to reduce costs:

```bash
# Stop VM when not in use (data is preserved on the disk)
gcloud compute instances stop fraud-pipeline-vm --zone=us-central1-a

# Start it again
gcloud compute instances start fraud-pipeline-vm --zone=us-central1-a

# Run on a preemptible VM (80% discount, stops after 24h max)
gcloud compute instances create fraud-pipeline-vm --preemptible ...
```

---

## 12. Cleanup

### Stop containers (keep VM and data):

```bash
# SSH into VM first, then:
make down
```

### Delete all GCP resources (PERMANENT — no recovery):

```bash
# From your LOCAL machine:

# 1. Delete the VM
gcloud compute instances delete fraud-pipeline-vm \
  --zone=us-central1-a \
  --project=$GCP_PROJECT_ID \
  --quiet

# 2. Delete the GCS bucket and all its contents
gsutil rm -r gs://fraud-flink-ckpt-YOUR_PROJECT_ID

# 3. Delete the firewall rule
gcloud compute firewall-rules delete fraud-pipeline-allow-web \
  --project=$GCP_PROJECT_ID \
  --quiet

# 4. Delete the service account
gcloud iam service-accounts delete \
  fraud-pipeline-sa@$GCP_PROJECT_ID.iam.gserviceaccount.com \
  --project=$GCP_PROJECT_ID \
  --quiet

# 5. (Optional) Delete the project entirely — this removes EVERYTHING
gcloud projects delete $GCP_PROJECT_ID
```

> **Warning:** Deleting the project is irreversible. Only do this if you're done with everything.

---

## Appendix: Environment Variable Reference

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | *(required)* | Claude API key from console.anthropic.com |
| `POSTGRES_DB` | `frauddb` | PostgreSQL database name |
| `POSTGRES_USER` | `frauduser` | PostgreSQL username |
| `POSTGRES_PASSWORD` | `fraudpass` | **CHANGE IN PRODUCTION** |
| `FLINK_CHECKPOINT_DIR` | `file:///flink-checkpoints` | Use `gs://bucket/path` for GCP |
| `GOOGLE_APPLICATION_CREDENTIALS` | *(empty)* | Path to SA key file (not needed on GCE) |
| `KAFKA_EXTERNAL_HOST` | `localhost` | External IP for Kafka clients outside docker |
| `GRAFANA_ADMIN_PASSWORD` | `admin` | **CHANGE IN PRODUCTION** |
| `CORS_ORIGINS` | `*` | Comma-separated allowed origins for AI Agent |
| `API_URL` | `http://ai-agent:8000` | URL the UI server uses to reach the AI Agent |
| `ANALYSIS_INTERVAL_SECONDS` | `30` | How often the AI agent polls for new events |
| `GENERATOR_RATE` | `100` | Load generator transactions per second |
| `GENERATOR_FRAUD_PCT` | `5` | Percentage of fraudulent transactions |
| `ICEBERG_WAREHOUSE_PATH` | `s3://fraud-warehouse` | Set to `gs://bucket/iceberg` for GCP |
