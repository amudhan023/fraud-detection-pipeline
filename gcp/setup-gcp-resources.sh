#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# setup-gcp-resources.sh
#
# One-shot script that creates all GCP resources needed to run the
# fraud-detection-pipeline on a Compute Engine VM.
#
# Run this from your LOCAL machine (requires gcloud CLI + billing enabled).
#
# Usage:
#   chmod +x gcp/setup-gcp-resources.sh
#   ./gcp/setup-gcp-resources.sh
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Configuration — edit these before running ──────────────────────────────
PROJECT_ID="${GCP_PROJECT_ID:-}"      # e.g. my-fraud-project-123
REGION="${GCP_REGION:-us-central1}"
ZONE="${GCP_ZONE:-us-central1-a}"
VM_NAME="${VM_NAME:-fraud-pipeline-vm}"
VM_MACHINE_TYPE="${VM_MACHINE_TYPE:-n2-standard-8}"   # 8 vCPU, 32 GB RAM
VM_DISK_SIZE="${VM_DISK_SIZE:-100GB}"
GCS_BUCKET="${GCS_BUCKET:-}"         # e.g. fraud-flink-ckpt-my-project
SA_NAME="fraud-pipeline-sa"
# ──────────────────────────────────────────────────────────────────────────

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Pre-flight checks ──────────────────────────────────────────────────────
command -v gcloud >/dev/null 2>&1 || error "gcloud CLI not found. Install from https://cloud.google.com/sdk/docs/install"

if [[ -z "$PROJECT_ID" ]]; then
  PROJECT_ID=$(gcloud config get-value project 2>/dev/null)
  [[ -z "$PROJECT_ID" ]] && error "Set GCP_PROJECT_ID env var or run: gcloud config set project YOUR_PROJECT"
fi

if [[ -z "$GCS_BUCKET" ]]; then
  GCS_BUCKET="fraud-flink-ckpt-${PROJECT_ID}"
fi

info "Project : $PROJECT_ID"
info "Region  : $REGION / $ZONE"
info "VM      : $VM_NAME ($VM_MACHINE_TYPE)"
info "Bucket  : $GCS_BUCKET"
echo ""

# ── 1. Set active project ──────────────────────────────────────────────────
info "Setting active project..."
gcloud config set project "$PROJECT_ID"

# ── 2. Enable required APIs ────────────────────────────────────────────────
info "Enabling GCP APIs (this may take 1-2 minutes)..."
gcloud services enable \
  compute.googleapis.com \
  storage.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  logging.googleapis.com \
  monitoring.googleapis.com \
  --project="$PROJECT_ID"
info "APIs enabled."

# ── 3. Create service account ──────────────────────────────────────────────
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
if ! gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT_ID" >/dev/null 2>&1; then
  info "Creating service account: $SA_EMAIL"
  gcloud iam service-accounts create "$SA_NAME" \
    --display-name="Fraud Pipeline Service Account" \
    --project="$PROJECT_ID"
else
  warn "Service account $SA_EMAIL already exists, skipping."
fi

# Grant GCS access for Flink checkpoints
info "Granting Storage Object Admin to service account..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.objectAdmin" \
  --quiet

# Grant Secret Manager access
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/secretmanager.secretAccessor" \
  --quiet

# Grant logging/monitoring write
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/monitoring.metricWriter" \
  --quiet

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/logging.logWriter" \
  --quiet

# ── 4. Create GCS bucket ───────────────────────────────────────────────────
if ! gsutil ls "gs://${GCS_BUCKET}" >/dev/null 2>&1; then
  info "Creating GCS bucket: gs://$GCS_BUCKET"
  gsutil mb -p "$PROJECT_ID" -l "$REGION" "gs://${GCS_BUCKET}"
  gsutil versioning set on "gs://${GCS_BUCKET}"
  gsutil lifecycle set /dev/stdin "gs://${GCS_BUCKET}" <<EOF
{
  "rule": [{
    "action": {"type": "Delete"},
    "condition": {"age": 30}
  }]
}
EOF
else
  warn "Bucket gs://$GCS_BUCKET already exists, skipping."
fi
info "GCS bucket ready: gs://$GCS_BUCKET"

# ── 5. Create firewall rule ────────────────────────────────────────────────
FIREWALL_RULE="fraud-pipeline-allow-web"
if ! gcloud compute firewall-rules describe "$FIREWALL_RULE" --project="$PROJECT_ID" >/dev/null 2>&1; then
  info "Creating firewall rule: $FIREWALL_RULE"
  gcloud compute firewall-rules create "$FIREWALL_RULE" \
    --direction=INGRESS \
    --priority=1000 \
    --network=default \
    --action=ALLOW \
    --rules=tcp:3000,tcp:3001,tcp:8080,tcp:8082,tcp:8083,tcp:9090,tcp:9080,tcp:16686 \
    --source-ranges=0.0.0.0/0 \
    --target-tags=fraud-pipeline \
    --description="Allow web UIs for fraud detection pipeline" \
    --project="$PROJECT_ID"
else
  warn "Firewall rule $FIREWALL_RULE already exists, skipping."
fi

# ── 6. Create Compute Engine VM ───────────────────────────────────────────
if ! gcloud compute instances describe "$VM_NAME" --zone="$ZONE" --project="$PROJECT_ID" >/dev/null 2>&1; then
  info "Creating VM: $VM_NAME (this takes ~2 minutes)..."
  gcloud compute instances create "$VM_NAME" \
    --project="$PROJECT_ID" \
    --zone="$ZONE" \
    --machine-type="$VM_MACHINE_TYPE" \
    --image-family=ubuntu-2204-lts \
    --image-project=ubuntu-os-cloud \
    --boot-disk-size="$VM_DISK_SIZE" \
    --boot-disk-type=pd-ssd \
    --service-account="${SA_EMAIL}" \
    --scopes=cloud-platform \
    --tags=fraud-pipeline \
    --metadata=startup-script='#!/bin/bash
set -e
# Install Docker
curl -fsSL https://get.docker.com | bash
usermod -aG docker ubuntu
# Install Docker Compose plugin
apt-get install -y docker-compose-plugin
# Install git and other tools
apt-get install -y git gettext-base curl wget make
echo "Startup script complete."'
  info "VM created: $VM_NAME"
else
  warn "VM $VM_NAME already exists, skipping."
fi

# ── 7. Get VM external IP ──────────────────────────────────────────────────
EXTERNAL_IP=$(gcloud compute instances describe "$VM_NAME" \
  --zone="$ZONE" \
  --project="$PROJECT_ID" \
  --format="get(networkInterfaces[0].accessConfigs[0].natIP)")

# ── 8. Print summary ───────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║          GCP RESOURCES CREATED SUCCESSFULLY                 ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""
info "VM External IP  : $EXTERNAL_IP"
info "GCS Bucket      : gs://$GCS_BUCKET"
info "Service Account : $SA_EMAIL"
echo ""
echo "─── Next Steps ───────────────────────────────────────────────"
echo ""
echo "1. SSH into the VM:"
echo "   gcloud compute ssh $VM_NAME --zone=$ZONE --project=$PROJECT_ID"
echo ""
echo "2. Wait ~2 minutes for the startup script to finish, then:"
echo "   sudo -u ubuntu bash   # switch to ubuntu user (has docker access)"
echo ""
echo "3. Clone the repo and configure:"
echo "   git clone https://github.com/YOUR_ORG/fraud-detection-pipeline.git"
echo "   cd fraud-detection-pipeline"
echo "   cp .env.gcp.example .env"
echo "   nano .env   # fill in your values"
echo ""
echo "4. Key .env values to set:"
echo "   ANTHROPIC_API_KEY=sk-ant-..."
echo "   POSTGRES_PASSWORD=<strong-password>"
echo "   FLINK_CHECKPOINT_DIR=gs://$GCS_BUCKET/flink-checkpoints"
echo "   KAFKA_EXTERNAL_HOST=$EXTERNAL_IP"
echo "   CORS_ORIGINS=http://$EXTERNAL_IP:3001"
echo "   GRAFANA_ADMIN_PASSWORD=<strong-password>"
echo ""
echo "5. Build and start:"
echo "   make up"
echo ""
echo "─── UI URLs (once running) ────────────────────────────────────"
echo "   Fraud Dashboard  → http://$EXTERNAL_IP:3001"
echo "   Grafana          → http://$EXTERNAL_IP:3000  (admin / your password)"
echo "   Flink UI         → http://$EXTERNAL_IP:8082"
echo "   Prometheus       → http://$EXTERNAL_IP:9090"
echo "   Kafka UI         → http://$EXTERNAL_IP:9080"
echo "   Jaeger           → http://$EXTERNAL_IP:16686"
echo ""
echo "See docs/GCP_DEPLOYMENT.md for the full step-by-step guide."
