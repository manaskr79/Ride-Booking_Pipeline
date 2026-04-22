#!/usr/bin/env bash
# =============================================================================
# Azure Infrastructure Provisioning Script
# Ride Booking Data Pipeline
# =============================================================================
# Requirements: Azure CLI, logged in with `az login`
# Usage: chmod +x setup_azure.sh && ./setup_azure.sh
# =============================================================================

set -euo pipefail

# ─── Configuration ────────────────────────────────────────────────────────────
RESOURCE_GROUP="rg-ride-pipeline"
LOCATION="eastus"
STORAGE_ACCOUNT="ridepipelineadls"        # Must be globally unique (3-24 chars)
EVENTHUB_NAMESPACE="ride-eventhub-ns"     # Azure Event Hubs (Kafka-compatible)
DATABRICKS_WORKSPACE="ride-databricks"
CONTAINER_DELTA="delta"
CONTAINER_CHECKPOINTS="checkpoints"
CONTAINER_RAW="raw"
SKU_STORAGE="Standard_LRS"
SKU_EVENTHUB="Standard"
EVENTHUB_CAPACITY=2                       # Throughput units

echo "=============================================="
echo " 🚗 Ride Booking Pipeline – Azure Setup"
echo "=============================================="

# ─── 1. Resource Group ────────────────────────────────────────────────────────
echo ""
echo "📦 Creating Resource Group: $RESOURCE_GROUP"
az group create \
  --name "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --output table

# ─── 2. Azure Data Lake Storage Gen2 (ADLS) ──────────────────────────────────
echo ""
echo "💾 Creating ADLS Gen2 Storage Account: $STORAGE_ACCOUNT"
az storage account create \
  --name "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku "$SKU_STORAGE" \
  --kind StorageV2 \
  --hierarchical-namespace true \
  --output table

# Get storage key
STORAGE_KEY=$(az storage account keys list \
  --resource-group "$RESOURCE_GROUP" \
  --account-name "$STORAGE_ACCOUNT" \
  --query "[0].value" -o tsv)

echo "🪣 Creating containers: $CONTAINER_DELTA, $CONTAINER_CHECKPOINTS, $CONTAINER_RAW"
for container in "$CONTAINER_DELTA" "$CONTAINER_CHECKPOINTS" "$CONTAINER_RAW"; do
  az storage container create \
    --name "$container" \
    --account-name "$STORAGE_ACCOUNT" \
    --account-key "$STORAGE_KEY" \
    --output table
done

# ─── 3. Azure Event Hubs (Kafka-compatible) ───────────────────────────────────
echo ""
echo "📡 Creating Event Hubs Namespace: $EVENTHUB_NAMESPACE"
az eventhubs namespace create \
  --name "$EVENTHUB_NAMESPACE" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku "$SKU_EVENTHUB" \
  --capacity "$EVENTHUB_CAPACITY" \
  --enable-kafka true \
  --output table

echo "📨 Creating Event Hubs (topics)..."
for topic in "ride-requests" "ride-completed" "driver-locations"; do
  az eventhubs eventhub create \
    --name "$topic" \
    --namespace-name "$EVENTHUB_NAMESPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --partition-count 4 \
    --message-retention 7 \
    --output table
done

# Get connection string
EH_CONN_STR=$(az eventhubs namespace authorization-rule keys list \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$EVENTHUB_NAMESPACE" \
  --name RootManageSharedAccessKey \
  --query "primaryConnectionString" -o tsv)

# ─── 4. Azure Databricks ──────────────────────────────────────────────────────
echo ""
echo "⚡ Creating Azure Databricks Workspace: $DATABRICKS_WORKSPACE"
az databricks workspace create \
  --name "$DATABRICKS_WORKSPACE" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --sku standard \
  --output table

# ─── 5. Output Config ─────────────────────────────────────────────────────────
echo ""
echo "=============================================="
echo " ✅ Infrastructure Provisioned!"
echo "=============================================="
echo ""
echo "📋 Update these values in your pipeline config:"
echo ""
echo "  STORAGE_ACCOUNT  = $STORAGE_ACCOUNT"
echo "  DELTA_BASE       = abfss://$CONTAINER_DELTA@${STORAGE_ACCOUNT}.dfs.core.windows.net/ride-pipeline"
echo "  CHECKPOINT_BASE  = abfss://$CONTAINER_CHECKPOINTS@${STORAGE_ACCOUNT}.dfs.core.windows.net/ride-pipeline"
echo ""
echo "  EVENTHUB_NAMESPACE = $EVENTHUB_NAMESPACE"
echo "  KAFKA_SERVERS      = ${EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"
echo "  EH_CONN_STR        = $EH_CONN_STR"
echo ""
echo "  DATABRICKS_URL     = $(az databricks workspace show \
    --name $DATABRICKS_WORKSPACE \
    --resource-group $RESOURCE_GROUP \
    --query workspaceUrl -o tsv)"
echo ""

# Write config to file
cat > .env.azure << EOF
STORAGE_ACCOUNT=$STORAGE_ACCOUNT
DELTA_BASE=abfss://${CONTAINER_DELTA}@${STORAGE_ACCOUNT}.dfs.core.windows.net/ride-pipeline
CHECKPOINT_BASE=abfss://${CONTAINER_CHECKPOINTS}@${STORAGE_ACCOUNT}.dfs.core.windows.net/ride-pipeline
EVENTHUB_NAMESPACE=$EVENTHUB_NAMESPACE
KAFKA_SERVERS=${EVENTHUB_NAMESPACE}.servicebus.windows.net:9093
EH_CONNECTION_STRING=$EH_CONN_STR
EOF

echo "📄 Config saved to .env.azure"
