# ArgoCD Demo — Macy's GKE Pipeline Setup

## Overview
This folder contains everything needed to demo ArgoCD pipeline setup for GKE apps.

## Structure
```
argocd-demo/
├── README.md                    # This file
├── setup/
│   ├── 01-create-cluster.sh     # Create GKE cluster
│   ├── 02-install-argocd.sh     # Install ArgoCD
│   └── 03-configure-access.sh   # Configure OpsRabbit access
├── apps/
│   ├── clientfacing-ui/         # Frontend UI app manifests
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   └── configmap.yaml
│   └── inventory-api/           # Backend API app manifests
│       ├── deployment.yaml
│       ├── service.yaml
│       └── configmap.yaml
└── argocd-apps/
    ├── clientfacing-ui-app.yaml # ArgoCD Application for UI
    └── inventory-api-app.yaml   # ArgoCD Application for API
```

## Setup Steps (GKE on GCP)

### 1. Create GKE Cluster
```bash
cd setup && bash 01-create-cluster.sh
```

### 2. Install ArgoCD
```bash
bash 02-install-argocd.sh
```

### 3. Access ArgoCD UI
```bash
# Get admin password
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d

# Port forward
kubectl port-forward svc/argocd-server -n argocd 8080:443

# Open https://localhost:8080 (user: admin)
```

### 4. Push this repo to GitHub
```bash
cd argocd-demo
git init
git add .
git commit -m "Initial ArgoCD demo setup"
gh repo create aaic-opsrabbit-demo/argocd-demo --public --push --source=.
```

### 5. Create ArgoCD Applications
```bash
kubectl apply -f argocd-apps/
```
