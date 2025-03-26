#!/bin/bash
export ZONE=europe-west4-a
export ZONE1=europe-west4
export GCP_PROJECT=gp2-release-terra
export CLUSTER_NAME=gtcluster-eu-west4
export BUCKET_NAME=gtserver-eu-west4-gp2-release-terra
export GCP_SA_NAME=gtcluster-eu-west4
export K8S_NAMESPACE=gke-ns-gtcluster-eu-west4
export K8S_SA_NAME=gke-sa-gtcluster-eu-west4


#create k8s cluster ($CLUSTER_NAME) with c4-standard-16 machine-type in zone ($ZONE) with one (worker) node in gcp projetc ($GCP_PROJECT). Also enable GcsFuseCsiDriver for gcp bucket access and secret manager for API key (currently not used)  
gcloud container clusters create $CLUSTER_NAME \
  --enable-secret-manager \
  --addons GcsFuseCsiDriver \
  --zone $ZONE \
  --workload-pool=$GCP_PROJECT.svc.id.goog \
  --project $GCP_PROJECT \
  --machine-type e2-standard-4 \
  --num-nodes=1

#Get the Auth Credentials for Your Cluster
gcloud container clusters get-credentials $CLUSTER_NAME --zone $ZONE
#enable HttpLoadBalancing add-on for ingress
gcloud container clusters update $CLUSTER_NAME --update-addons=HttpLoadBalancing=ENABLED --zone $ZONE
#create a service account to use with the k8s cluster
gcloud iam service-accounts create $GCP_SA_NAME --project=$GCP_PROJECT
#create k8s namespace to better manage cluster resources in case we have multiple clusters
kubectl create namespace $K8S_NAMESPACE
#set current names space
kubectl config set-context --current --namespace=gke-ns-gtcluster-eu-west4
#create k8s service account
kubectl create serviceaccount $K8S_SA_NAME --namespace $K8S_NAMESPACE
#Set up a sample cloud storage bucket and upload test objects. PLEASE IGNORE IF bucket is already there
gcloud storage buckets create gs://$BUCKET_NAME \
  --location $ZONE1 --uniform-bucket-level-access --project $GCP_PROJECT
#set IAM policy for the bucket and allow access via service account we created above
gcloud storage buckets add-iam-policy-binding gs://$BUCKET_NAME \
  --member "serviceAccount:$GCP_SA_NAME@$GCP_PROJECT.iam.gserviceaccount.com" \
  --role "roles/storage.objectAdmin" \
  --project $GCP_PROJECT
#create annotated kubernetes service account for $GCP_SA_NAME (that has access to gcp bucket) with name ksa in the namespac ${K8S_NAMESPACE}
cat <<EOF | kubectl apply -f -
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ksa
  namespace: ${K8S_NAMESPACE}
  annotations:
    iam.gke.io/gcp-service-account: ${GCP_SA_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com
EOF
# Also bind Kubernetes ServiceAccount to access Secret Manager -  not implemented yet
# gcloud secrets add-iam-policy-binding SECRET_NAME \
#     --role=roles/secretmanager.secretAccessor \
#     --member=principal://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/PROJECT_ID.svc.id.goog/subject/ns/NAMESPACE/sa/KSA_NAME

# now bind this derived service account (ksa) service to impersonate as real gcp service account via ${GCP_SA_NAME} for bucket access
gcloud iam service-accounts add-iam-policy-binding $GCP_SA_NAME@$GCP_PROJECT.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:$GCP_PROJECT.svc.id.goog[$K8S_NAMESPACE/ksa]" \
  --project $GCP_PROJECT  

#create persisten volume (PV) and provide gcp bucket

cat <<EOF | kubectl apply -f -
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: gtserver-pv
  namespace: ${K8S_NAMESPACE}
spec:
  accessModes:
  - ReadWriteMany
  capacity:
    storage: 500Gi
  storageClassName: gtserver-pv
  mountOptions:
    - implicit-dirs
    # - uid=1001
    # - gid=3003
  csi:
    driver: gcsfuse.csi.storage.gke.io
    volumeHandle: gtserver-eu-west4-gp2-release-terra
EOF
#also create persisten volume claim (PVC) to use PV
cat <<EOF | kubectl apply -f -
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: gtserver-pvc
  namespace: ${K8S_NAMESPACE}
spec:
  accessModes:
  - ReadWriteMany
  resources:
    requests:
      storage: 500Gi
  volumeName: gtserver-pv
  storageClassName: gtserver-pv
EOF

#if pv or pvc can not be deleted
# kubectl patch pv sample-app-pv -p '{"metadata":{"finalizers":null}}' -n namespace1
