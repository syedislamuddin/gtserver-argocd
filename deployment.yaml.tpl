---
## Deployment ###
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sample-deployment
  namespace: argocd
  labels:
    app: sample-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: sample-app
  template:
    metadata:
      labels:
        app: sample-app
    spec:
      containers:
      - name: sample-app
        image: europe-west4-docker.pkg.dev/GOOGLE_CLOUD_PROJECT/gt-server/sample:COMMIT_SHA
        ports:
        - containerPort: 8080
