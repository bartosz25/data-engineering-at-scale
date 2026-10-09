# Apache Spark on Kubernetes


## Prerequisites

1. Install minikube on macOS:
```bash
brew install minikube
```
2. Start Docker and Minikube:
```bash
colima start
minikube start --driver=docker --memory=4096 --cpus=4
```
3. Download Apache Spark binaries to use `spark-submit` which is the command sending your code for execution:
```bash
uv venv .venv 
source .venv/bin/activate
uv pip install pyspark==4.0.0 
spark-submit --version 
```

## Packaging
The Spark Operator schedules pods on Kubernetes nodes. A pod runs a Docker image so our Python
code must be packaged as an image and uploaded to Kubernetes.
For our demo the image must be present inside minikube so pods can pull it with `imagePullPolicy: Never` (no external registry needed).

1. Build the Docker image with the latest tag for simplicity:
```bash
docker build -t spark-repartition-demo:latest .
```

2. Load the image into minikube's image store:
```bash
minikube image load spark-repartition-demo:latest
```

3. Verify it is visible to minikube:

```bash
minikube image ls | grep spark-repartition
```

## Kubernetes setup

1. Create a namespace (isolated environment for executing apps) and roles for managing Kuberentes resources:
```bash
kubectl create namespace demo-07

kubectl create serviceaccount spark-editor -n demo-07

kubectl create rolebinding spark-editor-role \
  --clusterrole=edit \
  --serviceaccount=demo-07:spark-editor \
  -n demo-07
```

## PySpark job submit
1. Run this command and record the master address:
```bash
kubectl cluster-info
```
2. Start the Kuberentes dashboard:
```bash
minikube dashboard 
```
3. The dashboard should open. Go to your namespace to follow progress of your job.
4. Use the master address in the following `spark-submit` to trigger your job to the cluster:
```bash
source .venv/bin/activate
spark-submit \
    --master k8s://https://127.0.0.1:32771 \
    --deploy-mode cluster \
    --name Demo \
    --conf spark.executor.instances=2 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-editor \
    --conf spark.kubernetes.namespace=demo-07 \
    --conf spark.kubernetes.container.image=spark-repartition-demo:latest \
    local:///opt/spark/work-dir/job.py
```

> The last argument is the path present in the Docker image not your local disk!

5. Check the logs of the driver:
```bash
# demo-3384eea121364d42-driver is the name of the driver from the Dashboard
kubectl logs demo-3384eea121364d42-driver -n demo-07
```

You should see a print of the DataFrame.

## Shut down the resources:
```bash
kubectl delete namespace demo-07
minikube stop
colima stop   # macOS only
```