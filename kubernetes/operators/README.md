# Kubernetes operators

This folder contains the source code for the Kubernetes operators and the Kubernetes manifests used to deploy stateful Digital Twins.

The operators can be run with Python.

## Python run

Install the required Python packages:

```bash
pip install -r requirements.txt
```

Run an operator with:

```bash
kopf run <operator_to_run.py>
```

To deploy stateful Digital Twins, apply the CRD first:

```bash
kubectl apply -f cyberphysical_application.yaml
```

Then, deploy the Digital Twin custom resource:

```bash
kubectl apply -f <required_cpa.yaml>
```

Replace `<required_cpa.yaml>` with the manifest of the Digital Twin to deploy.