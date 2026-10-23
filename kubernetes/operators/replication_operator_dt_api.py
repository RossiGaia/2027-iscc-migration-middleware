import kopf, requests, json, random, datetime, os
from kubernetes import client
from kubernetes.utils import create_from_dict
from k8s_utils import delete_from_dict
import time


@kopf.on.create("cyberphysicalapplications")
def create_fn(spec, name, namespace, meta, logger, **kwargs):
    k8s_client = client.ApiClient()
    k8s_custom_object = client.CustomObjectsApi()

    deployments = spec.get("deployments")
    preferred_affinity = spec.get("requirements").get("preferredAffinity")

    deployment_affinity = None
    deployment_configs = None
    deployment_namespace = None
    deployment_app_name = None
    deployment_prometheus_url = None

    if preferred_affinity == "" or preferred_affinity is None:
        deployment_configs = deployments[0].get("configs")
        deployment_affinity = deployments[0].get("affinity")

    else:
        for deployment in deployments:
            deployment_affinity = deployment.get("affinity")
            if deployment_affinity == preferred_affinity:
                deployment_configs = deployment.get("configs")
                break

    for config in deployment_configs:
        if config.get("kind") == "Deployment":
            config["spec"]["template"]["spec"].update(
                {"nodeSelector": {"zone": f"{deployment_affinity}"}}
            )
            deployment_namespace = config.get("metadata").get("namespace")
            deployment_app_name = config.get("metadata").get("labels").get("app")
            deployment_prometheus_url = (
                config.get("spec")
                .get("template")
                .get("metadata")
                .get("annotations")
                .get("prometheusUrl")
            )

        if config.get("kind") == "Service":
            kopf.label(config, {"debug": "current-service"})

        kopf.label(config, {"related-to": f"{name}"})
        kopf.adopt(config)
        try:
            create_from_dict(k8s_client, config)
        except Exception as e:
            logger.exception("Exception in object creation.")

    annotations_patch = {"metadata": {"annotations": dict(meta.annotations)}}
    annotations_patch["metadata"]["annotations"][
        "child-deployment-namespace"
    ] = deployment_namespace
    annotations_patch["metadata"]["annotations"][
        "child-deployment-app-name"
    ] = deployment_app_name
    annotations_patch["metadata"]["annotations"][
        "child-deployment-prometheus-url"
    ] = deployment_prometheus_url
    annotations_patch["metadata"]["annotations"][
        "child-deployment-affinity"
    ] = deployment_affinity

    group = "test.dev"
    version = "v1"
    plural = "cyberphysicalapplications"
    resp = k8s_custom_object.patch_namespaced_custom_object(
        group, version, namespace, plural, name, body=annotations_patch
    )


def get_prometheus_odte(prometheus_url, twin_of, logger):
    """Fetch ODTE value from Prometheus."""

    query_url = f'{prometheus_url}/api/v1/query?query=odte[pt="{twin_of}"]'.replace(
        "[", "{"
    ).replace("]", "}")
    logger.debug(query_url)

    odte = None
    try:
        resp = requests.get(query_url)
        odte = float(json.loads(resp.text)["data"]["result"][0]["value"][1])
        return odte

    except Exception as e:
        logger.warn(f"Prometheus not available. {e}")
        raise Exception


def choose_next_deployment(deployments, current_deployment_affinity):
    """Select a new deployment avoiding the current one."""

    available_deployments = [
        d for d in deployments if d.get("affinity") != current_deployment_affinity
    ]
    return random.choice(available_deployments) if available_deployments else None


import time

def ensure_pods_ready(k8s_core_v1, app_name, namespace, logger, poll_s=1):
    """Wait until all pods of the deployment are ready."""
    label_selector = f"app={app_name}"

    pods = None
    while not pods:
        try:
            resp = k8s_core_v1.list_namespaced_pod(namespace, label_selector=label_selector)
        except Exception as e:
            logger.error("Cannot list pods: %s", e)
            return

        pods = resp.items or []
        if not pods:
            time.sleep(poll_s)

    for pod in pods:
        pod_ready = False
        pod_name = pod.metadata.name

        while not pod_ready:
            try:
                pod_status = k8s_core_v1.read_namespaced_pod_status(pod_name, namespace)
            except Exception as e:
                logger.error("Cannot read pod status for %s: %s", pod_name, e)
                time.sleep(poll_s)
                continue

            conditions = (pod_status.status.conditions or [])
            for condition in conditions:
                if condition.type == "Ready" and condition.status == "True":
                    pod_ready = True
                    break

            if not pod_ready:
                time.sleep(poll_s)

    return pod_name


def ensure_pod_termination(k8s_core_v1, app_name, namespace, logger):
    terminated = False
    label_selector = f"app={app_name}"
    pod_name = None
    while not terminated:
        try:
            resp = k8s_core_v1.list_namespaced_pod(
                namespace, label_selector=label_selector
            )
            if len(resp.items) == 0:
                terminated = True
            else:
                pod_name = resp.items[0].metadata.name
        except Exception as e:
            logger.info(f"Cannot list pods. {e}")
    return pod_name

@kopf.on.update("cyberphysicalapplications")
def update_fn(name, spec, namespace, **kwargs):

    k8s_client_custom_object = client.CustomObjectsApi()
    migrate_field = spec.get("migrate", None)
    if migrate_field:
        cpa_patch = {"spec": {"migrate": migrate_field}}
        group = "test.dev"
        version = "v1"
        plural = "cyberphysicalapplications"
        obj = k8s_client_custom_object.patch_namespaced_custom_object(
            group, version, namespace, plural, name, body=cpa_patch
        )


@kopf.on.field("cyberphysicalapplications", field="spec.migrate", old=False, new=True)
def migrate_fn(spec, namespace, meta, name, old, new, logger, **_):

    migration_start_ts = time.time()

    k8s_client = client.ApiClient()
    k8s_core_v1 = client.CoreV1Api()
    k8s_custom_object = client.CustomObjectsApi()

    # create new instance
    deployments = spec.get("deployments")
    current_deployment_affinity = meta.get("annotations").get(
        "child-deployment-affinity"
    )
    current_deployment_namespace = meta.get("annotations").get(
        "child-deployment-namespace"
    )
    current_deployment_app_name = meta.get("annotations").get(
        "child-deployment-app-name"
    )
    next_deployment = choose_next_deployment(deployments, current_deployment_affinity)
    next_deployment_configs = next_deployment.get("configs")
    next_deployment_affinity = next_deployment.get("affinity")

    # start the new instance with env vars so it knows it is migrated
    label_selector = "debug=current-service"
    resp = k8s_core_v1.list_namespaced_service(
        current_deployment_namespace, label_selector=label_selector
    )
    current_deployment_service_name = resp.items[0].metadata.name
    current_deployment_service_namespace = resp.items[0].metadata.namespace
    current_deployment_service_port = resp.items[0].spec.ports[0].target_port

    SOURCE_DT_URL_DELTA = f"http://{current_deployment_service_name}.{current_deployment_service_namespace}.svc.cluster.local:{current_deployment_service_port}/delta"
    SOURCE_DT_URL_DISCONNECT = f"http://{current_deployment_service_name}.{current_deployment_service_namespace}.svc.cluster.local:{current_deployment_service_port}/disconnect"

    annotations_patch = {"metadata": {"annotations": dict(meta.annotations)}}
    for config in next_deployment_configs:
        if config.get("kind") == "Deployment":
            config["spec"]["template"]["spec"].update(
                {"nodeSelector": {"zone": f"{next_deployment_affinity}"}}
            )
            next_deployment_namespace = config.get("metadata").get("namespace")
            next_deployment_app_name = config.get("metadata").get("labels").get("app")
            annotations_patch["metadata"]["annotations"][
                "child-deployment-namespace"
            ] = next_deployment_namespace
            annotations_patch["metadata"]["annotations"][
                "child-deployment-app-name"
            ] = (config.get("metadata").get("labels").get("app"))
            annotations_patch["metadata"]["annotations"][
                "child-deployment-prometheus-url"
            ] = (
                config.get("spec")
                .get("template")
                .get("metadata")
                .get("annotations")
                .get("prometheusUrl")
            )
            annotations_patch["metadata"]["annotations"][
                "child-deployment-affinity"
            ] = next_deployment_affinity

            if "env" in config["spec"]["template"]["spec"]["containers"][0]:
                config["spec"]["template"]["spec"]["containers"][0]["env"].append(
                    {"name": "SOURCE_DT_URL_DELTA", "value": SOURCE_DT_URL_DELTA}
                )
            else:
                config["spec"]["template"]["spec"]["containers"][0]["env"] = [
                    {
                        "name": "SOURCE_DT_URL_DELTA",
                        "value": SOURCE_DT_URL_DELTA,
                    }
                ]

            config["spec"]["template"]["spec"]["containers"][0]["env"].append(
                {"name": "SOURCE_DT_URL_DISCONNECT", "value": SOURCE_DT_URL_DISCONNECT}
            )

        if config.get("kind") == "Service":
            next_deployment_service_name = config.get("metadata").get("name")
            next_deployment_service = config

        kopf.adopt(config)
        kopf.label(config, {"related-to": f"{name}"})
        try:
            create_from_dict(k8s_client, config)
        except:
            logger.exception("Exception creating new object.")

    # wait for it to start correctly
    new_pod_name = ensure_pods_ready(
        k8s_core_v1, next_deployment_app_name, next_deployment_namespace, logger
    )
    logger.info("Deployment's pods started.")

    # need to wait the new instance to do the migration
    resp = k8s_core_v1.read_namespaced_service(
        next_deployment_service_name, next_deployment_namespace
    )
    next_deployment_service_port = resp.spec.ports[0].node_port
    control_plane_ip = "10.16.11.142"
    target_dt_migration_done_url = (
        f"http://{control_plane_ip}:{next_deployment_service_port}/migration_status"
    )

    max_retries = 10
    retries = 0
    resp = None
    while not resp or resp.json()["status"] != True:
        if retries == max_retries:
            logger.error("Hit max retries, returning.")
            return
        try:
            resp = requests.get(url=target_dt_migration_done_url)
        except requests.exceptions.ConnectionError:
            logger.info("Cannot connect to target dt.")
            retries += 1
            time.sleep(2.0)

    # re route traffic to the new instance
    # since it is supposed to use MQTT, no specific rerouting is necessary

    # delete old instance
    for depl in deployments:
        if depl.get("affinity") == current_deployment_affinity:
            for config in depl.get("configs"):
                delete_from_dict(k8s_client, config)

    old_pod_name = ensure_pod_termination(k8s_core_v1, current_deployment_app_name, namespace, logger)
    pod_deletion_ts = time.time()
    logger.info(f"Pod deleted at: {pod_deletion_ts}")

    kopf.label(next_deployment_service, {"debug": "current-service"})
    resp = k8s_core_v1.patch_namespaced_service(
        next_deployment_service_name,
        next_deployment_namespace,
        next_deployment_service,
    )

    migration_end_ts = time.time()

    annotations_patch["metadata"]["annotations"]["migration-start-ts"] = str(migration_start_ts)
    annotations_patch["metadata"]["annotations"]["migration-end-ts"] = str(migration_end_ts)
    annotations_patch["metadata"]["annotations"]["pod-deletion-ts"] = str(pod_deletion_ts)
    annotations_patch["metadata"]["annotations"]["new-pod-name"] = new_pod_name
    annotations_patch["metadata"]["annotations"]["old-pod-name"] = old_pod_name

    group = "test.dev"
    version = "v1"
    plural = "cyberphysicalapplications"
    resp = k8s_custom_object.patch_namespaced_custom_object(
        group, version, namespace, plural, name, body=annotations_patch
    )

    return
