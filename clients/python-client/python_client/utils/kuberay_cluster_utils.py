"""
Set of helper methods to manage rayclusters. Requires Python 3.9 and higher
"""

import constants
import logging
import copy
from typing import Any, Tuple


log = logging.getLogger(__name__)
if logging.getLevelName(log.level) == "NOTSET":
    logging.basicConfig(format="%(asctime)s %(message)s", level=constants.LOGLEVEL)

"""
ClusterUtils contains methods to facilitate modifying/populating the config of a raycluster
"""


class ClusterUtils:
    """
    ClusterUtils - Utility class for populating cluster information

    Methods:
    - populate_meta(cluster: dict, name: str, k8s_namespace: str, labels: dict, ray_version: str) -> dict:
    - populate_ray_head(cluster: dict, ray_image: str,service_type: str, cpu_requests: str, memory_requests: str, cpu_limits: str, memory_limits: str, ray_start_params: dict) -> Tuple[dict, bool]:
    - populate_worker_group(cluster: dict, group_name: str, ray_image: str, ray_command: Any, init_image: str, cpu_requests: str, memory_requests: str, cpu_limits: str, memory_limits: str, replicas: int, min_replicas: int, max_replicas: int, ray_start_params: dict) -> Tuple[dict, bool]:
    - update_worker_group_replicas(cluster: dict, group_name: str, max_replicas: int, min_replicas: int, replicas: int) -> Tuple[dict, bool]:
    """

    def populate_meta(
        self,
        cluster: dict,
        name: str,
        k8s_namespace: str,
        labels: dict,
        ray_version: str,
    ) -> dict:
        """Populate the metadata and ray version of the cluster.

        Parameters:
        - cluster (dict): A dictionary representing a cluster.
        - name (str): The name of the cluster.
        - k8s_namespace (str): The namespace of the cluster.
        - labels (dict): A dictionary of labels to be applied to the cluster.
        - ray_version (str): The version of Ray to use in the cluster.

        Returns:
            dict: The updated cluster dictionary with metadata and ray version populated.
        """
        cluster["apiVersion"] = "{group}/{version}".format(
            group=constants.GROUP, version=constants.VERSION
        )
        cluster["kind"] = constants.KIND
        cluster["metadata"] = {
            "name": name,
            "namespace": k8s_namespace,
            "labels": labels,
        }
        cluster["spec"] = {"rayVersion": ray_version}
        return cluster

    def populate_ray_head(
        self,
        cluster: dict,
        ray_image: str,
        service_type: str,
        cpu_requests: str,
        memory_requests: str,
        cpu_limits: str,
        memory_limits: str,
        ray_start_params: dict,
    ) -> Tuple[dict, bool]:
        """Populate the ray head specs of the cluster
        Parameters:
        - cluster (dict): The dictionary representation of the cluster.
        - ray_image (str): The name of the ray image to use for the head node.
        - service_type (str): The type of service to run for the head node.
        - cpu_requests (str): The CPU resource requests for the head node.
        - memory_requests (str): The memory resource requests for the head node.
        - cpu_limits (str): The CPU resource limits for the head node.
        - memory_limits (str): The memory resource limits for the head node.
        - ray_start_params (dict): The parameters for starting the Ray cluster.

        Returns:
        - Tuple (dict, bool): The updated cluster, and a boolean indicating whether the update was successful.
        """
        # validate arguments
        try:
            arguments = locals()
            for k, v in arguments.items():
                assert v
        except AssertionError as e:
            log.error(
                "error creating ray head, the parameters are not fully defined. {} = {}".format(
                    k, v
                )
            )
            return cluster, False

        # make sure metadata exists
        if "spec" in cluster.keys():
            if "headGroupSpec" not in cluster.keys():
                log.info(
                    "setting the headGroupSpec for cluster {}".format(
                        cluster["metadata"]["name"]
                    )
                )
                cluster["spec"]["headGroupSpec"] = []
        else:
            log.error("error creating ray head, the spec and/or metadata is not define")
            return cluster, False

        # populate headGroupSpec
        cluster["spec"]["headGroupSpec"] = {
            "serviceType": service_type,
            "rayStartParams": ray_start_params,
            "template": {
                "spec": {
                    "containers": [
                        {
                            "image": ray_image,
                            "name": "ray-head",
                            "ports": [
                                {
                                    "containerPort": 6379,
                                    "name": "gcs-server",
                                    "protocol": "TCP",
                                },
                                {
                                    "containerPort": 8265,
                                    "name": "dashboard",
                                    "protocol": "TCP",
                                },
                                {
                                    "containerPort": 10001,
                                    "name": "client",
                                    "protocol": "TCP",
                                },
                            ],
                            "resources": {
                                "requests": {
                                    "cpu": cpu_requests,
                                    "memory": memory_requests,
                                },
                                "limits": {"cpu": cpu_limits, "memory": memory_limits},
                            },
                            "volumeMounts": [
                                {"mountPath": "/tmp/ray", "name": "ray-logs"}
                            ],
                        }
                    ],
                    "volumes": [{"emptyDir": {}, "name": "ray-logs"}],
                }
            },
        }

        return cluster, True

    def populate_worker_group(
        self,
        cluster: dict,
        group_name: str,
        ray_image: str,
        ray_command: Any,
        init_image: str,
        cpu_requests: str,
        memory_requests: str,
        cpu_limits: str,
        memory_limits: str,
        replicas: int,
        min_replicas: int,
        max_replicas: int,
        ray_start_params: dict,
    ) -> Tuple[dict, bool]:
        """Populate the worker group specification in the cluster dictionary.

        Parameters:
        - cluster (dict): Dictionary representing the cluster spec.
        - group_name (str): The name of the worker group.
        - ray_image (str): The image to use for the Ray worker containers.
        - ray_command (Any): The command to run in the Ray worker containers.
        - init_image (str): The init container image to use.
        - cpu_requests (str): The requested CPU resources for the worker containers.
        - memory_requests (str): The requested memory resources for the worker containers.
        - cpu_limits (str): The limit on CPU resources for the worker containers.
        - memory_limits (str): The limit on memory resources for the worker containers.
        - replicas (int): The desired number of replicas for the worker group.
        - min_replicas (int): The minimum number of replicas for the worker group.
        - max_replicas (int): The maximum number of replicas for the worker group.
        - ray_start_params (dict): The parameters to pass to the Ray worker start command.

        Returns:
        - Tuple[dict, bool]: A tuple of the cluster specification and a boolean indicating
            whether the worker group was successfully populated.
        """
        # validate arguments
        try:
            arguments = locals()
            for k, v in arguments.items():
                if k != "min_replicas":
                    assert v
        except AssertionError as e:
            log.error(
                "error creating ray head, the parameters are not fully defined. {} = {}".format(
                    k, v
                )
            )
            return cluster, False

        assert max_replicas >= min_replicas

        worker_group: dict[str, Any] = {
            "groupName": group_name,
            "maxReplicas": max_replicas,
            "minReplicas": min_replicas,
            "rayStartParams": ray_start_params,
            "replicas": replicas,
            "template": {
                "spec": {
                    "containers": [
                        {
                            "image": ray_image,
                            "command": ray_command,
                            "lifecycle": {
                                "preStop": {
                                    "exec": {"command": ["/bin/sh", "-c", "ray stop"]}
                                }
                            },
                            "name": "ray-worker",
                            "resources": {
                                "requests": {
                                    "cpu": cpu_requests,
                                    "memory": memory_requests,
                                },
                                "limits": {
                                    "cpu": cpu_limits,
                                    "memory": memory_limits,
                                },
                            },
                            "volumeMounts": [
                                {"mountPath": "/tmp/ray", "name": "ray-logs"}
                            ],
                        }
                    ],
                    "initContainers": [
                        {
                            "command": [
                                "sh",
                                "-c",
                                "until nslookup $RAY_IP.$(cat /var/run/secrets/kubernetes.io/serviceaccount/namespace).svc.cluster.local; do echo waiting for K8s Service $RAY_IP; sleep 2; done",
                            ],
                            "image": init_image,
                            "name": "init",
                        }
                    ],
                    "volumes": [{"emptyDir": {}, "name": "ray-logs"}],
                }
            },
        }

        return worker_group, True

    def update_worker_group_replicas(
        self,
        cluster: dict,
        group_name: str,
        max_replicas: int,
        min_replicas: int,
        replicas: int,
    ) -> Tuple[dict, bool]:
        """Update the number of replicas for a worker group in the cluster.

        Parameters:
        - cluster (dict): The cluster to update.
        - group_name (str): The name of the worker group to update.
        - max_replicas (int): The maximum number of replicas for the worker group.
        - min_replicas (int): The minimum number of replicas for the worker group.
        - replicas (int): The desired number of replicas for the worker group.

        Returns:
        Tuple[dict, bool]: A tuple containing the updated cluster and a flag indicating whether the update was successful.
        """
        try:
            arguments = locals()
            for k, v in arguments.items():
                if k != "min_replicas":
                    assert v
        except AssertionError as e:
            log.error(
                "error creating ray head, the parameters are not fully defined. {} = {}".format(
                    k, v
                )
            )
            return cluster, False

        assert cluster["spec"]["workerGroupSpecs"]
        assert max_replicas >= min_replicas

        for i in range(len(cluster["spec"]["workerGroupSpecs"])):
            if cluster["spec"]["workerGroupSpecs"][i]["groupName"] == group_name:

                cluster["spec"]["workerGroupSpecs"][i]["maxReplicas"] = max_replicas
                cluster["spec"]["workerGroupSpecs"][i]["minReplicas"] = min_replicas
                cluster["spec"]["workerGroupSpecs"][i]["replicas"] = replicas
                return cluster, True

        return cluster, False

    def duplicate_worker_group(
        self,
        cluster: dict,
        group_name: str,
        new_group_name: str,
    ) -> Tuple[dict, bool]:
        """Duplicate a worker group in the cluster.

        Parameters:
        - cluster (dict): The cluster definition.
        - group_name (str): The name of the worker group to be duplicated.
        - new_group_name (str): The name for the duplicated worker group.

        Returns:
        Tuple[dict, bool]: A tuple containing the updated cluster definition and a boolean indicating the success of the operation.
        """
        try:
            arguments = locals()
            for k, v in arguments.items():
                assert v
        except AssertionError as e:
            log.error(
                "error creating ray head, the parameters are not fully defined. {} = {}".format(
                    k, v
                )
            )
            return cluster, False

        assert cluster["spec"]["workerGroupSpecs"]

        for i in range(len(cluster["spec"]["workerGroupSpecs"])):
            if cluster["spec"]["workerGroupSpecs"][i]["groupName"] == group_name:

                duplicate_group = copy.deepcopy(cluster["spec"]["workerGroupSpecs"][i])
                duplicate_group["groupName"] = new_group_name
                cluster["spec"]["workerGroupSpecs"].append(duplicate_group)
                return cluster, True

        return cluster, False
