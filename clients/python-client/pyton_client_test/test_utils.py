import unittest
from utils import kuberay_cluster_utils, kuberay_cluster_builder


class TestUtils(unittest.TestCase):
    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.director = kuberay_cluster_builder.Director()
        self.utils = kuberay_cluster_utils.ClusterUtils()

    def test_update_worker_group_replicas(self):
        cluster = self.director.build_small_cluster(name="small-cluster")

        actual = cluster["metadata"]["name"]
        expected = "small-cluster"
        self.assertEqual(actual, expected)

        cluster, succeeded = self.utils.update_worker_group_replicas(
            cluster,
            group_name="small-cluster-workers",
            max_replicas=10,
            min_replicas=1,
            replicas=5,
        )

        self.assertEqual(succeeded, True)

        # testing the workergroup
        actual = cluster["spec"]["workerGroupSpecs"][0]["replicas"]
        expected = 5
        self.assertEqual(actual, expected)

        actual = cluster["spec"]["workerGroupSpecs"][0]["maxReplicas"]
        expected = 10
        self.assertEqual(actual, expected)

        actual = cluster["spec"]["workerGroupSpecs"][0]["minReplicas"]
        expected = 1
        self.assertEqual(actual, expected)

    def test_duplicate_worker_group(self):
        cluster = self.director.build_small_cluster(name="small-cluster")

        actual = cluster["metadata"]["name"]
        expected = "small-cluster"
        self.assertEqual(actual, expected)

        cluster, succeeded = self.utils.duplicate_worker_group(
            cluster,
            group_name="small-cluster-workers",
            new_group_name="small-cluster-workers-2",
        )
        self.assertEqual(succeeded, True)

        # testing the workergroup
        actual = len(cluster["spec"]["workerGroupSpecs"])
        expected = 2
        self.assertEqual(actual, expected)

        actual = cluster["spec"]["workerGroupSpecs"][1]["groupName"]
        expected = "small-cluster-workers-2"
        self.assertEqual(actual, expected)

        actual = cluster["spec"]["workerGroupSpecs"][1]["maxReplicas"]
        expected = 2
        self.assertEqual(actual, expected)
