import unittest
from types import SimpleNamespace

from coordinator.placement import select_workers


class TestPlacement(unittest.TestCase):

    def setUp(self):
        self.workers = [
            SimpleNamespace(worker_id=f"worker{i}", status="HEALTHY")
            for i in range(1, 5)
        ]

    def test_selects_two_workers(self):
        selected = select_workers("chunk-1", self.workers)
        self.assertEqual(len(selected), 2)

    def test_workers_are_distinct(self):
        selected = select_workers("chunk-1", self.workers)
        self.assertNotEqual(
            selected[0].worker_id,
            selected[1].worker_id
        )

    def test_unhealthy_workers_are_excluded(self):
        self.workers[0].status = "UNHEALTHY"

        selected = select_workers("chunk-1", self.workers)

        self.assertNotIn(
            "worker1",
            [worker.worker_id for worker in selected]
        )

    def test_insufficient_workers(self):
        with self.assertRaises(RuntimeError):
            select_workers(
                "chunk-1",
                self.workers[:1]
            )

    def test_deterministic_placement(self):
        first = select_workers("chunk-1", self.workers)
        second = select_workers("chunk-1", self.workers)

        self.assertEqual(
            [w.worker_id for w in first],
            [w.worker_id for w in second]
        )

    def test_load_aware_prefers_least_loaded(self):
        load = {"worker1": 9, "worker2": 0, "worker3": 9, "worker4": 1}

        selected = select_workers("chunk-1", self.workers, 2, load)

        self.assertEqual(
            sorted(w.worker_id for w in selected),
            ["worker2", "worker4"]
        )

    def test_load_aware_keeps_cluster_balanced(self):
        load = {}

        for index in range(400):

            for worker in select_workers(
                f"chunk-{index}", self.workers, 2, load
            ):
                load[worker.worker_id] = load.get(worker.worker_id, 0) + 1

        self.assertLessEqual(max(load.values()) - min(load.values()), 1)

    def test_load_aware_still_excludes_unhealthy(self):
        self.workers[1].status = "UNHEALTHY"

        selected = select_workers(
            "chunk-1", self.workers, 2, {"worker1": 100}
        )

        self.assertNotIn("worker2", [w.worker_id for w in selected])


if __name__ == "__main__":
    unittest.main()