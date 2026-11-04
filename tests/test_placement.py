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


if __name__ == "__main__":
    unittest.main()