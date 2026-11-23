import os
import tempfile
import unittest

# Must be set before the controller module (and its database) is imported.
os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")

from fastapi.testclient import TestClient  # noqa: E402

from coordinator.main import app  # noqa: E402


class TestErrorContract(unittest.TestCase):
    """Status codes and JSON error bodies required by the API spec."""

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app)  # no lifespan: no background tasks
        cls.base = "/api/v1/objects"

    def test_unknown_object_returns_404_json(self):
        for method, path in (
            ("get", f"{self.base}/nope"),
            ("get", f"{self.base}/nope/metadata"),
            ("delete", f"{self.base}/nope"),
        ):
            response = getattr(self.client, method)(path)
            self.assertEqual(response.status_code, 404, path)
            self.assertIn("detail", response.json())

        response = self.client.patch(
            f"{self.base}/nope/metadata", json={"a": "b"}
        )
        self.assertEqual(response.status_code, 404)

    def test_upload_without_file_is_400(self):
        response = self.client.post(self.base)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "Invalid request")

    def test_upload_with_no_healthy_workers_is_503(self):
        response = self.client.post(
            self.base, files={"file": ("a.txt", b"hello", "text/plain")}
        )
        self.assertEqual(response.status_code, 503)

    def test_list_is_empty_json(self):
        response = self.client.get(self.base)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["total_objects"], 0)


if __name__ == "__main__":
    unittest.main()
