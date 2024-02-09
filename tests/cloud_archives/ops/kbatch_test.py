import unittest
from unittest.mock import MagicMock, patch

from cloud_archives.ops.kbatch import (
    KbatchJobException,
    wait_for_status_change,
)


def generate_mock_list_pods_response(status: str) -> dict:
    return {
        "items": [
            {
                "status": {"phase": status, "container_statuses": [{"state": status}]},
                "metadata": {"name": "test_pod"},
            },
        ],
    }


class TestWaitForStatusChange(unittest.TestCase):

    @patch("time.sleep", return_value=None)
    @patch(
        "cloud_archives.ops.kbatch.kbc.list_pods",
        side_effect=[
            generate_mock_list_pods_response("Pending"),
            generate_mock_list_pods_response("Pending"),
            generate_mock_list_pods_response("Running"),
        ],
    )
    def test_wait_for_status_change_successful(
        self,
        mock_list_pods: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        new_status = wait_for_status_change(old_status="Pending", job_name="test-job")
        self.assertEqual(new_status, "Running")
        # Make sure list_pods is called for each status check
        self.assertEqual(mock_list_pods.call_count, 3)

    @patch("time.sleep", return_value=None)
    @patch(
        "cloud_archives.ops.kbatch.kbc.list_pods",
        side_effect=[
            generate_mock_list_pods_response("Pending"),
            generate_mock_list_pods_response("Failed"),
        ],
    )
    def test_wait_for_status_change_failed(
        self,
        mock_list_pods: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        new_status = wait_for_status_change(old_status="Pending", job_name="test-job")
        self.assertEqual(new_status, "Failed")
        # Make sure list_pods is called
        self.assertEqual(mock_list_pods.call_count, 2)

    @patch("time.sleep", return_value=None)
    @patch(
        "cloud_archives.ops.kbatch.kbc.list_pods",
        return_value=generate_mock_list_pods_response("Pending"),
    )
    def test_wait_for_status_change_timeout(
        self,
        mock_list_pods: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        with self.assertRaises(KbatchJobException):
            new_status = wait_for_status_change(old_status="Pending", job_name="test-job")
            self.assertEqual(new_status, "Pending")
        # Make sure list_pods is called
        self.assertGreater(mock_list_pods.call_count, 1)
        # Make sure time.sleep is called multiple times
        self.assertGreater(mock_sleep.call_count, 1)


if __name__ == "__main__":
    unittest.main()
