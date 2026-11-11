
import unittest
from unittest.mock import MagicMock, patch

from rca.log_retention import count_eligible_logs


class TestLogRetention(unittest.TestCase):

    def test_rejects_zero_retention_days(self):
        with self.assertRaises(ValueError):
            count_eligible_logs("postgresql://unused", 0)

    def test_rejects_negative_retention_days(self):
        with self.assertRaises(ValueError):
            count_eligible_logs("postgresql://unused", -1)

    @patch("rca.log_retention.psycopg.connect")
    def test_counts_eligible_logs(self, mock_connect):
        connection = MagicMock()
        cursor = MagicMock()

        mock_connect.return_value.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.return_value = (15,)

        result = count_eligible_logs("postgresql://unused", 30)

        self.assertEqual(result, 15)
        cursor.execute.assert_called_once()

        query, params = cursor.execute.call_args.args
        self.assertIn("completed", query)
        self.assertIn("failed", query)
        self.assertIn("received_at", query)
        self.assertEqual(params[0], "00000000-0000-0000-0000-000000000000")

    @patch("rca.log_retention.psycopg.connect")
    def test_no_eligible_logs(self, mock_connect):
        connection = MagicMock()
        cursor = MagicMock()

        mock_connect.return_value.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.return_value = (0,)

        self.assertEqual(
            count_eligible_logs("postgresql://unused", 30),
            0,
        )

    @patch("rca.log_retention.psycopg.connect")
    def test_does_not_delete_records(self, mock_connect):
        connection = MagicMock()
        cursor = MagicMock()

        mock_connect.return_value.__enter__.return_value = connection
        connection.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchone.return_value = (5,)

        count_eligible_logs("postgresql://unused", 30)

        query = cursor.execute.call_args.args[0]
        self.assertTrue(query.strip().upper().startswith("SELECT"))
        self.assertNotIn("DELETE", query.upper())


if __name__ == "__main__":
    unittest.main()
