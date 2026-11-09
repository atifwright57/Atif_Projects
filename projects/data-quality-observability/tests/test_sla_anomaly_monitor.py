import unittest

from monitoring.sla_anomaly_monitor import evaluate


class EvaluateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.history = [9, 10, 11, 10, 8, 12, 10, 9, 11, 10, 10, 9, 12, 10]

    def test_in_contract_volume_and_freshness_are_healthy(self) -> None:
        result = evaluate(current_volume=10, history=self.history, lag=5, sla=30)

        self.assertEqual(result["status"], "healthy")
        self.assertEqual(result["alerts"], [])

    def test_late_data_emits_sla_alert(self) -> None:
        result = evaluate(current_volume=10, history=self.history, lag=90, sla=30)

        self.assertEqual(result["alerts"][0]["type"], "freshness_sla_breach")

    def test_unusual_volume_emits_anomaly_alert(self) -> None:
        result = evaluate(current_volume=40, history=self.history, lag=5, sla=30)

        self.assertEqual(result["alerts"][0]["type"], "volume_anomaly")

    def test_zero_mad_flags_any_deviation(self) -> None:
        result = evaluate(current_volume=11, history=[10, 10, 10], lag=0, sla=30)

        self.assertEqual(result["alerts"][0]["type"], "volume_anomaly")
        self.assertIsNone(result["alerts"][0]["robust_z_score"])


if __name__ == "__main__":
    unittest.main()
