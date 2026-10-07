import unittest

from polar_flight_ops.acceptance import run


class FlightOpsAcceptanceTest(unittest.TestCase):
    def test_offline_acceptance(self):
        result = run()
        self.assertEqual("ok", result["status"])
        self.assertTrue(result["audit_valid"])
        self.assertEqual(1, result["sealed_version"])
        self.assertEqual(3, result["cancelled_by_weather"])
        self.assertTrue(result["duplicate_alert"])
        self.assertEqual(1, result["waitlist"])


if __name__ == "__main__":
    unittest.main()
