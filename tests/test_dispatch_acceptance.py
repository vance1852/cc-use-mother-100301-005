import unittest

from polar_station_foundation.dispatch_acceptance import run


class DispatchAcceptanceTest(unittest.TestCase):
    def test_offline_acceptance(self):
        result = run()
        self.assertEqual("ok", result["status"])
        self.assertTrue(result["audit_valid"])
        self.assertTrue(result["dual_sealed"])
        self.assertTrue(result["fault_replanned_once"])
        self.assertTrue(result["duplicate_fault_replayed"])
        self.assertTrue(result["duplicate_fault_no_effects"])
        self.assertTrue(result["departed_commitment_kept"])
        self.assertEqual("completed", result["final_mission_state"])
        self.assertTrue(result["medevac_preempted_low"])
        self.assertTrue(result["medevac_plan_pending_approval"])
        self.assertGreater(result["recovered_held_leases"], 0)
        self.assertGreaterEqual(result["recovered_pending_plans"], 1)
        self.assertTrue(result["waitlist_preserved_order"])


if __name__ == "__main__":
    unittest.main()
