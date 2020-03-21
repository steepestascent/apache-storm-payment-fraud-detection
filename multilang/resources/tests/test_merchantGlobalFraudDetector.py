from unittest import TestCase
from merchant_global_fraud_detector import MerchantGlobalFraudDetector


class TestMerchantGlobalFraudDetector(TestCase):
    def test_get_stream(self):
        detector = MerchantGlobalFraudDetector()
        self.assertEqual('merchantGlobalAlertStream', detector.get_stream())
