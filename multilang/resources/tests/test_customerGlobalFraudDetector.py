from unittest import TestCase
from customer_global_fraud_detector import CustomerGlobalFraudDetector


class TestCustomerGlobalFraudDetector(TestCase):
    def test_get_stream(self):
        detector = CustomerGlobalFraudDetector()
        self.assertEqual('customerGlobalAlertStream', detector.get_stream())
