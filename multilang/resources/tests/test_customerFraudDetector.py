from unittest import TestCase
from unittest.mock import patch, call
from tests.storm_mock import Tuple
from customer_fraud_detector import CustomerFraudDetector
import numpy as np


class TestCustomerFraudDetector(TestCase):

    def test_process_customer_is_added_to_dict_if_never_processed_accountId_before(self):
        # arrange
        detector = CustomerFraudDetector()
        tuples = []
        for i in range(10):
            tup = Tuple()
            tup.values = [str(i), 'A', 5]
            tuples.append(tup)

        # preliminary assert
        for i in range(10):
            self.assertFalse(str(i) in detector.entities.keys())

        # act
        for tup in tuples:
            detector.process(tup)

        # assert
        self.assertTrue(10, len(detector.entities.keys()))
        for i in range(10):
            self.assertTrue(str(i) in detector.entities.keys())

    def test_process_customer_transactions_are_added_to_correct_customer_transaction_array(self):
        # arrange
        detector = CustomerFraudDetector()
        tuples = []
        for i in range(10):
            tup = Tuple()
            tup.values = [str(i % 2), 'A', i]
            tuples.append(tup)

        std_customer_0 = np.array([0, 2, 4, 6, 8]).std()
        std_customer_1 = np.array([1, 3, 5, 7, 9]).std()
        # preliminary assert
        for i in range(2):
            self.assertFalse(str(i) in detector.entities.keys())

        # act
        for tup in tuples:
            detector.process(tup)

        # assert
        self.assertTrue(2, len(detector.entities.keys()))
        np.testing.assert_almost_equal(std_customer_0, detector.entities['0'].std)
        np.testing.assert_almost_equal(std_customer_1, detector.entities['1'].std)

    @patch('storm.emitBolt')
    def test_process_storm_emitBolt_is_called_with_correct_parameters(self, emitBolt):
        # arrange
        detector = CustomerFraudDetector()
        tuples = []
        calls = []
        for i in range(10):
            tup = Tuple()
            tup.values = [str(i % 2), 'A', 10]
            tuples.append(tup)
            calls.append(call(tup.values + [True], stream='customerGlobalStream'))
            calls.append(call([str(i % 2), int(i / 2) + 1, 10, 10.0, 0.0, 10, 10, 1.0], stream='customerStatsStream'))

        anomaly_tup, not_anomaly_tup = Tuple(), Tuple()
        anomaly_tup.values, not_anomaly_tup.values = ['0', 'A', 101], ['1', 'A', 12]

        calls.append(call(anomaly_tup.values + [type(detector).__name__], stream='customerAlertStream'))
        calls.append(call(anomaly_tup.values + [False], stream='customerGlobalStream'))
        calls.append(
            call(['0', 6, 101, 25.166666666666664, 33.91369765874681, 10, 101, 10.1], stream='customerStatsStream')
        )

        calls.append(call(not_anomaly_tup.values + [False], stream='customerGlobalStream'))
        calls.append(call(['1', 6, 12, 10.333333333333334, 0.7453559924999298, 10, 12, 1.2], stream='customerStatsStream'))

        # preliminary assert
        for i in range(2):
            self.assertFalse(str(i) in detector.entities.keys())

        # act
        for tup in tuples:
            detector.process(tup)

        detector.process(anomaly_tup)
        detector.process(not_anomaly_tup)

        # assert
        emitBolt.assert_has_calls(calls, any_order=False)
        self.assertTrue(2, len(detector.entities.keys()))


