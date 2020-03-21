from unittest import TestCase
from unittest.mock import patch, call
from tests.storm_mock import Tuple
from merchant_fraud_detector import MerchantFraudDetector
import numpy as np


class TestMerchantFraudDetector(TestCase):

    def test_process_merchant_is_added_to_dict_if_never_processed_accountId_before(self):
        # arrange
        detector = MerchantFraudDetector()
        tuples = []
        for idx, merchant in enumerate([chr(a) for a in range(65, 91)]):
            tup = Tuple()
            tup.values = [idx, merchant, 5]
            tuples.append(tup)

        # preliminary assert
        for m in [chr(a) for a in range(65, 91)]:
            self.assertFalse(m in detector.entities.keys())

        # act
        for tup in tuples:
            detector.process(tup)

        # assert
        self.assertTrue(26, len(detector.entities.keys()))
        for m in [chr(a) for a in range(65, 91)]:
            self.assertTrue(m in detector.entities.keys())

    def test_process_merchant_transactions_are_added_to_correct_customer_transaction_array(self):
        # arrange
        detector = MerchantFraudDetector()
        tuples = []
        merchants = ['A', 'B']
        for i in range(10):
            tup = Tuple()
            tup.values = [str(i), merchants[i % 2], i]
            tuples.append(tup)

        std_merchant_0 = np.array([0, 2, 4, 6, 8]).std()
        std_merchant_1 = np.array([1, 3, 5, 7, 9]).std()
        # preliminary assert
        for m in merchants:
            self.assertFalse(m in detector.entities.keys())

        # act
        for tup in tuples:
            detector.process(tup)

        # assert
        self.assertTrue(2, len(detector.entities.keys()))
        np.testing.assert_almost_equal(std_merchant_0, detector.entities['A'].std)
        np.testing.assert_almost_equal(std_merchant_1, detector.entities['B'].std)

    @patch('storm.emitBolt')
    def test_process_storm_emitBolt_is_called_with_correct_parameters(self, emitBolt):
        # arrange
        detector = MerchantFraudDetector()
        tuples = []
        calls = []
        merchants = ['A', 'B']
        for i in range(10):
            tup = Tuple()
            tup.values = [str(i), merchants[i % 2], 10]
            tuples.append(tup)
            calls.append(call(tup.values + [True], stream='merchantGlobalStream'))
            calls.append(call([merchants[i % 2], str(i), int(i / 2) + 1, 10, 10.0, 0.0, 10, 10, 1.0], stream='merchantStatsStream'))

        anomaly_tup, not_anomaly_tup = Tuple(), Tuple()
        anomaly_tup.values, not_anomaly_tup.values = ['0', merchants[0], 101], ['1', merchants[1], 12]

        calls.append(call(anomaly_tup.values + [type(detector).__name__], stream='merchantAlertStream'))
        calls.append(call(anomaly_tup.values + [False], stream='merchantGlobalStream'))
        calls.append(call(['A', '0', 6, 101, 25.166666666666664, 33.91369765874681, 10, 101, 10.1], stream='merchantStatsStream'))

        calls.append(call(not_anomaly_tup.values + [False], stream='merchantGlobalStream'))
        calls.append(call(['B', '1', 6, 12, 10.333333333333334, 0.7453559924999298, 10, 12, 1.2], stream='merchantStatsStream'))

        # preliminary assert
        for m in merchants:
            self.assertFalse(m in detector.entities.keys())

        # act
        for tup in tuples:
            detector.process(tup)

        detector.process(anomaly_tup)
        detector.process(not_anomaly_tup)

        # assert
        emitBolt.assert_has_calls(calls, any_order=False)
        self.assertTrue(2, len(detector.entities.keys()))
