from unittest import TestCase
from unittest.mock import patch
from tests.storm_mock import Tuple
from global_fraud_detector import GlobalDetector


class TestGlobalDetector(TestCase):

    @patch('storm.emitBolt')
    def test_process_should_record_customer_transaction_and_alert_on_anomaly(self, emitBolt):
        # arrange
        detector = GlobalDetector()
        tuples = []

        for i in range(10):
            tup = Tuple()
            tup.values = [str(i), 'A', 5, True]
            tuples.append(tup)

        different_value_tup = Tuple()
        different_value_tup.values = ['1', 'A', 6, True]
        anomaly_tup = Tuple()
        anomaly_tup. values = ['0', 'A', 25, True]

        # act
        for tup in tuples:
            detector.process(tup)

        detector.process(different_value_tup)
        detector.process(anomaly_tup)

        # assert
        self.assertEqual(12, detector.entity.number_of_transactions)
        self.assertEqual(6.75, detector.entity.mean)
        self.assertEqual(5.51, round(detector.entity.std, 2))
        self.assertEqual(1, emitBolt.call_count)
