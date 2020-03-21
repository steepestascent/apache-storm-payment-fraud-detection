from unittest import TestCase
from transaction_tracker import TransactionTracker
import numpy as np


class TestTransactionTracker(TestCase):

    def test_update_statistics_increments_Number_of_transactions_by_1_after_each_call(self):
        # arrange
        tracker = TransactionTracker()

        # preliminary assert
        self.assertEqual(tracker.number_of_transactions, 0)

        # act and assert
        for i in range(1, 10):
            tracker.update_statistics(10 + i)
        # assert in loop
            self.assertEqual(tracker.number_of_transactions, i)

    def test_zscore_should_return_accurate_results(self):
        # arrange
        tracker = TransactionTracker()

        for i in range(11):
            tracker.update_statistics(10 + i)

        # act
        zscore_100 = tracker.zscore(100)
        zscore_25 = tracker.zscore(25)

        # assert
        self.assertEqual(round(zscore_100, 2), 26.88)
        self.assertEqual(round(zscore_25, 2), 3.16)

    def test_zscore_should_return_0_if_Number_of_transactions_is_0(self):
        # arrange
        tracker = TransactionTracker()

        # act
        zscore_100 = tracker.zscore(100)
        zscore_25 = tracker.zscore(25)

        # assert
        self.assertEqual(round(zscore_100, 2), 0)
        self.assertEqual(round(zscore_25, 2), 0)

    def test_minimum_value_is_updated_after_each_transaction(self):
        # arrange
        tracker = TransactionTracker()
        minimum = 1

        # act
        for i in range(minimum, 10):
            tracker.update_statistics(i)

        # assert in for loop
            self.assertEqual(minimum, tracker.minimum)

    def test_maximum_value_is_updated_after_each_transaction(self):
        # arrange
        tracker = TransactionTracker()
        minimum = 1

        # act
        for i in range(minimum, 10):
            tracker.update_statistics(i)
        # assert in for loop
            self.assertEqual(i, tracker.maximum)

    def test_current_transaction_value_and_historical_minimum_is_updated_after_each_transaction(self):
        # arrange
        tracker = TransactionTracker()
        minimum = 1
        ratios = [r / minimum for r in range(1, 10)]

        # act
        for idx, v in enumerate(range(minimum, 10)):
            tracker.set_current_value_and_historical_minimum_ratio(v)
            tracker.update_statistics(v)
        # assert in for loop
            self.assertEqual(ratios[idx], tracker.current_value_and_historical_minimum_ratio)

    def test_update_statistics_should_not_update_std_if_count_is_less_than_2(self):
        # arrange
        tracker = TransactionTracker()
        transaction_amount = 10

        # act
        tracker.update_statistics(transaction_amount)

        # assert
        self.assertEqual(0, tracker.std)

    def test_update_statistics_should_update_mean_to_current_transaction_if_first_transaction(self):
        # arrange
        tracker = TransactionTracker()
        transaction_amount = 10

        # act
        tracker.update_statistics(transaction_amount)

        # assert
        self.assertEqual(10, tracker.mean)

    def test_update_statistics_should_increment_number_of_transactions_by_1_for_each_transaction(self):
        # arrange
        tracker = TransactionTracker()

        # act
        for i in range(10):
            tracker.update_statistics(i)

        # assert
        self.assertEqual(10, tracker.number_of_transactions)

    def test_update_statistics_should_update_mean_after_each_transaction(self):
        # arrange
        tracker = TransactionTracker()
        transaction_amounts = np.array([i for i in range(10)])

        # act
        for i in range(10):
            tracker.update_statistics(i)
        # assert in for loop
            self.assertEqual(transaction_amounts[:i + 1].mean(), tracker.mean)

    def test_update_statistics_should_update_M2_after_each_transaction(self):
        # arrange
        tracker = TransactionTracker()
        M2 = [0.0, 0.5, 2.0, 5.0, 10.0, 17.5, 28.0, 42.0, 60.0, 82.5]

        # act
        for i in range(10):
            tracker.update_statistics(i)
            self.assertEqual(M2[i], tracker.M2)

    def test_update_statistics_should_update_std_after_each_transaction(self):
        # arrange
        tracker = TransactionTracker()
        transaction_amounts = np.array([i for i in range(10)])
        test_stds = [transaction_amounts[:i+1].std() for i in range(10)]
        # making explicit std should be 0 when number_of_transactions < 2
        test_stds[0] = 0
        # act
        for i in range(10):
            tracker.update_statistics(i)
            self.assertEqual(test_stds[i], tracker.std)

    def test_update_statistics_update_minimum(self):
        # arrange
        tracker = TransactionTracker()
        minimum = 1

        # act
        for i in range(minimum, 10):
            tracker.update_statistics(i)

        # assert
        tracker.update_statistics(0.5)
        self.assertEqual(0.5, tracker.minimum)
        tracker.update_statistics(0.01)
        self.assertEqual(0.01, tracker.minimum)

    def test_update_statistics_updates_maximum(self):
        # arrange
        tracker = TransactionTracker()
        minimum = 1
        # act
        for i in range(minimum, 10):
            tracker.update_statistics(i)

        # assert
        self.assertEqual(9, tracker.maximum)
        tracker.update_statistics(20)
        self.assertEqual(20, tracker.maximum)
        tracker.update_statistics(100)
        self.assertEqual(100, tracker.maximum)
        tracker.update_statistics(50)
        self.assertEqual(100, tracker.maximum)
