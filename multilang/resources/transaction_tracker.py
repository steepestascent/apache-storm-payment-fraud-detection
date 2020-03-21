import numpy as np


class TransactionTracker:

    def __init__(self):
        self.number_of_transactions = 0
        self.mean = 0.0
        self.M2 = 0.0
        self.std = 0.0
        self.minimum = 0.0
        self.maximum = 0.0
        self.current_value_and_historical_minimum_ratio = 1.0

    def set_current_value_and_historical_minimum_ratio(self, transaction_amount):
        if self.minimum > 0:
            self.current_value_and_historical_minimum_ratio = transaction_amount / self.minimum

    def update_minimum(self, transaction_amount):
        if self.number_of_transactions == 1:
            self.minimum = transaction_amount
        elif self.minimum > transaction_amount:
            self.minimum = transaction_amount

    def update_maximum(self, transaction_amount):
        if self.number_of_transactions == 1:
            self.maximum = transaction_amount
        elif transaction_amount > self.maximum:
            self.maximum = transaction_amount

    def zscore(self, value):
        zscore = 0
        if self.number_of_transactions > 1:
            if self.std > 0:
                zscore = (value - self.mean) / self.std
        return zscore

    def update_statistics(self, transaction_amount):
        self.number_of_transactions += 1
        delta = transaction_amount - self.mean
        self.mean += delta / self.number_of_transactions
        delta2 = transaction_amount - self.mean
        self.M2 += delta * delta2
        self.update_minimum(transaction_amount)
        self.update_maximum(transaction_amount)
        if self.number_of_transactions < 2:
            return
        self.std = np.sqrt(self.M2 / self.number_of_transactions)

