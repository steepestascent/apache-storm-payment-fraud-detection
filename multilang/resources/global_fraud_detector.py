import storm
from transaction_tracker import TransactionTracker
import abc


class GlobalDetector(storm.BasicBolt):
    MIN_NUMBER_OF_TRANSACTIONS = 5

    def __init__(self):
        # only one entity to keep track of global
        # Storm topology will make sure this is the only Bolt
        self.entity = TransactionTracker()
        self.bolt_name = type(self).__name__

    @abc.abstractmethod
    def get_stream(self):
        return

    def process(self, tup):
        account_id, merchant_id, transaction_amount, should_alert = tup.values

        if should_alert:
            if (self.entity.number_of_transactions > self.MIN_NUMBER_OF_TRANSACTIONS and
                    self.entity.zscore(transaction_amount) >= 3):
                storm.emitBolt([account_id, merchant_id, transaction_amount, self.bolt_name],
                               stream=self.get_stream())

        self.entity.update_statistics(transaction_amount)
