import storm
from fraud_detector import FraudDetector


class CustomerFraudDetector(FraudDetector):

    def get_entity_id(self, tup):
        account_id, merchant_id, transaction_amount = tup.values
        return account_id

    def emit_alert(self, tup, entity):
        account_id, merchant_id, transaction_amount = tup.values
        if entity.number_of_transactions >= 5 and entity.zscore(transaction_amount) >= 3:
            storm.emitBolt([account_id, merchant_id, transaction_amount, self.bolt_name], stream='customerAlertStream')
        elif entity.current_value_and_historical_minimum_ratio > 10:
            storm.emitBolt([account_id, merchant_id, transaction_amount, self.bolt_name], stream='customerAlertStream')

    def emit_global(self, tup, entity):
        account_id, merchant_id, transaction_amount = tup.values

        allow_global_detector_to_alert = False

        if entity.number_of_transactions < 5:
            allow_global_detector_to_alert = True

        storm.emitBolt([account_id, merchant_id, transaction_amount, allow_global_detector_to_alert],
                       stream='customerGlobalStream')

    def emit_stats(self, tup, entity):
        account_id, merchant_id, transaction_amount = tup.values
        storm.emitBolt(
            [
                account_id,
                entity.number_of_transactions,
                transaction_amount,
                entity.mean,
                entity.std,
                entity.minimum,
                entity.maximum,
                entity.current_value_and_historical_minimum_ratio
            ],
            stream='customerStatsStream')


CustomerFraudDetector().run()
