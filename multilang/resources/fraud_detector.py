import storm
from transaction_tracker import TransactionTracker
import abc


class FraudDetector(storm.BasicBolt):

    def __init__(self):
        self.entities = {}
        self.bolt_name = type(self).__name__

    def process(self, tup):
        transaction_amount = tup.values[2]

        entity_id = self.get_entity_id(tup)
        entity = self.get_entity(entity_id)
        entity.set_current_value_and_historical_minimum_ratio(transaction_amount)

        self.emit_alert(tup, entity)
        self.emit_global(tup, entity)
        entity.update_statistics(transaction_amount)
        self.emit_stats(tup, entity)

    def get_entity(self, entity_id):
        entity = None
        if entity_id in self.entities.keys():
            entity = self.entities[entity_id]
        else:
            entity = TransactionTracker()
            self.entities[entity_id] = entity
        return entity

    @abc.abstractmethod
    def emit_alert(self, tup, entity):
        return

    @abc.abstractmethod
    def emit_global(self, tup, entity):
        return

    @abc.abstractmethod
    def emit_stats(self, tup, entity):
        return

    @abc.abstractmethod
    def get_entity_id(self, tup):
        return