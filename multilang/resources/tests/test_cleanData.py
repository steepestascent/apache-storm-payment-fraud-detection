from unittest import TestCase
from unittest.mock import patch, call
from tests.storm_mock import Tuple
from clean_data import CleanData


class TestCleanDataBolt(TestCase):
    @patch('storm.emitBolt')
    def test_process_should_emit_to_customer_and_merchant_bolts(self, emitBolt):
        # arrange
        clean = CleanData()
        tup = Tuple()
        tup.values = ['1,amazon,45.44']
        calls = [
            call(['1', 'amazon', 45.44], stream='customerDetectorStream'),
            call(['1', 'amazon', 45.44], stream='merchantDetectorStream')
        ]

        # act
        clean.process(tup)

        # assert
        emitBolt.assert_has_calls(calls, any_order=False)

    @patch('storm.emitBolt')
    def test_process_should_not_emit_to_customer_and_merchant_bolts_if_transaction_amount_is_None(self, emitBolt):
        # arrange
        clean = CleanData()
        tup = Tuple()
        tup.values = ['1,amazon,']

        # act
        clean.process(tup)

        # assert
        self.assertFalse(emitBolt.called)

    @patch('storm.emitBolt')
    def test_process_should_emit_to_customer_and_merchant_bolts_if_transaction_amount_is_in_4th_index(self, emitBolt):
        # arrange
        clean = CleanData()
        tup = Tuple()
        tup.values = ['1,amazon,prime,45.44']
        calls = [
            call(['1', 'amazon', 45.44], stream='customerDetectorStream'),
            call(['1', 'amazon', 45.44], stream='merchantDetectorStream')
        ]

        # act
        clean.process(tup)

        # assert
        emitBolt.assert_has_calls(calls, any_order=False)

    @patch('storm.emitBolt')
    def test_process_should_not_emit_to_customer_and_merchant_bolts_if_transaction_amount_is_string(self, emitBolt):
        # arrange
        clean = CleanData()
        tup = Tuple()
        tup.values = ['1,amazon,prime']

        # act
        clean.process(tup)

        # assert
        self.assertFalse(emitBolt.called)

    @patch('storm.emitBolt')
    def test_process_should_not_emit_to_customer_and_merchant_bolts_if_both_possible_transaction_amount_is_string(self, emitBolt):
        # arrange
        clean = CleanData()
        tup = Tuple()
        tup.values = ['1,amazon,prime,sale']

        # act
        clean.process(tup)

        # assert
        self.assertFalse(emitBolt.called)