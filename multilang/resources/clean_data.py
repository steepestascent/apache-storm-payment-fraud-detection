import storm


class CleanData(storm.BasicBolt):
    def process(self, tup):
        features = tup.values[0].split(",")
        transaction_amount = None

        if len(features) < 3:
            return

        try:
            transaction_amount = float(features[2].strip())
        except ValueError:
            try:
                if len(features) > 3:
                    transaction_amount = float(features[3].strip())
                else:
                    return
            except ValueError:
                return

        merchant_id = features[1]
        if merchant_id is None or len(merchant_id) < 1:
            merchant_id = 'unknown'

        account_id = features[0].strip()
        if transaction_amount is not None:
            storm.emitBolt([account_id, merchant_id, transaction_amount], stream='customerDetectorStream')
            storm.emitBolt([account_id, merchant_id, transaction_amount], stream='merchantDetectorStream')


CleanData().run()
