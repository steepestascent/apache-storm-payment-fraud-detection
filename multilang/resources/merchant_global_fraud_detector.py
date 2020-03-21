from global_fraud_detector import GlobalDetector


class MerchantGlobalFraudDetector(GlobalDetector):
    def get_stream(self):
        return 'merchantGlobalAlertStream'

MerchantGlobalFraudDetector().run()