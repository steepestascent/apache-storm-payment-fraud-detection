from global_fraud_detector import GlobalDetector


class CustomerGlobalFraudDetector(GlobalDetector):
    def get_stream(self):
        return 'customerGlobalAlertStream'


CustomerGlobalFraudDetector().run()