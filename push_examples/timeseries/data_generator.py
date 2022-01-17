class DataGeneratorTask:
    def __init__(self, _ts=None):
        from boot_common import repl_ts
        self.ts = _ts or repl_ts

    def apply(self, control):
        print(f"Data Generator starting!")

        try:
            import datetime
            from datetime import timezone
            import random
            import time

            while control.running:
                symbols = ['MSFT', 'TWTR', 'EBAY', 'CVX', 'W', 'GOOG', 'FB']
                now = datetime.datetime.now(timezone.utc)
                d = [random.uniform(10, 100) for _ in symbols]
                self.ts.append(now, symbols, d)
                time.sleep(1)
        except Exception as e:
            print(e)
        finally:
            print(f"Data Generator stopping!")
