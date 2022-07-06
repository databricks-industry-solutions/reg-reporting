import unittest

import pandas as pd
import json
import datetime


class UdfTest(unittest.TestCase):

    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 100)
    pd.set_option('display.colheader_justify', 'left')
    pd.set_option('display.precision', 5)

    def test_return_df(self):
        from utils.dlt_utils import events_to_dataframe
        jobject = {
            'flow_progress': {
                'data_quality': {
                    'expectations': [
                        {
                            'dataset': 'dataset1',
                            'name': 'is not null',
                            'passed_records': 100,
                            'failed_records': 0
                        },
                        {
                            'dataset': 'dataset1',
                            'name': 'is not empty',
                            'passed_records': 0,
                            'failed_records': 100
                        }
                    ]
                }
            }
        }

        jstring = json.dumps(jobject)
        now = datetime.datetime.now()
        pdf = pd.DataFrame([
            [now, 'flow_progress', jstring]
        ], columns=['timestamp', 'event_type', 'details'])

        events_pdf = events_to_dataframe(pdf)
        self.assertEqual(events_pdf.shape[0], 2)
        record = events_pdf.iloc[0]
        self.assertEqual(record.timestamp, now)
        self.assertEqual(record.step, 'dataset1')
        self.assertEqual(record.expectation, 'is not null')
        self.assertEqual(record.passed, 100)
        self.assertEqual(record.failed, 0)

        record = events_pdf.iloc[1]
        self.assertEqual(record.timestamp, now)
        self.assertEqual(record.step, 'dataset1')
        self.assertEqual(record.expectation, 'is not empty')
        self.assertEqual(record.passed, 0)
        self.assertEqual(record.failed, 100)
        print(events_pdf)


if __name__ == '__main__':
    unittest.main()
