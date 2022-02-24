import pandas as pd


class Tools(object):

    def __init__(self):
        self.default_col = ['ts', 'open', 'high', 'low', 'close', 'vol']
        self.ftx_col = ['startTime', 'volume']

    def records_to_df(self, records):
        col = self.default_col
        records = self.records_cut(records)
        df = pd.DataFrame(
            data=records, index=list(range(len(records))), columns=col
        )
        for i in range(len(col)):
            if i == 0:
                continue
            df[col[i]] = df[col[i]].apply(pd.to_numeric, errors='coerce')
        return df

    def records_cut(self, records):
        col = [
            self.ftx_col[0], self.default_col[1], self.default_col[2],
            self.default_col[3], self.default_col[4], self.ftx_col[1]
        ]
        if isinstance(records[0], list):
            for i in range(len(records)):
                records[i] = records[i][:6]
        elif isinstance(records[0], dict):
            import time

            def utc_to_timestamp(utc_time_str):
                time_arr = str(utc_time_str).split('T')
                time_str = time_arr[0] + ' ' + time_arr[1]
                time_struct = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                return int(time.mktime(time_struct) * 1000)

            for i in range(len(records)):
                times = utc_to_timestamp(str(records[i][col[0]]).split('+')[0])
                records[i] = [
                    times, records[i][col[1]], records[i][col[2]],
                    records[i][col[3]], records[i][col[4]], records[i][col[5]]
                ]

        return records