import pandas as pd
from fredapi import Fred

from .nonfarm import NonfarmProcessor


class FredHandler:
    def __init__(self, api_key):
        self.fred = Fred(api_key=api_key)
        self.nonfarm_processor = None

    def get_data(self, name: str, index_name: str, unit: str):
        # Special Case : Nonfarm Payroll
        if self.nonfarm_processor is None and index_name == "PAYEMS":
            all_record = self.fred.get_series_all_releases(index_name)
            self.nonfarm_processor = NonfarmProcessor(all_record)

        # Get result
        if name == "gdp_now_first":
            # Special Case : The first released GDPNow value
            all_record = self.fred.get_series_all_releases(index_name)
            all_record = all_record.sort_values(by=["realtime_start", "date"])
            first_release = all_record.groupby(all_record["date"]).head(1)

            date, val = first_release["date"], first_release["value"]

        elif self.nonfarm_processor and name == "nonfarm_1st":
            # Special Case : Nonfarm Payroll
            date, val = self.nonfarm_processor.diff_1st_release()

        elif self.nonfarm_processor and name == "nonfarm_2nd":
            # Special Case : Nonfarm Payroll
            date, val = self.nonfarm_processor.diff_2nd_release()

        elif self.nonfarm_processor and name == "nonfarm_3rd":
            # Special Case : Nonfarm Payroll
            date, val = self.nonfarm_processor.diff_3rd_release()

        else:
            data = self.fred.get_series(index_name, units=unit)
            date, val = data.index, data.values

        # to utc timestamp
        df = pd.DataFrame()
        df["timestamp"] = pd.to_datetime(date).astype(int) / 10**9
        df["value"] = val
        df = df.dropna().reset_index(drop=True)

        return df
