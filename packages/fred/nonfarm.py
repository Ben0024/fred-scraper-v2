import pandas as pd


class NonfarmProcessor:
    def __init__(self, df_nonfarm_all_record: pd.DataFrame):
        self.df_nonfarm_all_record: pd.DataFrame = df_nonfarm_all_record.drop(
            df_nonfarm_all_record[
                df_nonfarm_all_record["realtime_start"] == "2020-05-08"
            ].index
        ).reset_index(drop=True)

        self.df_nonfarm_list: list[pd.DataFrame] = [
            (
                self.df_nonfarm_all_record.groupby("date", as_index=False)
                .nth(0)
                .reset_index(drop=True)
            ),
            (
                self.df_nonfarm_all_record.groupby("date", as_index=False)
                .nth(1)
                .reset_index(drop=True)
            ),
            (
                self.df_nonfarm_all_record.groupby("date", as_index=False)
                .nth(2)
                .reset_index(drop=True)
            ),
        ]

    def diff_1st_release(self):
        nonfarm_1st = self.df_nonfarm_list[0].copy()
        nonfarm_2nd = self.df_nonfarm_list[1].copy()

        nonfarm_1st = nonfarm_1st.rename(columns={"value": "value_1st"})
        nonfarm_2nd = nonfarm_2nd.rename(columns={"value": "value_2nd"})

        nonfarm_2nd["date"] = nonfarm_2nd["date"] + pd.DateOffset(months=1)

        nonfarm_merged = nonfarm_1st.merge(nonfarm_2nd, on=["date"])
        nonfarm_merged["diff"] = (
            nonfarm_merged["value_1st"] - nonfarm_merged["value_2nd"]
        )

        result = nonfarm_merged[["date", "diff"]]

        return result["date"], result["diff"]

    def diff_2nd_release(self):
        nonfarm_2nd = self.df_nonfarm_list[1].copy()
        nonfarm_3rd = self.df_nonfarm_list[2].copy()

        nonfarm_2nd = nonfarm_2nd.rename(columns={"value": "value_2nd"})
        nonfarm_3rd = nonfarm_3rd.rename(columns={"value": "value_3rd"})

        nonfarm_3rd["date"] = nonfarm_3rd["date"] + pd.DateOffset(months=1)

        nonfarm_merged = nonfarm_2nd.merge(nonfarm_3rd, on=["date"])
        nonfarm_merged["diff"] = (
            nonfarm_merged["value_2nd"] - nonfarm_merged["value_3rd"]
        )

        result = nonfarm_merged[["date", "diff"]]

        return result["date"], result["diff"]

    def diff_3rd_release(self):
        df_nonfarm_all_record = self.df_nonfarm_all_record.copy()

        nonfarm_3rd = self.df_nonfarm_list[2].copy()

        nonfarm_3rd = nonfarm_3rd.rename(columns={"value": "value_3rd"})

        nonfarm_3rd["diff"] = nonfarm_3rd["value_3rd"].diff()

        df_nonfarm_all_record["date"] = df_nonfarm_all_record[
            "date"
        ] + pd.DateOffset(months=1)
        nonfarm_3rd_temp = nonfarm_3rd[
            ["realtime_start", "date", "value_3rd"]
        ].copy()
        nonfarm_merged = nonfarm_3rd_temp.merge(
            df_nonfarm_all_record, on=["realtime_start", "date"]
        )
        nonfarm_merged["diff"] = (
            nonfarm_merged["value_3rd"] - nonfarm_merged["value"]
        )

        for i in range(0, len(nonfarm_merged)):
            date_index = nonfarm_3rd[
                nonfarm_3rd["date"] == nonfarm_merged["date"].iloc[i]
            ].index
            new_value = nonfarm_merged["diff"].iloc[i]

            nonfarm_3rd.loc[date_index, "diff"] = new_value

        result = nonfarm_3rd[["date", "diff"]]

        return result["date"], result["diff"]
