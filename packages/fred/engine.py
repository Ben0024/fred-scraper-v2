import logging
import multiprocessing
import os
import time

from .handler import FredHandler


def get_fred_index_dict():
    # Units Values
    # "lin" - Levels (No transformation)
    # "chg" - Change
    # "ch1" - Change from 1 year ago
    # "pch" - Percent change
    # "pc1" - Percent change from 1 year ago
    # "pca" - Compounded annual rate of change
    # "cch" - Continuously compounded rate of change
    # "cca" - Continuously compounded annual rate of change
    # "log" - Natural log

    return {
        # CPI Details
        "cpi_food": {"name": "CPIUFDNS", "unit": "pc1"},
        "cpi_food_at_home": {"name": "CUUR0000SAF11", "unit": "pc1"},
        "cpi_food_away_from_home": {"name": "CUUR0000SEFV", "unit": "pc1"},
        "cpi_energy": {"name": "CPIENGNS", "unit": "pc1"},
        "cpi_energy_commodities": {"name": "CUUR0000SACE", "unit": "pc1"},
        "cpi_energy_services": {"name": "CUUR0000SEHF", "unit": "pc1"},
        "cpi_all_less_food_energy": {"name": "CPILFENS", "unit": "pc1"},
        "cpi_commodities_less_food_energy": {
            "name": "CUUR0000SACL1E",
            "unit": "pc1",
        },
        "cpi_services_less_food_energy": {
            "name": "CUUR0000SASLE",
            "unit": "pc1",
        },
        # GDP
        "gdp": {"name": "GDP", "unit": "pc1"},
        "real_gdp": {"name": "GDPC1", "unit": "pc1"},
        "gdp_now": {"name": "GDPNOW", "unit": "lin"},
        "gdp_now_first": {"name": "GDPNOW", "unit": "lin"},
        "nonfarm_original": {"name": "PAYEMS", "unit": "lin"},
        "nonfarm_1st": {"name": "PAYEMS", "unit": "lin"},
        "nonfarm_2nd": {"name": "PAYEMS", "unit": "lin"},
        "nonfarm_3rd": {"name": "PAYEMS", "unit": "lin"},
        # Population
        "population": {"name": "POPTHM", "unit": "lin"},
        "working_age_population": {"name": "LFWA64TTUSM647S", "unit": "lin"},
        # Durable Goods
        "durable_order": {"name": "DGORDER", "unit": "lin"},
        "durable_order_no_defense": {"name": "UDXDNO", "unit": "lin"},
        "durable_order_no_defense_aircraft": {
            "name": "NEWORDER",
            "unit": "lin",
        },
        # Retail Sales
        "retail_sales_no_auto": {"name": "RSFSXMV", "unit": "lin"},
        "retail_sales_only_auto": {"name": "RSAOMV", "unit": "lin"},
        "retail_sales_food": {"name": "RSFSDP", "unit": "lin"},
        "retail_sales_gas": {"name": "RSGASS", "unit": "lin"},
        # U.S. Michigan Consumer Sentiment
        "michigan_ics": {"name": "UMCSENT", "unit": "lin"},
    }


class FredEngine:
    def __init__(self, api_key: str, storage_dir: str, logger: logging.Logger):
        self.storage_dir = storage_dir
        self.logger = logger

        self.fred_handler = FredHandler(api_key=api_key)
        self.index_dict = get_fred_index_dict()

    def once(self):
        for key, value in self.index_dict.items():
            # Crawling Progress
            self.logger.info(f"Crawling Fred: {key}")

            # FRED API crawling
            crawled_data = self.fred_handler.get_data(key, value["name"], value["unit"])

            filepath = os.path.join(self.storage_dir, key, "1M.csv")
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w") as f:
                f.write("timestamp,value\n")
                for _, row in crawled_data.iterrows():
                    f.write("{},{}\n".format(row["timestamp"], row["value"]))

    def start(self):
        current_process = multiprocessing.current_process()

        while current_process.exitcode is None:
            try:
                self.once()
            except Exception as e:
                self.logger.error(e)
            time.sleep(60 * 60 * 24)
