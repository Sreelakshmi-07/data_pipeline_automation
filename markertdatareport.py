from time import sleep
from datetime import datetime
import pygsheets
import logging
from pymongo import MongoClient

# Set timezone and date
today_date = datetime.now().astimezone(pytz.timezone("Asia/Kolkata"))
delivery_day = today_date.strftime("%Y_%m_%d")

# Constants for collections and sheet names
MASTERDB_COLLECTION = "database_urls"
SHEET_NAME = "Global Market Data Report"

class MarketDataReport:
    # country and competitor data
    COUNTRY_DATA = {
        "1": ("Fantasia", "FN", ["martina", "grocers", "dailyplus", "hyperworld"]),
        "2": ("Atlantis", "AT", ["supermart", "freshzone", "globalshop", "healthystore"]),
        "3": ("Zytheria", "ZY", ["maximart", "localplus", "healthstore"]),
    }

    FREQUENCY_OPTIONS = {
        "1": "Daily",
        "2": "Weekly",
        "3": "Monthly"
    }

    def __init__(self):
        self.need_to_continue = "Y"
        self.client = MongoClient("mongodb://localhost:27017")  # Local MongoDB connection

    def start(self):
        while True:
            if self.need_to_continue.upper() != "Y":
                break
            
            logging.warning(self.COUNTRY_DATA)
            country_index = input("Enter the Country : ")
            if country_index not in self.COUNTRY_DATA:
                print("Invalid country choice.")
                break
            country_info = self.COUNTRY_DATA[country_index]
            country, country_code, competitor_list = country_info
            
            logging.warning(self.FREQUENCY_OPTIONS)
            frequency_index = input("Enter the Frequency : ")
            if frequency_index not in self.FREQUENCY_OPTIONS:
                print("Invalid frequency choice.")
                break
            frequency = self.FREQUENCY_OPTIONS[frequency_index]

            #Connecting to database
            db_name = f"marketdata_{country.lower()}_{frequency.lower()}_{delivery_day}"
            db = self.client[db_name]
            date = f"2024-{db_name.split('_2024')[-1].replace('_','-')}"
            
            # Exit if competitors list is empty
            if not competitor_list:
                print(f"No competitors listed for {country}.")
                break

            # Process data from MongoDB and update the Google Sheet
            self.db_info(db, competitor_list, date, country_code)

    def db_info(self, db, competitor_list, date, country_code):
        query = {'site': {'$in': competitor_list}}
        documents = db[MASTERDB_COLLECTION].find(query)
        info = {doc['site']: doc['info'] for doc in documents}
        
        self.sheet_update(info, date, country_code, db)

    def sheet_update(self, info, date, country_code, db):
        gc = pygsheets.authorize(client_secret="sheet-automation.json")
        sheet = gc.open(SHEET_NAME)
        
        for site_name, doc_info in info.items():
            col_name = f"{site_name}_data_records"
            col_count = db[col_name].estimated_document_count()
            data_row = [
                date,                                 # Record Date
                col_count,                            # Total Products Available
                doc_info.get("200", ""),              # URLs with Status 200
                doc_info.get("total_items_count", ""),# Total Items Count in Master Data
                doc_info.get("new_entries", ""),      # New Entries Count
                doc_info.get("404", ""),              # URLs with Status 404
                doc_info.get("missing_items", "")     # Missing Items Count
            ]
            
            worksheet_title = f"{site_name.capitalize()}_{country_code}"
            wk_sheet = sheet.worksheet_by_title(worksheet_title)
            
            if not wk_sheet:
                wk_sheet = sheet.add_worksheet(worksheet_title, rows=250, cols=20)
                headers = ["DATE", "TOTAL PRODUCTS", "URL (200)", "MASTER DATA COUNT", "NEW ENTRIES", "URL (404)", "MISSING ITEMS"]
                wk_sheet.update_values(crange="A1", values=[headers])
            
            if wk_sheet.get_value(addr="A3"):
                wk_sheet.insert_rows(2, number=1)
            
            wk_sheet.update_values(crange="A3", values=[data_row])

if __name__ == "__main__":
    report_generator = MarketDataReport()
    report_generator.start()
