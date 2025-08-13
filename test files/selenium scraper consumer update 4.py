import os
import json
import time
import hashlib
import schedule
import psycopg2
import logging
import threading
import tkinter as tk
from tkinter import ttk
from dotenv import load_dotenv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from tenacity import retry, stop_after_attempt, wait_fixed

load_dotenv()

logging.basicConfig(level=logging.INFO)
scrape_lock = threading.Lock()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Example mapping for 4 real websites
WEBSITE_CONFIGS = [
    {
        "name": "Site1",
        "url": os.getenv("SITE1_URL"),
        "field_mapping": {
            # "Modulo richiesta" field name : website form field id or name
            "Field1": "site1_field1",
            "Field2": "site1_field2",
            # Add all mappings here
        }
    },
    {
        "name": "Site2",
        "url": os.getenv("SITE2_URL"),
        "field_mapping": {
            "Field1": "site2_field1",
            "Field2": "site2_field2",
            # Add all mappings here
        }
    },
    {
        "name": "Site3",
        "url": os.getenv("SITE3_URL"),
        "field_mapping": {
            "Field1": "site3_field1",
            "Field2": "site3_field2",
            # Add all mappings here
        }
    },
    {
        "name": "Site4",
        "url": os.getenv("SITE4_URL"),
        "field_mapping": {
            "Field1": "site4_field1",
            "Field2": "site4_field2",
            # Add all mappings here
        }
    }
]

class DatabaseManager:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        self.cur = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS customer_detail_records (
              protocollo TEXT PRIMARY KEY,
              indirizzo TEXT,
              sesso TEXT,
              ateco TEXT,
              codice_fiscale TEXT,
              legale_rappresentante TEXT,
              telefono TEXT,
              settore TEXT,
              partita_iva TEXT,
              codice_fiscale_legale_rappresentante TEXT,
              email TEXT,
              content_hash TEXT UNIQUE
            )
        """)
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS modulo_richiesta_records (
              protocollo TEXT,
              field_name TEXT,
              field_value TEXT,
              content_hash TEXT UNIQUE
            )
        """)
        self.conn.commit()

    def protocollo_exists(self, protocollo):
        self.cur.execute(
            "SELECT 1 FROM customer_detail_records WHERE protocollo = %s", (protocollo,)
        )
        return self.cur.fetchone() is not None

    def hash_exists_customer(self, content_hash):
        self.cur.execute(
            "SELECT 1 FROM customer_detail_records WHERE content_hash = %s", (content_hash,)
        )
        return self.cur.fetchone() is not None

    def hash_exists_modulo(self, content_hash):
        self.cur.execute(
            "SELECT 1 FROM modulo_richiesta_records WHERE content_hash = %s", (content_hash,)
        )
        return self.cur.fetchone() is not None

    def insert_customer_batch(self, records):
        insert_data = []
        for record, content_hash in records:
            insert_data.append((
                record["protocollo"],
                record["indirizzo"],
                record["sesso"],
                record["ateco"],
                record["codice_fiscale"],
                record["legale_rappresentante"],
                record["telefono"],
                record["settore"],
                record["partita_iva"],
                record["codice_fiscale_legale_rappresentante"],
                record["email"],
                content_hash,
            ))
        self.cur.executemany(
            """
            INSERT INTO customer_detail_records (
              protocollo, indirizzo, sesso, ateco,
              codice_fiscale, legale_rappresentante, telefono, settore,
              partita_iva, codice_fiscale_legale_rappresentante, email, content_hash
            ) VALUES (
              %s, %s, %s, %s,
              %s, %s, %s, %s,
              %s, %s, %s, %s
            ) ON CONFLICT (protocollo) DO NOTHING
            """,
            insert_data,
        )
        self.conn.commit()

    def insert_modulo_batch(self, records):
        insert_data = []
        for record in records:
            insert_data.append((
                record["protocollo"],
                record["field_name"],
                record["field_value"],
                record["content_hash"]
            ))
        self.cur.executemany(
            """
            INSERT INTO modulo_richiesta_records (
              protocollo, field_name, field_value, content_hash
            ) VALUES (
              %s, %s, %s, %s
            ) ON CONFLICT (content_hash) DO NOTHING
            """,
            insert_data,
        )
        self.conn.commit()

    def close(self):
        self.conn.close()

class PopupNotifier:
    def show(self, msg, title="Success"):
        root = tk.Tk()
        root.overrideredirect(True)
        root.attributes("-topmost", True)

        width, height = 360, 110
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        x = screen_width - width - 20
        y = screen_height - height - 120
        root.geometry(f"{width}x{height}+{x}+{y}")

        container = tk.Frame(root, bg="white", bd=0)
        container.place(relwidth=1, relheight=1)

        stripe_margin_x = 8
        stripe_margin_y = 12
        stripe_height = height - stripe_margin_y * 2
        stripe = tk.Canvas(container, width=12, height=stripe_height, bg="white", highlightthickness=0)
        stripe.place(x=stripe_margin_x, y=stripe_margin_y)
        stripe.create_rectangle(0, 0, 12, stripe_height, fill="#27ae60", outline="")

        title_label = tk.Label(
            container,
            text=title,
            bg="white",
            fg="#27ae60",
            font=("Century Gothic", 14, "bold"),
            anchor="w"
        )
        title_label.place(x=28, y=18)

        message = tk.Label(
            container,
            text=msg,
            bg="white",
            fg="#2c3e50",
            font=("Century Gothic", 11),
            anchor="w",
            justify="left",
            wraplength=300
        )
        message.place(x=28, y=50)

        root.after(4000, root.destroy)
        root.mainloop()

class SeleniumHelper:
    @staticmethod
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
    def click(driver_or_elem, element=None):
        if element:
            element.click()
        else:
            driver_or_elem.click()

    @staticmethod
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
    def safe_send_keys(element, text):
        element.send_keys(text)

    @staticmethod
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
    def extract_customer_detail_panel(wait):
        try:
            panel = wait.until(
                EC.visibility_of_element_located((By.CLASS_NAME, "detail-panel"))
            )
            tables = panel.find_elements(By.TAG_NAME, "table")
            data = {}
            for table in tables:
                rows = table.find_elements(By.TAG_NAME, "tr")
                if len(rows) < 2:
                    continue
                keys = [td.text.strip().lower().replace(" ", "_") for td in rows[0].find_elements(By.TAG_NAME, "span") if td.text.strip()]
                values = [td.text.strip() for td in rows[1].find_elements(By.TAG_NAME, "span") if td.text.strip() or td.text == "-"]
                if not values:
                    values = [td.text.strip() for td in rows[1].find_elements(By.TAG_NAME, "td")]
                for i, key in enumerate(keys):
                    if i < len(values):
                        data[key] = values[i]
            return data
        except Exception as e:
            logging.error(f"Customer detail extraction error: {e}", exc_info=True)
            return None

    @staticmethod
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
    def extract_modulo_richiesta_panel(wait):
        try:
            tab = wait.until(
                EC.element_to_be_clickable((By.XPATH, "//span[contains(text(),'Modulo richiesta')]"))
            )
            tab.click()
            panel = wait.until(
                EC.visibility_of_element_located((By.XPATH, "//div[contains(@class,'detail-panel') and .//span[contains(text(),'Modulo richiesta')]]"))
            )
            tables = panel.find_elements(By.TAG_NAME, "table")
            data = []
            for table in tables:
                rows = table.find_elements(By.TAG_NAME, "tr")
                if len(rows) < 2:
                    continue
                header_tds = rows[0].find_elements(By.TAG_NAME, "td")
                value_tds = rows[1].find_elements(By.TAG_NAME, "td")
                for i, header_td in enumerate(header_tds):
                    field_name = header_td.text.strip()
                    value_td = value_tds[i] if i < len(value_tds) else None
                    field_value = ""
                    checkbox = None
                    try:
                        checkbox = value_td.find_element(By.XPATH, ".//input[@type='checkbox']")
                    except Exception:
                        checkbox = None
                    if checkbox:
                        field_value = "checked" if checkbox.is_selected() else "unchecked"
                    else:
                        spans = value_td.find_elements(By.TAG_NAME, "span")
                        if spans:
                            field_value = spans[0].text.strip()
                        else:
                            field_value = value_td.text.strip()
                    if not field_value:
                        field_value = ""
                    data.append((field_name, field_value))
            return data
        except Exception as e:
            logging.error(f"Modulo richiesta extraction error: {e}", exc_info=True)
            return []

class HashUtil:
    @staticmethod
    def hash_content(data_dict):
        filtered_dict = {k: v for k, v in data_dict.items() if k != "protocollo"}
        content_str = "|".join(str(v) for v in filtered_dict.values())
        return hashlib.sha256(content_str.encode("utf-8")).hexdigest()

    @staticmethod
    def hash_modulo(protocollo, field_name, field_value):
        content_str = f"{protocollo}|{field_name}|{field_value}"
        return hashlib.sha256(content_str.encode("utf-8")).hexdigest()

class WebsiteFormSubmitter:
    @staticmethod
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
    def submit_form(driver, site_config, modulo_data_dict):
        driver.get(site_config["url"])
        wait = WebDriverWait(driver, 20)
        for field_name, field_value in modulo_data_dict.items():
            target_field = site_config["field_mapping"].get(field_name)
            if not target_field:
                continue
            try:
                # Checkbox handling
                if field_value in ["checked", "unchecked"]:
                    checkbox_elem = wait.until(EC.presence_of_element_located((By.NAME, target_field)))
                    is_checked = checkbox_elem.is_selected()
                    should_check = field_value == "checked"
                    if is_checked != should_check:
                        checkbox_elem.click()
                else:
                    input_elem = wait.until(EC.presence_of_element_located((By.NAME, target_field)))
                    input_elem.clear()
                    input_elem.send_keys(field_value)
            except Exception as e:
                logging.warning(f"Could not set field {field_name} on {site_config['name']}: {e}")
        # Submit the form (assuming a submit button with type='submit')
        try:
            submit_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@type='submit']")))
            submit_btn.click()
        except Exception as e:
            logging.warning(f"Could not submit form on {site_config['name']}: {e}")

class Scraper:
    def __init__(self, driver, driver1, wait, db_manager, notifier):
        self.driver = driver
        self.driver1 = driver1
        self.wait = wait
        self.db = db_manager
        self.notifier = notifier
        self.success_log = []
        self.failed_log = []

    def decide_websites(self, modulo_data_dict):
        # Example logic: send to all if "SomeCheckbox" is checked, else skip Site4
        send_sites = []
        some_checkbox = modulo_data_dict.get("SomeCheckbox", "")
        if some_checkbox == "checked":
            send_sites = WEBSITE_CONFIGS
        else:
            send_sites = [site for site in WEBSITE_CONFIGS if site["name"] != "Site4"]
        return send_sites

    def scrape(self):
        logging.info("Scraping started...")
        new_data_to_send = []
        batch_customer_records = []
        batch_modulo_records = []
        modulo_batch_for_websites = []

        try:
            self.driver.get(os.getenv("OMNIA_URL"))

            for tab in ["navigationForm:portfolio", "navigationForm:opportunities", "navigationForm:requestsDashboard"]:
                try:
                    WebDriverWait(self.driver, 7).until(EC.element_to_be_clickable((By.ID, tab))).click()
                except Exception:
                    logging.warning(f"Tab {tab} not clickable, skipping.")

            table = WebDriverWait(self.driver, 7).until(EC.presence_of_element_located((By.ID, "module:tblRequestsDashboard")))

            spans = table.find_elements(By.XPATH, ".//tbody//tr//span[contains(@id,'j_id484')]")

            for span in spans:
                protocollo = span.text.strip()
                if not protocollo:
                    continue

                if self.db.protocollo_exists(protocollo):
                    print(f"Skipping existing protocollo {protocollo}")
                    logging.debug(f"Skipping existing protocollo {protocollo}")
                    continue

                row = span.find_element(By.XPATH, "./ancestor::tr")
                button = row.find_element(By.CSS_SELECTOR, "a.icon-search")
                try:
                    WebDriverWait(self.driver, 3).until(EC.element_to_be_clickable(button)).click()
                except Exception:
                    SeleniumHelper.click(self.driver, button)

                detail_panel = self.wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "detail-panel")))

                try:
                    cliente_link = detail_panel.find_element(By.XPATH, ".//span[text()='Cliente']/ancestor::td/following-sibling::td//a")
                    SeleniumHelper.click(cliente_link)
                except Exception as e:
                    logging.error(f"Could not click Cliente link: {e}", exc_info=True)
                    continue

                customer_data = SeleniumHelper.extract_customer_detail_panel(self.wait)
                if not customer_data:
                    continue

                customer_data["protocollo"] = protocollo
                content_hash = HashUtil.hash_content(customer_data)

                if self.db.hash_exists_customer(content_hash):
                    print(f"Duplicate hash for {protocollo}, skipping insert")
                    logging.warning(f"Duplicate hash for {protocollo}, skipping insert")
                else:
                    batch_customer_records.append((customer_data, content_hash))
                    new_data_to_send.append({
                        "indirizzo": customer_data.get("indirizzo", ""),
                        "sesso": customer_data.get("sesso", ""),
                        "ateco": customer_data.get("ateco", ""),
                        "codice_fiscale": customer_data.get("codice_fiscale", ""),
                        "legale_rappresentante": customer_data.get("legale_rappresentante", ""),
                        "telefono": customer_data.get("telefono", ""),
                        "settore": customer_data.get("settore", ""),
                        "partita_iva": customer_data.get("partita_iva", ""),
                        "codice_fiscale_legale_rappresentante": customer_data.get("codice_fiscale_legale_rappresentante", ""),
                        "email": customer_data.get("email", "")
                    })
                    self.notifier.show(f"Inserted new customer record: {customer_data.get('indirizzo', '')}")

                modulo_data = SeleniumHelper.extract_modulo_richiesta_panel(self.wait)
                modulo_data_dict = {field_name: field_value for field_name, field_value in modulo_data}
                new_modulo_found = False
                for field_name, field_value in modulo_data:
                    modulo_hash = HashUtil.hash_modulo(protocollo, field_name, field_value)
                    if not self.db.hash_exists_modulo(modulo_hash):
                        batch_modulo_records.append({
                            "protocollo": protocollo,
                            "field_name": field_name,
                            "field_value": field_value,
                            "content_hash": modulo_hash
                        })
                        new_modulo_found = True
                if new_modulo_found:
                    modulo_batch_for_websites.append((protocollo, modulo_data_dict))

            if batch_customer_records:
                self.db.insert_customer_batch(batch_customer_records)
                logging.info(f"Inserted {len(batch_customer_records)} new customer records in batch.")

            if batch_modulo_records:
                self.db.insert_modulo_batch(batch_modulo_records)
                logging.info(f"Inserted {len(batch_modulo_records)} new modulo richiesta records in batch.")

            # Send new modulo richiesta data to websites
            for protocollo, modulo_data_dict in modulo_batch_for_websites:
                target_sites = self.decide_websites(modulo_data_dict)
                for site_config in target_sites:
                    try:
                        WebsiteFormSubmitter.submit_form(self.driver1, site_config, modulo_data_dict)
                        self.success_log.append(f"Sent modulo richiesta for {protocollo} to {site_config['name']}")
                    except Exception as e:
                        self.failed_log.append(f"Failed to send modulo richiesta for {protocollo} to {site_config['name']}: {e}")

        except Exception as e:
            logging.error(f"General scraping error: {e}", exc_info=True)

        finally:
            with open("form_submission_report.txt", "w", encoding="utf-8") as rpt:
                rpt.write("Successful Submissions\n=======================\n\n")
                rpt.write("\n".join(self.success_log))

            with open("failed_submission_log.txt", "w", encoding="utf-8") as rpt:
                rpt.write("Failed Submissions\n===================\n\n")
                rpt.write("\n".join(self.failed_log))

            print("\n📁 Reports saved.")

class ScrapeScheduler:
    def __init__(self, scraper):
        self.scraper = scraper

    def threaded_scrape(self):
        if scrape_lock.locked():
            print("⛔ Scrape already running; skipping this round.")
            return

        def run_task():
            with scrape_lock:
                self.scraper.scrape()

        threading.Thread(target=run_task, name="ScrapeThread").start()

    def start(self, interval_seconds=20):
        schedule.every(interval_seconds).seconds.do(self.threaded_scrape)
        print(f"🧵 Multithreaded scheduler is running every {interval_seconds} seconds...\n")
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("🛑 Script interrupted. Cleaning up...")
            self.scraper.db.close()
            self.scraper.driver.quit()
            self.scraper.driver1.quit()
            print("✅ Script completed successfully.")

if __name__ == "__main__":
    options = webdriver.ChromeOptions()
    #options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 20)

    options1 = webdriver.ChromeOptions()
    options1.add_argument("--start-maximized")
    driver1 = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options1)

    db_manager = DatabaseManager()
    notifier = PopupNotifier()
    scraper = Scraper(driver, driver1, wait, db_manager, notifier)
    scheduler = ScrapeScheduler(scraper)
    scheduler.start(interval_seconds=20)
    print("✅ Script started successfully.")




### Added 4 websites mapping functionaluity but the actual elements of the real 4 websites are unknown so its
### in dev and test for now. Continue with selenium_Scraper_consumer_update_3.py to test how the script extract
### data from new detail-panel and from "Modulo richiesto" and if they are properly checked or not and if they
### are inserted in the dfatabase properly.