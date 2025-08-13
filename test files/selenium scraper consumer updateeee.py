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

# ------------------ Logging Setup ------------------
logging.basicConfig(level=logging.INFO)
scrape_lock = threading.Lock()

# ------------------ Database Credentials ------------------
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

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
        self.create_table()
        self.create_modulo_richiesta_table()

    def create_table(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS scraped_records_test (
              protocollo TEXT PRIMARY KEY,
              avanzamento TEXT,
              inserita_il DATE,
              prodotto TEXT,
              assegnata_a TEXT,
              richiedente TEXT,
              referente TEXT,
              cliente TEXT,
              progetto TEXT,
              collegato_a TEXT,
              content_hash TEXT UNIQUE
            )
        """)
        self.conn.commit()

    def create_modulo_richiesta_table(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS modulo_richiesta (
                protocollo TEXT,
                field_name TEXT,
                field_value TEXT,
                PRIMARY KEY (protocollo, field_name)
            )
        """)
        self.conn.commit()

    def protocollo_exists(self, protocollo):
        self.cur.execute(
            "SELECT 1 FROM scraped_records_test WHERE protocollo = %s", (protocollo,)
        )
        return self.cur.fetchone() is not None

    def hash_exists(self, content_hash):
        self.cur.execute(
            "SELECT 1 FROM scraped_records_test WHERE content_hash = %s", (content_hash,)
        )
        return self.cur.fetchone() is not None

    def insert_records_batch(self, records):
        insert_data = []
        for record, content_hash in records:
            insert_data.append((
                record["protocollo"],
                record["avanzamento"],
                datetime.strptime(record["inserita il"], "%d/%m/%Y %H:%M").date(),
                record["prodotto"],
                record["assegnata a"],
                record["richiedente"],
                record["referente destinatario"],
                record["cliente"],
                record["progetto"],
                record["collegato a"],
                content_hash,
            ))
        self.cur.executemany(
            """
            INSERT INTO scraped_records_test (
              protocollo, avanzamento, inserita_il,
              prodotto, assegnata_a, richiedente,
              referente, cliente, progetto,
              collegato_a, content_hash
            ) VALUES (
              %s, %s, %s,
              %s, %s, %s,
              %s, %s, %s,
              %s, %s
            ) ON CONFLICT (protocollo) DO NOTHING
            """,
            insert_data,
        )
        self.conn.commit()

    def insert_modulo_richiesta(self, protocollo, fields_dict):
        insert_data = []
        for field, value in fields_dict.items():
            insert_data.append((protocollo, field, value))
        self.cur.executemany(
            """
            INSERT INTO modulo_richiesta (protocollo, field_name, field_value)
            VALUES (%s, %s, %s)
            ON CONFLICT (protocollo, field_name) DO UPDATE SET field_value = EXCLUDED.field_value
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
    def extract_modulo_richiesta(wait, driver):
        try:
            # Click the "Modulo richiesta" tab
            tab = wait.until(
                EC.element_to_be_clickable((By.ID, "module:j_id1488:0.1"))
            )
            tab.click()
            panel = wait.until(
                EC.visibility_of_element_located((By.ID, "module:j_id1488:0td2"))
            )
            fields = {}

            # Extract all text fields and their labels
            for row in panel.find_elements(By.XPATH, ".//tr"):
                tds = row.find_elements(By.TAG_NAME, "td")
                i = 0
                while i < len(tds):
                    # Label
                    label_elems = tds[i].find_elements(By.XPATH, ".//span[contains(@class, 'field-label')]")
                    label = label_elems[0].text.strip() if label_elems else None
                    # Value
                    value = ""
                    if label:
                        # Text input
                        input_elems = tds[i+1].find_elements(By.XPATH, ".//input[@type='text']")
                        if input_elems:
                            value = input_elems[0].get_attribute("value").strip()
                        # Textarea
                        textarea_elems = tds[i+1].find_elements(By.XPATH, ".//textarea")
                        if textarea_elems:
                            value = textarea_elems[0].get_attribute("value") or textarea_elems[0].text or ""
                        fields[label] = value
                        i += 2
                    else:
                        i += 1

            # Extract all checkboxes and their labels
            for div in panel.find_elements(By.XPATH, ".//div[contains(@class, 'icePnlGrp')]"):
                checkbox = div.find_elements(By.XPATH, ".//input[@type='checkbox']")
                label_elem = div.find_elements(By.XPATH, ".//span[contains(@class, 'field-label')]")
                if checkbox and label_elem:
                    label = label_elem[0].text.strip()
                    checked = "checked" if checkbox[0].is_selected() else "unchecked"
                    fields[label] = checked

            # Extract all textareas with their section label (for notes)
            for textarea in panel.find_elements(By.XPATH, ".//textarea"):
                parent_row = textarea.find_element(By.XPATH, "./ancestor::tr[1]")
                # Try to find a preceding sibling row with a label
                label = None
                try:
                    prev_row = parent_row.find_element(By.XPATH, "preceding-sibling::tr[1]")
                    label_elem = prev_row.find_elements(By.XPATH, ".//span[contains(@class, 'text-semibold') or contains(@class, 'title')]")
                    if label_elem:
                        label = label_elem[0].text.strip()
                except Exception:
                    pass
                if not label:
                    # fallback: use textarea id
                    label = textarea.get_attribute("id")
                value = textarea.get_attribute("value") or textarea.text or ""
                fields[label] = value

            return fields
        except Exception as e:
            logging.error(f"Modulo richiesta extraction error: {e}", exc_info=True)
            return {}

    @staticmethod
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
    def extract_detail_data(wait):
        try:
            panel = wait.until(
                EC.visibility_of_element_located((By.ID, "module:j_id1410"))
            )
            keys = panel.find_elements(By.CSS_SELECTOR, "span.detail-title")
            rows = panel.find_elements(By.CLASS_NAME, "icePnlGrdRow2")
            values = []
            for row in rows:
                for cell in row.find_elements(By.TAG_NAME, "td"):
                    values.append(cell.text.strip())
            values.pop(0)
            data = {}
            for i, key in enumerate(keys):
                data[key.text.strip().lower()] = values[i]
            return data
        except Exception as e:
            logging.error(f"Extraction error: {e}", exc_info=True)
            return None

class FormSubmitter:
    @staticmethod
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
    def submit_form(driver, data):
        driver.get(os.getenv("TEST_SITE"))

        def login():
            SeleniumHelper.safe_send_keys(driver.find_element(By.ID, "login-username"), "admin")
            SeleniumHelper.safe_send_keys(driver.find_element(By.ID, "login-password"), "1234")
            SeleniumHelper.click(driver, driver.find_element(By.CSS_SELECTOR, "button[onclick='simulateLogin()']"))
            time.sleep(1)
            try:
                return driver.find_element(By.ID, "main-content").is_displayed()
            except:
                return False

        try:
            for attempt in range(3):
                if login():
                    break
                driver.refresh()
            else:
                raise Exception("Login failed after 3 attempts: wrong username or password")

            SeleniumHelper.safe_send_keys(driver.find_element(By.ID, "avanzamento"), data["avanzamento"])
            SeleniumHelper.safe_send_keys(driver.find_element(By.ID, "inserita-il"), data["inserita-il"].strftime("%Y-%m-%d"))
            SeleniumHelper.safe_send_keys(driver.find_element(By.ID, "prodotto"), data["prodotto"])
            SeleniumHelper.safe_send_keys(driver.find_element(By.ID, "assegnata-a"), data["assegnata-a"])
            SeleniumHelper.safe_send_keys(driver.find_element(By.ID, "richiedente"), data["richiedente"])
            SeleniumHelper.safe_send_keys(driver.find_element(By.ID, "referente"), data["referente"])
            SeleniumHelper.safe_send_keys(driver.find_element(By.ID, "cliente"), data["cliente"])
            SeleniumHelper.safe_send_keys(driver.find_element(By.ID, "progetto"), data["progetto"])
            SeleniumHelper.safe_send_keys(driver.find_element(By.ID, "collegato-a"), data["collegato-a"])
            SeleniumHelper.click(driver, driver.find_element(By.CSS_SELECTOR, "button[type='submit']"))

            return True

        except Exception as e:
            raise e

class HashUtil:
    @staticmethod
    def hash_content(data_dict):
        filtered_dict = {k: v for k, v in data_dict.items() if k != "protocollo"}
        content_str = "|".join(str(v) for v in filtered_dict.values())
        return hashlib.sha256(content_str.encode("utf-8")).hexdigest()

class Scraper:
    def __init__(self, driver, driver1, wait, db_manager, notifier):
        self.driver = driver
        self.driver1 = driver1
        self.wait = wait
        self.db = db_manager
        self.notifier = notifier
        self.success_log = []
        self.failed_log = []

    def scrape(self):
        logging.info("Scraping started...")
        new_data_to_send = []
        batch_records = []

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

                # 1. Click modulo richiesta and extract all fields, insert to DB
                modulo_fields = SeleniumHelper.extract_modulo_richiesta(self.wait, self.driver)
                if modulo_fields:
                    self.db.insert_modulo_richiesta(protocollo, modulo_fields)
                    self.notifier.show(f"Inserted modulo richiesta for: {protocollo}")

                # 2. Extract detail data (already present functionality)
                record = SeleniumHelper.extract_detail_data(self.wait)
                if not record:
                    continue

                record["protocollo"] = protocollo
                content_hash = HashUtil.hash_content(record)

                if self.db.hash_exists(content_hash):
                    print(f"Duplicate hash for {protocollo}, skipping insert")
                    logging.warning(f"Duplicate hash for {protocollo}, skipping insert")
                    continue

                batch_records.append((record, content_hash))

                new_data_to_send.append({
                    "avanzamento": record["avanzamento"],
                    "inserita-il": datetime.strptime(record["inserita il"], "%d/%m/%Y %H:%M").date(),
                    "prodotto": record["prodotto"],
                    "assegnata-a": record["assegnata a"],
                    "richiedente": record["richiedente"],
                    "referente": record["referente destinatario"],
                    "cliente": record["cliente"],
                    "progetto": record["progetto"],
                    "collegato-a": record["collegato a"]
                })

                self.notifier.show(f"Inserted new record: {record['progetto']}")

            # Batch insert at the end
            if batch_records:
                self.db.insert_records_batch(batch_records)
                logging.info(f"Inserted {len(batch_records)} new records in batch.")

        except Exception as e:
            logging.error(f"General scraping error: {e}", exc_info=True)

        finally:
            if new_data_to_send:
                print(f"\n📤 Submitting {len(new_data_to_send)} new records to form...")
                for rec in new_data_to_send:
                    try:
                        success = FormSubmitter.submit_form(self.driver1, rec)
                        if success:
                            self.notifier.show(f"✅ Form submitted for: {rec['progetto']}")
                            print(f"✅ Form submitted for: {rec['progetto']}")
                            self.success_log.append(f"Form sent: {rec['progetto']}")
                        else:
                            self.failed_log.append(f"Form failed: {rec['progetto']}")
                    except Exception as e:
                        self.failed_log.append(f"Error submitting {rec['progetto']}: {e}")

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