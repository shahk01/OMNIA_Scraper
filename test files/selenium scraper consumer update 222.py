import os
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

# 43 fields for modulo richiesta (use snake_case for SQL compatibility)
MODULO_RICHIESTA_FIELDS = [
    "comune", "provincia", "indirizzo", "cap", "piano",
    "appartamento_in_cond", "villa_a_schiera", "villa_isolata", "dimora_abituale", "dimora_saltuaria", "dimora_locata_a_terzi",
    "anno_di_costruzione", "anno_ristrutturazione_impianti",
    "struttura_portante_in_muratura", "cappotto_termico", "struttura_portante_in_cemento_armato", "pannelli_solari_e_o_fotovoltaici",
    "struttura_portante_in_acciaio", "antisismico", "presenza_struttura_commerciale_e_o_ricreative",
    "vincolo", "ente_vincolatario", "scadenza",
    "immobile",
    "incendio_fabbricato", "incendio_fabbricato_importo",
    "incendio_contenuto", "incendio_contenuto_importo",
    "rischio_locativo", "rischio_locativo_importo",
    "ricorso_terzi", "ricorso_terzi_importo",
    "fenomeno_elettrico", "fenomeno_elettrico_importo",
    "acqua_condotta", "acqua_condotta_importo",
    "spese_ricerca_e_riparazione_guasti", "spese_ricerca_e_riparazione_guasti_importo",
    "cristalli", "cristalli_importo",
    "eventi_atmosferici", "eventi_atmosferici_importo",
    "eventi_sociopolitici", "eventi_sociopolitici_importo",
    "pacchetto_extra", "pacchetto_extra_importo",
    "rct", "rct_importo",
    "furto_contenuto", "furto_contenuto_importo",
    "furto_gioielli_e_valori", "furto_gioielli_e_valori_importo",
    "furto_rapina_estorsione", "furto_rapina_estorsione_importo",
    "allagamento_locali", "allagamento_locali_importo",
    "pronto_intervento_per_danni_acqua", "pronto_intervento_per_danni_acqua_importo",
    "invio_fabbro_per_interventi_di_emergenza", "invio_fabbro_per_interventi_di_emergenza_importo",
    "invio_elettricista_per_interventi_di_emergenza", "invio_elettricista_per_interventi_di_emergenza_importo",
    "tutela_legale", "tutela_legale_importo",
    "note_sezione_incendio", "note_sezione_rc", "note_sezione_furto", "note_sezione_assistenza", "note_sezione_tutela_legale"
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
        # Build SQL for 43 fields + protocollo as PK
        columns = ",\n".join([f"{field} TEXT" for field in MODULO_RICHIESTA_FIELDS])
        sql = f"""
            CREATE TABLE IF NOT EXISTS modulo_richiesta (
                protocollo TEXT PRIMARY KEY,
                {columns}
            )
        """
        self.cur.execute(sql)
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
        # Prepare columns and values for insert
        columns = ["protocollo"] + MODULO_RICHIESTA_FIELDS
        values = [protocollo] + [fields_dict.get(f, "") for f in MODULO_RICHIESTA_FIELDS]
        placeholders = ", ".join(["%s"] * len(columns))
        update_stmt = ", ".join([f"{f}=EXCLUDED.{f}" for f in MODULO_RICHIESTA_FIELDS])
        sql = f"""
            INSERT INTO modulo_richiesta ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (protocollo) DO UPDATE SET {update_stmt}
        """
        self.cur.execute(sql, values)
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
            tab = wait.until(
                EC.element_to_be_clickable((By.ID, "module:j_id1488:0.1"))
            )
            tab.click()

            # --- ADD THIS WAIT: Wait for Comune field to be present ---
            wait.until(EC.presence_of_element_located((By.ID, "module:j_id1488:0:j_id1849")))

            panel = wait.until(
                EC.visibility_of_element_located((By.ID, "module:j_id1488:0td2"))
            )
            fields = {}

            # --- Text fields by label id ---
            def get_input_value(input_id):
                try:
                    print(f"Looking for input: {input_id}")
                    elem = panel.find_element(By.ID, input_id)
                    print(f"Found input: {input_id}, value: {elem.get_attribute('value')}")
                    return elem.get_attribute("value").strip()
                except Exception as e:
                    print(f"Could not find input: {input_id} ({e})")
                    return ""

            def get_checkbox_value(input_id):
                try:
                    return "checked" if panel.find_element(By.ID, input_id).is_selected() else "unchecked"
                except Exception:
                    return "unchecked"

            def get_textarea_value(textarea_id):
                try:
                    elem = panel.find_element(By.ID, textarea_id)
                    return elem.get_attribute("value") or elem.text or ""
                except Exception:
                    return ""

            # Fill all 43 fields by their HTML id (from your extract.html)
            fields["comune"] = get_input_value("module:j_id1488:0:j_id1849")
            fields["provincia"] = get_input_value("module:j_id1488:0:j_id1851")
            fields["indirizzo"] = get_input_value("module:j_id1488:0:j_id1853")
            fields["cap"] = get_input_value("module:j_id1488:0:j_id1855")
            fields["piano"] = get_input_value("module:j_id1488:0:j_id1857")

            fields["appartamento_in_cond"] = get_checkbox_value("module:j_id1488:0:j_id1865")
            fields["villa_a_schiera"] = get_checkbox_value("module:j_id1488:0:j_id1868")
            fields["villa_isolata"] = get_checkbox_value("module:j_id1488:0:j_id1871")
            fields["dimora_abituale"] = get_checkbox_value("module:j_id1488:0:j_id1874")
            fields["dimora_saltuaria"] = get_checkbox_value("module:j_id1488:0:j_id1877")
            fields["dimora_locata_a_terzi"] = get_checkbox_value("module:j_id1488:0:j_id1880")

            fields["anno_di_costruzione"] = get_input_value("module:j_id1488:0:annoCostruzione")
            fields["anno_ristrutturazione_impianti"] = get_input_value("module:j_id1488:0:annoRistrutturazioneImpianti")

            fields["struttura_portante_in_muratura"] = get_checkbox_value("module:j_id1488:0:j_id1890")
            fields["cappotto_termico"] = get_checkbox_value("module:j_id1488:0:j_id1893")
            fields["struttura_portante_in_cemento_armato"] = get_checkbox_value("module:j_id1488:0:j_id1896")
            fields["pannelli_solari_e_o_fotovoltaici"] = get_checkbox_value("module:j_id1488:0:j_id1899")
            fields["struttura_portante_in_acciaio"] = get_checkbox_value("module:j_id1488:0:j_id1902")
            fields["antisismico"] = get_checkbox_value("module:j_id1488:0:j_id1905")
            fields["presenza_struttura_commerciale_e_o_ricreative"] = get_checkbox_value("module:j_id1488:0:j_id1908")

            fields["vincolo"] = get_checkbox_value("module:j_id1488:0:j_id1926")
            fields["ente_vincolatario"] = get_input_value("module:j_id1488:0:j_id1929")
            fields["scadenza"] = get_input_value("module:j_id1488:0:j_id1931")

            fields["immobile"] = get_textarea_value("module:j_id1488:0:j_id1937")

            # Sezione Incendio
            fields["incendio_fabbricato"] = get_checkbox_value("module:j_id1488:0:j_id1947")
            fields["incendio_fabbricato_importo"] = get_input_value("module:j_id1488:0:j_id1949")
            fields["incendio_contenuto"] = get_checkbox_value("module:j_id1488:0:j_id1951")
            fields["incendio_contenuto_importo"] = get_input_value("module:j_id1488:0:j_id1953")
            fields["rischio_locativo"] = get_checkbox_value("module:j_id1488:0:j_id1955")
            fields["rischio_locativo_importo"] = get_input_value("module:j_id1488:0:j_id1957")
            fields["ricorso_terzi"] = get_checkbox_value("module:j_id1488:0:j_id1959")
            fields["ricorso_terzi_importo"] = get_input_value("module:j_id1488:0:j_id1961")
            fields["fenomeno_elettrico"] = get_checkbox_value("module:j_id1488:0:j_id1963")
            fields["fenomeno_elettrico_importo"] = get_input_value("module:j_id1488:0:j_id1965")
            fields["acqua_condotta"] = get_checkbox_value("module:j_id1488:0:j_id1967")
            fields["acqua_condotta_importo"] = get_input_value("module:j_id1488:0:j_id1969")
            fields["spese_ricerca_e_riparazione_guasti"] = get_checkbox_value("module:j_id1488:0:j_id1971")
            fields["spese_ricerca_e_riparazione_guasti_importo"] = get_input_value("module:j_id1488:0:j_id1973")
            fields["cristalli"] = get_checkbox_value("module:j_id1488:0:j_id1975")
            fields["cristalli_importo"] = get_input_value("module:j_id1488:0:j_id1977")
            fields["eventi_atmosferici"] = get_checkbox_value("module:j_id1488:0:j_id1979")
            fields["eventi_atmosferici_importo"] = get_input_value("module:j_id1488:0:j_id1981")
            fields["eventi_sociopolitici"] = get_checkbox_value("module:j_id1488:0:j_id1983")
            fields["eventi_sociopolitici_importo"] = get_input_value("module:j_id1488:0:j_id1985")
            fields["pacchetto_extra"] = get_checkbox_value("module:j_id1488:0:j_id1987")
            fields["pacchetto_extra_importo"] = get_input_value("module:j_id1488:0:j_id1989")

            # Sezione RC
            fields["rct"] = get_checkbox_value("module:j_id1488:0:j_id1999")
            fields["rct_importo"] = get_input_value("module:j_id1488:0:j_id2001")

            # Sezione Furto
            fields["furto_contenuto"] = get_checkbox_value("module:j_id1488:0:j_id2011")
            fields["furto_contenuto_importo"] = get_input_value("module:j_id1488:0:j_id2013")
            fields["furto_gioielli_e_valori"] = get_checkbox_value("module:j_id1488:0:j_id2015")
            fields["furto_gioielli_e_valori_importo"] = get_input_value("module:j_id1488:0:j_id2017")
            fields["furto_rapina_estorsione"] = get_checkbox_value("module:j_id1488:0:j_id2019")
            fields["furto_rapina_estorsione_importo"] = get_input_value("module:j_id1488:0:j_id2021")

            # Sezione Assistenza
            fields["allagamento_locali"] = get_checkbox_value("module:j_id1488:0:j_id2031")
            fields["allagamento_locali_importo"] = get_input_value("module:j_id1488:0:j_id2033")
            fields["pronto_intervento_per_danni_acqua"] = get_checkbox_value("module:j_id1488:0:j_id2035")
            fields["pronto_intervento_per_danni_acqua_importo"] = get_input_value("module:j_id1488:0:j_id2037")
            fields["invio_fabbro_per_interventi_di_emergenza"] = get_checkbox_value("module:j_id1488:0:j_id2039")
            fields["invio_fabbro_per_interventi_di_emergenza_importo"] = get_input_value("module:j_id1488:0:j_id2041")
            fields["invio_elettricista_per_interventi_di_emergenza"] = get_checkbox_value("module:j_id1488:0:j_id2043")
            fields["invio_elettricista_per_interventi_di_emergenza_importo"] = get_input_value("module:j_id1488:0:j_id2045")

            # Sezione Tutela Legale
            fields["tutela_legale"] = get_checkbox_value("module:j_id1488:0:j_id2055")
            fields["tutela_legale_importo"] = get_input_value("module:j_id1488:0:j_id2057")

            # Note (textarea)
            fields["note_sezione_incendio"] = get_textarea_value("module:j_id1488:0:j_id2064")
            fields["note_sezione_rc"] = get_textarea_value("module:j_id1488:0:j_id2066")
            fields["note_sezione_furto"] = get_textarea_value("module:j_id1488:0:j_id2068")
            fields["note_sezione_assistenza"] = get_textarea_value("module:j_id1488:0:j_id2070")
            fields["note_sezione_tutela_legale"] = get_textarea_value("module:j_id1488:0:j_id2072")

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
                    print(modulo_fields)
                    self.db.insert_modulo_richiesta(protocollo, modulo_fields)
                    self.notifier.show(f"Inserted modulo richiesta for: {protocollo}")

                # 2. Extract detail data (already present functionality)
                record = SeleniumHelper.extract_detail_data(self.wait)
                if not record:
                    continue

                record["protocollo"] = protocollo
                content_hash = hashlib.sha256(
                    "|".join(str(record.get(k, "")) for k in record if k != "protocollo").encode("utf-8")
                ).hexdigest()

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
                # ... form submission logic if needed ...
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