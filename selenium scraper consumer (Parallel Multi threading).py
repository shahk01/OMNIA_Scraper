import os
import time
import queue
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

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

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

CUSTOMER_DETAIL_FIELDS = [
    "nome_cliente",
    "indirizzo", "sesso", "ateco", "codice_fiscale", "legale_rappresentante", "telefono",
    "settore", "partita_iva", "codice_fiscale_legale_rappresentante", "email"
]

RICHIESTA_CONTRATTO_FIELDS = [
    "protocollo", "avanzamento", "inserita_il", "prodotto",
    "assegnata_a", "richiedente", "referente_destinatario",
    "cliente", "progetto", "collegato_a"
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
        self.create_richiesta_contratto_table()
        self.create_modulo_richiesta_table()
        self.create_customer_detail_table()

    def create_richiesta_contratto_table(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS richiesta_contratto (
              protocollo TEXT PRIMARY KEY,
              avanzamento TEXT,
              inserita_il TEXT,
              prodotto TEXT,
              assegnata_a TEXT,
              richiedente TEXT,
              referente_destinatario TEXT,
              cliente TEXT,
              progetto TEXT,
              collegato_a TEXT
            )
        """)
        self.conn.commit()

    def create_modulo_richiesta_table(self):
        columns = ",\n".join([f"{field} TEXT" for field in MODULO_RICHIESTA_FIELDS])
        sql = f"""
            CREATE TABLE IF NOT EXISTS modulo_richiesta (
                protocollo TEXT PRIMARY KEY,
                {columns}
            )
        """
        self.cur.execute(sql)
        self.conn.commit()

    def create_customer_detail_table(self):
        columns = ",\n".join([f"{field} TEXT" for field in CUSTOMER_DETAIL_FIELDS])
        sql = f"""
            CREATE TABLE IF NOT EXISTS customer_detail (
                protocollo TEXT PRIMARY KEY,
                {columns}
            )
        """
        self.cur.execute(sql)
        self.conn.commit()

    def protocollo_exists(self, protocollo):
        self.cur.execute(
            "SELECT 1 FROM modulo_richiesta WHERE protocollo = %s", (protocollo,)
        )
        return self.cur.fetchone() is not None

    def richiesta_contratto_exists(self, protocollo):
        self.cur.execute(
            "SELECT 1 FROM richiesta_contratto WHERE protocollo = %s", (protocollo,)
        )
        return self.cur.fetchone() is not None

    def insert_richiesta_contratto(self, data):
        columns = RICHIESTA_CONTRATTO_FIELDS
        values = [data.get(f, "") for f in columns]
        placeholders = ", ".join(["%s"] * len(columns))
        update_stmt = ", ".join([f"{f}=EXCLUDED.{f}" for f in columns[1:]])
        sql = f"""
            INSERT INTO richiesta_contratto ({', '.join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (protocollo) DO UPDATE SET {update_stmt}
        """
        self.cur.execute(sql, values)
        self.conn.commit()

    def insert_modulo_richiesta(self, protocollo, fields_dict):
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

    def insert_customer_detail(self, protocollo, fields_dict):
        columns = ["protocollo"] + CUSTOMER_DETAIL_FIELDS
        values = [protocollo] + [fields_dict.get(f, "") for f in CUSTOMER_DETAIL_FIELDS]
        placeholders = ", ".join(["%s"] * len(columns))
        update_stmt = ", ".join([f"{f}=EXCLUDED.{f}" for f in CUSTOMER_DETAIL_FIELDS])
        sql = f"""
            INSERT INTO customer_detail ({', '.join(columns)})
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
    def extract_richiesta_contratto(wait, driver):
        # ...existing extraction code...
        try:
            panel = wait.until(
                EC.visibility_of_element_located((By.ID, "module:j_id1350"))
            )
            data = {}
            try:
                protocollo_full = panel.find_element(By.ID, "module:j_id1356").text.strip()
                protocollo = protocollo_full.split()[0].split('(')[0].strip()
                data["protocollo"] = protocollo
            except Exception:
                data["protocollo"] = ""
            try:
                avanzamento = panel.find_element(By.ID, "module:j_id1416").text.strip()
                data["avanzamento"] = avanzamento
            except Exception:
                data["avanzamento"] = ""
            try:
                inserita_il = panel.find_element(By.ID, "module:j_id1425").text.strip()
                data["inserita_il"] = inserita_il
            except Exception:
                data["inserita_il"] = ""
            try:
                prodotto = panel.find_element(By.ID, "module:j_id1426").text.strip()
                data["prodotto"] = prodotto
            except Exception:
                data["prodotto"] = ""
            try:
                assegnata_a = panel.find_element(By.ID, "module:j_id1432").text.strip()
                data["assegnata_a"] = assegnata_a
            except Exception:
                data["assegnata_a"] = ""
            try:
                richiedente = panel.find_element(By.ID, "module:j_id1434").text.strip()
                data["richiedente"] = richiedente
            except Exception:
                data["richiedente"] = ""
            try:
                referente_destinatario = panel.find_element(By.ID, "module:j_id1436").text.strip()
                data["referente_destinatario"] = referente_destinatario
            except Exception:
                data["referente_destinatario"] = ""
            try:
                cliente = panel.find_element(By.ID, "module:j_id1444").text.strip()
                data["cliente"] = cliente
            except Exception:
                data["cliente"] = ""
            try:
                progetto = panel.find_element(By.ID, "module:j_id1452").text.strip()
                data["progetto"] = progetto
            except Exception:
                data["progetto"] = ""
            try:
                collegato_a = panel.find_element(By.ID, "module:j_id1459").text.strip()
                data["collegato_a"] = collegato_a
            except Exception:
                data["collegato_a"] = ""
            return data
        except Exception as e:
            logging.error(f"Richiesta contratto extraction error: {e}", exc_info=True)
            return {}

    @staticmethod
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
    def extract_modulo_richiesta(wait, driver):
        # ...existing extraction code...
        try:
            tab = wait.until(
                EC.element_to_be_clickable((By.ID, "module:j_id1488:0.1"))
            )
            tab.click()
            wait.until(EC.presence_of_element_located((By.ID, "module:j_id1488:0:j_id1849")))
            panel = wait.until(
                EC.visibility_of_element_located((By.ID, "module:j_id1488:0td2"))
            )
            fields = {}

            def get_input_value(input_id):
                try:
                    elem = panel.find_element(By.ID, input_id)
                    return elem.get_attribute("value").strip()
                except Exception:
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

            fields["rct"] = get_checkbox_value("module:j_id1488:0:j_id1999")
            fields["rct_importo"] = get_input_value("module:j_id1488:0:j_id2001")

            fields["furto_contenuto"] = get_checkbox_value("module:j_id1488:0:j_id2011")
            fields["furto_contenuto_importo"] = get_input_value("module:j_id1488:0:j_id2013")
            fields["furto_gioielli_e_valori"] = get_checkbox_value("module:j_id1488:0:j_id2015")
            fields["furto_gioielli_e_valori_importo"] = get_input_value("module:j_id1488:0:j_id2017")
            fields["furto_rapina_estorsione"] = get_checkbox_value("module:j_id1488:0:j_id2019")
            fields["furto_rapina_estorsione_importo"] = get_input_value("module:j_id1488:0:j_id2021")

            fields["allagamento_locali"] = get_checkbox_value("module:j_id1488:0:j_id2031")
            fields["allagamento_locali_importo"] = get_input_value("module:j_id1488:0:j_id2033")
            fields["pronto_intervento_per_danni_acqua"] = get_checkbox_value("module:j_id1488:0:j_id2035")
            fields["pronto_intervento_per_danni_acqua_importo"] = get_input_value("module:j_id1488:0:j_id2037")
            fields["invio_fabbro_per_interventi_di_emergenza"] = get_checkbox_value("module:j_id1488:0:j_id2039")
            fields["invio_fabbro_per_interventi_di_emergenza_importo"] = get_input_value("module:j_id1488:0:j_id2041")
            fields["invio_elettricista_per_interventi_di_emergenza"] = get_checkbox_value("module:j_id1488:0:j_id2043")
            fields["invio_elettricista_per_interventi_di_emergenza_importo"] = get_input_value("module:j_id1488:0:j_id2045")

            fields["tutela_legale"] = get_checkbox_value("module:j_id1488:0:j_id2055")
            fields["tutela_legale_importo"] = get_input_value("module:j_id1488:0:j_id2057")

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
    def extract_customer_detail(wait, driver):
        # ...existing extraction code...
        try:
            panel = wait.until(
                EC.visibility_of_element_located((By.ID, "customerViewForm:j_id2429"))
            )
            data = {}
            try:
                nome_cliente_div = driver.find_element(By.ID, "customerViewForm:j_id2433")
                nome_cliente_a = nome_cliente_div.find_element(By.TAG_NAME, "a")
                data["nome_cliente"] = nome_cliente_a.text.strip()
            except Exception:
                data["nome_cliente"] = ""
            table1 = panel.find_element(By.ID, "customerViewForm:j_id2586")
            rows1 = table1.find_elements(By.TAG_NAME, "tr")
            if len(rows1) >= 2:
                indirizzo = rows1[1].find_elements(By.TAG_NAME, "td")[0].text.strip().replace("\n", " ")
                sesso = rows1[1].find_elements(By.TAG_NAME, "td")[1].text.strip()
                ateco = rows1[1].find_elements(By.TAG_NAME, "td")[2].text.strip()
                data["indirizzo"] = indirizzo
                data["sesso"] = sesso
                data["ateco"] = ateco
            table2 = panel.find_element(By.ID, "customerViewForm:j_id2606")
            rows2 = table2.find_elements(By.TAG_NAME, "tr")
            if len(rows2) >= 2:
                codice_fiscale = rows2[1].find_elements(By.TAG_NAME, "td")[0].text.strip()
                legale_rappresentante = rows2[1].find_elements(By.TAG_NAME, "td")[1].text.strip()
                telefono = rows2[1].find_elements(By.TAG_NAME, "td")[2].text.strip()
                settore = rows2[1].find_elements(By.TAG_NAME, "td")[3].text.strip()
                data["codice_fiscale"] = codice_fiscale
                data["legale_rappresentante"] = legale_rappresentante
                data["telefono"] = telefono
                data["settore"] = settore
            table3 = panel.find_element(By.ID, "customerViewForm:j_id2621")
            rows3 = table3.find_elements(By.TAG_NAME, "tr")
            if len(rows3) >= 2:
                partita_iva = rows3[1].find_elements(By.TAG_NAME, "td")[0].text.strip()
                codice_fiscale_legale_rappresentante = rows3[1].find_elements(By.TAG_NAME, "td")[1].text.strip()
                email = rows3[1].find_elements(By.TAG_NAME, "td")[2].text.strip()
                data["partita_iva"] = partita_iva
                data["codice_fiscale_legale_rappresentante"] = codice_fiscale_legale_rappresentante
                data["email"] = email
            return data
        except Exception as e:
            logging.error(f"Customer detail extraction error: {e}", exc_info=True)
            return {}

def submit_to_test_website(driver, wait, modulo_data, customer_data, notifier):
    # Open the test website
    driver.get("file:///C:/Users/shehzad/Desktop/test%20website%20upgraded.html")
    # Login (hardcoded credentials)
    wait.until(EC.visibility_of_element_located((By.ID, "username"))).send_keys("admin")
    wait.until(EC.visibility_of_element_located((By.ID, "password"))).send_keys("password")
    wait.until(EC.element_to_be_clickable((By.ID, "loginBtn"))).click()
    wait.until(EC.visibility_of_element_located((By.ID, "app")))

    # --- Submit Modulo Richiesta to Request tab ---
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".nav-btn[data-tab='request']"))).click()
    wait.until(EC.visibility_of_element_located((By.ID, "requestForm")))
    form = driver.find_element(By.ID, "requestForm")
    for field in ["protocollo"] + MODULO_RICHIESTA_FIELDS:
        elem = form.find_element(By.NAME, field)
        value = modulo_data.get(field, "")
        if elem.get_attribute("type") == "checkbox":
            if value in ["on", "checked", "true", "1"]:
                if not elem.is_selected():
                    elem.click()
            else:
                if elem.is_selected():
                    elem.click()
        else:
            elem.clear()
            elem.send_keys(value)
    form.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
    time.sleep(1)

    # --- Profile: Search for existing profile before submitting ---
    wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".nav-btn[data-tab='profile']"))).click()
    wait.until(EC.visibility_of_element_located((By.ID, "profileForm")))

    # Use the search bar to check for existing profile
    search_input = driver.find_element(By.ID, "profileSearch")
    search_btn = driver.find_element(By.ID, "profileSearchBtn")
    search_term = f"{customer_data.get('nome_cliente','')}".strip()
    codice_fiscale = f"{customer_data.get('codice_fiscale','')}".strip()
    # Search by name first
    search_input.clear()
    search_input.send_keys(search_term)
    driver.execute_script("arguments[0].scrollIntoView(true);", search_btn)
    time.sleep(0.3)
    try:
        search_btn.click()
    except Exception:
        driver.execute_script("arguments[0].click();", search_btn)
    time.sleep(1)
    # Check results
    exists = False
    try:
        result_table = driver.find_element(By.ID, "profileSearchResult")
        rows = result_table.find_elements(By.TAG_NAME, "tr")
        for row in rows[1:]:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 3:
                name_val = cols[1].text.strip().lower()
                cf_val = cols[2].text.strip().lower()
                if name_val == search_term.lower() and cf_val == codice_fiscale.lower():
                    exists = True
                    break
    except Exception:
        pass

    # If not found by name, search by codice fiscale
    if not exists and codice_fiscale:
        search_input.clear()
        search_input.send_keys(codice_fiscale)
        driver.execute_script("arguments[0].scrollIntoView(true);", search_btn)
        time.sleep(0.3)
        try:
            search_btn.click()
        except Exception:
            driver.execute_script("arguments[0].click();", search_btn)
        time.sleep(1)
        try:
            result_table = driver.find_element(By.ID, "profileSearchResult")
            rows = result_table.find_elements(By.TAG_NAME, "tr")
            for row in rows[1:]:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 3:
                    name_val = cols[1].text.strip().lower()
                    cf_val = cols[2].text.strip().lower()
                    if name_val == search_term.lower() and cf_val == codice_fiscale.lower():
                        exists = True
                        break
        except Exception:
            pass

    # Close search result
    try:
        close_btn = driver.find_element(By.XPATH, "//button[contains(@onclick,'closeProfileSearch')]")
        driver.execute_script("arguments[0].scrollIntoView(true);", close_btn)
        time.sleep(0.2)
        try:
            close_btn.click()
        except Exception:
            driver.execute_script("arguments[0].click();", close_btn)
        time.sleep(1)
    except Exception:
        pass

    if exists:
        notifier.show(f"Profile already exists for: {search_term} / {codice_fiscale}", title="Profile Exists")
    else:
        # Submit Customer Detail to Profile tab
        form = driver.find_element(By.ID, "profileForm")
        for field in CUSTOMER_DETAIL_FIELDS:
            elem = form.find_element(By.NAME, field)
            value = customer_data.get(field, "")
            elem.clear()
            elem.send_keys(value)
        form.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
        time.sleep(1)


# ...replace your ParallelScraper class with this...
extraction_lock = threading.Lock()

class ParallelScraper:
    def __init__(self, driver, wait, db_manager, notifier, num_workers=3, queue_size=1000):
        self.driver = driver
        self.wait = wait
        self.db = db_manager
        self.notifier = notifier
        self.submission_queue = queue.Queue(maxsize=queue_size)
        self.num_workers = num_workers
        self.stop_event = threading.Event()
        self.workers = []

    def extract_protocollo_records(self):
        logging.info("Extraction started...")
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
                    logging.debug(f"Skipping existing protocollo {protocollo}")
                    continue

                row = span.find_element(By.XPATH, "./ancestor::tr")
                button = row.find_element(By.CSS_SELECTOR, "a.icon-search")
                try:
                    WebDriverWait(self.driver, 3).until(EC.element_to_be_clickable(button)).click()
                except Exception:
                    SeleniumHelper.click(self.driver, button)

                richiesta_contratto_data = SeleniumHelper.extract_richiesta_contratto(self.wait, self.driver)
                modulo_fields = SeleniumHelper.extract_modulo_richiesta(self.wait, self.driver)
                customer_fields = {}
                try:
                    cliente_link = self.driver.find_element(By.ID, "module:j_id1444")
                    cliente_link.click()
                    customer_fields = SeleniumHelper.extract_customer_detail(self.wait, self.driver)
                except Exception as e:
                    logging.error(f"Could not extract customer detail for {protocollo}: {e}", exc_info=True)

                self.submission_queue.put({
                    "protocollo": protocollo,
                    "richiesta_contratto": richiesta_contratto_data,
                    "modulo_fields": modulo_fields,
                    "customer_fields": customer_fields
                })
        except Exception as e:
            logging.error(f"General extraction error: {e}", exc_info=True)
        finally:
            logging.info("Extraction finished.")

    def driver_factory(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_experimental_option("excludeSwitches", ["enable-logging"])
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def submission_worker(self):
        while not self.stop_event.is_set():
            try:
                record = self.submission_queue.get(timeout=2)
            except queue.Empty:
                continue
            protocollo = record["protocollo"]
            richiesta_contratto_data = record["richiesta_contratto"]
            modulo_fields = record["modulo_fields"]
            customer_fields = record["customer_fields"]

            try:
                if richiesta_contratto_data and richiesta_contratto_data.get("protocollo"):
                    self.db.insert_richiesta_contratto(richiesta_contratto_data)
                    self.notifier.show(f"Inserted richiesta_contratto for: {protocollo}")
                if modulo_fields:
                    self.db.insert_modulo_richiesta(protocollo, modulo_fields)
                    self.notifier.show(f"Inserted modulo richiesta for: {protocollo}")
                if customer_fields:
                    self.db.insert_customer_detail(protocollo, customer_fields)
                    self.notifier.show(f"Inserted customer detail for: {protocollo}")

                driver = self.driver_factory()
                try:
                    submit_to_test_website(driver, WebDriverWait(driver, 20),
                                           {"protocollo": protocollo, **modulo_fields}, customer_fields, self.notifier)
                    self.notifier.show(f"Submitted to test website for: {protocollo}")
                finally:
                    driver.quit()
            except Exception as e:
                logging.error(f"Submission error for {protocollo}: {e}", exc_info=True)
            finally:
                self.submission_queue.task_done()

    def start_workers(self):
        for _ in range(self.num_workers):
            t = threading.Thread(target=self.submission_worker, daemon=True)
            t.start()
            self.workers.append(t)

    def threaded_scrape(self):
        # Only start a new extraction if the previous one is finished
        def run_extraction_with_lock():
            if extraction_lock.acquire(blocking=False):
                try:
                    self.extract_protocollo_records()
                finally:
                    extraction_lock.release()
            else:
                logging.info("Previous extraction still running. Skipping this cycle.")

        threading.Thread(target=run_extraction_with_lock, name="ExtractThread").start()

    def start(self, interval_seconds=20):
        self.start_workers()
        schedule.every(interval_seconds).seconds.do(self.threaded_scrape)
        print(f"🧵 Parallel scheduler running every {interval_seconds} seconds with {self.num_workers} workers...\n")
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("🛑 Script interrupted. Cleaning up...")
            self.stop_event.set()
            self.db.close()
            self.driver.quit()
            print("✅ Script completed successfully.")


if __name__ == "__main__":
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 20)

    db_manager = DatabaseManager()
    notifier = PopupNotifier()
    # Use ParallelScraper only!
    parallel_scraper = ParallelScraper(driver, wait, db_manager, notifier, num_workers=3, queue_size=1000)
    parallel_scraper.start(interval_seconds=20)