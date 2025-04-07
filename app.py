# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from zeep import Client
from zeep.transports import Transport
from zeep.exceptions import Fault
from requests import Session
from lxml import etree
import os
from zeep.wsse.username import UsernameToken
import logging
from datetime import date
import re # Dla normalize_name

# Konfiguracja logowania dla Zeep (opcjonalnie)
# logging.basicConfig(level=logging.DEBUG)
# logging.getLogger('zeep.transports').setLevel(logging.DEBUG)

# --- Konfiguracja ---
WSDL_URL = 'https://uslugaterytws1.stat.gov.pl/wsdl/terytws1.wsdl'
CSV_FILE_PATH = 'kody_pocztowe.csv'
CSV_ENCODING = 'utf-8'
CSV_SEPARATOR = ';'
TODAY_DATE_STR = date.today().strftime('%Y-%m-%d') # Używane w get_streets

# --- Funkcje pomocnicze ---

@st.cache_resource(ttl=3600)
def get_teryt_client(username, password):
    """Inicjalizuje i zwraca klienta SOAP TERYT z uwierzytelnieniem WS-Security UsernameToken."""
    st.write("Próba połączenia z TERYT...")
    try:
        session = Session()
        transport = Transport(session=session)
        wsse = UsernameToken(username, password, use_digest=False)
        transport.operation_timeout = 120; transport.timeout = 120
        try:
            from zeep.cache import SqliteCache
            client = Client(wsdl=WSDL_URL, transport=transport, wsse=wsse, cache=SqliteCache(timeout=600))
            st.info("Użyto cache Zeep (Sqlite).")
        except ImportError:
             st.warning("Brak SqliteCache, tworzenie klienta bez cache.")
             client = Client(wsdl=WSDL_URL, transport=transport, wsse=wsse)
        except Exception as cache_e:
             st.warning(f"Problem z cache Zeep: {cache_e}. Tworzenie klienta bez cache.")
             client = Client(wsdl=WSDL_URL, transport=transport, wsse=wsse)
        try:
             with st.spinner("Testowanie połączenia..."): is_logged_in = client.service.CzyZalogowany()
             st.success(f"Połączono i uwierzytelniono (Zalogowany: {is_logged_in}).")
             return client
        except Fault as f: st.error(f"Błąd SOAP po połączeniu: {f.message}"); return None
        except Exception as conn_test_e: st.warning(f"Błąd testu CzyZalogowany: {conn_test_e}."); return client
    except Exception as e: st.error(f"Krytyczny błąd inicjalizacji klienta: {e}"); return None

@st.cache_data
def load_csv_data(file_path, encoding, separator):
    """Wczytuje dane z pliku CSV."""
    # ... (bez zmian - kod z poprzednich odpowiedzi) ...
    if not os.path.exists(file_path): st.error(f"Nie znaleziono pliku CSV: {file_path}"); return None
    try:
        df = pd.read_csv(file_path, sep=separator, encoding=encoding, dtype=str)
        df.columns = df.columns.str.strip()
        for col in df.columns:
             if df[col].dtype == 'object': df[col] = df[col].str.strip()
        st.success(f"Wczytano dane z {file_path}.")
        return df
    except Exception as e: st.error(f"Błąd wczytywania CSV ({file_path}): {e}"); return None

def normalize_name(name):
    """Pomocnik do normalizacji nazw przed porównaniem."""
    # ... (bez zmian - kod z poprzednich odpowiedzi) ...
    if not isinstance(name, str): return ""
    name = name.lower().strip()
    name = re.sub(r"^(województwo|powiat|gmina|gm\.?|miasto|m\.?\s*st\.?|obszar wiejski)\s+", "", name).strip()
    name = re.sub(r"\(.*\)", "", name).strip()
    name = re.sub(r"\s+-.*$", "", name).strip()
    name = re.sub(r"\s+", " ", name).strip()
    return name

# --- OSTATECZNA WERSJA find_terc_code UŻYWAJĄCA WyszukajJPT(nazwa=...) + FILTROWANIE ---
def find_terc_code(client, woj_name, pow_name, gmi_name):
    """Wyszukuje kod TERC gminy (7 cyfr) na podstawie nazw, używając WyszukajJPT(nazwa=...) i BARDZO ostrożnie filtrując wyniki."""
    if not client: return None
    full_terc = None; woj_terc = None; pow_terc = None # pow_terc to tylko 2 cyfry

    norm_woj_name = normalize_name(woj_name)
    norm_pow_name = normalize_name(pow_name)
    norm_gmi_name = normalize_name(gmi_name)

    st.info(f"Szukam TERC dla (norm): Woj='{norm_woj_name}', Pow='{norm_pow_name}', Gmi='{norm_gmi_name}' [Metoda: WyszukajJPT(nazwa=...)+Filtrowanie]")
    if not norm_woj_name or not norm_pow_name or not norm_gmi_name:
        st.warning("Brak pełnych (po normalizacji) nazw do wyszukania TERC.")
        return None

    try:
        # --- Krok 1: Znajdź WOJ ---
        with st.spinner(f"1. Wyszukiwanie Województwa '{woj_name}'..."):
            result_raw = client.service.WyszukajJPT(nazwa=woj_name) # Tylko 'nazwa'
        found_woj_obj = None
        if result_raw:
            possible_woj = []
            st.write(f"--- Analiza wyników dla Województwa '{woj_name}' ---")
            for i, unit in enumerate(result_raw):
                woj_code = getattr(unit, 'WOJ', None); pow_code = getattr(unit, 'POW', None); gmi_code = getattr(unit, 'GMI', None)
                unit_name_raw = getattr(unit, 'NAZWA', ''); unit_name_norm = normalize_name(unit_name_raw)
                st.write(f"  Wynik {i+1}: WOJ='{woj_code}', POW='{pow_code}', GMI='{gmi_code}', Nazwa='{unit_name_raw}' (Norm: '{unit_name_norm}')")
                if woj_code and (pow_code is None or pow_code == '') and (gmi_code is None or gmi_code == ''): # Warunek strukturalny na WOJ
                    st.write(f"    -> Potencjalne województwo.")
                    if norm_woj_name == unit_name_norm: possible_woj.append({'unit': unit, 'match_type': 'exact'}); st.write(f"      -> Dokładne dopasowanie nazwy!")
                    elif norm_woj_name in unit_name_norm or unit_name_norm in norm_woj_name: possible_woj.append({'unit': unit, 'match_type': 'partial'}); st.write(f"      -> Częściowe dopasowanie nazwy.")
                    else: possible_woj.append({'unit': unit, 'match_type': 'structure_only'}); st.write(f"      -> Tylko struktura TERC pasuje.")
            # Wybór najlepszego kandydata
            if possible_woj:
                exact_matches = [p for p in possible_woj if p['match_type'] == 'exact']
                if len(exact_matches) >= 1: found_woj_obj = exact_matches[0]['unit']; st.write("Wybrano pierwsze dokładne dopasowanie nazwy województwa.")
                else:
                     partial_matches = [p for p in possible_woj if p['match_type'] == 'partial']
                     if len(partial_matches) >= 1: st.warning(f"Brak dokładnego dopasowania nazwy woj., wybieram pierwszy z {len(partial_matches)} częściowych."); found_woj_obj = partial_matches[0]['unit']
                     elif len(possible_woj) >= 1: st.warning(f"Brak dopasowania nazwy woj., wybieram pierwszy z {len(possible_woj)} pasujących strukturą."); found_woj_obj = possible_woj[0]['unit']
        # Walidacja
        if found_woj_obj:
            woj_terc_candidate = getattr(found_woj_obj, 'WOJ', None)
            if woj_terc_candidate and str(woj_terc_candidate).isdigit(): woj_terc = str(woj_terc_candidate).zfill(2); st.success(f"=> Etap 1: OK, WOJ: {woj_terc}")
            else: st.warning(f"Kod WOJ ('{woj_terc_candidate}') nieprawidłowy."); return None
        else: st.warning(f"Nie znaleziono województwa dla '{woj_name}'."); return None

        # --- Krok 2: Znajdź POW ---
        with st.spinner(f"2. Wyszukiwanie Powiatu '{pow_name}'..."): result_raw = client.service.WyszukajJPT(nazwa=pow_name)
        found_pow_obj = None
        if result_raw:
            possible_pow = []
            st.write(f"--- Analiza wyników dla Powiatu '{pow_name}' (w woj. {woj_terc}) ---")
            for i, unit in enumerate(result_raw):
                 unit_woj = str(getattr(unit, 'WOJ', '')).zfill(2); unit_pow_candidate = getattr(unit, 'POW', None); gmi_code = getattr(unit, 'GMI', None)
                 unit_name_raw = getattr(unit, 'NAZWA', ''); unit_name_norm = normalize_name(unit_name_raw)
                 st.write(f"  Wynik {i+1}: WOJ='{unit_woj}', POW='{unit_pow_candidate}', GMI='{gmi_code}', Nazwa='{unit_name_raw}' (Norm: '{unit_name_norm}')")
                 if unit_woj == woj_terc and unit_pow_candidate and str(unit_pow_candidate) != '' and (gmi_code is None or gmi_code == ''): # Warunek strukturalny na POW
                      st.write(f"    -> Potencjalny powiat w dobrym województwie.")
                      if norm_pow_name == unit_name_norm: possible_pow.append({'unit': unit, 'match_type': 'exact'}); st.write(f"      -> Dokładne dopasowanie nazwy!")
                      elif norm_pow_name in unit_name_norm or unit_name_norm in norm_pow_name: possible_pow.append({'unit': unit, 'match_type': 'partial'}); st.write(f"      -> Częściowe dopasowanie nazwy.")
                      else: possible_pow.append({'unit': unit, 'match_type': 'structure_only'}); st.write(f"      -> Tylko struktura TERC pasuje.")
            # Wybór
            if possible_pow:
                exact_matches = [p for p in possible_pow if p['match_type'] == 'exact']
                if len(exact_matches) >= 1: found_pow_obj = exact_matches[0]['unit']; st.write("Wybrano pierwsze dokładne dopasowanie nazwy powiatu.")
                else:
                     partial_matches = [p for p in possible_pow if p['match_type'] == 'partial']
                     if len(partial_matches) >= 1: st.warning(f"Brak dokładnego dopasowania nazwy pow., wybieram pierwszy z {len(partial_matches)} częściowych."); found_pow_obj = partial_matches[0]['unit']
                     elif len(possible_pow) >= 1: st.warning(f"Brak dopasowania nazwy pow., wybieram pierwszy z {len(possible_pow)} pasujących strukturą."); found_pow_obj = possible_pow[0]['unit']
        # Walidacja
        if found_pow_obj:
            pow_terc_candidate = getattr(found_pow_obj, 'POW', None)
            if pow_terc_candidate and str(pow_terc_candidate).isdigit(): pow_terc = str(pow_terc_candidate).zfill(2); st.success(f"=> Etap 2: OK, POW: {pow_terc}")
            else: st.warning(f"Kod POW ('{pow_terc_candidate}') nieprawidłowy."); return None
        else: st.warning(f"Nie znaleziono powiatu '{pow_name}' w woj. {woj_terc}."); return None

        # --- Krok 3: Znajdź GMI + RODZ ---
        with st.spinner(f"3. Wyszukiwanie Gminy '{gmi_name}'..."): result_raw = client.service.WyszukajJPT(nazwa=gmi_name)
        found_gmi_obj = None
        if result_raw:
            possible_gmi = []
            st.write(f"--- Analiza wyników dla Gminy '{gmi_name}' (w pow. {woj_terc}{pow_terc}) ---")
            for i, unit in enumerate(result_raw):
                 unit_woj = str(getattr(unit, 'WOJ', '')).zfill(2); unit_pow = str(getattr(unit, 'POW', '')).zfill(2); unit_gmi_candidate = getattr(unit, 'GMI', None); unit_rodz_candidate = getattr(unit, 'RODZ', None)
                 unit_name_raw = getattr(unit, 'NAZWA', ''); unit_name_norm = normalize_name(unit_name_raw)
                 st.write(f"  Wynik {i+1}: WOJ='{unit_woj}', POW='{unit_pow}', GMI='{unit_gmi_candidate}', RODZ='{unit_rodz_candidate}', Nazwa='{unit_name_raw}' (Norm: '{unit_name_norm}')")
                 # Ścisły warunek na gminę: WOJ, POW pasują, GMI, RODZ istnieją i są cyframi
                 if unit_woj == woj_terc and unit_pow == pow_terc and \
                    unit_gmi_candidate and str(unit_gmi_candidate).isdigit() and str(unit_gmi_candidate) != '' and \
                    unit_rodz_candidate and str(unit_rodz_candidate).isdigit() and str(unit_rodz_candidate) != '':
                      st.write(f"    -> Potencjalna gmina w dobrym powiecie.")
                      norm_gmi_name_alt = re.sub(r"^gm\.?\s+", "", norm_gmi_name).strip()
                      if norm_gmi_name == unit_name_norm or norm_gmi_name_alt == unit_name_norm: possible_gmi.append({'unit': unit, 'match_type': 'exact'}); st.write(f"      -> Dokładne dopasowanie nazwy!")
                      elif norm_gmi_name in unit_name_norm or unit_name_norm in norm_gmi_name: possible_gmi.append({'unit': unit, 'match_type': 'partial'}); st.write(f"      -> Częściowe dopasowanie nazwy.")
                      else: possible_gmi.append({'unit': unit, 'match_type': 'structure_only'}); st.write(f"      -> Tylko struktura TERC pasuje.")
            # Wybór
            if possible_gmi:
                exact_matches = [p for p in possible_gmi if p['match_type'] == 'exact']
                if len(exact_matches) >= 1: found_gmi_obj = exact_matches[0]['unit']; st.write("Wybrano pierwsze dokładne dopasowanie nazwy gminy.")
                else:
                     partial_matches = [p for p in possible_gmi if p['match_type'] == 'partial']
                     if len(partial_matches) >= 1: st.warning(f"Brak dokładnego dopasowania nazwy gminy, wybieram pierwszy z {len(partial_matches)} częściowych."); found_gmi_obj = partial_matches[0]['unit']
                     elif len(possible_gmi) >= 1: st.warning(f"Brak dopasowania nazwy gminy, wybieram pierwszy z {len(possible_gmi)} pasujących strukturą."); found_gmi_obj = possible_gmi[0]['unit']
        # Walidacja
        if found_gmi_obj:
             gmi_code = str(getattr(found_gmi_obj, 'GMI')).zfill(2)
             rodz_code = str(getattr(found_gmi_obj, 'RODZ'))
             full_terc = f"{woj_terc}{pow_terc}{gmi_code}{rodz_code}"
             st.success(f"=> Etap 3: OK, TERC: {full_terc} dla '{getattr(found_gmi_obj, 'NAZWA', '')}'")
        else: st.warning(f"Nie znaleziono gminy '{gmi_name}' w pow. {woj_terc}{pow_terc}.");

    except Fault as f: st.error(f"Błąd SOAP podczas WyszukajJPT(nazwa=...): {f.message}"); full_terc = None
    except TypeError as te: st.error(f"Błąd typu (TypeError) podczas WyszukajJPT: {te}"); full_terc = None
    except Exception as e: st.error(f"Nieoczekiwany błąd podczas wyszukiwania TERC: {e}"); full_terc = None
    finally:
        if full_terc and isinstance(full_terc, str) and len(full_terc) == 7 and full_terc.isdigit(): return full_terc
        else:
             if full_terc: st.warning(f"Znaleziony kod TERC '{full_terc}' ma zły format.")
             return None
# --- KONIEC WERSJI find_terc_code ---


# --- Reszta kodu (find_simc_symbol, get_streets, główna logika Streamlit) ---
# --- POZOSTAJE BEZ ZMIAN w stosunku do poprzedniej odpowiedzi ---
# --- (find_simc_symbol używa 'nazwaMiejscowosci') ---
# --- (get_streets używa 'WojewodztwoId', 'PowiatId', itd.) ---

def find_simc_symbol(client, locality_name, terc_gmi_full):
    """Wyszukuje symbol SIMC miejscowości (7 cyfr), opcjonalnie filtrując po TERC gminy."""
    if not client: return None
    simc_symbol = None
    try:
        miejsc_result_general = None
        with st.spinner(f"Wyszukiwanie symbolu SIMC dla: {locality_name}..."):
             miejsc_result_general = client.service.WyszukajMiejscowosc(nazwaMiejscowosci=locality_name)

        if not miejsc_result_general:
             st.warning(f"Nie znaleziono miejscowości: {locality_name} w TERYT.")
             return None

        best_match = None
        if terc_gmi_full: # Filtrowanie po TERC, jeśli dostępny
             terc_woj = terc_gmi_full[0:2]; terc_pow = terc_gmi_full[2:4]; terc_gmi = terc_gmi_full[4:6]
             potential_matches = [
                 m for m in miejsc_result_general
                 if hasattr(m, 'GmiSymbol') and hasattr(m, 'PowSymbol') and hasattr(m, 'WojSymbol') and
                    str(getattr(m, 'WojSymbol', '')).zfill(2) == terc_woj and
                    str(getattr(m, 'PowSymbol', '')).zfill(2) == terc_pow and
                    str(getattr(m, 'GmiSymbol', ''))[:2] == terc_gmi
             ]
             if potential_matches:
                 potential_matches.sort(key=lambda x: getattr(x, 'Symbol', 0))
                 best_match = potential_matches[0]
             else:
                 st.warning(f"Nie znaleziono '{locality_name}' pasującej do gminy {terc_gmi_full}. Zwracam pierwszy znaleziony.")
                 miejsc_result_general.sort(key=lambda x: getattr(x, 'Symbol', 0))
                 best_match = miejsc_result_general[0] if miejsc_result_general else None
        else: # Brak TERC, bierzemy pierwszy posortowany
            miejsc_result_general.sort(key=lambda x: getattr(x, 'Symbol', 0))
            best_match = miejsc_result_general[0] if miejsc_result_general else None

        if best_match:
            simc_symbol = getattr(best_match, 'Symbol', None)
            if simc_symbol is not None: simc_symbol = str(simc_symbol)

    except Fault as f: st.error(f"Błąd SOAP podczas wyszukiwania SIMC dla '{locality_name}': {f.message}")
    except TypeError as te: st.error(f"Błąd typu (TypeError) podczas wywołania API dla SIMC '{locality_name}': {te}")
    except Exception as e: st.error(f"Nieoczekiwany błąd podczas wyszukiwania SIMC dla '{locality_name}': {e}")
    finally:
         if simc_symbol and isinstance(simc_symbol, str) and simc_symbol.isdigit():
              simc_symbol = simc_symbol.zfill(7)
              if len(simc_symbol) == 7: return simc_symbol
         if simc_symbol: st.warning(f"Znaleziony symbol SIMC '{simc_symbol}' ma nieprawidłowy format.")
         return None


def get_streets(client, full_terc_code, simc_symbol):
    """Pobiera listę ulic dla danej miejscowości używając PobierzListeUlicDlaMiejscowosci."""
    if not client or not full_terc_code or not simc_symbol or len(full_terc_code) != 7:
        return []

    all_streets = []
    try:
        woj_id = full_terc_code[0:2]; pow_id = full_terc_code[2:4]; gmi_id = full_terc_code[4:6]; rodz_id = full_terc_code[6:7]

        with st.spinner(f"Pobieranie ulic dla SIMC: {simc_symbol}..."):
            streets_result = client.service.PobierzListeUlicDlaMiejscowosci(
                WojewodztwoId=woj_id, PowiatId=pow_id, GminaId=gmi_id, GminaRodzaj=rodz_id, MiejscowoscId=simc_symbol, DataStanu=TODAY_DATE_STR
            )

            if streets_result and hasattr(streets_result, 'Ulica') and streets_result.Ulica:
                seen_ids = set()
                for ulica in streets_result.Ulica:
                     if not hasattr(ulica, 'Identyfikator') or not ulica.Identyfikator: continue
                     if ulica.Identyfikator not in seen_ids:
                        cecha = getattr(ulica, 'Cecha', ''); nazwa1 = getattr(ulica, 'Nazwa1', ''); nazwa2 = getattr(ulica, 'Nazwa2', '')
                        symbol_ulic = getattr(ulica, 'Symbol', ''); id_ulic = getattr(ulica, 'Identyfikator', '')
                        if isinstance(symbol_ulic, str) and symbol_ulic.isdigit():
                             symbol_ulic = symbol_ulic.zfill(5)
                             if len(symbol_ulic) == 5:
                                 all_streets.append({'Nazwa': f"{cecha} {nazwa1} {nazwa2 or ''}".strip(), 'Symbol': symbol_ulic, 'Identyfikator': id_ulic})
                                 seen_ids.add(id_ulic)

        if not all_streets: st.info(f"Nie znaleziono żadnych ulic dla SIMC: {simc_symbol} (TERC: {full_terc_code}).")
        all_streets.sort(key=lambda x: x['Nazwa'])
        return all_streets

    except Fault as f:
        # --- POPRAWKA: Sprawdzenie czy błąd to TypeError dla tej metody ---
        # Jeśli tak, to znaczy, że nazwy argumentów (WojewodztwoId itp.) są jednak złe
        if isinstance(f.detail, etree._Element) and "argument" in f.message.lower():
             st.error(f"Błąd SOAP (prawdopodobnie złe argumenty) przy PobierzListeUlicDlaMiejscowosci: {f.message}")
             st.error("Wygląda na to, że nazwy argumentów WojewodztwoId, PowiatId itd. są niepoprawne wg WSDL.")
        elif "Nie znaleziono ulic" in str(f.message) or "brak danych" in str(f.message).lower() or "nie istnieje" in str(f.message).lower():
             st.info(f"Nie znaleziono żadnych ulic dla SIMC {simc_symbol} / TERC {full_terc_code} (info z API).")
        else: st.error(f"Błąd SOAP podczas pobierania ulic: {f.message}")
        return []
    except TypeError as te: # Złapmy też TypeError bezpośrednio
        st.error(f"Błąd typu (TypeError) podczas wywołania PobierzListeUlicDlaMiejscowosci: {te}")
        st.error("Sprawdź nazwy i typy argumentów (WojewodztwoId, PowiatId, GminaId, GminaRodzaj, MiejscowoscId)!")
        return []
    except Exception as e:
        st.error(f"Nieoczekiwany błąd podczas pobierania ulic: {e}")
        return []


# --- Główna część aplikacji Streamlit ---
# ... (Reszta kodu BEZ ZMIAN - tak jak w poprzedniej odpowiedzi) ...
st.set_page_config(layout="wide")
st.title("Wyszukiwarka Adresów z TERYT")

# --- Inicjalizacja stanu sesji ---
if 'teryt_client' not in st.session_state: st.session_state.teryt_client = None
if 'selected_locality' not in st.session_state: st.session_state.selected_locality = ""
if 'selected_street_name' not in st.session_state: st.session_state.selected_street_name = ""
if 'teryt_streets' not in st.session_state: st.session_state.teryt_streets = []
if 'simc_symbol_found' not in st.session_state: st.session_state.simc_symbol_found = None
if 'terc_code_found' not in st.session_state: st.session_state.terc_code_found = None
if 'postal_code_input' not in st.session_state: st.session_state.postal_code_input = ""
if 'teryt_user_input' not in st.session_state: st.session_state.teryt_user_input = ""
if '_fetched_locality' not in st.session_state: st.session_state._fetched_locality = None # Flaga kontrolna

# --- Panel boczny ---
with st.sidebar:
    st.header("Dane Logowania TERYT")
    teryt_user = st.text_input("Nazwa użytkownika TERYT", value=st.session_state.teryt_user_input, key="teryt_user_widget")
    teryt_pass = st.text_input("Hasło TERYT", type="password", key="teryt_pass_widget")

    st.header("Kod Pocztowy")
    postal_code = st.text_input("Wprowadź kod pocztowy (np. 00-001)", value=st.session_state.postal_code_input, key="postal_code_widget")

    st.session_state.teryt_user_input = teryt_user
    st.session_state.postal_code_input = postal_code

    if st.button("Połącz z TERYT", key="connect_button"):
        if teryt_user and teryt_pass:
            st.session_state.teryt_client = None
            st.session_state.teryt_client = get_teryt_client(teryt_user, teryt_pass)
            st.session_state.terc_code_found = None; st.session_state.simc_symbol_found = None
            st.session_state.teryt_streets = []; st.session_state.selected_street_name = ""
            st.session_state._fetched_locality = None
            st.rerun()
        else:
            st.warning("Wprowadź nazwę użytkownika i hasło TERYT.")
            st.session_state.teryt_client = None

# Pobierz klienta ze stanu sesji
client = st.session_state.teryt_client

# --- Wczytanie danych CSV ---
df_kody = load_csv_data(CSV_FILE_PATH, CSV_ENCODING, CSV_SEPARATOR)

# --- Logika aplikacji ---
selected_locality_data = None
localities_list = [""]
filtered_df = pd.DataFrame()

# Reset stanu jeśli zmieniono kod pocztowy
if postal_code != st.session_state.get("_last_postal_code", None):
     if st.session_state.selected_locality != "":
        st.session_state.selected_locality = ""
        st.session_state.selected_street_name = ""
        st.session_state.teryt_streets = []
        st.session_state.simc_symbol_found = None
        st.session_state.terc_code_found = None
        st.session_state._fetched_locality = None
        st.rerun()
st.session_state._last_postal_code = postal_code

# Logika wczytywania miejscowości z CSV
if df_kody is not None and postal_code:
    postal_code_csv = postal_code.strip()
    if len(postal_code_csv) == 6 and postal_code_csv[2] == '-': pass
    elif len(postal_code_csv) == 5 and postal_code_csv.isdigit():
         postal_code_csv = f"{postal_code_csv[:2]}-{postal_code_csv[2:]}"
    else: postal_code_csv = None

    if postal_code_csv and 'PNA' in df_kody.columns:
        try:
            filtered_df = df_kody[df_kody['PNA'] == postal_code_csv].copy()
            if not filtered_df.empty:
                unique_localities = sorted(filtered_df['MIEJSCOWOŚĆ'].drop_duplicates().dropna().tolist())
                localities_list = [""] + unique_localities
            else: localities_list = [""]
        except Exception as e:
            st.error(f"Błąd podczas filtrowania CSV dla kodu {postal_code_csv}: {e}"); localities_list = [""]
    elif 'PNA' not in df_kody.columns and df_kody is not None:
        st.error("W pliku CSV brakuje kolumny 'PNA'."); localities_list = [""]

# --- Wybór miejscowości ---
st.header("1. Wybierz Miejscowość")
selected_locality_prev = st.session_state.selected_locality
st.session_state.selected_locality = st.selectbox(
     "Miejscowość:", options=localities_list, key="locality_selector_widget",
     index=localities_list.index(st.session_state.selected_locality) if st.session_state.selected_locality in localities_list else 0
)
selected_locality = st.session_state.selected_locality

# Reset danych TERYT jeśli zmieniono miejscowość
if selected_locality != selected_locality_prev:
    st.session_state.selected_street_name = ""
    st.session_state.teryt_streets = []
    st.session_state.simc_symbol_found = None
    st.session_state.terc_code_found = None
    st.session_state._fetched_locality = None
    st.rerun()

# --- Wyświetlanie danych z CSV ---
display_woj = "Brak danych"; display_pow = "Brak danych"; display_gmi = "Brak danych"
if selected_locality and not filtered_df.empty:
    matching_rows = filtered_df.loc[filtered_df['MIEJSCOWOŚĆ'] == selected_locality]
    if not matching_rows.empty:
         selected_locality_data = matching_rows.iloc[0]
         display_woj = selected_locality_data.get('WOJEWÓDZTWO', 'Brak danych')
         display_pow = selected_locality_data.get('POWIAT', 'Brak danych')
         display_gmi = selected_locality_data.get('GMINA', 'Brak danych')

col1, col2, col3 = st.columns(3)
with col1: st.metric("Województwo", display_woj)
with col2: st.metric("Powiat", display_pow)
with col3: st.metric("Gmina", display_gmi)

# --- Logika Interakcji z TERYT ---
needs_teryt_fetch = (
    selected_locality and client and
    (st.session_state.get("_fetched_locality", None) != selected_locality)
)

if needs_teryt_fetch:
    st.info("Pobieranie danych TERYT...")
    st.session_state._fetched_locality = selected_locality
    woj_name = display_woj if display_woj != "Brak danych" else None
    pow_name = display_pow if display_pow != "Brak danych" else None
    gmi_name = display_gmi if display_gmi != "Brak danych" else None

    st.session_state.terc_code_found = None; st.session_state.simc_symbol_found = None; st.session_state.teryt_streets = []

    # Krok 1: Znajdź TERC (wersja z WyszukajJPT(nazwa=...) i filtrowaniem)
    st.session_state.terc_code_found = find_terc_code(client, woj_name, pow_name, gmi_name)

    # Krok 2: Znajdź SIMC
    st.session_state.simc_symbol_found = find_simc_symbol(client, selected_locality, st.session_state.terc_code_found)

    # Krok 3: Pobierz ulice
    if st.session_state.terc_code_found and st.session_state.simc_symbol_found:
        st.session_state.teryt_streets = get_streets(client, st.session_state.terc_code_found, st.session_state.simc_symbol_found)

    # Nie ma st.rerun()

# --- Wyświetlanie statusu TERYT ---
st.subheader("Status TERYT")
if selected_locality and client:
     terc_to_display = st.session_state.terc_code_found
     simc_to_display = st.session_state.simc_symbol_found
     if terc_to_display: st.success(f"Znaleziony TERC gminy: {terc_to_display}")
     # Komunikaty ostrzegawcze/błędu są już w funkcji find_terc_code

     if simc_to_display: st.success(f"Znaleziony SIMC miejscowości: {simc_to_display}")
     # Komunikaty ostrzegawcze/błędu są już w funkcji find_simc_symbol

     if simc_to_display and not terc_to_display:
          st.warning("Brak kodu TERC gminy uniemożliwia pobranie listy ulic.")
elif selected_locality and not client and st.session_state.teryt_user_input:
     st.warning("Połączenie z TERYT nie jest aktywne.")
elif selected_locality:
     st.info("Połącz z TERYT, aby pobrać dane SIMC i listę ulic.")


# --- Wybór ulicy ---
st.header("2. Wybierz Ulicę (z TERYT)")
street_options_display = [""]
teryt_streets_from_state = st.session_state.teryt_streets

if teryt_streets_from_state:
    street_options_display.extend(sorted([s['Nazwa'] for s in teryt_streets_from_state]))
elif st.session_state.simc_symbol_found and st.session_state.terc_code_found and client:
    street_options_display = ["", "Brak ulic w TERYT dla tej miejscowości"]

selected_street_prev = st.session_state.selected_street_name
st.session_state.selected_street_name = st.selectbox(
     "Ulica:", options=street_options_display, key="street_selector_widget",
     index=street_options_display.index(st.session_state.selected_street_name) if st.session_state.selected_street_name in street_options_display else 0,
     disabled=(not teryt_streets_from_state)
)
selected_street_name = st.session_state.selected_street_name

# --- Wyświetlanie symbolu ulicy ---
selected_street_symbol = None; selected_street_id = None
valid_street_selected = selected_street_name and selected_street_name != "" and selected_street_name != "Brak ulic w TERYT dla tej miejscowości"

if valid_street_selected and teryt_streets_from_state:
    selected_street_info = next((s for s in teryt_streets_from_state if s['Nazwa'] == selected_street_name), None)
    if selected_street_info:
        selected_street_symbol = selected_street_info['Symbol']
        selected_street_id = selected_street_info['Identyfikator']
        st.metric("Symbol ULIC", selected_street_symbol)
        st.caption(f"Identyfikator ULIC: {selected_street_id}")

# --- Wprowadzanie numeru domu/mieszkania ---
st.header("3. Wprowadź Numer Domu / Mieszkania")
col_nr1, col_nr2 = st.columns(2)
with col_nr1: house_number = st.text_input("Numer domu", key="house_no")
with col_nr2: apartment_number = st.text_input("Numer mieszkania (opcjonalnie)", key="apt_no")

# --- Podsumowanie ---
st.header("Podsumowanie Adresu")
if selected_locality:
    terc_to_display = st.session_state.terc_code_found
    simc_to_display = st.session_state.simc_symbol_found

    address_parts = [
        f"Kod pocztowy: {st.session_state.postal_code_input}" if st.session_state.postal_code_input else None,
        f"Miejscowość: {selected_locality}",
        f"Ulica: {selected_street_name}" if valid_street_selected else None,
        f"Nr domu: {house_number}" if house_number else None,
        f"Nr mieszkania: {apartment_number}" if apartment_number else None,
        f"Gmina: {display_gmi}" if display_gmi != "Brak danych" else None,
        f"Powiat: {display_pow}" if display_pow != "Brak danych" else None,
        f"Województwo: {display_woj}" if display_woj != "Brak danych" else None,
        f"Symbol SIMC: {simc_to_display}" if simc_to_display else None,
        f"Pełny TERC gminy: {terc_to_display}" if terc_to_display else None,
        f"Symbol ULIC: {selected_street_symbol}" if selected_street_symbol else None,
    ]
    final_address = "\n".join(filter(None, address_parts))
    st.text_area("Zebrane dane:", final_address, height=270)
else:
    st.info("Wprowadź kod pocztowy i wybierz miejscowość, aby rozpocząć.")