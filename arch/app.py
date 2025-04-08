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
import traceback # Import do pełnego tracebacku

# Konfiguracja logowania dla Zeep (opcjonalnie, do debugowania)
# logging.basicConfig(level=logging.DEBUG)
# logging.getLogger('zeep.transports').setLevel(logging.DEBUG)
# logging.getLogger('zeep.wsdl').setLevel(logging.DEBUG)

# --- Konfiguracja ---
WSDL_URL = 'https://uslugaterytws1.stat.gov.pl/wsdl/terytws1.wsdl'
CSV_FILE_PATH = 'kody_pocztowe.csv'
CSV_ENCODING = 'utf-8'
CSV_SEPARATOR = ';'
TODAY_DATE_STR = date.today().strftime('%Y-%m-%d') # Używane w wielu funkcjach API

# --- Funkcje pomocnicze ---

@st.cache_resource(ttl=3600)
def get_teryt_client(username, password):
    """Inicjalizuje i zwraca klienta SOAP TERYT z uwierzytelnieniem WS-Security UsernameToken."""
    st.write("Próba połączenia z TERYT...")
    try:
        session = Session()
        transport = Transport(session=session)
        wsse = UsernameToken(username, password, use_digest=False)
        transport.operation_timeout = 180
        transport.timeout = 180

        try:
            from zeep.cache import SqliteCache
            client = Client(wsdl=WSDL_URL, transport=transport, wsse=wsse, cache=SqliteCache(timeout=3600))
            st.info("Użyto cache Zeep (Sqlite).")
        except ImportError:
            st.warning("Brak SqliteCache, tworzenie klienta bez cache.")
            client = Client(wsdl=WSDL_URL, transport=transport, wsse=wsse)
        except Exception as cache_e:
            st.warning(f"Problem z cache Zeep: {cache_e}. Tworzenie klienta bez cache.")
            client = Client(wsdl=WSDL_URL, transport=transport, wsse=wsse)

        try:
            with st.spinner("Testowanie połączenia..."):
                is_logged_in = client.service.CzyZalogowany()
            if is_logged_in:
                 st.success(f"Połączono i uwierzytelniono (Zalogowany: {is_logged_in}).")
                 return client
            else:
                 st.error("Uwierzytelnienie nie powiodło się (CzyZalogowany zwrócił False). Sprawdź dane logowania.")
                 return None
        except Fault as f:
            st.error(f"Błąd SOAP po połączeniu (CzyZalogowany): {f.message}")
            if f.detail is not None:
                st.error(f"Szczegóły błędu SOAP: {etree.tostring(f.detail, pretty_print=True).decode()}")
            return None
        except Exception as conn_test_e:
            st.error(f"Błąd podczas testu połączenia (CzyZalogowany): {conn_test_e}")
            return None

    except Exception as e:
        st.error(f"Krytyczny błąd inicjalizacji klienta TERYT: {e}")
        return None

@st.cache_data
def load_csv_data(file_path, encoding, separator):
    """Wczytuje dane z pliku CSV."""
    if not os.path.exists(file_path):
        st.error(f"Nie znaleziono pliku CSV: {file_path}")
        return None
    try:
        df = pd.read_csv(file_path, sep=separator, encoding=encoding, dtype=str)
        df.columns = df.columns.str.strip()
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()
        st.success(f"Wczytano dane z {file_path}.")
        return df
    except pd.errors.ParserError as pe:
        st.error(f"Błąd parsowania CSV ({file_path}): {pe}. Sprawdź separator ('{separator}') i kodowanie ('{encoding}').")
        return None
    except FileNotFoundError:
        st.error(f"Nie znaleziono pliku CSV: {file_path}")
        return None
    except Exception as e:
        st.error(f"Błąd wczytywania CSV ({file_path}): {e}")
        return None

def normalize_name(name):
    """Pomocnik do normalizacji nazw przed porównaniem."""
    if not isinstance(name, str):
        return ""
    name = name.lower().strip()
    name = re.sub(r"^(województwo|powiat|gmina|gm\.?|miasto|m\.?\s*st\.?|obszar wiejski)\s+", "", name, flags=re.IGNORECASE).strip()
    name = re.sub(r"\(.*\)", "", name).strip()
    name = re.sub(r"\s+-.*$", "", name).strip()
    name = re.sub(r"\s+", " ", name).strip()
    name = name.replace('m.st. ', '')
    return name

# --- Funkcja find_terc_code (poprawiona w poprzednich krokach) ---
def find_terc_code(client, woj_name, pow_name, gmi_name):
    """Wyszukuje kod TERC gminy (7 cyfr) na podstawie nazw, używając metod PobierzListe*."""
    if not client: return None
    if not woj_name or not pow_name or not gmi_name:
        st.warning("Brak pełnych nazw (województwo, powiat, gmina) do wyszukania TERC.")
        return None

    norm_woj_name = normalize_name(woj_name)
    norm_pow_name = normalize_name(pow_name)
    norm_gmi_name = normalize_name(gmi_name)
    norm_gmi_name_alt = re.sub(r"^gm\.?\s+", "", norm_gmi_name).strip()

    #st.info(f"Szukam TERC dla (norm): Woj='{norm_woj_name}', Pow='{norm_pow_name}', Gmi='{norm_gmi_name}'/'{norm_gmi_name_alt}' [Metoda: PobierzListe*]")

    woj_symbol = None
    pow_symbol = None
    gmi_symbol = None
    rodz_symbol = None
    full_terc = None

    try:
        # Krok 1: Znajdź WOJ
        #st.write("--- Etap 1: Wyszukiwanie Województwa ---")
        with st.spinner(f"1. Pobieranie listy województw..."):
            woj_list_raw = client.service.PobierzListeWojewodztw(DataStanu=TODAY_DATE_STR)
        if not woj_list_raw: st.error("Nie udało się pobrać listy województw."); return None
        found_woj = None
        #st.write(f"[DEBUG] Szukam województwa pasującego do (z CSV, norm): '{norm_woj_name}'")
        for woj in woj_list_raw:
            current_woj_name_raw = getattr(woj, 'NAZWA', '')
            current_woj_name_norm = normalize_name(current_woj_name_raw)
            #st.write(f"[DEBUG Porównanie WOJ] API: '{current_woj_name_norm}' (raw: '{current_woj_name_raw}') vs CSV: '{norm_woj_name}'")
            if current_woj_name_norm == norm_woj_name: found_woj = woj; #st.write(f"   -> Dopasowano!");
            break
        if found_woj:
            woj_symbol_candidate = getattr(found_woj, 'WOJ', None)
            if woj_symbol_candidate and str(woj_symbol_candidate).isdigit(): woj_symbol = str(woj_symbol_candidate).zfill(2); #st.success(f"Etap 1: OK, WOJ: {woj_symbol} dla '{getattr(found_woj, 'NAZWA', '')}'")
            else: st.error(f"Znaleziono województwo '{getattr(found_woj, 'NAZWA', '')}', ale ma nieprawidłowy symbol: '{woj_symbol_candidate}'."); return None
        else: st.error(f"Nie znaleziono województwa o nazwie (po normalizacji): '{norm_woj_name}'"); return None

        # Krok 2: Znajdź POW
        #st.write("--- Etap 2: Wyszukiwanie Powiatu ---")
        with st.spinner(f"2. Pobieranie listy powiatów dla woj. {woj_symbol}..."):
            pow_list_raw = client.service.PobierzListePowiatow(Woj=woj_symbol, DataStanu=TODAY_DATE_STR)
        if not pow_list_raw: st.error(f"Nie udało się pobrać listy powiatów dla województwa {woj_symbol}."); return None
        found_pow = None
        #st.write(f"[DEBUG] Szukam powiatu pasującego do (z CSV, norm): '{norm_pow_name}'")
        for pow_ in pow_list_raw:
            current_pow_name_raw = getattr(pow_, 'NAZWA', '')
            current_pow_name_norm = normalize_name(current_pow_name_raw)
            #st.write(f"[DEBUG Porównanie POW] API: '{current_pow_name_norm}' (raw: '{current_pow_name_raw}') vs CSV: '{norm_pow_name}'")
            if current_pow_name_norm == norm_pow_name: found_pow = pow_; #st.write(f"   -> Dopasowano!");
            break
        if found_pow:
            pow_symbol_candidate = getattr(found_pow, 'POW', None)
            if pow_symbol_candidate and str(pow_symbol_candidate).isdigit(): pow_symbol = str(pow_symbol_candidate).zfill(2); #st.success(f"Etap 2: OK, POW: {pow_symbol} dla '{getattr(found_pow, 'NAZWA', '')}'")
            else: st.error(f"Znaleziono powiat '{getattr(found_pow, 'NAZWA', '')}', ale ma nieprawidłowy symbol: '{pow_symbol_candidate}'."); return None
        else: st.error(f"Nie znaleziono powiatu o nazwie (po normalizacji): '{norm_pow_name}' w woj. {woj_symbol}"); return None

        # Krok 3: Znajdź GMI + RODZ
        #st.write("--- Etap 3: Wyszukiwanie Gminy ---")
        with st.spinner(f"3. Pobieranie listy gmin dla pow. {woj_symbol}{pow_symbol}..."):
            gmi_list_raw = client.service.PobierzListeGmin(Woj=woj_symbol, Pow=pow_symbol, DataStanu=TODAY_DATE_STR)
        if not gmi_list_raw: st.error(f"Nie udało się pobrać listy gmin dla powiatu {woj_symbol}{pow_symbol}."); return None
        found_gmi = None
        possible_matches = []
        #st.write(f"[DEBUG] Szukam gminy pasującej do (z CSV, norm): '{norm_gmi_name}' lub '{norm_gmi_name_alt}'")
        for gmi in gmi_list_raw:
            current_gmi_name_raw = getattr(gmi, 'NAZWA', '')
            current_gmi_name_norm = normalize_name(current_gmi_name_raw)
            #st.write(f"[DEBUG Porównanie GMI] API: '{current_gmi_name_norm}' (raw: '{current_gmi_name_raw}') vs CSV: '{norm_gmi_name}' lub '{norm_gmi_name_alt}'")
            if current_gmi_name_norm == norm_gmi_name or current_gmi_name_norm == norm_gmi_name_alt: #st.write(f"   -> Dopasowano!");
             possible_matches.append(gmi)
        if len(possible_matches) == 1: found_gmi = possible_matches[0]; #st.write(f"Znaleziono jednoznaczne dopasowanie gminy: '{getattr(found_gmi, 'NAZWA', '')}'")
        elif len(possible_matches) > 1: st.warning(f"Znaleziono {len(possible_matches)} gminy pasujące do nazwy '{gmi_name}' (norm: '{norm_gmi_name}'/'{norm_gmi_name_alt}'). Wybieram pierwszą."); found_gmi = possible_matches[0]
        if found_gmi:
            gmi_symbol_candidate = getattr(found_gmi, 'GMI', None)
            rodz_symbol_candidate = getattr(found_gmi, 'RODZ', None)
            if gmi_symbol_candidate and str(gmi_symbol_candidate).isdigit() and rodz_symbol_candidate and str(rodz_symbol_candidate).isdigit():
                gmi_symbol = str(gmi_symbol_candidate).zfill(2); rodz_symbol = str(rodz_symbol_candidate)
                full_terc = f"{woj_symbol}{pow_symbol}{gmi_symbol}{rodz_symbol}"
                st.success(f"Etap 3: OK, TERC: {full_terc} dla '{getattr(found_gmi, 'NAZWA', '')}'")
            else: st.error(f"Znaleziono gminę '{getattr(found_gmi, 'NAZWA', '')}', ale ma nieprawidłowe symbole GMI/RODZ: GMI='{gmi_symbol_candidate}', RODZ='{rodz_symbol_candidate}'."); return None
        else: st.error(f"Nie znaleziono gminy o nazwie (po normalizacji): '{norm_gmi_name}' lub '{norm_gmi_name_alt}' w pow. {woj_symbol}{pow_symbol}"); return None

    except Fault as f:
        st.error(f"Błąd SOAP podczas wyszukiwania TERC (PobierzListe*): {f.message}")
        if f.detail is not None:
             st.error(f"Szczegóły błędu SOAP: {etree.tostring(f.detail, pretty_print=True).decode()}")
        return None
    except TypeError as te:
        st.error(f"Błąd typu (TypeError) podczas wyszukiwania TERC (PobierzListe*): {te}. Sprawdź nazwy i typy argumentów przekazywanych do API (np. Woj, Pow, DataStanu).")
        st.error(traceback.format_exc())
        return None
    except Exception as e:
        st.error(f"Nieoczekiwany błąd podczas wyszukiwania TERC (PobierzListe*): {e}")
        st.error(traceback.format_exc())
        return None

    if full_terc and isinstance(full_terc, str) and len(full_terc) == 7 and full_terc.isdigit(): return full_terc
    else: st.warning(f"Ostateczny kod TERC '{full_terc}' ma nieprawidłowy format lub nie został znaleziony."); return None

# --- Funkcja find_simc_symbol (poprawiona w poprzednich krokach) ---
def find_simc_symbol(client, locality_name, terc_gmi_full):
    """Wyszukuje symbol SIMC miejscowości (7 cyfr) na podstawie nazwy.
       Najpierw próbuje PobierzListeMiejscowosciWGminie, a w razie niepowodzenia
       używa WyszukajMiejscowosc jako fallback."""
    if not client: return None
    if not locality_name:
        st.warning("Brak nazwy miejscowości do wyszukania SIMC.")
        return None

    simc_symbol = None
    found_locality = None
    norm_locality_name = normalize_name(locality_name)
    locality_display_name = "???" # Domyślna wartość dla komunikatu sukcesu

    # --- PRÓBA 1: Użycie PobierzListeMiejscowosciWGminie (metoda preferowana) ---
    if terc_gmi_full and len(terc_gmi_full) == 7 and terc_gmi_full.isdigit():
        try:
            woj_id = terc_gmi_full[0:2]
            pow_id = terc_gmi_full[2:4]
            gmi_id = terc_gmi_full[4:6]

            #st.info(f"Szukam SIMC dla (norm): '{norm_locality_name}' w gminie TERC: {terc_gmi_full} [Metoda 1: PobierzListeMiejscowosciWGminie]")

            miejsc_list_raw = None
            with st.spinner(f"Pobieranie listy miejscowości dla gminy {terc_gmi_full} (W:{woj_id} P:{pow_id} G:{gmi_id})..."):
                miejsc_list_raw = client.service.PobierzListeMiejscowosciWGminie(
                    Wojewodztwo=woj_id, Powiat=pow_id, Gmina=gmi_id, DataStanu=TODAY_DATE_STR
                )

            if miejsc_list_raw:
                #st.write(f"--- Analiza wyników dla Gminy {terc_gmi_full} (Metoda 1) ---")
                #st.write(f"[DEBUG] Szukam miejscowości pasującej do (z CSV, norm): '{norm_locality_name}'")
                for m in miejsc_list_raw:
                    current_m_name_raw = getattr(m, 'NAZWA', '')
                    current_m_name_norm = normalize_name(current_m_name_raw)
                    current_m_simc = getattr(m, 'Symbol', '')
                    #st.write(f"[DEBUG Porównanie MIEJSCOWOŚCI] API: '{current_m_name_norm}' (raw: '{current_m_name_raw}', SIMC: '{current_m_simc}') vs CSV: '{norm_locality_name}'")
                    if current_m_name_norm == norm_locality_name:
                        found_locality = m
                        locality_display_name = current_m_name_raw # Zapisz oryginalną nazwę
                        #st.write(f"   -> Dopasowano (Metoda 1)!")
                        break
            else:
                 st.warning(f"Metoda 1: Nie znaleziono żadnych miejscowości w TERYT dla gminy TERC: {terc_gmi_full}.")

        except Fault as f: st.warning(f"Metoda 1: Błąd SOAP podczas wyszukiwania SIMC dla gminy '{terc_gmi_full}': {f.message}")
        except TypeError as te: st.warning(f"Metoda 1: Błąd typu (TypeError) podczas wywołania API dla SIMC w gminie '{terc_gmi_full}': {te}.")
        except Exception as e: st.warning(f"Metoda 1: Nieoczekiwany błąd podczas wyszukiwania SIMC w gminie '{terc_gmi_full}': {e}")

    # --- PRÓBA 2: Fallback używający WyszukajMiejscowosc (jeśli metoda 1 zawiodła) ---
    if not found_locality:
        st.warning(f"Metoda 1 nie znalazła miejscowości '{locality_name}'. Próbuję metody zapasowej...")
        #st.info(f"Szukam SIMC dla (norm): '{norm_locality_name}' [Metoda 2: WyszukajMiejscowosc]")
        try:
            miejsc_result_general = None
            with st.spinner(f"Wyszukiwanie miejscowości '{locality_name}' (Metoda 2)..."):
                miejsc_result_general = client.service.WyszukajMiejscowosc(nazwaMiejscowosci=locality_name)

            if not miejsc_result_general: st.error(f"Metoda 2: Nie znaleziono miejscowości '{locality_name}' w TERYT (wg WyszukajMiejscowosc)."); return None

            #st.write(f"--- Analiza wyników dla Miejscowości '{locality_name}' (Metoda 2) ---")
            #st.write(f"[DEBUG] Szukam miejscowości pasującej do (z CSV, norm): '{norm_locality_name}'")
            for i, m in enumerate(miejsc_result_general):
                m_nazwa_raw = getattr(m, 'Nazwa', '')
                m_nazwa_norm = normalize_name(m_nazwa_raw)
                m_sym = getattr(m, 'Symbol', '')
                m_woj = str(getattr(m, 'WojSymbol', '')).zfill(2); m_pow = str(getattr(m, 'PowSymbol', '')).zfill(2); m_gmi = str(getattr(m, 'GmiSymbol', '')).zfill(2); m_rodz = str(getattr(m, 'GmiRodzaj', ''))
                #st.write(f" Wynik {i+1}: Nazwa='{m_nazwa_raw}' (Norm: '{m_nazwa_norm}'), Symbol='{m_sym}', TERC wg API='{m_woj}{m_pow}{m_gmi}{m_rodz}'")
                if m_nazwa_norm == norm_locality_name:
                    st.warning(f"Metoda 2: Znaleziono miejscowość '{m_nazwa_raw}' pasującą po nazwie. Używam tego wyniku jako fallback (TERC z API może być niespójny).")
                    found_locality = m
                    locality_display_name = m_nazwa_raw # Zapisz oryginalną nazwę
                    #st.write(f"   -> Dopasowano (Metoda 2)!")
                    break
            if not found_locality: st.error(f"Metoda 2: Znaleziono wyniki dla '{locality_name}', ale żaden nie pasował dokładnie po normalizacji nazwy do '{norm_locality_name}'.")

        except Fault as f:
             # --- POPRAWKA SKŁADNI ---
             st.error(f"Metoda 2: Błąd SOAP podczas wyszukiwania SIMC dla '{locality_name}': {f.message}")
             if f.detail is not None:
                 st.error(f"Szczegóły błędu SOAP: {etree.tostring(f.detail, pretty_print=True).decode()}")
             # --- KONIEC POPRAWKI SKŁADNI ---
        except TypeError as te: st.error(f"Metoda 2: Błąd typu (TypeError) podczas wywołania API WyszukajMiejscowosc dla '{locality_name}': {te}."); st.error(traceback.format_exc())
        except Exception as e: st.error(f"Metoda 2: Nieoczekiwany błąd podczas wyszukiwania SIMC dla '{locality_name}': {e}"); st.error(traceback.format_exc())

    # --- Przetwarzanie wyniku (jeśli znaleziono przez którąkolwiek metodę) ---
    if found_locality:
        simc_symbol_candidate = getattr(found_locality, 'Symbol', None)
        if simc_symbol_candidate:
            simc_symbol = str(simc_symbol_candidate).zfill(7)
            if len(simc_symbol) == 7 and simc_symbol.isdigit():
                 st.success(f"=> Znaleziono SIMC: {simc_symbol} dla '{locality_display_name}'")
            else: st.warning(f"Znaleziony symbol SIMC '{simc_symbol}' dla miejscowości '{locality_display_name}' ma nieprawidłowy format."); simc_symbol = None
        else: st.warning(f"Znaleziona miejscowość '{locality_display_name}' nie ma atrybutu 'Symbol'.")

    # Ostateczne sprawdzenie przed zwróceniem
    if simc_symbol and isinstance(simc_symbol, str) and len(simc_symbol) == 7 and simc_symbol.isdigit(): return simc_symbol
    else:
        if locality_name: st.error(f"Nie udało się ostatecznie znaleźć poprawnego symbolu SIMC dla miejscowości: '{locality_name}'.")
        return None

# --- Funkcja get_streets (POPRAWIONA W TYM KROKU) ---
def get_streets(client, full_terc_code, simc_symbol):
    """Pobiera listę ulic dla danej miejscowości używając PobierzListeUlicDlaMiejscowosci."""
    if not client: return []
    if not full_terc_code or not simc_symbol: st.warning("Brak kodu TERC lub symbolu SIMC do pobrania ulic."); return []
    if len(full_terc_code) != 7 or not full_terc_code.isdigit(): st.warning(f"Nieprawidłowy format kodu TERC '{full_terc_code}' do pobrania ulic."); return []
    if len(simc_symbol) != 7 or not simc_symbol.isdigit(): st.warning(f"Nieprawidłowy format symbolu SIMC '{simc_symbol}' do pobrania ulic."); return []

    all_streets = []
    try:
        # Rozpakowanie kodu TERC na części
        woj_id = full_terc_code[0:2]
        pow_id = full_terc_code[2:4]
        gmi_id = full_terc_code[4:6]
        rodz_id = full_terc_code[6:7]

        #st.info(f"Pobieram ulice dla TERC={full_terc_code}, SIMC={simc_symbol} [Metoda: PobierzListeUlicDlaMiejscowosci]")
        with st.spinner(f"Pobieranie ulic dla SIMC: {simc_symbol}..."):
            # Użycie nazw parametrów małymi literami + flagi boolean
            streets_result = client.service.PobierzListeUlicDlaMiejscowosci(
                woj=woj_id,
                pow=pow_id,
                gmi=gmi_id,
                rodzaj=rodz_id,
                msc=simc_symbol,
                czyWersjaUrzedowa=False, # Chcemy wersję adresową
                czyWersjaAdresowa=True,
                DataStanu=TODAY_DATE_STR
            )

            # Sprawdzenie, czy wynik zawiera listę ulic
            ulice_list = None
            if streets_result:
                 if isinstance(streets_result, list): ulice_list = streets_result
                 elif hasattr(streets_result, 'Ulica') and isinstance(getattr(streets_result, 'Ulica', None), list): ulice_list = streets_result.Ulica
                 elif hasattr(streets_result, 'element') and isinstance(getattr(streets_result, 'element', None), list): ulice_list = streets_result.element
                 else: st.warning(f"Nie rozpoznano struktury odpowiedzi z PobierzListeUlicDlaMiejscowosci: {type(streets_result)}")

            if ulice_list:
                seen_ids = set()
                for ulica in ulice_list:
                    if not hasattr(ulica, 'Identyfikator') or not getattr(ulica, 'Identyfikator', None): continue
                    id_ulic = getattr(ulica, 'Identyfikator')
                    if id_ulic not in seen_ids:
                        cecha = getattr(ulica, 'Cecha', ''); nazwa1 = getattr(ulica, 'Nazwa1', ''); nazwa2 = getattr(ulica, 'Nazwa2', '')
                        symbol_ulic_candidate = getattr(ulica, 'Symbol', '')
                        full_street_name = f"{cecha} {nazwa1}".strip();
                        if nazwa2: full_street_name += f" {nazwa2}"
                        full_street_name = re.sub(r'\s+', ' ', full_street_name).strip()
                        symbol_ulic = None
                        if symbol_ulic_candidate and isinstance(symbol_ulic_candidate, str) and symbol_ulic_candidate.isdigit():
                            symbol_ulic = symbol_ulic_candidate.zfill(5)
                            if len(symbol_ulic) != 5: symbol_ulic = None
                        if full_street_name and symbol_ulic:
                            all_streets.append({'Nazwa': full_street_name, 'Symbol': symbol_ulic, 'Identyfikator': id_ulic})
                            seen_ids.add(id_ulic)

        if not all_streets: st.info(f"Nie znaleziono żadnych ulic (z poprawnymi danymi) dla SIMC: {simc_symbol} (TERC: {full_terc_code}).")
        all_streets.sort(key=lambda x: x['Nazwa'])
        st.success(f"Pobrano {len(all_streets)} ulic.")
        return all_streets

    except Fault as f:
        fault_msg = str(f.message).lower(); fault_detail_str = etree.tostring(f.detail, pretty_print=True).decode().lower() if f.detail is not None else ""
        if "argument" in fault_msg or "parameter" in fault_msg or "required element" in fault_detail_str:
             st.error(f"Błąd SOAP (prawdopodobnie złe argumenty) przy PobierzListeUlicDlaMiejscowosci: {f.message}")
             st.error("Sprawdź, czy nazwy parametrów (woj, pow, gmi, rodzaj, msc, czyWersja*, DataStanu) są zgodne z definicją WSDL.")
             if f.detail is not None: st.error(f"Szczegóły błędu SOAP: {fault_detail_str}")
        elif "nie znaleziono ulic" in fault_msg or "brak danych" in fault_msg or "nie istnieje" in fault_msg: st.info(f"API TERYT poinformowało o braku ulic dla SIMC {simc_symbol} / TERC {full_terc_code}.")
        else:
            # --- POPRAWKA SKŁADNI ---
            st.error(f"Błąd SOAP podczas pobierania ulic: {f.message}")
            if f.detail is not None:
                st.error(f"Szczegóły błędu SOAP: {fault_detail_str}")
            # --- KONIEC POPRAWKI SKŁADNI ---
        return []
    except TypeError as te:
        st.error(f"Błąd typu (TypeError) podczas wywołania PobierzListeUlicDlaMiejscowosci: {te}")
        st.error("To zazwyczaj oznacza błąd w nazwach lub typach przekazywanych parametrów. Sprawdź: woj, pow, gmi, rodzaj, msc, czyWersjaUrzedowa, czyWersjaAdresowa, DataStanu.")
        st.error(traceback.format_exc())
        return []
    except Exception as e:
        st.error(f"Nieoczekiwany błąd podczas pobierania ulic: {e}")
        st.error(traceback.format_exc())
        return []


# --- Główna część aplikacji Streamlit (bez zmian) ---
st.set_page_config(layout="wide")
st.title("Wyszukiwarka Adresów z TERYT")

# --- Inicjalizacja stanu sesji ---
if '_teryt_client' not in st.session_state: st.session_state._teryt_client = None
if '_selected_locality' not in st.session_state: st.session_state._selected_locality = ""
if '_selected_street_name' not in st.session_state: st.session_state._selected_street_name = ""
if '_teryt_streets' not in st.session_state: st.session_state._teryt_streets = []
if '_simc_symbol_found' not in st.session_state: st.session_state._simc_symbol_found = None
if '_terc_code_found' not in st.session_state: st.session_state._terc_code_found = None
if '_postal_code_input' not in st.session_state: st.session_state._postal_code_input = ""
if '_teryt_user_input' not in st.session_state: st.session_state._teryt_user_input = ""
if '_last_processed_locality' not in st.session_state: st.session_state._last_processed_locality = None
if '_last_postal_code' not in st.session_state: st.session_state._last_postal_code = None

# --- Panel boczny ---
with st.sidebar:
    st.header("Dane Logowania TERYT")
    teryt_user = st.text_input("Nazwa użytkownika TERYT", value=st.session_state._teryt_user_input, key="teryt_user_widget")
    teryt_pass = st.text_input("Hasło TERYT", type="password", key="teryt_pass_widget")
    st.header("Kod Pocztowy")
    postal_code = st.text_input("Wprowadź kod pocztowy (np. 00-001)", value=st.session_state._postal_code_input, key="postal_code_widget")
    st.session_state._teryt_user_input = teryt_user
    st.session_state._postal_code_input = postal_code
    if st.button("Połącz / Odśwież Połączenie", key="connect_button"):
        if teryt_user and teryt_pass:
            st.session_state._teryt_client = None; st.session_state._terc_code_found = None; st.session_state._simc_symbol_found = None
            st.session_state._teryt_streets = []; st.session_state._selected_street_name = ""; st.session_state._last_processed_locality = None
            st.session_state._teryt_client = get_teryt_client(teryt_user, teryt_pass)
            st.rerun()
        else:
            st.warning("Wprowadź nazwę użytkownika i hasło TERYT.")
            if st.session_state._teryt_client is not None: st.session_state._teryt_client = None; st.rerun()

client = st.session_state._teryt_client
with st.sidebar:
    if client: st.success("Połączono z TERYT.")
    elif st.session_state._teryt_user_input: st.error("Brak aktywnego połączenia z TERYT. Sprawdź dane i kliknij 'Połącz'.")
    else: st.info("Wprowadź dane logowania TERYT i kliknij 'Połącz'.")

# --- Wczytanie danych CSV ---
df_kody = load_csv_data(CSV_FILE_PATH, CSV_ENCODING, CSV_SEPARATOR)

# --- Logika aplikacji ---
selected_locality_data = None
localities_list = [""]
filtered_df = pd.DataFrame()

postal_code_changed = (postal_code != st.session_state._last_postal_code)
if postal_code_changed:
    #st.write(f"Zmieniono kod pocztowy na: {postal_code}")
    st.session_state._selected_locality = ""; st.session_state._selected_street_name = ""
    st.session_state._teryt_streets = []; st.session_state._simc_symbol_found = None
    st.session_state._terc_code_found = None; st.session_state._last_processed_locality = None
    st.session_state._last_postal_code = postal_code

if df_kody is not None and postal_code:
    postal_code_csv = postal_code.strip()
    if len(postal_code_csv) == 6 and postal_code_csv[2] == '-': pass
    elif len(postal_code_csv) == 5 and postal_code_csv.isdigit(): postal_code_csv = f"{postal_code_csv[:2]}-{postal_code_csv[2:]}"
    else:
        if postal_code: st.warning(f"Nieprawidłowy format kodu pocztowego: '{postal_code}'. Oczekiwano XX-XXX lub XXXXX.")
        postal_code_csv = None
    if postal_code_csv and 'PNA' in df_kody.columns:
        try:
            filtered_df = df_kody[df_kody['PNA'] == postal_code_csv].copy()
            if not filtered_df.empty:
                if 'MIEJSCOWOŚĆ' in filtered_df.columns:
                    unique_localities = sorted(filtered_df['MIEJSCOWOŚĆ'].drop_duplicates().dropna().tolist())
                    localities_list = [""] + unique_localities
                    if st.session_state._selected_locality not in localities_list:
                         st.session_state._selected_locality = ""
                         st.session_state._selected_street_name = ""; st.session_state._teryt_streets = []
                         st.session_state._simc_symbol_found = None; st.session_state._terc_code_found = None
                         st.session_state._last_processed_locality = None
                else: st.error("W pliku CSV brakuje kolumny 'MIEJSCOWOŚĆ'."); localities_list = [""]
            else:
                #st.info(f"Nie znaleziono miejscowości dla kodu pocztowego: {postal_code_csv}"); localities_list = [""]
                if st.session_state._selected_locality != "":
                    st.session_state._selected_locality = ""; st.session_state._selected_street_name = ""
                    st.session_state._teryt_streets = []; st.session_state._simc_symbol_found = None
                    st.session_state._terc_code_found = None; st.session_state._last_processed_locality = None
        except KeyError as ke: st.error(f"Brak oczekiwanej kolumny w CSV: {ke}"); localities_list = [""]
        except Exception as e: st.error(f"Błąd podczas filtrowania CSV dla kodu {postal_code_csv}: {e}"); localities_list = [""]
    elif postal_code_csv and 'PNA' not in df_kody.columns and df_kody is not None: st.error("W pliku CSV brakuje kolumny 'PNA'."); localities_list = [""]

# --- Wybór miejscowości ---
st.header("1. Wybierz Miejscowość")
selected_locality_prev = st.session_state._selected_locality
try:
    if localities_list: current_locality_index = localities_list.index(st.session_state._selected_locality)
    else: current_locality_index = 0
except ValueError: current_locality_index = 0
st.session_state._selected_locality = st.selectbox(
    "Miejscowość:", options=localities_list, key="locality_selector_widget", index=current_locality_index,
    disabled=(not localities_list or localities_list == [""])
)
selected_locality = st.session_state._selected_locality
locality_changed = (selected_locality != selected_locality_prev)
if locality_changed:
    #st.write(f"Zmieniono miejscowość na: {selected_locality}")
    st.session_state._selected_street_name = ""; st.session_state._teryt_streets = []
    st.session_state._simc_symbol_found = None; st.session_state._terc_code_found = None
    st.session_state._last_processed_locality = None

# --- Wyświetlanie danych z CSV ---
display_woj = "Brak danych"; display_pow = "Brak danych"; display_gmi = "Brak danych"
if selected_locality and not filtered_df.empty:
    required_cols = ['MIEJSCOWOŚĆ', 'WOJEWÓDZTWO', 'POWIAT', 'GMINA']
    if all(col in filtered_df.columns for col in required_cols):
        matching_rows = filtered_df.loc[filtered_df['MIEJSCOWOŚĆ'] == selected_locality]
        if not matching_rows.empty:
            selected_locality_data = matching_rows.iloc[0]
            display_woj = selected_locality_data.get('WOJEWÓDZTWO', 'Brak danych')
            display_pow = selected_locality_data.get('POWIAT', 'Brak danych')
            display_gmi = selected_locality_data.get('GMINA', 'Brak danych')
        # else: st.warning(f"Nie znaleziono danych w przefiltrowanym DataFrame dla wybranej miejscowości: {selected_locality}") # Mniej gadatliwe
    else: st.error(f"W pliku CSV brakuje wymaganych kolumn: {', '.join(required_cols)}")
col1, col2, col3 = st.columns(3)
with col1: st.metric("Województwo (z CSV)", display_woj)
with col2: st.metric("Powiat (z CSV)", display_pow)
with col3: st.metric("Gmina (z CSV)", display_gmi)

# --- Logika Interakcji z TERYT ---
needs_teryt_fetch = (selected_locality and client and (st.session_state._last_processed_locality != selected_locality))
if needs_teryt_fetch:
    st.info(f"Rozpoczynanie pobierania danych TERYT dla: {selected_locality}...")
    st.session_state._last_processed_locality = selected_locality
    woj_name = display_woj if display_woj != "Brak danych" else None
    pow_name = display_pow if display_pow != "Brak danych" else None
    gmi_name = display_gmi if display_gmi != "Brak danych" else None
    st.session_state._terc_code_found = None; st.session_state._simc_symbol_found = None
    st.session_state._teryt_streets = []; st.session_state._selected_street_name = ""
    if woj_name and pow_name and gmi_name:
        st.session_state._terc_code_found = find_terc_code(client, woj_name, pow_name, gmi_name)
        st.session_state._simc_symbol_found = find_simc_symbol(client, selected_locality, st.session_state._terc_code_found)
        if st.session_state._terc_code_found and st.session_state._simc_symbol_found:
            st.session_state._teryt_streets = get_streets(client, st.session_state._terc_code_found, st.session_state._simc_symbol_found)
        elif st.session_state._terc_code_found: st.warning("Nie udało się znaleźć symbolu SIMC, nie można pobrać ulic.")
        else: st.warning("Nie udało się znaleźć kodu TERC, nie można pobrać symbolu SIMC ani ulic.")
    else: st.warning("Brak pełnych danych adresowych (woj/pow/gmi) z pliku CSV, aby wyszukać dane TERYT.")
    st.info("Zakończono pobieranie danych TERYT.")
    #st.rerun()

# --- Wyświetlanie statusu TERYT ---
st.subheader("Status Danych TERYT")
if selected_locality:
    if client:
        terc_to_display = st.session_state._terc_code_found; simc_to_display = st.session_state._simc_symbol_found
        if terc_to_display: st.success(f"Znaleziony TERC gminy: {terc_to_display}")
        elif st.session_state._last_processed_locality == selected_locality: st.warning("Nie udało się znaleźć kodu TERC gminy w TERYT.")
        if simc_to_display: st.success(f"Znaleziony SIMC miejscowości: {simc_to_display}")
        elif st.session_state._last_processed_locality == selected_locality: st.warning("Nie udało się znaleźć symbolu SIMC miejscowości w TERYT.")
    elif st.session_state._teryt_user_input: st.warning("Połączenie z TERYT nie jest aktywne. Sprawdź dane i kliknij 'Połącz'.")
    else: st.info("Połącz z TERYT (w panelu bocznym), aby pobrać dane SIMC i listę ulic.")

# --- Wybór ulicy ---
st.header("2. Wybierz Ulicę (z TERYT)")
street_options_display = [""]
teryt_streets_from_state = st.session_state._teryt_streets
if teryt_streets_from_state: street_options_display.extend(sorted([s['Nazwa'] for s in teryt_streets_from_state]))
elif st.session_state._simc_symbol_found and st.session_state._terc_code_found and client and st.session_state._last_processed_locality == selected_locality: street_options_display = ["", "Brak ulic w TERYT dla tej miejscowości"]
try:
    if street_options_display: current_street_index = street_options_display.index(st.session_state._selected_street_name)
    else: current_street_index = 0
except ValueError: current_street_index = 0
st.session_state._selected_street_name = st.selectbox(
    "Ulica:", options=street_options_display, key="street_selector_widget", index=current_street_index,
    disabled=(len(street_options_display) <= 1 or (len(street_options_display) == 2 and street_options_display[1].startswith("Brak ulic")))
)
selected_street_name = st.session_state._selected_street_name

# --- Wyświetlanie symbolu ulicy ---
selected_street_symbol = None; selected_street_id = None
valid_street_selected = selected_street_name and not selected_street_name.startswith("Brak ulic")
if valid_street_selected and teryt_streets_from_state:
    selected_street_info = next((s for s in teryt_streets_from_state if s['Nazwa'] == selected_street_name), None)
    if selected_street_info:
        selected_street_symbol = selected_street_info.get('Symbol'); selected_street_id = selected_street_info.get('Identyfikator')
        if selected_street_symbol: st.metric("Symbol ULIC", selected_street_symbol)
        if selected_street_id: st.caption(f"Identyfikator ULIC: {selected_street_id}")
    elif selected_street_name: st.warning(f"Wybrana poprzednio ulica '{selected_street_name}' nie została znaleziona w aktualnych danych TERYT.")

# --- Wprowadzanie numeru domu/mieszkania ---
st.header("3. Wprowadź Numer Domu / Mieszkania")
col_nr1, col_nr2 = st.columns(2)
with col_nr1: house_number = st.text_input("Numer domu", key="house_no_widget")
with col_nr2: apartment_number = st.text_input("Numer mieszkania (opcjonalnie)", key="apt_no_widget")

# --- Podsumowanie ---
st.header("Podsumowanie Adresu")
if selected_locality:
    terc_to_display = st.session_state._terc_code_found; simc_to_display = st.session_state._simc_symbol_found
    address_parts = []
    if st.session_state._postal_code_input: address_parts.append(f"Kod pocztowy: {st.session_state._postal_code_input}")
    address_parts.append(f"Miejscowość: {selected_locality}")
    if valid_street_selected: address_parts.append(f"Ulica: {selected_street_name}")
    if house_number: address_parts.append(f"Nr domu: {house_number}")
    if apartment_number: address_parts.append(f"Nr mieszkania: {apartment_number}")
    if display_gmi != "Brak danych": address_parts.append(f"Gmina (z CSV): {display_gmi}")
    if display_pow != "Brak danych": address_parts.append(f"Powiat (z CSV): {display_pow}")
    if display_woj != "Brak danych": address_parts.append(f"Województwo (z CSV): {display_woj}")
    if simc_to_display: address_parts.append(f"Symbol SIMC (TERYT): {simc_to_display}")
    if terc_to_display: address_parts.append(f"Pełny TERC gminy (TERYT): {terc_to_display}")
    if selected_street_symbol: address_parts.append(f"Symbol ULIC (TERYT): {selected_street_symbol}")
    if selected_street_id: address_parts.append(f"ID ULIC (TERYT): {selected_street_id}")
    final_address = "\n".join(filter(None, address_parts))
    st.text_area("Zebrane dane:", final_address, height=300)
else: st.info("Wprowadź kod pocztowy i wybierz miejscowość, aby rozpocząć.")

# --- Sekcja Debugowania (opcjonalna) ---
# Usunięto zakomentowane logowanie debugowania, aby kod był czystszy
# with st.expander("Informacje Debugowania Stanu Sesji"):
#      st.json({k: v for k, v in st.session_state.items() if isinstance(v, (str, int, float, bool, list, dict, type(None)))})

