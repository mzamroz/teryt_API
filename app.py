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
        # Zwiększenie timeoutów dla potencjalnie długich operacji
        transport.operation_timeout = 180 # sekundy
        transport.timeout = 180 # sekundy

        # Próba użycia cache, jeśli dostępny
        try:
            from zeep.cache import SqliteCache
            # Zwiększenie timeoutu cache
            client = Client(wsdl=WSDL_URL, transport=transport, wsse=wsse, cache=SqliteCache(timeout=3600)) # 1 godzina
            st.info("Użyto cache Zeep (Sqlite).")
        except ImportError:
            st.warning("Brak SqliteCache, tworzenie klienta bez cache.")
            client = Client(wsdl=WSDL_URL, transport=transport, wsse=wsse)
        except Exception as cache_e:
            st.warning(f"Problem z cache Zeep: {cache_e}. Tworzenie klienta bez cache.")
            client = Client(wsdl=WSDL_URL, transport=transport, wsse=wsse)

        # Test połączenia i uwierzytelnienia
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
            # Wypisz szczegóły błędu, jeśli dostępne
            if f.detail is not None:
                st.error(f"Szczegóły błędu SOAP: {etree.tostring(f.detail, pretty_print=True).decode()}")
            return None
        except Exception as conn_test_e:
            st.error(f"Błąd podczas testu połączenia (CzyZalogowany): {conn_test_e}")
            return None # Zwracamy None, bo nie wiemy czy klient działa poprawnie

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
        # Czyszczenie nazw kolumn i wartości
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
    # Usunięcie typowych prefiksów
    name = re.sub(r"^(województwo|powiat|gmina|gm\.?|miasto|m\.?\s*st\.?|obszar wiejski)\s+", "", name, flags=re.IGNORECASE).strip()
    # Usunięcie zawartości w nawiasach (często dopiski)
    name = re.sub(r"\(.*\)", "", name).strip()
    # Usunięcie części po myślniku (np. " - część wiejska")
    name = re.sub(r"\s+-.*$", "", name).strip()
    # Normalizacja wielokrotnych spacji
    name = re.sub(r"\s+", " ", name).strip()
    # Specyficzne zamiany dla porównań
    name = name.replace('m.st. ', '') # np. 'm.st. Warszawa' -> 'warszawa'
    return name

# --- NOWA WERSJA find_terc_code UŻYWAJĄCA PobierzListe* ---
def find_terc_code(client, woj_name, pow_name, gmi_name):
    """Wyszukuje kod TERC gminy (7 cyfr) na podstawie nazw, używając metod PobierzListe*."""
    if not client: return None
    if not woj_name or not pow_name or not gmi_name:
        st.warning("Brak pełnych nazw (województwo, powiat, gmina) do wyszukania TERC.")
        return None

    norm_woj_name = normalize_name(woj_name)
    norm_pow_name = normalize_name(pow_name)
    norm_gmi_name = normalize_name(gmi_name)
    # Dodatkowa normalizacja dla gmin (czasem w CSV jest 'gm. XXX' a w TERYT 'XXX')
    norm_gmi_name_alt = re.sub(r"^gm\.?\s+", "", norm_gmi_name).strip()


    st.info(f"Szukam TERC dla (norm): Woj='{norm_woj_name}', Pow='{norm_pow_name}', Gmi='{norm_gmi_name}' [Metoda: PobierzListe*]")

    woj_symbol = None
    pow_symbol = None
    gmi_symbol = None
    rodz_symbol = None
    full_terc = None

    try:
        # --- Krok 1: Znajdź WOJ ---
        with st.spinner(f"1. Pobieranie listy województw..."):
            woj_list_raw = client.service.PobierzListeWojewodztw(DataStanu=TODAY_DATE_STR)

        if not woj_list_raw:
            st.error("Nie udało się pobrać listy województw z TERYT.")
            return None

        found_woj = None
        for woj in woj_list_raw:
            current_woj_name_raw = getattr(woj, 'NAZWA', '')
            current_woj_name_norm = normalize_name(current_woj_name_raw)
            if current_woj_name_norm == norm_woj_name:
                found_woj = woj
                break # Znaleziono dokładne dopasowanie

        if found_woj:
            woj_symbol_candidate = getattr(found_woj, 'WOJ', None)
            if woj_symbol_candidate and str(woj_symbol_candidate).isdigit():
                woj_symbol = str(woj_symbol_candidate).zfill(2)
                st.success(f"Etap 1: OK, WOJ: {woj_symbol} dla '{getattr(found_woj, 'NAZWA', '')}'")
            else:
                st.error(f"Znaleziono województwo '{getattr(found_woj, 'NAZWA', '')}', ale ma nieprawidłowy symbol: '{woj_symbol_candidate}'.")
                return None
        else:
            st.error(f"Nie znaleziono województwa o nazwie (po normalizacji): '{norm_woj_name}'")
            # Opcjonalnie: Pokaż listę dostępnych województw dla debugowania
            # available_woj = [f"{normalize_name(getattr(w, 'NAZWA', ''))} ({getattr(w, 'WOJ', '')})" for w in woj_list_raw]
            # st.info(f"Dostępne województwa (znormalizowane): {', '.join(available_woj)}")
            return None

        # --- Krok 2: Znajdź POW ---
        with st.spinner(f"2. Pobieranie listy powiatów dla woj. {woj_symbol}..."):
            # Upewnij się, że przekazujesz symbol WOJ jako string
            pow_list_raw = client.service.PobierzListePowiatow(Wojewodztwo=woj_symbol, DataStanu=TODAY_DATE_STR)

        if not pow_list_raw:
            st.error(f"Nie udało się pobrać listy powiatów dla województwa {woj_symbol}.")
            return None

        found_pow = None
        for pow_ in pow_list_raw:
            current_pow_name_raw = getattr(pow_, 'NAZWA', '')
            current_pow_name_norm = normalize_name(current_pow_name_raw)
            if current_pow_name_norm == norm_pow_name:
                found_pow = pow_
                break

        if found_pow:
            pow_symbol_candidate = getattr(found_pow, 'POW', None)
            if pow_symbol_candidate and str(pow_symbol_candidate).isdigit():
                pow_symbol = str(pow_symbol_candidate).zfill(2)
                st.success(f"Etap 2: OK, POW: {pow_symbol} dla '{getattr(found_pow, 'NAZWA', '')}'")
            else:
                st.error(f"Znaleziono powiat '{getattr(found_pow, 'NAZWA', '')}', ale ma nieprawidłowy symbol: '{pow_symbol_candidate}'.")
                return None
        else:
            st.error(f"Nie znaleziono powiatu o nazwie (po normalizacji): '{norm_pow_name}' w woj. {woj_symbol}")
            # Opcjonalnie: Pokaż listę dostępnych powiatów dla debugowania
            # available_pow = [f"{normalize_name(getattr(p, 'NAZWA', ''))} ({getattr(p, 'POW', '')})" for p in pow_list_raw]
            # st.info(f"Dostępne powiaty (znormalizowane): {', '.join(available_pow)}")
            return None

        # --- Krok 3: Znajdź GMI + RODZ ---
        with st.spinner(f"3. Pobieranie listy gmin dla pow. {woj_symbol}{pow_symbol}..."):
            # Upewnij się, że przekazujesz symbole WOJ i POW jako stringi
            gmi_list_raw = client.service.PobierzListeGmin(Wojewodztwo=woj_symbol, Powiat=pow_symbol, DataStanu=TODAY_DATE_STR)

        if not gmi_list_raw:
            st.error(f"Nie udało się pobrać listy gmin dla powiatu {woj_symbol}{pow_symbol}.")
            return None

        found_gmi = None
        possible_matches = []
        for gmi in gmi_list_raw:
            current_gmi_name_raw = getattr(gmi, 'NAZWA', '')
            current_gmi_name_norm = normalize_name(current_gmi_name_raw)
            # Porównaj z obiema znormalizowanymi formami nazwy gminy
            if current_gmi_name_norm == norm_gmi_name or current_gmi_name_norm == norm_gmi_name_alt:
                possible_matches.append(gmi)

        if len(possible_matches) == 1:
            found_gmi = possible_matches[0]
            st.write(f"Znaleziono jednoznaczne dopasowanie gminy: '{getattr(found_gmi, 'NAZWA', '')}'")
        elif len(possible_matches) > 1:
            st.warning(f"Znaleziono {len(possible_matches)} gminy pasujące do nazwy '{gmi_name}' (norm: '{norm_gmi_name}'/'{norm_gmi_name_alt}'). Wybieram pierwszą.")
            # Można dodać logikę wyboru, np. na podstawie typu gminy, jeśli jest dostępny w CSV
            found_gmi = possible_matches[0] # Wybierz pierwszą jako domyślną
        # else: found_gmi pozostaje None

        if found_gmi:
            gmi_symbol_candidate = getattr(found_gmi, 'GMI', None)
            rodz_symbol_candidate = getattr(found_gmi, 'RODZ', None)

            if gmi_symbol_candidate and str(gmi_symbol_candidate).isdigit() and \
               rodz_symbol_candidate and str(rodz_symbol_candidate).isdigit():
                gmi_symbol = str(gmi_symbol_candidate).zfill(2)
                rodz_symbol = str(rodz_symbol_candidate) # RODZ to pojedyncza cyfra
                full_terc = f"{woj_symbol}{pow_symbol}{gmi_symbol}{rodz_symbol}"
                st.success(f"Etap 3: OK, TERC: {full_terc} dla '{getattr(found_gmi, 'NAZWA', '')}'")
            else:
                st.error(f"Znaleziono gminę '{getattr(found_gmi, 'NAZWA', '')}', ale ma nieprawidłowe symbole GMI/RODZ: GMI='{gmi_symbol_candidate}', RODZ='{rodz_symbol_candidate}'.")
                return None
        else:
            st.error(f"Nie znaleziono gminy o nazwie (po normalizacji): '{norm_gmi_name}' lub '{norm_gmi_name_alt}' w pow. {woj_symbol}{pow_symbol}")
            # Opcjonalnie: Pokaż listę dostępnych gmin dla debugowania
            # available_gmi = [f"{normalize_name(getattr(g, 'NAZWA', ''))} ({getattr(g, 'GMI', '')}{getattr(g, 'RODZ', '')})" for g in gmi_list_raw]
            # st.info(f"Dostępne gminy (znormalizowane): {', '.join(available_gmi)}")
            return None

    except Fault as f:
        st.error(f"Błąd SOAP podczas wyszukiwania TERC (PobierzListe*): {f.message}")
        if f.detail is not None:
             st.error(f"Szczegóły błędu SOAP: {etree.tostring(f.detail, pretty_print=True).decode()}")
        return None
    except TypeError as te:
        st.error(f"Błąd typu (TypeError) podczas wyszukiwania TERC (PobierzListe*): {te}. Sprawdź nazwy i typy argumentów przekazywanych do API.")
        return None
    except Exception as e:
        st.error(f"Nieoczekiwany błąd podczas wyszukiwania TERC (PobierzListe*): {e}")
        return None

    # Ostateczna walidacja formatu
    if full_terc and isinstance(full_terc, str) and len(full_terc) == 7 and full_terc.isdigit():
        return full_terc
    else:
        st.warning(f"Ostateczny kod TERC '{full_terc}' ma nieprawidłowy format lub nie został znaleziony.")
        return None
# --- KONIEC NOWEJ WERSJI find_terc_code ---


def find_simc_symbol(client, locality_name, terc_gmi_full):
    """Wyszukuje symbol SIMC miejscowości (7 cyfr), używając WyszukajMiejscowosc i filtrując po TERC gminy."""
    if not client: return None
    if not locality_name:
        st.warning("Brak nazwy miejscowości do wyszukania SIMC.")
        return None

    simc_symbol = None
    norm_locality_name = normalize_name(locality_name)
    st.info(f"Szukam SIMC dla (norm): '{norm_locality_name}' [Metoda: WyszukajMiejscowosc + Filtrowanie TERC]")

    try:
        miejsc_result_general = None
        with st.spinner(f"Wyszukiwanie miejscowości '{locality_name}'..."):
            # Użycie WyszukajMiejscowosc, która wydaje się bardziej odpowiednia
            miejsc_result_general = client.service.WyszukajMiejscowosc(nazwaMiejscowosci=locality_name)

        if not miejsc_result_general:
            st.warning(f"Nie znaleziono miejscowości '{locality_name}' w TERYT (wg WyszukajMiejscowosc).")
            return None

        # Przygotowanie do filtrowania po TERC (jeśli dostępny)
        terc_woj, terc_pow, terc_gmi, terc_rodz = (None,) * 4
        if terc_gmi_full and len(terc_gmi_full) == 7:
            terc_woj = terc_gmi_full[0:2]
            terc_pow = terc_gmi_full[2:4]
            terc_gmi = terc_gmi_full[4:6]
            terc_rodz = terc_gmi_full[6:7]
            st.write(f"Filtruję wyniki SIMC dla TERC: {terc_gmi_full} (W:{terc_woj} P:{terc_pow} G:{terc_gmi} R:{terc_rodz})")

        potential_matches = []
        st.write(f"--- Analiza wyników dla Miejscowości '{locality_name}' ---")
        for i, m in enumerate(miejsc_result_general):
            m_nazwa_raw = getattr(m, 'Nazwa', '')
            m_nazwa_norm = normalize_name(m_nazwa_raw)
            m_sym = getattr(m, 'Symbol', '')
            m_woj = str(getattr(m, 'WojSymbol', '')).zfill(2)
            m_pow = str(getattr(m, 'PowSymbol', '')).zfill(2)
            m_gmi = str(getattr(m, 'GmiSymbol', '')).zfill(2) # GmiSymbol w wyniku to tylko 2 cyfry GMI
            m_rodz = str(getattr(m, 'GmiRodzaj', '')) # GmiRodzaj w wyniku to RODZ

            st.write(f" Wynik {i+1}: Nazwa='{m_nazwa_raw}' (Norm: '{m_nazwa_norm}'), Symbol='{m_sym}', TERC='{m_woj}{m_pow}{m_gmi}{m_rodz}'")

            # Sprawdzenie dopasowania TERC (jeśli szukamy z TERC)
            terc_match = False
            if terc_gmi_full:
                if m_woj == terc_woj and m_pow == terc_pow and m_gmi == terc_gmi and m_rodz == terc_rodz:
                    terc_match = True
                    st.write("   -> TERC pasuje.")
                else:
                    st.write("   -> TERC nie pasuje.")
                    continue # Przejdź do następnego wyniku, jeśli TERC nie pasuje
            else:
                terc_match = True # Jeśli nie filtrujemy po TERC, każdy wynik jest potencjalnie dobry

            # Sprawdzenie dopasowania nazwy (po normalizacji)
            name_match_exact = (m_nazwa_norm == norm_locality_name)
            if name_match_exact:
                 st.write("   -> Nazwa pasuje dokładnie.")
                 potential_matches.append({'m': m, 'match_type': 'exact'})
            # Można dodać logikę dla częściowego dopasowania, jeśli potrzebne
            # elif norm_locality_name in m_nazwa_norm:
            #     st.write("   -> Nazwa pasuje częściowo.")
            #     potential_matches.append({'m': m, 'match_type': 'partial'})


        # Wybór najlepszego dopasowania
        best_match_obj = None
        if potential_matches:
            exact_matches = [p for p in potential_matches if p['match_type'] == 'exact']
            if len(exact_matches) >= 1:
                # Jeśli jest wiele dokładnych dopasowań (rzadkie, ale możliwe), wybierz pierwsze
                best_match_obj = exact_matches[0]['m']
                st.write("Wybrano pierwsze dokładne dopasowanie nazwy (i TERC, jeśli filtrowano).")
            # Można dodać logikę wyboru dla częściowych dopasowań, jeśli nie ma dokładnych
            # elif potential_matches:
            #    best_match_obj = potential_matches[0]['m'] # Wybierz pierwszy jakikolwiek pasujący
            #    st.warning("Brak dokładnego dopasowania nazwy, wybrano pierwszy pasujący TERC (jeśli filtrowano).")

        # Jeśli nie znaleziono pasującego (lub nie filtrowano i nie było dokładnego dopasowania nazwy)
        if not best_match_obj and not terc_gmi_full and miejsc_result_general:
             st.warning(f"Brak dokładnego dopasowania nazwy dla '{locality_name}'. Zwracam pierwszy znaleziony wynik z API.")
             # Sortowanie może pomóc uzyskać bardziej prawdopodobny wynik
             try:
                 miejsc_result_general.sort(key=lambda x: (
                     normalize_name(getattr(x, 'Nazwa', '')) != norm_locality_name, # Najpierw te z pasującą nazwą (nawet jeśli nie 'exact' wg logiki wyżej)
                     int(getattr(x, 'Symbol', 0)) # Potem wg symbolu
                 ))
             except: pass # Ignoruj błędy sortowania
             best_match_obj = miejsc_result_general[0]


        # Pobranie symbolu SIMC z najlepszego dopasowania
        if best_match_obj:
            simc_symbol_candidate = getattr(best_match_obj, 'Symbol', None)
            if simc_symbol_candidate:
                simc_symbol = str(simc_symbol_candidate).zfill(7)
                if len(simc_symbol) == 7 and simc_symbol.isdigit():
                    st.success(f"=> Znaleziono SIMC: {simc_symbol} dla '{getattr(best_match_obj, 'Nazwa', '')}'")
                else:
                    st.warning(f"Znaleziony symbol SIMC '{simc_symbol}' ma nieprawidłowy format.")
                    simc_symbol = None # Resetuj, jeśli format zły
            else:
                 st.warning(f"Wybrany obiekt miejscowości '{getattr(best_match_obj, 'Nazwa', '')}' nie ma atrybutu 'Symbol'.")
        else:
             st.warning(f"Nie udało się wybrać jednoznacznego dopasowania dla miejscowości '{locality_name}'" + (f" w gminie {terc_gmi_full}." if terc_gmi_full else "."))


    except Fault as f:
        st.error(f"Błąd SOAP podczas wyszukiwania SIMC dla '{locality_name}': {f.message}")
        if f.detail is not None:
             st.error(f"Szczegóły błędu SOAP: {etree.tostring(f.detail, pretty_print=True).decode()}")
    except TypeError as te:
        st.error(f"Błąd typu (TypeError) podczas wywołania API dla SIMC '{locality_name}': {te}")
    except Exception as e:
        st.error(f"Nieoczekiwany błąd podczas wyszukiwania SIMC dla '{locality_name}': {e}")

    # Ostateczne sprawdzenie przed zwróceniem
    if simc_symbol and isinstance(simc_symbol, str) and len(simc_symbol) == 7 and simc_symbol.isdigit():
        return simc_symbol
    else:
        return None


# --- POPRAWIONA WERSJA get_streets Z PRAWIDŁOWYMI NAZWAMI PARAMETRÓW ---
def get_streets(client, full_terc_code, simc_symbol):
    """Pobiera listę ulic dla danej miejscowości używając PobierzListeUlicDlaMiejscowosci."""
    if not client: return []
    if not full_terc_code or not simc_symbol:
        st.warning("Brak kodu TERC lub symbolu SIMC do pobrania ulic.")
        return []
    if len(full_terc_code) != 7 or not full_terc_code.isdigit():
         st.warning(f"Nieprawidłowy format kodu TERC '{full_terc_code}' do pobrania ulic.")
         return []
    if len(simc_symbol) != 7 or not simc_symbol.isdigit():
         st.warning(f"Nieprawidłowy format symbolu SIMC '{simc_symbol}' do pobrania ulic.")
         return []

    all_streets = []
    try:
        # Rozpakowanie kodu TERC na części
        woj_id = full_terc_code[0:2]
        pow_id = full_terc_code[2:4]
        gmi_id = full_terc_code[4:6]
        rodz_id = full_terc_code[6:7]

        st.info(f"Pobieram ulice dla TERC={full_terc_code}, SIMC={simc_symbol} [Metoda: PobierzListeUlicDlaMiejscowosci]")
        with st.spinner(f"Pobieranie ulic dla SIMC: {simc_symbol}..."):
            # --- POPRAWKA: Użycie nazw parametrów zgodnych z WSDL ---
            # Zamiast WojewodztwoId -> Wojewodztwo
            # Zamiast PowiatId -> Powiat
            # Zamiast GminaId -> Gmina
            # GminaRodzaj i MiejscowoscId wydają się OK, DataStanu też
            streets_result = client.service.PobierzListeUlicDlaMiejscowosci(
                Wojewodztwo=woj_id,      # Poprawiona nazwa
                Powiat=pow_id,         # Poprawiona nazwa
                Gmina=gmi_id,          # Poprawiona nazwa
                GminaRodzaj=rodz_id,
                MiejscowoscId=simc_symbol,
                DataStanu=TODAY_DATE_STR
            )

            # Sprawdzenie, czy wynik zawiera listę ulic
            # Struktura odpowiedzi może być różna, np. bezpośrednio lista lub obiekt z polem 'Ulica'
            ulice_list = None
            if streets_result:
                 if isinstance(streets_result, list):
                      ulice_list = streets_result
                 elif hasattr(streets_result, 'Ulica') and isinstance(streets_result.Ulica, list):
                      ulice_list = streets_result.Ulica
                 # Można dodać inne sprawdzenia struktury, jeśli API zwraca inaczej

            if ulice_list:
                seen_ids = set() # Do obsługi duplikatów, jeśli API je zwraca
                for ulica in ulice_list:
                    # Sprawdzenie czy obiekt ulicy ma potrzebne atrybuty
                    if not hasattr(ulica, 'Identyfikator') or not getattr(ulica, 'Identyfikator', None):
                        st.warning(f"Pominięto ulicę bez identyfikatora: {getattr(ulica, 'Nazwa1', 'Brak nazwy')}")
                        continue

                    id_ulic = getattr(ulica, 'Identyfikator')
                    if id_ulic not in seen_ids:
                        cecha = getattr(ulica, 'Cecha', '') # np. 'ul.', 'al.', 'pl.'
                        nazwa1 = getattr(ulica, 'Nazwa1', '') # Główna część nazwy
                        nazwa2 = getattr(ulica, 'Nazwa2', '') # Druga część nazwy (rzadziej używana)
                        symbol_ulic_candidate = getattr(ulica, 'Symbol', '') # Symbol ULIC (5 cyfr)

                        # Konstrukcja pełnej nazwy
                        full_street_name = f"{cecha} {nazwa1}".strip()
                        if nazwa2:
                            full_street_name += f" {nazwa2}"
                        full_street_name = re.sub(r'\s+', ' ', full_street_name).strip() # Usuń podwójne spacje

                        # Walidacja i formatowanie symbolu ULIC
                        symbol_ulic = None
                        if symbol_ulic_candidate and isinstance(symbol_ulic_candidate, str) and symbol_ulic_candidate.isdigit():
                            symbol_ulic = symbol_ulic_candidate.zfill(5)
                            if len(symbol_ulic) != 5:
                                st.warning(f"Symbol ULIC '{symbol_ulic_candidate}' dla ulicy '{full_street_name}' ma nieprawidłową długość po uzupełnieniu.")
                                symbol_ulic = None # Zresetuj, jeśli zły format

                        if full_street_name and symbol_ulic: # Dodaj tylko jeśli mamy nazwę i poprawny symbol
                            all_streets.append({
                                'Nazwa': full_street_name,
                                'Symbol': symbol_ulic,
                                'Identyfikator': id_ulic
                            })
                            seen_ids.add(id_ulic)
                        else:
                             st.warning(f"Pominięto ulicę z brakującą nazwą lub nieprawidłowym symbolem ULIC: ID={id_ulic}, Nazwa='{full_street_name}', Symbol='{symbol_ulic_candidate}'")

        if not all_streets:
            st.info(f"Nie znaleziono żadnych ulic (z poprawnymi danymi) dla SIMC: {simc_symbol} (TERC: {full_terc_code}).")

        # Sortowanie alfabetyczne
        all_streets.sort(key=lambda x: x['Nazwa'])
        st.success(f"Pobrano {len(all_streets)} ulic.")
        return all_streets

    except Fault as f:
        # Sprawdzenie czy błąd to problem z argumentami (często wskazuje na złe nazwy)
        fault_msg = str(f.message).lower()
        fault_detail_str = etree.tostring(f.detail, pretty_print=True).decode().lower() if f.detail is not None else ""

        if "argument" in fault_msg or "parameter" in fault_msg or "required element" in fault_detail_str:
             st.error(f"Błąd SOAP (prawdopodobnie złe argumenty) przy PobierzListeUlicDlaMiejscowosci: {f.message}")
             st.error("Sprawdź, czy nazwy parametrów (Wojewodztwo, Powiat, Gmina, GminaRodzaj, MiejscowoscId, DataStanu) są zgodne z definicją WSDL.")
             if f.detail is not None: st.error(f"Szczegóły błędu SOAP: {fault_detail_str}")
        elif "nie znaleziono ulic" in fault_msg or "brak danych" in fault_msg or "nie istnieje" in fault_msg:
            st.info(f"API TERYT poinformowało o braku ulic dla SIMC {simc_symbol} / TERC {full_terc_code}.")
        else:
            st.error(f"Błąd SOAP podczas pobierania ulic: {f.message}")
            if f.detail is not None: st.error(f"Szczegóły błędu SOAP: {fault_detail_str}")
        return [] # Zwróć pustą listę w przypadku błędu SOAP
    except TypeError as te:
        st.error(f"Błąd typu (TypeError) podczas wywołania PobierzListeUlicDlaMiejscowosci: {te}")
        st.error("To zazwyczaj oznacza błąd w nazwach lub typach przekazywanych parametrów. Sprawdź: Wojewodztwo, Powiat, Gmina, GminaRodzaj, MiejscowoscId, DataStanu.")
        return []
    except Exception as e:
        st.error(f"Nieoczekiwany błąd podczas pobierania ulic: {e}")
        return []


# --- Główna część aplikacji Streamlit ---
st.set_page_config(layout="wide")
st.title("Wyszukiwarka Adresów z TERYT")

# --- Inicjalizacja stanu sesji ---
# Używamy prefiksu '_', aby uniknąć konfliktów z kluczami widgetów
if '_teryt_client' not in st.session_state: st.session_state._teryt_client = None
if '_selected_locality' not in st.session_state: st.session_state._selected_locality = ""
if '_selected_street_name' not in st.session_state: st.session_state._selected_street_name = ""
if '_teryt_streets' not in st.session_state: st.session_state._teryt_streets = []
if '_simc_symbol_found' not in st.session_state: st.session_state._simc_symbol_found = None
if '_terc_code_found' not in st.session_state: st.session_state._terc_code_found = None
if '_postal_code_input' not in st.session_state: st.session_state._postal_code_input = ""
if '_teryt_user_input' not in st.session_state: st.session_state._teryt_user_input = ""
if '_last_processed_locality' not in st.session_state: st.session_state._last_processed_locality = None # Flaga kontrolna przetwarzania
if '_last_postal_code' not in st.session_state: st.session_state._last_postal_code = None # Flaga kontrolna zmiany kodu

# --- Panel boczny ---
with st.sidebar:
    st.header("Dane Logowania TERYT")
    # Używamy wartości ze stanu sesji jako domyślnych dla inputów
    teryt_user = st.text_input("Nazwa użytkownika TERYT", value=st.session_state._teryt_user_input, key="teryt_user_widget")
    teryt_pass = st.text_input("Hasło TERYT", type="password", key="teryt_pass_widget") # Hasła nie przechowujemy w stanie

    st.header("Kod Pocztowy")
    postal_code = st.text_input("Wprowadź kod pocztowy (np. 00-001)", value=st.session_state._postal_code_input, key="postal_code_widget")

    # Aktualizacja stanu sesji po zmianie wartości w polach input
    st.session_state._teryt_user_input = teryt_user
    st.session_state._postal_code_input = postal_code

    if st.button("Połącz / Odśwież Połączenie", key="connect_button"):
        if teryt_user and teryt_pass:
            # Resetujemy klienta i zależne dane przed próbą nowego połączenia
            st.session_state._teryt_client = None
            st.session_state._terc_code_found = None
            st.session_state._simc_symbol_found = None
            st.session_state._teryt_streets = []
            st.session_state._selected_street_name = ""
            st.session_state._last_processed_locality = None # Wymuś ponowne pobranie danych TERYT
            # Wywołujemy funkcję łączącą
            st.session_state._teryt_client = get_teryt_client(teryt_user, teryt_pass)
            st.rerun() # Odśwież interfejs po próbie połączenia
        else:
            st.warning("Wprowadź nazwę użytkownika i hasło TERYT.")
            # Jeśli brakuje danych, upewnij się, że klient jest None
            if st.session_state._teryt_client is not None:
                 st.session_state._teryt_client = None
                 st.rerun() # Odśwież, aby pokazać brak połączenia

# Pobierz klienta ze stanu sesji do zmiennej lokalnej dla łatwiejszego dostępu
client = st.session_state._teryt_client

# Wyświetlenie statusu połączenia TERYT
with st.sidebar:
    if client:
        st.success("Połączono z TERYT.")
    elif st.session_state._teryt_user_input: # Jeśli użytkownik coś wpisał, ale nie ma klienta
        st.error("Brak aktywnego połączenia z TERYT. Sprawdź dane logowania i kliknij 'Połącz'.")
    else: # Jeśli nic nie wpisano
        st.info("Wprowadź dane logowania TERYT i kliknij 'Połącz'.")


# --- Wczytanie danych CSV ---
# Robimy to tylko raz na początku lub gdy plik się zmieni (cache_data)
df_kody = load_csv_data(CSV_FILE_PATH, CSV_ENCODING, CSV_SEPARATOR)

# --- Logika aplikacji ---
selected_locality_data = None
localities_list = [""] # Lista miejscowości dla wybranego kodu pocztowego
filtered_df = pd.DataFrame() # DataFrame przefiltrowany po kodzie pocztowym

# Sprawdzenie, czy zmieniono kod pocztowy
postal_code_changed = (postal_code != st.session_state._last_postal_code)
if postal_code_changed:
    st.write(f"Zmieniono kod pocztowy na: {postal_code}")
    # Reset stanu zależnego od kodu pocztowego i miejscowości
    st.session_state._selected_locality = ""
    st.session_state._selected_street_name = ""
    st.session_state._teryt_streets = []
    st.session_state._simc_symbol_found = None
    st.session_state._terc_code_found = None
    st.session_state._last_processed_locality = None
    st.session_state._last_postal_code = postal_code # Zapisz nowy kod
    # st.rerun() # Niekoniecznie potrzebne tutaj, logika poniżej i tak się wykona

# Logika filtrowania CSV i tworzenia listy miejscowości
if df_kody is not None and postal_code:
    postal_code_csv = postal_code.strip()
    # Walidacja i formatowanie kodu pocztowego
    if len(postal_code_csv) == 6 and postal_code_csv[2] == '-':
        pass # Format XX-XXX jest OK
    elif len(postal_code_csv) == 5 and postal_code_csv.isdigit():
        postal_code_csv = f"{postal_code_csv[:2]}-{postal_code_csv[2:]}" # Zamień XXXXX na XX-XXX
    else:
        st.warning(f"Nieprawidłowy format kodu pocztowego: '{postal_code}'. Oczekiwano XX-XXX lub XXXXX.")
        postal_code_csv = None # Zresetuj, jeśli format zły

    if postal_code_csv and 'PNA' in df_kody.columns:
        try:
            filtered_df = df_kody[df_kody['PNA'] == postal_code_csv].copy()
            if not filtered_df.empty:
                # Upewnij się, że kolumna 'MIEJSCOWOŚĆ' istnieje
                if 'MIEJSCOWOŚĆ' in filtered_df.columns:
                    unique_localities = sorted(filtered_df['MIEJSCOWOŚĆ'].drop_duplicates().dropna().tolist())
                    localities_list = [""] + unique_localities
                    # Jeśli poprzednio wybrana miejscowość nie jest już dostępna, zresetuj wybór
                    if st.session_state._selected_locality not in localities_list:
                         st.session_state._selected_locality = ""
                         # Reset zależnych danych TERYT
                         st.session_state._selected_street_name = ""
                         st.session_state._teryt_streets = []
                         st.session_state._simc_symbol_found = None
                         st.session_state._terc_code_found = None
                         st.session_state._last_processed_locality = None
                else:
                    st.error("W pliku CSV brakuje kolumny 'MIEJSCOWOŚĆ'.")
                    localities_list = [""]
            else:
                st.info(f"Nie znaleziono miejscowości dla kodu pocztowego: {postal_code_csv}")
                localities_list = [""]
                # Resetuj wybór, jeśli nie ma miejscowości
                if st.session_state._selected_locality != "":
                    st.session_state._selected_locality = ""
                    st.session_state._selected_street_name = ""
                    st.session_state._teryt_streets = []
                    st.session_state._simc_symbol_found = None
                    st.session_state._terc_code_found = None
                    st.session_state._last_processed_locality = None

        except KeyError as ke:
             st.error(f"Brak oczekiwanej kolumny w CSV: {ke}")
             localities_list = [""]
        except Exception as e:
            st.error(f"Błąd podczas filtrowania CSV dla kodu {postal_code_csv}: {e}")
            localities_list = [""]
    elif 'PNA' not in df_kody.columns and df_kody is not None:
        st.error("W pliku CSV brakuje kolumny 'PNA'.")
        localities_list = [""]

# --- Wybór miejscowości ---
st.header("1. Wybierz Miejscowość")
# Zapamiętaj poprzedni wybór, aby wykryć zmianę
selected_locality_prev = st.session_state._selected_locality

# Użyj wartości ze stanu sesji jako indeksu startowego dla selectboxa
try:
    current_locality_index = localities_list.index(st.session_state._selected_locality)
except ValueError:
    current_locality_index = 0 # Jeśli zapisana wartość nie istnieje na liście, wybierz pustą

st.session_state._selected_locality = st.selectbox(
    "Miejscowość:",
    options=localities_list,
    key="locality_selector_widget",
    index=current_locality_index,
    disabled=(not localities_list or localities_list == [""]) # Wyłącz, jeśli brak opcji
)
selected_locality = st.session_state._selected_locality

# Sprawdzenie, czy zmieniono miejscowość
locality_changed = (selected_locality != selected_locality_prev)
if locality_changed:
    st.write(f"Zmieniono miejscowość na: {selected_locality}")
    # Reset danych TERYT zależnych od miejscowości
    st.session_state._selected_street_name = ""
    st.session_state._teryt_streets = []
    st.session_state._simc_symbol_found = None
    st.session_state._terc_code_found = None
    st.session_state._last_processed_locality = None # Wymuś ponowne pobranie danych TERYT
    # st.rerun() # Niekoniecznie potrzebne, logika poniżej się wykona

# --- Wyświetlanie danych z CSV dla wybranej miejscowości ---
display_woj = "Brak danych"; display_pow = "Brak danych"; display_gmi = "Brak danych"
if selected_locality and not filtered_df.empty:
    # Upewnij się, że kolumny istnieją
    required_cols = ['MIEJSCOWOŚĆ', 'WOJEWÓDZTWO', 'POWIAT', 'GMINA']
    if all(col in filtered_df.columns for col in required_cols):
        matching_rows = filtered_df.loc[filtered_df['MIEJSCOWOŚĆ'] == selected_locality]
        if not matching_rows.empty:
            # Weź pierwszy pasujący wiersz (zakładamy, że dla danej miejscowości w kodzie pocztowym dane są spójne)
            selected_locality_data = matching_rows.iloc[0]
            display_woj = selected_locality_data.get('WOJEWÓDZTWO', 'Brak danych')
            display_pow = selected_locality_data.get('POWIAT', 'Brak danych')
            display_gmi = selected_locality_data.get('GMINA', 'Brak danych')
        else:
             # To nie powinno się zdarzyć, jeśli selected_locality pochodzi z localities_list
             st.warning(f"Nie znaleziono danych w przefiltrowanym DataFrame dla wybranej miejscowości: {selected_locality}")
    else:
        st.error(f"W pliku CSV brakuje wymaganych kolumn: {', '.join(required_cols)}")


col1, col2, col3 = st.columns(3)
with col1: st.metric("Województwo (z CSV)", display_woj)
with col2: st.metric("Powiat (z CSV)", display_pow)
with col3: st.metric("Gmina (z CSV)", display_gmi)

# --- Logika Interakcji z TERYT ---
# Sprawdź, czy trzeba pobrać dane TERYT:
# 1. Wybrano miejscowość
# 2. Jest aktywne połączenie z TERYT (klient istnieje)
# 3. Wybrana miejscowość jest inna niż ostatnio przetwarzana LUB dane TERYT nie zostały jeszcze pobrane (np. po zmianie kodu pocztowego)
needs_teryt_fetch = (
    selected_locality and client and
    (st.session_state._last_processed_locality != selected_locality)
)

if needs_teryt_fetch:
    st.info(f"Rozpoczynanie pobierania danych TERYT dla: {selected_locality}...")
    # Zapisz, że przetwarzamy tę miejscowość
    st.session_state._last_processed_locality = selected_locality

    # Pobierz nazwy z wyświetlanych danych (pochodzących z CSV)
    woj_name = display_woj if display_woj != "Brak danych" else None
    pow_name = display_pow if display_pow != "Brak danych" else None
    gmi_name = display_gmi if display_gmi != "Brak danych" else None

    # Reset poprzednich wyników TERYT
    st.session_state._terc_code_found = None
    st.session_state._simc_symbol_found = None
    st.session_state._teryt_streets = []
    st.session_state._selected_street_name = "" # Resetuj też wybraną ulicę

    # Sprawdź, czy mamy wszystkie potrzebne nazwy
    if woj_name and pow_name and gmi_name:
        # Krok 1: Znajdź TERC (nowa metoda)
        st.session_state._terc_code_found = find_terc_code(client, woj_name, pow_name, gmi_name)

        # Krok 2: Znajdź SIMC (używa nazwy miejscowości i opcjonalnie TERC do filtrowania)
        st.session_state._simc_symbol_found = find_simc_symbol(client, selected_locality, st.session_state._terc_code_found)

        # Krok 3: Pobierz ulice (tylko jeśli mamy TERC i SIMC)
        if st.session_state._terc_code_found and st.session_state._simc_symbol_found:
            st.session_state._teryt_streets = get_streets(client, st.session_state._terc_code_found, st.session_state._simc_symbol_found)
        elif st.session_state._terc_code_found:
             st.warning("Nie udało się znaleźć symbolu SIMC, nie można pobrać ulic.")
        else:
             st.warning("Nie udało się znaleźć kodu TERC, nie można pobrać symbolu SIMC ani ulic.")
    else:
        st.warning("Brak pełnych danych adresowych (woj/pow/gmi) z pliku CSV, aby wyszukać dane TERYT.")

    st.info("Zakończono pobieranie danych TERYT.")
    st.rerun() # Odśwież interfejs, aby pokazać nowe dane TERYT i zaktualizować listę ulic

# --- Wyświetlanie statusu TERYT (wyników pobierania) ---
st.subheader("Status Danych TERYT")
if selected_locality:
    if client:
        terc_to_display = st.session_state._terc_code_found
        simc_to_display = st.session_state._simc_symbol_found

        if terc_to_display:
            st.success(f"Znaleziony TERC gminy: {terc_to_display}")
        elif st.session_state._last_processed_locality == selected_locality: # Jeśli próbowano pobrać dla tej miejscowości
             st.warning("Nie udało się znaleźć kodu TERC gminy w TERYT.")

        if simc_to_display:
            st.success(f"Znaleziony SIMC miejscowości: {simc_to_display}")
        elif st.session_state._last_processed_locality == selected_locality: # Jeśli próbowano pobrać
             st.warning("Nie udało się znaleźć symbolu SIMC miejscowości w TERYT.")

        # Komunikaty o błędach i ostrzeżenia są teraz głównie wewnątrz funkcji find_* i get_*
    elif st.session_state._teryt_user_input: # Wybrano miejscowość, wpisano login, ale nie ma połączenia
        st.warning("Połączenie z TERYT nie jest aktywne. Sprawdź dane i kliknij 'Połącz'.")
    else: # Wybrano miejscowość, ale nie podano danych logowania
        st.info("Połącz z TERYT (w panelu bocznym), aby pobrać dane SIMC i listę ulic.")
# else: # Nie wybrano miejscowości
#     st.info("Wybierz miejscowość, aby zobaczyć status TERYT.")


# --- Wybór ulicy ---
st.header("2. Wybierz Ulicę (z TERYT)")
street_options_display = [""] # Domyślnie pusta opcja
teryt_streets_from_state = st.session_state._teryt_streets # Pobierz listę ulic ze stanu

# Przygotuj opcje dla selectboxa
if teryt_streets_from_state:
    # Dodaj posortowane nazwy ulic
    street_options_display.extend(sorted([s['Nazwa'] for s in teryt_streets_from_state]))
elif st.session_state._simc_symbol_found and st.session_state._terc_code_found and client and st.session_state._last_processed_locality == selected_locality:
    # Jeśli próbowano pobrać ulice (mamy SIMC i TERC), ale lista jest pusta
    street_options_display = ["", "Brak ulic w TERYT dla tej miejscowości"]
# W przeciwnym razie (brak SIMC/TERC lub brak połączenia), zostaje tylko [""]

# Ustalenie indeksu dla selectboxa
try:
    current_street_index = street_options_display.index(st.session_state._selected_street_name)
except ValueError:
    current_street_index = 0 # Domyślnie puste

# Selectbox ulicy
st.session_state._selected_street_name = st.selectbox(
    "Ulica:",
    options=street_options_display,
    key="street_selector_widget",
    index=current_street_index,
    # Wyłącz, jeśli nie ma ulic do wyboru (tylko pusta opcja lub komunikat o braku)
    disabled=(len(street_options_display) <= 1 or (len(street_options_display) == 2 and street_options_display[1].startswith("Brak ulic")))
)
selected_street_name = st.session_state._selected_street_name

# --- Wyświetlanie symbolu ulicy ---
selected_street_symbol = None
selected_street_id = None
# Sprawdź, czy wybrano poprawną nazwę ulicy (nie pustą i nie komunikat)
valid_street_selected = selected_street_name and not selected_street_name.startswith("Brak ulic")

if valid_street_selected and teryt_streets_from_state:
    # Znajdź dane wybranej ulicy w liście ze stanu
    selected_street_info = next((s for s in teryt_streets_from_state if s['Nazwa'] == selected_street_name), None)
    if selected_street_info:
        selected_street_symbol = selected_street_info.get('Symbol') # Użyj .get dla bezpieczeństwa
        selected_street_id = selected_street_info.get('Identyfikator')
        if selected_street_symbol:
            st.metric("Symbol ULIC", selected_street_symbol)
        if selected_street_id:
            st.caption(f"Identyfikator ULIC: {selected_street_id}")
    elif selected_street_name: # Jeśli wybrano coś, co nie jest już na liście
         st.warning(f"Wybrana poprzednio ulica '{selected_street_name}' nie została znaleziona w aktualnych danych TERYT.")


# --- Wprowadzanie numeru domu/mieszkania ---
st.header("3. Wprowadź Numer Domu / Mieszkania")
col_nr1, col_nr2 = st.columns(2)
with col_nr1:
    house_number = st.text_input("Numer domu", key="house_no_widget")
with col_nr2:
    apartment_number = st.text_input("Numer mieszkania (opcjonalnie)", key="apt_no_widget")

# --- Podsumowanie ---
st.header("Podsumowanie Adresu")
if selected_locality:
    # Pobierz najnowsze wartości ze stanu sesji
    terc_to_display = st.session_state._terc_code_found
    simc_to_display = st.session_state._simc_symbol_found

    # Budowanie listy części adresu
    address_parts = []
    if st.session_state._postal_code_input:
        address_parts.append(f"Kod pocztowy: {st.session_state._postal_code_input}")
    address_parts.append(f"Miejscowość: {selected_locality}")
    if valid_street_selected:
        address_parts.append(f"Ulica: {selected_street_name}")
    if house_number:
        address_parts.append(f"Nr domu: {house_number}")
    if apartment_number:
        address_parts.append(f"Nr mieszkania: {apartment_number}")
    if display_gmi != "Brak danych":
        address_parts.append(f"Gmina (z CSV): {display_gmi}")
    if display_pow != "Brak danych":
        address_parts.append(f"Powiat (z CSV): {display_pow}")
    if display_woj != "Brak danych":
        address_parts.append(f"Województwo (z CSV): {display_woj}")
    # Dodanie danych TERYT, jeśli dostępne
    if simc_to_display:
        address_parts.append(f"Symbol SIMC (TERYT): {simc_to_display}")
    if terc_to_display:
        address_parts.append(f"Pełny TERC gminy (TERYT): {terc_to_display}")
    if selected_street_symbol:
        address_parts.append(f"Symbol ULIC (TERYT): {selected_street_symbol}")
    if selected_street_id:
         address_parts.append(f"ID ULIC (TERYT): {selected_street_id}")


    # Połącz części adresu w jeden tekst
    final_address = "\n".join(address_parts)
    st.text_area("Zebrane dane:", final_address, height=300) # Zwiększona wysokość
else:
    st.info("Wprowadź kod pocztowy i wybierz miejscowość, aby rozpocząć.")

# --- Sekcja Debugowania (opcjonalna) ---
# with st.expander("Informacje Debugowania Stanu Sesji"):
#      st.json(st.session_state.to_dict())

