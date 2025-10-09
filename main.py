# main.py
import os
import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Path, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import List, Optional, Dict, Any
import logging
from pydantic import BaseModel, Field
import uvicorn # Potrzebne do uruchomienia
from contextlib import asynccontextmanager

# --- Konfiguracja ---
DATA_DIR = os.getenv('DATA_DIR', './dane')
TERC_FILENAME = os.getenv('TERC_FILENAME', 'TERC_Adresowy_2025-07-30.csv')
SIMC_FILENAME = os.getenv('SIMC_FILENAME', 'SIMC_Adresowy_2025-07-30.csv')
ULIC_FILENAME = os.getenv('ULIC_FILENAME', 'ULIC_Adresowy_2025-07-30.csv')
KODY_POCZTOWE_FILENAME = os.getenv('KODY_POCZTOWE_FILENAME', 'kody_pocztowe.csv')

COLUMN_DTYPES = {
    'WOJ': str, 'POW': str, 'GMI': str, 'RODZ': str, 'RODZ_GMI': str,
    'SYM': str, 'SYM_UL': str, 'SYMPOD': str, 'PNA': str
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Globalne zmienne na DataFrame'y
dataframes: Dict[str, pd.DataFrame] = {}
terc_data: Optional[pd.DataFrame] = None
simc_data: Optional[pd.DataFrame] = None
ulic_data: Optional[pd.DataFrame] = None
ulic_data_enriched: Optional[pd.DataFrame] = None
kody_pocztowe_data: Optional[pd.DataFrame] = None

# --- Konfiguracja autentykacji ---
API_TOKEN = os.getenv("API_TOKEN", "7h3Oo9kg32B3LEy32Ec5dk810ydT8CwB")  # Ustaw swój token lub pobierz z env

security = HTTPBearer()

def verify_token(credentials: HTTPAuthorizationCredentials = Security(security)):
    if credentials.scheme != "Bearer" or credentials.credentials != API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing authentication token")

# --- Funkcje pomocnicze ---

def load_data_on_startup():
    """Ładuje pliki CSV do globalnych DataFrame'ów podczas startu aplikacji."""
    global dataframes, terc_data, simc_data, ulic_data, kody_pocztowe_data, ulic_data_enriched
    logger.info(f"Rozpoczynanie ładowania danych z katalogu: {DATA_DIR}")
    if not os.path.exists(DATA_DIR):
        logger.error(f"Katalog '{DATA_DIR}' nie istnieje. Nie można załadować danych.")
        return
    all_files = os.listdir(DATA_DIR)
    required_files = [TERC_FILENAME, SIMC_FILENAME, ULIC_FILENAME, KODY_POCZTOWE_FILENAME]
    missing_files = [f for f in required_files if f not in all_files]
    if missing_files:
        logger.warning(f"Brakujące wymagane pliki w '{DATA_DIR}': {', '.join(missing_files)}")

    loaded_files_count = 0
    for file_name in required_files:
        if file_name in all_files:
            file_path = os.path.join(DATA_DIR, file_name)
            df = None
            try:
                # Próba odczytu z UTF-8
                df = pd.read_csv(file_path, delimiter=';', on_bad_lines='warn', encoding='utf-8', dtype=COLUMN_DTYPES, low_memory=False)
                logger.info(f"Załadowano {file_name} (UTF-8).")
            except UnicodeDecodeError:
                try:
                    # Fallback na Latin-1
                    df = pd.read_csv(file_path, delimiter=';', on_bad_lines='warn', encoding='latin1', dtype=COLUMN_DTYPES, low_memory=False)
                    logger.warning(f"Plik {file_name} załadowano używając kodowania 'latin1' zamiast 'utf-8'.")
                except Exception as e_inner:
                    logger.error(f"Nie udało się załadować {file_name} ani z UTF-8, ani z Latin-1: {e_inner}")
                    continue
            except pd.errors.ParserError as e_parser:
                logger.error(f"Błąd parsowania {file_name}: {e_parser}. Sprawdź strukturę pliku i separator.")
                continue
            except Exception as e_outer:
                logger.error(f"Nieoczekiwany błąd podczas ładowania {file_name}: {e_outer}")
                continue

            if df is not None:
                df.columns = df.columns.str.strip() # Usuń białe znaki z nazw kolumn
                dataframes[file_name] = df
                loaded_files_count += 1
                # Przypisz do dedykowanych zmiennych globalnych
                if file_name == TERC_FILENAME: terc_data = df
                elif file_name == SIMC_FILENAME: simc_data = df
                elif file_name == ULIC_FILENAME: ulic_data = df
                elif file_name == KODY_POCZTOWE_FILENAME: kody_pocztowe_data = df
        else:
            logger.warning(f"Plik {file_name} nie znaleziony w {DATA_DIR}.")

    logger.info(f"Zakończono ładowanie danych. Załadowano {loaded_files_count} z {len(required_files)} wymaganych plików.")

    # Wzbogacanie danych ULIC po załadowaniu
    if ulic_data is not None and simc_data is not None:
        ulic_data_enriched = enrich_ulic_data(ulic_data, simc_data)
        if ulic_data_enriched is not None:
            logger.info("Pomyślnie wzbogacono dane ULIC o nazwy miejscowości.")
        else:
            logger.warning("Nie udało się wzbogacić danych ULIC.")
    else:
        logger.warning("Nie można wzbogacić danych ULIC, ponieważ brakuje danych ULIC lub SIMC.")

    # Przygotowanie danych kodów pocztowych
    if kody_pocztowe_data is not None:
        try:
            if 'PNA' in kody_pocztowe_data.columns:
                kody_pocztowe_data['PNA'] = kody_pocztowe_data['PNA'].astype(str)
            else:
                 logger.error(f"Kolumna 'PNA' nie znaleziona w {KODY_POCZTOWE_FILENAME}")
                 kody_pocztowe_data = None # Ustaw na None jeśli brakuje kluczowej kolumny

            if kody_pocztowe_data is not None and 'MIEJSCOWOŚĆ' in kody_pocztowe_data.columns:
                 # Wyodrębnij czystą nazwę miejscowości (bez części w nawiasach) i usuń białe znaki
                 kody_pocztowe_data['MIEJSCOWOŚĆ_CLEAN'] = kody_pocztowe_data['MIEJSCOWOŚĆ'].str.extract(r'\((.*?)\)', expand=False).fillna(kody_pocztowe_data['MIEJSCOWOŚĆ']).str.strip()
            elif kody_pocztowe_data is not None:
                 logger.error(f"Kolumna 'MIEJSCOWOŚĆ' nie znaleziona w {KODY_POCZTOWE_FILENAME}")
                 kody_pocztowe_data = None

            if kody_pocztowe_data is not None:
                 logger.info("Przygotowano dane kodów pocztowych.")
        except Exception as e:
            logger.error(f"Błąd podczas przygotowywania danych kodów pocztowych: {e}")
            kody_pocztowe_data = None


def enrich_ulic_data(ulic_df, simc_df):
    """Wzbogaca dane ULIC o nazwy miejscowości z SIMC."""
    if ulic_df is None or simc_df is None:
        logger.warning("Próba wzbogacenia danych ULIC, ale brakuje danych wejściowych.")
        return None
    try:
        # Sprawdź, czy potrzebne kolumny istnieją w SIMC
        required_simc_cols = ['WOJ', 'POW', 'GMI', 'RODZ_GMI', 'SYM', 'NAZWA']
        if not all(col in simc_df.columns for col in required_simc_cols):
            missing_cols = [col for col in required_simc_cols if col not in simc_df.columns]
            logger.error(f"Brakujące kolumny w danych SIMC potrzebne do wzbogacenia: {missing_cols}")
            return None

        simc_to_merge = simc_df[required_simc_cols].copy()
        simc_to_merge.rename(columns={'NAZWA': 'NAZWA_MIEJSCOWOSCI'}, inplace=True)

        ulic_enriched = ulic_df.copy()

        # Sprawdź, czy potrzebne kolumny istnieją w ULIC
        nazwa1_col = 'NAZWA_1' if 'NAZWA_1' in ulic_enriched.columns else None
        nazwa2_col = 'NAZWA_2' if 'NAZWA_2' in ulic_enriched.columns else None

        if nazwa1_col and nazwa2_col:
            ulic_enriched['NAZWA_ULICY_FULL'] = ulic_enriched[nazwa2_col].fillna('') + ' ' + ulic_enriched[nazwa1_col].fillna('')
            ulic_enriched['NAZWA_ULICY_FULL'] = ulic_enriched['NAZWA_ULICY_FULL'].str.strip()
        elif nazwa1_col:
            ulic_enriched['NAZWA_ULICY_FULL'] = ulic_enriched[nazwa1_col].fillna('').str.strip()
            logger.warning("Kolumna 'NAZWA_2' brakuje w danych ULIC. Użyto tylko 'NAZWA_1'.")
        else:
            logger.error("Kolumna 'NAZWA_1' (i być może 'NAZWA_2') brakuje w danych ULIC. Nie można utworzyć pełnej nazwy ulicy.")
            return ulic_df # Zwróć oryginalny df, jeśli nie można stworzyć nazwy

        # Sprawdź, czy kolumny do merge'a istnieją w obu DataFrame'ach
        merge_cols = ['WOJ', 'POW', 'GMI', 'RODZ_GMI', 'SYM']
        if not all(col in ulic_enriched.columns for col in merge_cols):
             missing_ulic = [col for col in merge_cols if col not in ulic_enriched.columns]
             logger.error(f"Brakujące kolumny w danych ULIC potrzebne do merge'a: {missing_ulic}")
             return ulic_df # Zwróć oryginalny, jeśli brakuje kluczy

        ulic_enriched = pd.merge(
            ulic_enriched,
            simc_to_merge,
            on=merge_cols,
            how='left' # Zachowaj wszystkie ulice, nawet jeśli nie znajdą dopasowania w SIMC
        )

        # Upewnij się, że kluczowa kolumna 'NAZWA_ULICY_FULL' nadal istnieje
        if 'NAZWA_ULICY_FULL' not in ulic_enriched.columns:
            logger.error("Kolumna 'NAZWA_ULICY_FULL' zaginęła po operacji merge. Sprawdź logikę i klucze merge'a.")
            return None # Zwróć None, bo coś poszło nie tak

        return ulic_enriched
    except Exception as e:
        logger.error(f"Błąd podczas wzbogacania danych ULIC: {e}")
        return None

def get_terc_codes(woj_nazwa, pow_nazwa, gmi_nazwa, miejscowosc_nazwa, rodz_gmi_hint=None):
    """Wyszukuje kody TERC dla województwa, powiatu i gminy.

    Args:
        woj_nazwa: Nazwa województwa
        pow_nazwa: Nazwa powiatu
        gmi_nazwa: Nazwa gminy
        miejscowosc_nazwa: Nazwa miejscowości
        rodz_gmi_hint: Opcjonalny hint dla typu gminy (4=miasto, 5=obszar wiejski) z SIMC
    """
    if terc_data is None:
        logger.error("Dane TERC nie są załadowane, nie można wyszukać kodów.")
        return None, None, None

    terc_woj, terc_pow, terc_gmi_full = None, None, None
    woj_code, pow_code = None, None

    try:
        # Wyszukiwanie województwa
        if woj_nazwa:
            woj_row = terc_data[terc_data['NAZWA'].str.lower() == woj_nazwa.lower()]
            if not woj_row.empty:
                woj_code = woj_row['WOJ'].iloc[0]
                terc_woj = woj_code
            else:
                logger.warning(f"Nie znaleziono kodu TERC dla województwa: {woj_nazwa}")

        # Wyszukiwanie powiatu (wymaga kodu województwa)
        if woj_code and pow_nazwa:
            pow_row = terc_data[
                (terc_data['NAZWA'].str.lower() == pow_nazwa.lower()) &
                (terc_data['WOJ'] == woj_code) &
                (terc_data['POW'].notna()) & # Powiat ma kod POW
                (terc_data['GMI'].isna())    # Powiat nie ma kodu GMI
            ]
            if not pow_row.empty:
                pow_code = pow_row['POW'].iloc[0]
                terc_pow = f"{woj_code}{pow_code}"
            else:
                logger.warning(f"Nie znaleziono kodu TERC dla powiatu: {pow_nazwa} w woj. {woj_nazwa}")

        # Wyszukiwanie gminy (wymaga kodu województwa i powiatu)
        if woj_code and pow_code and gmi_nazwa:
             # Szukaj najpierw po nazwie gminy, potem po nazwie miejscowości jako fallback
             gmi_row = terc_data[
                 ((terc_data['NAZWA'].str.lower() == gmi_nazwa.lower()) | (terc_data['NAZWA'].str.lower() == miejscowosc_nazwa.lower())) &
                 (terc_data['WOJ'] == woj_code) &
                 (terc_data['POW'] == pow_code) &
                 (terc_data['GMI'].notna()) & # Gmina ma kod GMI
                 (terc_data['RODZ'].notna())  # Gmina ma rodzaj
             ]

             # Jeśli mamy hint RODZ_GMI z SIMC, użyj go do precyzyjnego wyboru
             if rodz_gmi_hint and not gmi_row.empty:
                 gmi_row_with_hint = gmi_row[gmi_row['RODZ'] == rodz_gmi_hint]
                 if not gmi_row_with_hint.empty:
                     gmi_row = gmi_row_with_hint
                     logger.info(f"Użyto RODZ_GMI hint ({rodz_gmi_hint}) dla gminy '{gmi_nazwa}'")

             if not gmi_row.empty:
                 if len(gmi_row) > 1:
                     # Jeśli znaleziono wiele, spróbuj dać priorytet dokładnemu dopasowaniu nazwy gminy
                     gmi_row_preferred = gmi_row[gmi_row['NAZWA'].str.lower() == gmi_nazwa.lower()]
                     if not gmi_row_preferred.empty:
                         gmi_row = gmi_row_preferred
                     else:
                         logger.info(f"Znaleziono wiele pasujących gmin TERC dla '{gmi_nazwa}'/'{miejscowosc_nazwa}'. Wybieram pierwszy znaleziony.")
                 gmi_data = gmi_row.iloc[0]
                 terc_gmi_full = f"{gmi_data['WOJ']}{gmi_data['POW']}{gmi_data['GMI']}{gmi_data['RODZ']}"
             else:
                 logger.warning(f"Nie znaleziono kodu TERC dla gminy: {gmi_nazwa} ani miejscowości: {miejscowosc_nazwa} w powiecie {pow_nazwa}")

    except Exception as e:
        logger.error(f"Błąd podczas wyszukiwania kodów TERC: {e}")
        return None, None, None # Zwróć None w przypadku błędu

    return terc_woj, terc_pow, terc_gmi_full

def get_rodz_gmi_from_simc(woj_nazwa, pow_nazwa, gmi_nazwa, miejscowosc_nazwa):
    """Wyszukuje RODZ_GMI dla miejscowości bezpośrednio z SIMC, aby określić czy to miasto czy wieś.

    Returns:
        rodz_gmi (str): '4' dla miasta, '5' dla obszaru wiejskiego, lub None
    """
    if simc_data is None or terc_data is None:
        logger.error("Dane SIMC lub TERC nie są załadowane.")
        return None

    try:
        # Najpierw znajdź kody województwa i powiatu
        woj_row = terc_data[terc_data['NAZWA'].str.lower() == woj_nazwa.lower()]
        if woj_row.empty:
            return None
        woj_code = woj_row['WOJ'].iloc[0]

        pow_row = terc_data[
            (terc_data['NAZWA'].str.lower() == pow_nazwa.lower()) &
            (terc_data['WOJ'] == woj_code) &
            (terc_data['POW'].notna()) &
            (terc_data['GMI'].isna())
        ]
        if pow_row.empty:
            return None
        pow_code = pow_row['POW'].iloc[0]

        # Znajdź kod gminy (bez RODZ)
        gmi_row = terc_data[
            ((terc_data['NAZWA'].str.lower() == gmi_nazwa.lower()) | (terc_data['NAZWA'].str.lower() == miejscowosc_nazwa.lower())) &
            (terc_data['WOJ'] == woj_code) &
            (terc_data['POW'] == pow_code) &
            (terc_data['GMI'].notna())
        ]
        if gmi_row.empty:
            return None
        gmi_code = gmi_row['GMI'].iloc[0]

        # Teraz szukaj miejscowości w SIMC
        matching_simc = simc_data[
            (simc_data['WOJ'] == woj_code) &
            (simc_data['POW'] == pow_code) &
            (simc_data['GMI'] == gmi_code) &
            (simc_data['NAZWA'].str.strip().str.lower() == miejscowosc_nazwa.strip().lower())
        ]

        if not matching_simc.empty:
            rodz_gmi = matching_simc['RODZ_GMI'].iloc[0]
            logger.info(f"Znaleziono RODZ_GMI={rodz_gmi} dla miejscowości '{miejscowosc_nazwa}' w SIMC")
            return rodz_gmi
        else:
            logger.warning(f"Nie znaleziono miejscowości '{miejscowosc_nazwa}' w SIMC dla gminy {gmi_nazwa}")
            return None

    except Exception as e:
        logger.error(f"Błąd podczas wyszukiwania RODZ_GMI z SIMC: {e}")
        return None

def get_simc_code(terc_gmi_full, miejscowosc_nazwa, gmina_nazwa):
    """Wyszukuje kod SIMC dla podanego TERC gminy i nazwy miejscowości (z fallbackiem na nazwę gminy)."""
    if simc_data is None:
        logger.error("Dane SIMC nie są załadowane, nie można wyszukać kodu.")
        return None, None
    if not terc_gmi_full or len(terc_gmi_full) != 7:
        logger.warning(f"Nieprawidłowy TERC gminy '{terc_gmi_full}' przekazany do wyszukiwania SIMC.")
        return None, None

    woj, pow, gmi, rodz_gmi = terc_gmi_full[:2], terc_gmi_full[2:4], terc_gmi_full[4:6], terc_gmi_full[6]
    sym_code, found_name = None, None

    try:
        # Krok 1: Wyszukaj po nazwie miejscowości
        matching_simc = simc_data[
            (simc_data['WOJ'] == woj) &
            (simc_data['POW'] == pow) &
            (simc_data['GMI'] == gmi) &
            (simc_data['RODZ_GMI'] == rodz_gmi) &
            (simc_data['NAZWA'].str.strip().str.lower() == miejscowosc_nazwa.strip().lower())
        ]
        if not matching_simc.empty:
            if len(matching_simc) > 1:
                logger.info(f"Znaleziono wiele wpisów SIMC dla miejscowości '{miejscowosc_nazwa}'. Wybieram pierwszy.")
            simc_details = matching_simc.iloc[0]
            sym_code = simc_details['SYM']
            found_name = simc_details['NAZWA'] # Zwróć oficjalną nazwę z SIMC
            logger.info(f"Znaleziono kod SIMC dla miejscowości '{miejscowosc_nazwa}': {sym_code}")
            return sym_code, found_name
        else:
            # Krok 2: Fallback - Wyszukaj po nazwie gminy
            logger.info(f"Nie znaleziono SIMC dla '{miejscowosc_nazwa}'. Próba dla nazwy gminy '{gmina_nazwa}'...")
            matching_simc_fallback = simc_data[
                (simc_data['WOJ'] == woj) &
                (simc_data['POW'] == pow) &
                (simc_data['GMI'] == gmi) &
                (simc_data['RODZ_GMI'] == rodz_gmi) &
                (simc_data['NAZWA'].str.strip().str.lower() == gmina_nazwa.strip().lower())
            ]
            if not matching_simc_fallback.empty:
                if len(matching_simc_fallback) > 1:
                    logger.info(f"Znaleziono wiele wpisów SIMC dla nazwy gminy '{gmina_nazwa}'. Wybieram pierwszy.")
                simc_details_fallback = matching_simc_fallback.iloc[0]
                sym_code = simc_details_fallback['SYM']
                found_name = simc_details_fallback['NAZWA'] # Zwróć oficjalną nazwę z SIMC
                logger.info(f"Znaleziono kod SIMC dla nazwy gminy '{gmina_nazwa}' (fallback): {sym_code}")
                return sym_code, found_name
            else:
                logger.warning(f"Nie znaleziono kodu SIMC ani dla miejscowości '{miejscowosc_nazwa}', ani dla nazwy gminy '{gmina_nazwa}' (TERC gminy: {terc_gmi_full})")
                return None, None
    except Exception as e:
        logger.error(f"Błąd podczas wyszukiwania kodu SIMC: {e}")
        return None, None

def get_ulic_data(terc_gmi_full, simc_code):
    """Wyszukuje dane ULIC dla podanego TERC GMI i kodu SIMC, zwraca DataFrame z angielskimi nazwami kolumn."""
    if ulic_data_enriched is None:
        logger.error("Wzbogacone dane ULIC nie są dostępne, nie można wyszukać ulic.")
        return pd.DataFrame() # Zwróć pusty DataFrame
    if not terc_gmi_full or len(terc_gmi_full) != 7 or not simc_code:
        logger.warning("Nie można wyszukać ULIC: brak wzbogaconych danych ULIC, nieprawidłowy TERC GMI lub brak kodu SIMC.")
        return pd.DataFrame()

    woj, pow, gmi, rodz_gmi = terc_gmi_full[:2], terc_gmi_full[2:4], terc_gmi_full[4:6], terc_gmi_full[6]

    try:
        # Sprawdź, czy wymagane kolumny istnieją
        required_ulic_cols = ['WOJ', 'POW', 'GMI', 'RODZ_GMI', 'SYM', 'SYM_UL', 'CECHA', 'NAZWA_ULICY_FULL', 'STAN_NA']
        if not all(col in ulic_data_enriched.columns for col in required_ulic_cols):
             missing_cols = [col for col in required_ulic_cols if col not in ulic_data_enriched.columns]
             logger.error(f"Brakujące kolumny w wzbogaconych danych ULIC: {missing_cols}")
             return pd.DataFrame()

        matching_ulic = ulic_data_enriched[
            (ulic_data_enriched['WOJ'] == woj) &
            (ulic_data_enriched['POW'] == pow) &
            (ulic_data_enriched['GMI'] == gmi) &
            (ulic_data_enriched['RODZ_GMI'] == rodz_gmi) &
            (ulic_data_enriched['SYM'] == simc_code)
        ].copy()

        if not matching_ulic.empty:
            logger.info(f"Znaleziono {len(matching_ulic)} ulic dla SIMC: {simc_code}")
            # Zmień nazwy kolumn na angielskie dla spójności API
            result_df = matching_ulic[['SYM_UL', 'CECHA', 'NAZWA_ULICY_FULL', 'STAN_NA']].rename(
                columns={
                    'SYM_UL': 'ulic_code',
                    'CECHA': 'feature_type', # np. 'ul.', 'al.', 'pl.'
                    'NAZWA_ULICY_FULL': 'street_name',
                    'STAN_NA': 'valid_as_of'
                }
            )
            # Konwertuj 'ulic_code' na string, zachowując wiodące zera jeśli istnieją
            result_df['ulic_code'] = result_df['ulic_code'].astype(str)
            
            # Fix: Replace NaN values with empty strings in 'feature_type'
            result_df['feature_type'] = result_df['feature_type'].fillna('')
            
            # Fix: Also replace NaN values in other string columns to avoid potential issues
            result_df['street_name'] = result_df['street_name'].fillna('')
            result_df['valid_as_of'] = result_df['valid_as_of'].fillna('')
            
            return result_df
        else:
            logger.warning(f"Nie znaleziono kodów ULIC dla SIMC: {simc_code} (TERC GMI: {terc_gmi_full}).")
            return pd.DataFrame()
    except KeyError as e:
        logger.error(f"Błąd klucza podczas wyszukiwania ULIC (brakująca kolumna?): {e}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Błąd podczas wyszukiwania danych ULIC: {e}")
        return pd.DataFrame()

# --- Pydantic Models (Definicje struktur danych dla API) ---

class LocalityListResponse(BaseModel):
    """Model odpowiedzi dla listy miejscowości dla danego kodu pocztowego."""
    postal_code: str
    localities: List[str]

class StreetInfo(BaseModel):
    """Model reprezentujący informacje o pojedynczej ulicy."""
    ulic_code: str
    feature_type: str
    street_name: str
    valid_as_of: Optional[str] = None

class PostalCodeDetailsResponse(BaseModel):
    """Model odpowiedzi dla szczegółowych danych dla kodu pocztowego i miejscowości."""
    query: Dict[str, Optional[str]] # Przechowuje parametry zapytania
    location_from_postal_code: Dict[str, Optional[str]] # Informacje z pliku kodów
    teryt_codes: Dict[str, Optional[str]] # Znalezione kody TERYT
    streets: List[StreetInfo] # Lista ulic dla danej miejscowości

class TerytCodesResponse(BaseModel):
    """Model odpowiedzi dla wyszukiwania kodów TERYT dla konkretnego adresu."""
    query: Dict[str, Optional[str]] # Przechowuje wejściowe parametry
    terc_voivodeship: Optional[str] = None
    terc_county: Optional[str] = None
    terc_municipality: Optional[str] = None
    simc: Optional[str] = None
    simc_official_name: Optional[str] = None # Dodano oficjalną nazwę z SIMC
    ulic_code: Optional[str] = None
    street_name_found: Optional[str] = None # Nazwa ulicy znaleziona w danych ULIC
    message: Optional[str] = None # Dodatkowe informacje/ostrzeżenia

# --- Inicjalizacja FastAPI ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Funkcja uruchamiana przy starcie i zamknięciu aplikacji FastAPI."""
    # Kod uruchamiany przy starcie
    load_data_on_startup()
    yield
    # Kod uruchamiany przy zamknięciu (jeśli potrzebny)

app = FastAPI(
    title="API Teryt",
    description="API do wyszukiwania informacji TERYT.",
    version="1.5.0",
    lifespan=lifespan # Dodane użycie nowego systemu lifespan
)

# --- API Endpoints ---

@app.get("/health", summary="Sprawdza status API", tags=["Status"])
async def health_check():
    """Zwraca status OK, jeśli API działa i podstawowe dane są załadowane."""
    # Sprawdź, czy kluczowe DataFrame'y zostały załadowane
    data_loaded = all(df is not None for df in [terc_data, simc_data, ulic_data_enriched, kody_pocztowe_data])
    status = "OK" if data_loaded else "WARN"
    detail = "Wszystkie wymagane dane załadowane." if data_loaded else "Nie wszystkie wymagane dane zostały załadowane. Sprawdź logi."
    return {"status": status, "data_loaded": data_loaded, "detail": detail}

@app.get(
    "/postal_codes/{postal_code}/localities",
    summary="Zwraca listę miejscowości dla podanego kodu pocztowego",
    tags=["Lookup"],
    response_model=LocalityListResponse,
    dependencies=[Depends(verify_token)]
)
async def get_localities_by_postal_code(
    postal_code: str = Path(..., description="Kod pocztowy w formacie XX-XXX", pattern=r"^\d{2}-\d{3}$")
):
    """
    Na podstawie kodu pocztowego zwraca posortowaną alfabetycznie listę nazw miejscowości
    przypisanych do tego kodu.
    """
    if kody_pocztowe_data is None:
        raise HTTPException(status_code=503, detail="Dane kodów pocztowych nie są załadowane. Spróbuj ponownie później.")
    if 'MIEJSCOWOŚĆ_CLEAN' not in kody_pocztowe_data.columns:
         raise HTTPException(status_code=500, detail="Błąd wewnętrzny serwera: Brak przetworzonej kolumny miejscowości.")

    postal_code = postal_code.strip()
    # Użyj 'PNA' do filtrowania
    pasujace_df = kody_pocztowe_data[kody_pocztowe_data['PNA'] == postal_code]

    if pasujace_df.empty:
        raise HTTPException(status_code=404, detail=f"Nie znaleziono miejscowości dla kodu pocztowego: {postal_code}")

    # Użyj 'MIEJSCOWOŚĆ_CLEAN' do uzyskania listy
    lista_miejscowosci = sorted(pasujace_df['MIEJSCOWOŚĆ_CLEAN'].unique())
    logger.info(f"Znaleziono {len(lista_miejscowosci)} miejscowości dla kodu {postal_code}: {', '.join(lista_miejscowosci)}")
    return LocalityListResponse(postal_code=postal_code, localities=lista_miejscowosci)


@app.get(
    "/postal_codes/{postal_code}/details",
    summary="Wyszukuje szczegółowe informacje adresowe dla kodu pocztowego",
    tags=["Lookup"],
    response_model=PostalCodeDetailsResponse, 
    dependencies=[Depends(verify_token)]
)
async def lookup_postal_code_details(
    postal_code: str = Path(..., description="Kod pocztowy w formacie XX-XXX", pattern=r"^\d{2}-\d{3}$"),
    locality: Optional[str] = Query(None, description="Opcjonalnie: Nazwa miejscowości (miasto/wieś) do zawężenia wyników (jeśli kod pocztowy obejmuje wiele miejscowości)")
):
    """
    Na podstawie kodu pocztowego zwraca szczegółowe dane TERYT (TERC, SIMC, ULIC).
    Jeśli kod pocztowy obejmuje wiele miejscowości, *musisz* podać parametr 'locality',
    aby uzyskać szczegóły dla konkretnej z nich. W przeciwnym razie, jeśli tylko jedna miejscowość
    pasuje do kodu, jej szczegóły są zwracane od razu.
    """
    if kody_pocztowe_data is None:
        raise HTTPException(status_code=503, detail="Dane kodów pocztowych nie są załadowane.")
    if 'MIEJSCOWOŚĆ_CLEAN' not in kody_pocztowe_data.columns:
        raise HTTPException(status_code=500, detail="Błąd wewnętrzny serwera: Brak przetworzonej kolumny miejscowości.")

    postal_code = postal_code.strip()
    pasujace_miejscowosci_df = kody_pocztowe_data[kody_pocztowe_data['PNA'] == postal_code]

    if pasujace_miejscowosci_df.empty:
        raise HTTPException(status_code=404, detail=f"Nie znaleziono miejscowości dla kodu pocztowego: {postal_code}")

    lista_miejscowosci_unikalne = sorted(pasujace_miejscowosci_df['MIEJSCOWOŚĆ_CLEAN'].unique())

    target_miejscowosc = None
    if locality:
        # Znajdź miejscowość pasującą do podanej (ignorując wielkość liter)
        locality_clean = locality.strip()
        target_miejscowosc = next((m for m in lista_miejscowosci_unikalne if m.lower() == locality_clean.lower()), None)
        if not target_miejscowosc:
            raise HTTPException(status_code=404, detail=f"Miejscowość '{locality}' nie znaleziona dla kodu pocztowego {postal_code}. Dostępne opcje: {', '.join(lista_miejscowosci_unikalne)}")
    elif len(lista_miejscowosci_unikalne) == 1:
        # Jeśli jest tylko jedna, wybierz ją automatycznie
        target_miejscowosc = lista_miejscowosci_unikalne[0]
    else:
        # Jeśli jest wiele, a użytkownik nie podał, zwróć błąd z listą opcji
         raise HTTPException(
             status_code=400, # Bad Request - użytkownik musi podać więcej informacji
             detail={
                 "message": f"Kod pocztowy {postal_code} obejmuje wiele miejscowości. Podaj parametr 'locality', aby wybrać jedną.",
                 "available_localities": lista_miejscowosci_unikalne
             }
         )

    # Pobierz wiersz danych dla wybranej miejscowości z pliku kodów pocztowych
    # Używamy .iloc[0], zakładając, że kombinacja PNA i MIEJSCOWOŚĆ_CLEAN jest unikalna lub bierzemy pierwszy pasujący
    dane_miejscowosci_row = pasujace_miejscowosci_df[pasujace_miejscowosci_df['MIEJSCOWOŚĆ_CLEAN'] == target_miejscowosc].iloc[0]

    woj_nazwa = dane_miejscowosci_row.get('WOJEWÓDZTWO')
    pow_nazwa = dane_miejscowosci_row.get('POWIAT')
    gmi_nazwa = dane_miejscowosci_row.get('GMINA')
    ulica_z_kodu = dane_miejscowosci_row.get('ULICA') if pd.notna(dane_miejscowosci_row.get('ULICA')) else None
    numery_z_kodu = dane_miejscowosci_row.get('NUMERY') if pd.notna(dane_miejscowosci_row.get('NUMERY')) else None

    # Sprawdzenie, czy mamy wszystkie potrzebne nazwy administracyjne
    if not all([woj_nazwa, pow_nazwa, gmi_nazwa]):
        missing_info = [name for name, val in [('WOJEWÓDZTWO', woj_nazwa), ('POWIAT', pow_nazwa), ('GMINA', gmi_nazwa)] if not val]
        logger.error(f"Niekompletne dane administracyjne w pliku kodów pocztowych dla {target_miejscowosc} ({postal_code}): {', '.join(missing_info)}")
        raise HTTPException(status_code=500, detail=f"Niekompletne dane w pliku kodów pocztowych dla {target_miejscowosc}. Brakuje: {', '.join(missing_info)}")

    # Najpierw znajdź RODZ_GMI dla miejscowości z SIMC (miasto vs wieś)
    rodz_gmi_hint = get_rodz_gmi_from_simc(woj_nazwa, pow_nazwa, gmi_nazwa, target_miejscowosc)

    # Wyszukaj kody TERC z hintem RODZ_GMI
    terc_woj, terc_pow, terc_gmi_full = get_terc_codes(woj_nazwa, pow_nazwa, gmi_nazwa, target_miejscowosc, rodz_gmi_hint)

    # Wyszukaj kod SIMC (wymaga pełnego TERC gminy)
    sym_code, simc_nazwa_oficjalna = None, None
    if terc_gmi_full:
        sym_code, simc_nazwa_oficjalna = get_simc_code(terc_gmi_full, target_miejscowosc, gmi_nazwa)
    else:
        logger.warning(f"Nie można wyszukać SIMC, ponieważ nie udało się ustalić pełnego kodu TERC gminy dla {target_miejscowosc}.")


    # Wyszukaj dane ULIC (wymaga pełnego TERC gminy i kodu SIMC)
    ulic_df = pd.DataFrame()
    if terc_gmi_full and sym_code:
        ulic_df = get_ulic_data(terc_gmi_full, sym_code)
    else:
         logger.warning(f"Nie można wyszukać ULIC dla {target_miejscowosc}, brak TERC gminy ({terc_gmi_full}) lub SIMC ({sym_code}).")


    # W przypadku problemu z konwersją DataFrame na słowniki dla Pydantic, dodaj dodatkową obróbkę
    streets_list = []
    if not ulic_df.empty:
        # Fix: Additional safety check before conversion
        ulic_df_clean = ulic_df.copy()
        # Ensure all string columns have string values (not NaN)
        for col in ['feature_type', 'street_name', 'valid_as_of']:
            if col in ulic_df_clean.columns:
                ulic_df_clean[col] = ulic_df_clean[col].fillna('')
        streets_list = ulic_df_clean.to_dict(orient='records')
    
    # Przygotuj odpowiedź
    response = PostalCodeDetailsResponse(
        query={"postal_code": postal_code, "locality_input": locality, "locality_selected": target_miejscowosc},
        location_from_postal_code={
            "locality": target_miejscowosc,
            "voivodeship_name": woj_nazwa,
            "county_name": pow_nazwa,
            "municipality_name": gmi_nazwa,
            "street_suggestion": ulica_z_kodu, # Ulica sugerowana przez kod pocztowy (może być None)
            "numbers_suggestion": numery_z_kodu # Numery sugerowane przez kod pocztowy (może być None)
        },
        teryt_codes={
            "terc_voivodeship": terc_woj,
            "terc_county": terc_pow,
            "terc_municipality": terc_gmi_full,
            "simc": sym_code,
            "simc_official_name": simc_nazwa_oficjalna # Dodano oficjalną nazwę
        },
        streets=streets_list # Użyj oczyszczonej listy zamiast bezpośredniej konwersji
    )

    return response


@app.get(
    "/lookup/address",
    summary="Wyszukuje kody TERYT dla konkretnego adresu",
    tags=["Lookup"],
    response_model=TerytCodesResponse, 
    dependencies=[Depends(verify_token)]
)
async def lookup_address_teryt_codes(
    postal_code: str = Query(..., description="Kod pocztowy (np. '55-011')", pattern=r"^\d{2}-\d{3}$"),
    locality: str = Query(..., description="Nazwa miejscowości", min_length=1),
    street_name: Optional[str] = Query(None, description="Nazwa ulicy (opcjonalnie, jeśli miejscowość nie ma ulic)", min_length=1)
):
    """
    Znajduje kody TERC, SIMC i ULIC dla konkretnego adresu zdefiniowanego przez
    kod pocztowy, nazwę miejscowości i (opcjonalnie) nazwę ulicy.
    """
    query_params = {"postal_code": postal_code, "locality": locality, "street_name": street_name}
    logger.info(f"Żądanie wyszukania adresu: {query_params}")

    # --- Walidacja danych wejściowych i dostępności danych ---
    if kody_pocztowe_data is None: raise HTTPException(status_code=503, detail="Dane kodów pocztowych nie są załadowane.")
    if 'MIEJSCOWOŚĆ_CLEAN' not in kody_pocztowe_data.columns: raise HTTPException(status_code=500, detail="Błąd wewnętrzny: Brak przetworzonej kolumny miejscowości w danych kodów.")
    if ulic_data_enriched is None: raise HTTPException(status_code=503, detail="Wzbogacone dane ulic (ULIC) nie są załadowane.")
    if 'NAZWA_ULICY_FULL' not in ulic_data_enriched.columns: raise HTTPException(status_code=500, detail="Błąd wewnętrzny: Brak przetworzonej kolumny nazwy ulicy w danych ULIC.")

    postal_code = postal_code.strip()
    locality_clean = locality.strip()
    street_name_clean = street_name.strip() if street_name else None

    # --- Krok 1: Sprawdź kod pocztowy i miejscowość ---
    pasujace_kody_df = kody_pocztowe_data[kody_pocztowe_data['PNA'] == postal_code]
    if pasujace_kody_df.empty:
        raise HTTPException(status_code=404, detail=f"Kod pocztowy nie znaleziony: {postal_code}")

    pasujaca_miejscowosc_df = pasujace_kody_df[pasujace_kody_df['MIEJSCOWOŚĆ_CLEAN'].str.lower() == locality_clean.lower()]
    if pasujaca_miejscowosc_df.empty:
        available_localities = sorted(pasujace_kody_df['MIEJSCOWOŚĆ_CLEAN'].unique())
        raise HTTPException(status_code=404, detail=f"Miejscowość '{locality}' nie znaleziona lub nie pasuje do kodu pocztowego {postal_code}. Dostępne dla tego kodu: {', '.join(available_localities)}")

    dane_miejscowosci_row = pasujaca_miejscowosc_df.iloc[0]
    woj_nazwa = dane_miejscowosci_row.get('WOJEWÓDZTWO')
    pow_nazwa = dane_miejscowosci_row.get('POWIAT')
    gmi_nazwa = dane_miejscowosci_row.get('GMINA')

    if not all([woj_nazwa, pow_nazwa, gmi_nazwa]):
        missing_info = [name for name, val in [('WOJEWÓDZTWO', woj_nazwa), ('POWIAT', pow_nazwa), ('GMINA', gmi_nazwa)] if not val]
        logger.error(f"Niekompletne dane administracyjne w pliku kodów pocztowych dla {locality_clean} ({postal_code}): {', '.join(missing_info)}")
        raise HTTPException(status_code=500, detail=f"Niekompletne dane administracyjne w pliku kodów pocztowych dla {locality_clean}. Brakuje: {', '.join(missing_info)}")

    # --- Krok 2: Znajdź kody TERC i SIMC ---
    # Najpierw znajdź RODZ_GMI dla miejscowości z SIMC (miasto vs wieś)
    rodz_gmi_hint = get_rodz_gmi_from_simc(woj_nazwa, pow_nazwa, gmi_nazwa, locality_clean)

    # Wyszukaj kody TERC z hintem RODZ_GMI
    terc_woj, terc_pow, terc_gmi_full = get_terc_codes(woj_nazwa, pow_nazwa, gmi_nazwa, locality_clean, rodz_gmi_hint)
    if not terc_gmi_full:
        logger.warning(f"Nie udało się ustalić pełnego kodu TERC gminy dla {locality_clean}, Gmina {gmi_nazwa}, Powiat {pow_nazwa}")
        raise HTTPException(status_code=404, detail="Nie udało się ustalić pełnego kodu TERC gminy dla podanych danych lokalizacyjnych.")

    sym_code, simc_nazwa_oficjalna = get_simc_code(terc_gmi_full, locality_clean, gmi_nazwa)
    if not sym_code:
        logger.warning(f"Nie udało się ustalić kodu SIMC dla {locality_clean} (TERC GMI: {terc_gmi_full})")
        raise HTTPException(status_code=404, detail=f"Nie udało się ustalić kodu SIMC dla miejscowości '{locality_clean}'.")

    # --- Krok 3: Znajdź kod ULIC dla podanej ulicy, jeśli podano ---
    ulic_code = None
    street_name_found = None
    message = None
    street_suggestions = None

    if street_name_clean:
        try:
            woj, pow, gmi, rodz_gmi = terc_gmi_full[:2], terc_gmi_full[2:4], terc_gmi_full[4:6], terc_gmi_full[6]
            candidate_streets_df = ulic_data_enriched[
                (ulic_data_enriched['WOJ'] == woj) &
                (ulic_data_enriched['POW'] == pow) &
                (ulic_data_enriched['GMI'] == gmi) &
                (ulic_data_enriched['RODZ_GMI'] == rodz_gmi) &
                (ulic_data_enriched['SYM'] == sym_code)
            ]
            if not candidate_streets_df.empty:
                matching_street_df = candidate_streets_df[
                    candidate_streets_df['NAZWA_ULICY_FULL'].str.strip().str.lower() == street_name_clean.lower()
                ]
                if len(matching_street_df) == 1:
                    ulic_code = matching_street_df['SYM_UL'].iloc[0]
                    street_name_found = matching_street_df['NAZWA_ULICY_FULL'].iloc[0]
                    logger.info(f"Znaleziono unikalny ULIC {ulic_code} dla ulicy '{street_name_clean}' w SIMC {sym_code}")
                elif len(matching_street_df) > 1:
                    ulic_codes_found = matching_street_df['SYM_UL'].tolist()
                    street_names_found = matching_street_df['NAZWA_ULICY_FULL'].unique().tolist()
                    message = f"Znaleziono wiele wpisów dla ulicy '{street_name}'. Dane mogą być niespójne. Znalezione kody ULIC: {ulic_codes_found}"
                    logger.warning(message)
                    ulic_code = ulic_codes_found[0]
                    street_name_found = street_names_found[0]
                else:
                    logger.warning(f"Ulica '{street_name_clean}' nie znaleziona w SIMC {sym_code} (TERC GMI: {terc_gmi_full})")
                    raise HTTPException(status_code=404, detail=f"Ulica '{street_name}' nie znaleziona w miejscowości '{locality}' (SIMC: {sym_code}).")
            else:
                logger.warning(f"Brak jakichkolwiek ulic w danych ULIC dla SIMC {sym_code} (TERC GMI: {terc_gmi_full}).")
                raise HTTPException(status_code=404, detail=f"Brak danych o ulicach dla miejscowości '{locality}' (SIMC: {sym_code}).")
        except HTTPException as http_exc:
            raise http_exc
        except Exception as e:
            logger.error(f"Błąd podczas wyszukiwania ULIC dla ulicy '{street_name_clean}': {e}")
            raise HTTPException(status_code=500, detail="Błąd wewnętrzny serwera podczas wyszukiwania ulicy.")
    else:
        # Jeśli nie podano ulicy, sprawdź czy są dostępne ulice dla tej miejscowości i podpowiedz je
        woj, pow, gmi, rodz_gmi = terc_gmi_full[:2], terc_gmi_full[2:4], terc_gmi_full[4:6], terc_gmi_full[6]
        candidate_streets_df = ulic_data_enriched[
            (ulic_data_enriched['WOJ'] == woj) &
            (ulic_data_enriched['POW'] == pow) &
            (ulic_data_enriched['GMI'] == gmi) &
            (ulic_data_enriched['RODZ_GMI'] == rodz_gmi) &
            (ulic_data_enriched['SYM'] == sym_code)
        ]
        if not candidate_streets_df.empty:
            street_suggestions = sorted(candidate_streets_df['NAZWA_ULICY_FULL'].dropna().unique())
            if street_suggestions:
                message = f"Dla tej miejscowości dostępne są następujące ulice: {', '.join(street_suggestions[:10])}{'...' if len(street_suggestions)>10 else ''}"

    # --- Krok 4: Zwróć wynik ---
    response = TerytCodesResponse(
        query=query_params,
        terc_voivodeship=terc_woj,
        terc_county=terc_pow,
        terc_municipality=terc_gmi_full,
        simc=sym_code,
        simc_official_name=simc_nazwa_oficjalna,
        ulic_code=str(ulic_code) if ulic_code else None,
        street_name_found=street_name_found,
        message=message
    )
    # Dodaj podpowiedzi ulic, jeśli są dostępne i nie podano ulicy
    if street_suggestions:
        response_dict = response.dict()
        response_dict['street_suggestions'] = street_suggestions
        return response_dict
    return response


# --- Uruchomienie aplikacji (jeśli plik jest uruchamiany bezpośrednio) ---
if __name__ == "__main__":
    # Uruchomienie serwera FastAPI za pomocą Uvicorn
    # host="0.0.0.0" pozwala na dostęp z innych maszyn w sieci (np. z kontenera Docker)
    # reload=True jest przydatne podczas developmentu, automatycznie restartuje serwer po zmianach w kodzie
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
