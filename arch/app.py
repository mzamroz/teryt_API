# main.py
import os
import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Path
from typing import List, Optional, Dict, Any
import logging
from pydantic import BaseModel, Field # Dodano Field dla lepszej walidacji Query

# --- Konfiguracja ---
# (bez zmian)
DATA_DIR = os.getenv('DATA_DIR', './dane')
TERC_FILENAME = os.getenv('TERC_FILENAME', 'TERC_Adresowy_2025-04-08.csv')
SIMC_FILENAME = os.getenv('SIMC_FILENAME', 'SIMC_Adresowy_2025-04-08.csv')
ULIC_FILENAME = os.getenv('ULIC_FILENAME', 'ULIC_Adresowy_2025-04-08.csv')
KODY_POCZTOWE_FILENAME = os.getenv('KODY_POCZTOWE_FILENAME', 'kody_pocztowe.csv')

COLUMN_DTYPES = {
    'WOJ': str, 'POW': str, 'GMI': str, 'RODZ': str, 'RODZ_GMI': str,
    'SYM': str, 'SYM_UL': str, 'SYMPOD': str, 'PNA': str
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dataframes: Dict[str, pd.DataFrame] = {}
terc_data: Optional[pd.DataFrame] = None
simc_data: Optional[pd.DataFrame] = None
ulic_data: Optional[pd.DataFrame] = None
ulic_data_enriched: Optional[pd.DataFrame] = None
kody_pocztowe_data: Optional[pd.DataFrame] = None

# --- Funkcje pomocnicze ---
# (load_data_on_startup, enrich_ulic_data, get_terc_codes, get_simc_code, get_ulic_data - bez zmian)
# ... (istniejące funkcje pomocnicze) ...
def load_data_on_startup():
    """Loads CSV files into global DataFrames on application startup."""
    global dataframes, terc_data, simc_data, ulic_data, kody_pocztowe_data, ulic_data_enriched
    logger.info(f"Starting data loading from directory: {DATA_DIR}")
    if not os.path.exists(DATA_DIR):
        logger.error(f"Directory '{DATA_DIR}' does not exist. Cannot load data.")
        return
    all_files = os.listdir(DATA_DIR)
    required_files = [TERC_FILENAME, SIMC_FILENAME, ULIC_FILENAME, KODY_POCZTOWE_FILENAME]
    missing_files = [f for f in required_files if f not in all_files]
    if missing_files: logger.warning(f"Missing required files in '{DATA_DIR}': {', '.join(missing_files)}")
    loaded_files_count = 0
    for file_name in required_files:
        if file_name in all_files:
            file_path = os.path.join(DATA_DIR, file_name)
            df = None
            try:
                df = pd.read_csv(file_path, delimiter=';', on_bad_lines='warn', encoding='utf-8',dtype=COLUMN_DTYPES, low_memory=False)
                logger.info(f"Loaded {file_name} (UTF-8).")
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(file_path, delimiter=';', on_bad_lines='warn', encoding='latin1', dtype=COLUMN_DTYPES, low_memory=False)
                    logger.warning(f"File {file_name} loaded using 'latin1' encoding instead of 'utf-8'.")
                except Exception as e_inner: logger.error(f"Failed to load {file_name} with both UTF-8 and Latin-1: {e_inner}"); continue
            except pd.errors.ParserError as e_parser: logger.error(f"Error parsing {file_name}: {e_parser}. Check file structure and delimiter."); continue
            except Exception as e_outer: logger.error(f"Unexpected error loading {file_name}: {e_outer}"); continue
            if df is not None:
                df.columns = df.columns.str.strip()
                dataframes[file_name] = df; loaded_files_count += 1
                if file_name == TERC_FILENAME: terc_data = df
                elif file_name == SIMC_FILENAME: simc_data = df
                elif file_name == ULIC_FILENAME: ulic_data = df
                elif file_name == KODY_POCZTOWE_FILENAME: kody_pocztowe_data = df
        else: logger.warning(f"File {file_name} not found in {DATA_DIR}.")
    logger.info(f"Finished data loading. Loaded {loaded_files_count} out of {len(required_files)} required files.")
    if ulic_data is not None and simc_data is not None:
        ulic_data_enriched = enrich_ulic_data(ulic_data, simc_data)
        if ulic_data_enriched is not None: logger.info("Successfully enriched ULIC data with locality names.")
        else: logger.warning("Failed to enrich ULIC data.")
    else: logger.warning("Cannot enrich ULIC data because ULIC or SIMC data is missing.")
    if kody_pocztowe_data is not None:
         try:
            if 'PNA' in kody_pocztowe_data.columns: kody_pocztowe_data['PNA'] = kody_pocztowe_data['PNA'].astype(str)
            else: logger.error(f"Column 'PNA' not found in {KODY_POCZTOWE_FILENAME}"); kody_pocztowe_data = None
            if kody_pocztowe_data is not None and 'MIEJSCOWOŚĆ' in kody_pocztowe_data.columns: kody_pocztowe_data['MIEJSCOWOŚĆ_CLEAN'] = kody_pocztowe_data['MIEJSCOWOŚĆ'].str.extract(r'\((.*?)\)', expand=False).fillna(kody_pocztowe_data['MIEJSCOWOŚĆ']).str.strip()
            elif kody_pocztowe_data is not None: logger.error(f"Column 'MIEJSCOWOŚĆ' not found in {KODY_POCZTOWE_FILENAME}"); kody_pocztowe_data = None
            if kody_pocztowe_data is not None: logger.info("Prepared postal code data.")
         except Exception as e: logger.error(f"Error preparing postal code data: {e}"); kody_pocztowe_data = None

def enrich_ulic_data(ulic_df, simc_df):
    """Enriches ULIC data with locality names from SIMC."""
    if ulic_df is None or simc_df is None: logger.warning("Attempting to enrich ULIC data, but input data is missing."); return None
    try:
        simc_to_merge = simc_df[['WOJ', 'POW', 'GMI', 'RODZ_GMI', 'SYM', 'NAZWA']].copy()
        simc_to_merge.rename(columns={'NAZWA': 'NAZWA_MIEJSCOWOSCI'}, inplace=True)
        ulic_enriched = ulic_df.copy()
        nazwa1_col = 'NAZWA_1' if 'NAZWA_1' in ulic_enriched.columns else None
        nazwa2_col = 'NAZWA_2' if 'NAZWA_2' in ulic_enriched.columns else None
        if nazwa1_col and nazwa2_col:
             ulic_enriched['NAZWA_ULICY_FULL'] = ulic_enriched[nazwa1_col].fillna('') + ' ' + ulic_enriched[nazwa2_col].fillna('')
             ulic_enriched['NAZWA_ULICY_FULL'] = ulic_enriched['NAZWA_ULICY_FULL'].str.strip()
        elif nazwa1_col:
             ulic_enriched['NAZWA_ULICY_FULL'] = ulic_enriched[nazwa1_col].fillna('').str.strip()
             logger.warning("Column 'NAZWA_2' missing in ULIC data. Using only 'NAZWA_1'.")
        else: logger.error("Column 'NAZWA_1' (and possibly 'NAZWA_2') missing in ULIC data. Cannot create full street name."); return ulic_df
        merge_cols = ['WOJ', 'POW', 'GMI', 'RODZ_GMI', 'SYM']
        if all(col in ulic_enriched.columns for col in merge_cols) and all(col in simc_to_merge.columns for col in merge_cols):
            ulic_enriched = pd.merge(ulic_enriched, simc_to_merge, on=merge_cols, how='left')
            # Upewnij się, że kluczowa kolumna istnieje po merge
            if 'NAZWA_ULICY_FULL' not in ulic_enriched.columns:
                 logger.error("Column 'NAZWA_ULICY_FULL' lost after merge. Check merge keys and logic."); return None
            return ulic_enriched
        else:
            missing_ulic = [col for col in merge_cols if col not in ulic_enriched.columns]
            missing_simc = [col for col in merge_cols if col not in simc_to_merge.columns]
            logger.error(f"Cannot merge ULIC and SIMC. Missing columns in ULIC: {missing_ulic}, in SIMC: {missing_simc}"); return ulic_df
    except Exception as e: logger.error(f"Error enriching ULIC data: {e}"); return None

def get_terc_codes(woj_nazwa, pow_nazwa, gmi_nazwa, miejscowosc_nazwa):
    """Looks up TERC codes for voivodeship, county, and municipality."""
    if terc_data is None: return None, None, None
    terc_woj, terc_pow, terc_gmi = None, None, None
    woj_code, pow_code = None, None
    try:
        if woj_nazwa:
            woj_row = terc_data[terc_data['NAZWA'].str.lower() == woj_nazwa.lower()]
            if not woj_row.empty: woj_code = woj_row['WOJ'].iloc[0]; terc_woj = woj_code
            else: logger.warning(f"TERC WOJ not found for: {woj_nazwa}")
        if woj_code and pow_nazwa:
            pow_row = terc_data[(terc_data['NAZWA'].str.lower() == pow_nazwa.lower()) & (terc_data['WOJ'] == woj_code) & (terc_data['POW'].notna()) & (terc_data['GMI'].isna())]
            if not pow_row.empty: pow_code = pow_row['POW'].iloc[0]; terc_pow = f"{woj_code}{pow_code}"
            else: logger.warning(f"TERC POW not found for: {pow_nazwa} in voiv. {woj_nazwa}")
        if woj_code and pow_code and gmi_nazwa:
             # Preferuj nazwę gminy, fallback na miejscowość
             gmi_row = terc_data[((terc_data['NAZWA'].str.lower() == gmi_nazwa.lower()) | (terc_data['NAZWA'].str.lower() == miejscowosc_nazwa.lower())) & (terc_data['WOJ'] == woj_code) & (terc_data['POW'] == pow_code) & (terc_data['GMI'].notna()) & (terc_data['RODZ'].notna())]
             if not gmi_row.empty:
                 if len(gmi_row) > 1:
                     # Jeśli jest gmina i miejscowość o tej samej nazwie, wybierz gminę
                     gmi_row_preferred = gmi_row[gmi_row['NAZWA'].str.lower() == gmi_nazwa.lower()]
                     if not gmi_row_preferred.empty: gmi_row = gmi_row_preferred
                     else: logger.info(f"Multiple TERC GMI found for '{gmi_nazwa}'/'{miejscowosc_nazwa}'. Selecting first match.")
                 gmi_data = gmi_row.iloc[0]; terc_gmi = f"{gmi_data['WOJ']}{gmi_data['POW']}{gmi_data['GMI']}{gmi_data['RODZ']}"
             else: logger.warning(f"TERC GMI not found for gmina: {gmi_nazwa} or locality: {miejscowosc_nazwa} in county {pow_nazwa}")
    except Exception as e: logger.error(f"Error looking up TERC codes: {e}"); return None, None, None
    return terc_woj, terc_pow, terc_gmi

def get_simc_code(terc_gmi_full, miejscowosc_nazwa, gmina_nazwa):
    """Looks up SIMC code for the given municipality TERC and locality name (with fallback to gmina name)."""
    if simc_data is None or not terc_gmi_full or len(terc_gmi_full) != 7: logger.warning("Cannot lookup SIMC: missing SIMC data or invalid TERC GMI."); return None, None
    woj, pow, gmi, rodz_gmi = terc_gmi_full[:2], terc_gmi_full[2:4], terc_gmi_full[4:6], terc_gmi_full[6]
    sym_code, found_name = None, None
    try:
        matching_simc = simc_data[(simc_data['WOJ'] == woj) & (simc_data['POW'] == pow) & (simc_data['GMI'] == gmi) & (simc_data['RODZ_GMI'] == rodz_gmi) & (simc_data['NAZWA'].str.strip().str.lower() == miejscowosc_nazwa.strip().lower())]
        if not matching_simc.empty:
            if len(matching_simc) > 1: logger.info(f"Multiple SIMC entries found for locality '{miejscowosc_nazwa}'. Selecting first.")
            simc_details = matching_simc.iloc[0]; sym_code = simc_details['SYM']; found_name = simc_details['NAZWA']
            logger.info(f"Found SIMC for locality '{miejscowosc_nazwa}': {sym_code}"); return sym_code, found_name
        else:
            logger.info(f"SIMC not found for '{miejscowosc_nazwa}'. Trying gmina name '{gmina_nazwa}' as fallback...")
            matching_simc_fallback = simc_data[(simc_data['WOJ'] == woj) & (simc_data['POW'] == pow) & (simc_data['GMI'] == gmi) & (simc_data['RODZ_GMI'] == rodz_gmi) & (simc_data['NAZWA'].str.strip().str.lower() == gmina_nazwa.strip().lower())]
            if not matching_simc_fallback.empty:
                if len(matching_simc_fallback) > 1: logger.info(f"Multiple SIMC entries found for gmina name '{gmina_nazwa}'. Selecting first.")
                simc_details_fallback = matching_simc_fallback.iloc[0]; sym_code = simc_details_fallback['SYM']; found_name = simc_details_fallback['NAZWA']
                logger.info(f"Found SIMC for gmina name '{gmina_nazwa}' (fallback): {sym_code}"); return sym_code, found_name
            else: logger.warning(f"SIMC not found for locality '{miejscowosc_nazwa}' or gmina name '{gmina_nazwa}' (TERC GMI: {terc_gmi_full})"); return None, None
    except Exception as e: logger.error(f"Error looking up SIMC code: {e}"); return None, None

def get_ulic_data(terc_gmi_full, simc_code):
    """Looks up ULIC data for the given TERC GMI and SIMC code, returns DataFrame with English columns."""
    if ulic_data_enriched is None or not terc_gmi_full or len(terc_gmi_full) != 7 or not simc_code:
         logger.warning("Cannot lookup ULIC: missing enriched ULIC data, invalid TERC GMI, or missing SIMC code.")
         return pd.DataFrame()
    woj, pow, gmi, rodz_gmi = terc_gmi_full[:2], terc_gmi_full[2:4], terc_gmi_full[4:6], terc_gmi_full[6]
    try:
        matching_ulic = ulic_data_enriched[(ulic_data_enriched['WOJ'] == woj) & (ulic_data_enriched['POW'] == pow) & (ulic_data_enriched['GMI'] == gmi) & (ulic_data_enriched['RODZ_GMI'] == rodz_gmi) & (ulic_data_enriched['SYM'] == simc_code)].copy()
        if not matching_ulic.empty:
            logger.info(f"Found {len(matching_ulic)} streets for SIMC: {simc_code}")
            # Rename columns to English for consistency
            result_df = matching_ulic[['SYM_UL', 'CECHA', 'NAZWA_ULICY_FULL', 'STAN_NA']].rename(
                columns={
                    'SYM_UL': 'ulic_code',
                    'CECHA': 'feature_type',
                    'NAZWA_ULICY_FULL': 'street_name',
                    'STAN_NA': 'valid_as_of'
                }
            )
            return result_df
        else:
            logger.warning(f"No ULIC codes found for SIMC: {simc_code} (TERC GMI: {terc_gmi_full}).")
            return pd.DataFrame()
    except KeyError as e: logger.error(f"Key error looking up ULIC (missing column?): {e}"); return pd.DataFrame()
    except Exception as e: logger.error(f"Error looking up ULIC data: {e}"); return pd.DataFrame()

# --- Pydantic Models ---
class LocalityListResponse(BaseModel):
    postal_code: str
    localities: List[str]

# Nowy model odpowiedzi dla TERYT codes
class TerytCodesResponse(BaseModel):
    query: Dict[str, Optional[str]] # Przechowuje wejściowe parametry
    terc_voivodeship: Optional[str] = None
    terc_county: Optional[str] = None
    terc_municipality: Optional[str] = None
    simc: Optional[str] = None
    ulic_code: Optional[str] = None
    street_name_found: Optional[str] = None # Nazwa ulicy znaleziona w danych ULIC
    message: Optional[str] = None # Dodatkowe informacje/ostrzeżenia

# --- FastAPI Initialization ---
app = FastAPI(
    title="Polish Address Data API",
    description="API for looking up Polish address information based on postal codes and TERYT data.",
    version="1.4.0" # Zwiększona wersja
)

# --- Event handler - startup data loading ---
@app.on_event("startup")
async def startup_event():
    load_data_on_startup()

# --- API Endpoints ---

@app.get("/health", summary="Checks the API status", tags=["Status"])
async def health_check():
    """Returns OK status if the API is running."""
    data_loaded = all(df is not None for df in [terc_data, simc_data, ulic_data_enriched, kody_pocztowe_data])
    return {"status": "OK", "data_loaded": data_loaded}

@app.get(
    "/postal_codes/{postal_code}/localities",
    summary="Returns a list of localities for the given postal code",
    tags=["Lookup"],
    response_model=LocalityListResponse
)
async def get_localities_by_postal_code(
    postal_code: str = Path(..., description="Postal code in XX-XXX format", regex=r"^\d{2}-\d{3}$")
):
    """
    Based on the postal code, returns a sorted list of unique locality names
    (towns, cities, villages) assigned to that code in the postal code file.
    """
    if kody_pocztowe_data is None: raise HTTPException(status_code=503, detail="Postal code data is not loaded. Please try again later.")
    if 'MIEJSCOWOŚĆ_CLEAN' not in kody_pocztowe_data.columns: raise HTTPException(status_code=500, detail="Internal server error: Missing processed locality column.")
    postal_code = postal_code.strip()
    pasujace_df = kody_pocztowe_data[kody_pocztowe_data['PNA'] == postal_code]
    if pasujace_df.empty: raise HTTPException(status_code=404, detail=f"No localities found for postal code: {postal_code}")
    lista_miejscowosci = sorted(pasujace_df['MIEJSCOWOŚĆ_CLEAN'].unique())
    logger.info(f"Found {len(lista_miejscowosci)} localities for code {postal_code}: {', '.join(lista_miejscowosci)}")
    return LocalityListResponse(postal_code=postal_code, localities=lista_miejscowosci)

# --- NOWY ENDPOINT ---
@app.get(
    "/lookup/address",
    summary="Looks up TERYT codes for a specific address",
    tags=["Lookup"],
    response_model=TerytCodesResponse
)
async def lookup_address_teryt_codes(
    postal_code: str = Query(..., description="Postal code (e.g., '55-011')", regex=r"^\d{2}-\d{3}$"),
    locality: str = Query(..., description="Locality name (town/city/village)", min_length=1),
    street_name: str = Query(..., description="Street name", min_length=1)
):
    """
    Finds TERC, SIMC, and ULIC codes for a specific address defined by
    postal code, locality name, and street name.
    """
    # Zapisz zapytanie
    query_params = {"postal_code": postal_code, "locality": locality, "street_name": street_name}
    logger.info(f"Address lookup requested: {query_params}")

    # --- Krok 1: Walidacja kodu pocztowego i miejscowości ---
    if kody_pocztowe_data is None: raise HTTPException(status_code=503, detail="Postal code data is not loaded.")
    if 'MIEJSCOWOŚĆ_CLEAN' not in kody_pocztowe_data.columns: raise HTTPException(status_code=500, detail="Internal server error: Missing processed locality column.")
    if ulic_data_enriched is None: raise HTTPException(status_code=503, detail="Enriched street (ULIC) data is not loaded.")
    if 'NAZWA_ULICY_FULL' not in ulic_data_enriched.columns: raise HTTPException(status_code=500, detail="Internal server error: Missing processed street name column in ULIC data.")


    postal_code = postal_code.strip()
    locality_clean = locality.strip()
    street_name_clean = street_name.strip()

    # Znajdź pasujące wpisy dla kodu pocztowego
    pasujace_kody_df = kody_pocztowe_data[kody_pocztowe_data['PNA'] == postal_code]
    if pasujace_kody_df.empty:
        raise HTTPException(status_code=404, detail=f"Postal code not found: {postal_code}")

    # Sprawdź, czy podana miejscowość pasuje do kodu pocztowego
    pasujaca_miejscowosc_df = pasujace_kody_df[pasujace_kody_df['MIEJSCOWOŚĆ_CLEAN'].str.lower() == locality_clean.lower()]
    if pasujaca_miejscowosc_df.empty:
        available_localities = sorted(pasujace_kody_df['MIEJSCOWOŚĆ_CLEAN'].unique())
        raise HTTPException(status_code=404, detail=f"Locality '{locality}' not found or does not match postal code {postal_code}. Available for this code: {', '.join(available_localities)}")

    # Pobierz dane administracyjne z pliku kodów pocztowych
    dane_miejscowosci_row = pasujaca_miejscowosc_df.iloc[0]
    woj_nazwa = dane_miejscowosci_row.get('WOJEWÓDZTWO')
    pow_nazwa = dane_miejscowosci_row.get('POWIAT')
    gmi_nazwa = dane_miejscowosci_row.get('GMINA')

    if not all([woj_nazwa, pow_nazwa, gmi_nazwa]):
         missing_info = [name for name, val in [('WOJEWÓDZTWO', woj_nazwa), ('POWIAT', pow_nazwa), ('GMINA', gmi_nazwa)] if not val]
         logger.error(f"Incomplete administrative data in postal code file for {locality_clean} ({postal_code}): {', '.join(missing_info)}")
         raise HTTPException(status_code=500, detail=f"Incomplete administrative data in postal code file for {locality_clean}. Missing: {', '.join(missing_info)}")

    # --- Krok 2: Znajdź kody TERC i SIMC ---
    terc_woj, terc_pow, terc_gmi_full = get_terc_codes(woj_nazwa, pow_nazwa, gmi_nazwa, locality_clean)
    if not terc_gmi_full:
         logger.warning(f"Could not determine full TERC code for {locality_clean}, Gmina {gmi_nazwa}, Powiat {pow_nazwa}")
         # Można zwrócić częściowe dane lub błąd
         # return TerytCodesResponse(query=query_params, terc_voivodeship=terc_woj, terc_county=terc_pow, message="Could not determine full TERC municipality code.")
         raise HTTPException(status_code=404, detail="Could not determine the full TERC municipality code for the provided location details.")


    sym_code, simc_nazwa_oficjalna = get_simc_code(terc_gmi_full, locality_clean, gmi_nazwa)
    if not sym_code:
         logger.warning(f"Could not determine SIMC code for {locality_clean} (TERC GMI: {terc_gmi_full})")
         # return TerytCodesResponse(query=query_params, terc_voivodeship=terc_woj, terc_county=terc_pow, terc_municipality=terc_gmi_full, message="Could not determine SIMC code.")
         raise HTTPException(status_code=404, detail=f"Could not determine the SIMC code for locality '{locality_clean}'.")

    # --- Krok 3: Znajdź kod ULIC dla podanej ulicy ---
    ulic_code = None
    street_name_found = None
    message = None

    try:
        woj, pow, gmi, rodz_gmi = terc_gmi_full[:2], terc_gmi_full[2:4], terc_gmi_full[4:6], terc_gmi_full[6]

        # Filtruj ULIC po kodzie TERC gminy i SIMC miejscowości
        candidate_streets_df = ulic_data_enriched[
            (ulic_data_enriched['WOJ'] == woj) &
            (ulic_data_enriched['POW'] == pow) &
            (ulic_data_enriched['GMI'] == gmi) &
            (ulic_data_enriched['RODZ_GMI'] == rodz_gmi) &
            (ulic_data_enriched['SYM'] == sym_code)
        ]

        if not candidate_streets_df.empty:
            # Szukaj ulicy (case-insensitive) w 'NAZWA_ULICY_FULL'
            matching_street_df = candidate_streets_df[
                candidate_streets_df['NAZWA_ULICY_FULL'].str.strip().str.lower() == street_name_clean.lower()
            ]

            if len(matching_street_df) == 1:
                ulic_code = matching_street_df['SYM_UL'].iloc[0]
                street_name_found = matching_street_df['NAZWA_ULICY_FULL'].iloc[0]
                logger.info(f"Found unique ULIC {ulic_code} for street '{street_name_clean}' in SIMC {sym_code}")
            elif len(matching_street_df) > 1:
                logger.warning(f"Multiple ULIC codes found for street '{street_name_clean}' in SIMC {sym_code}. Returning list.")
                # Zwróć błąd lub listę - na razie błąd
                ulic_codes_found = matching_street_df['SYM_UL'].tolist()
                street_names_found = matching_street_df['NAZWA_ULICY_FULL'].unique().tolist()
                message = f"Multiple streets found matching '{street_name}'. Be more specific. Found: {', '.join(street_names_found)} (ULIC codes: {ulic_codes_found})"
                raise HTTPException(status_code=409, detail=message) # 409 Conflict
            else:
                # Brak dokładnego dopasowania, spróbuj 'contains' (opcjonalnie)
                # matching_street_contains_df = candidate_streets_df[candidate_streets_df['NAZWA_ULICY_FULL'].str.contains(street_name_clean, case=False, na=False)]
                # if ... (dalsza logika dla contains)

                logger.warning(f"Street '{street_name_clean}' not found in SIMC {sym_code} (TERC GMI: {terc_gmi_full})")
                raise HTTPException(status_code=404, detail=f"Street '{street_name}' not found in locality '{locality}' (SIMC: {sym_code}).")
        else:
            logger.warning(f"No streets found in ULIC data for SIMC {sym_code} (TERC GMI: {terc_gmi_full}) at all.")
            raise HTTPException(status_code=404, detail=f"No street data available for locality '{locality}' (SIMC: {sym_code}).")

    except HTTPException as http_exc:
        # Przekaż wyjątki HTTP dalej
        raise http_exc
    except Exception as e:
        logger.error(f"Error during ULIC lookup for street '{street_name_clean}': {e}")
        raise HTTPException(status_code=500, detail="Internal server error during street lookup.")


    # --- Krok 4: Zwróć wynik ---
    return TerytCodesResponse(
        query=query_params,
        terc_voivodeship=terc_woj,
        terc_county=terc_pow,
        terc_municipality=terc_gmi_full,
        simc=sym_code,
        ulic_code=str(ulic_code) if ulic_code else None, # Upewnij się, że ULIC jest stringiem
        street_name_found=street_name_found,
        message=message
    )


@app.get(
    "/postal_codes/{postal_code}/details",
    summary="Looks up detailed address information for a postal code",
    tags=["Lookup"],
    response_model=Dict[str, Any]
)
async def lookup_postal_code_details(
    postal_code: str = Path(..., description="Postal code in XX-XXX format", regex=r"^\d{2}-\d{3}$"),
    locality: Optional[str] = Query(None, description="Optional: Name of the locality (town/city) to narrow down results (if the postal code covers multiple localities)")
):
    """
    Based on the postal code, returns detailed TERYT data (TERC, SIMC, ULIC).
    If the postal code covers multiple localities, you *must* provide the 'locality'
    query parameter to get specific details. Otherwise, if only one locality matches
    the code, its details are returned automatically.
    Use the `/postal_codes/{postal_code}/localities` endpoint first if unsure.
    """
    if kody_pocztowe_data is None: raise HTTPException(status_code=503, detail="Postal code data is not loaded. Please try again later.")
    if 'MIEJSCOWOŚĆ_CLEAN' not in kody_pocztowe_data.columns: raise HTTPException(status_code=500, detail="Internal server error: Missing processed locality column.")

    postal_code = postal_code.strip()
    pasujace_miejscowosci_df = kody_pocztowe_data[kody_pocztowe_data['PNA'] == postal_code]
    if pasujace_miejscowosci_df.empty: raise HTTPException(status_code=404, detail=f"No localities found for postal code: {postal_code}")
    lista_miejscowosci = sorted(pasujace_miejscowosci_df['MIEJSCOWOŚĆ_CLEAN'].unique())
    if not locality and len(lista_miejscowosci) > 1:
        return {
            "postal_code": postal_code,
            "message": f"The postal code {postal_code} covers multiple localities. Please provide the 'locality' query parameter to get details for a specific one. Use the GET /postal_codes/{postal_code}/localities endpoint to see the list.",
            "available_localities": lista_miejscowosci
        }
    target_miejscowosc = None
    if locality:
        target_miejscowosc = next((m for m in lista_miejscowosci if m.lower() == locality.strip().lower()), None)
        if not target_miejscowosc: raise HTTPException(status_code=404, detail=f"Locality '{locality}' not found for postal code {postal_code}. Available options: {', '.join(lista_miejscowosci)}")
    elif len(lista_miejscowosci) == 1: target_miejscowosc = lista_miejscowosci[0]
    else: raise HTTPException(status_code=400, detail="Cannot determine locality. Please provide the 'locality' query parameter.")

    dane_miejscowosci_row = pasujace_miejscowosci_df[pasujace_miejscowosci_df['MIEJSCOWOŚĆ_CLEAN'] == target_miejscowosc].iloc[0]
    woj_nazwa = dane_miejscowosci_row.get('WOJEWÓDZTWO'); pow_nazwa = dane_miejscowosci_row.get('POWIAT'); gmi_nazwa = dane_miejscowosci_row.get('GMINA')
    ulica_z_kodu = dane_miejscowosci_row.get('ULICA') if pd.notna(dane_miejscowosci_row.get('ULICA')) else None
    numery_z_kodu = dane_miejscowosci_row.get('NUMERY') if pd.notna(dane_miejscowosci_row.get('NUMERY')) else None
    if not all([woj_nazwa, pow_nazwa, gmi_nazwa]):
         missing_info = [name for name, val in [('WOJEWÓDZTWO', woj_nazwa), ('POWIAT', pow_nazwa), ('GMINA', gmi_nazwa)] if not val]
         logger.error(f"Missing data in postal code file for {target_miejscowosc} ({postal_code}): {', '.join(missing_info)}")
         raise HTTPException(status_code=500, detail=f"Incomplete data in postal code file for {target_miejscowosc}. Missing: {', '.join(missing_info)}")
    terc_woj, terc_pow, terc_gmi_full = get_terc_codes(woj_nazwa, pow_nazwa, gmi_nazwa, target_miejscowosc)
    sym_code, simc_nazwa_oficjalna = None, None
    if terc_gmi_full: sym_code, simc_nazwa_oficjalna = get_simc_code(terc_gmi_full, target_miejscowosc, gmi_nazwa)
    ulic_df = pd.DataFrame()
    if terc_gmi_full and sym_code: ulic_df = get_ulic_data(terc_gmi_full, sym_code)
    response = {
        "query": {"postal_code": postal_code, "locality_input": locality, "locality_selected": target_miejscowosc},
        "location_from_postal_code": {"locality": target_miejscowosc, "voivodeship_name": woj_nazwa, "county_name": pow_nazwa, "municipality_name": gmi_nazwa, "street_suggestion": ulica_z_kodu, "numbers_suggestion": numery_z_kodu},
        "teryt_codes": {"terc_voivodeship": terc_woj, "terc_county": terc_pow, "terc_municipality": terc_gmi_full, "simc": sym_code, "simc_official_name": simc_nazwa_oficjalna},
        "streets": ulic_df.to_dict(orient='records') if not ulic_df.empty else []
    }
    return response


# --- Uruchomienie aplikacji ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)