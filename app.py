# main.py
import os
import pandas as pd
from fastapi import FastAPI, HTTPException, Query, Path
# Dodaj List do importów
from typing import List, Optional, Dict, Any
import logging
# Dodaj BaseModel z Pydantic dla lepszej definicji odpowiedzi
from pydantic import BaseModel

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
# (bez zmian - load_data_on_startup, enrich_ulic_data, get_terc_codes, get_simc_code, get_ulic_data)
# ... (cała reszta funkcji pomocniczych jak poprzednio) ...

def load_data_on_startup():
    """Ładuje pliki CSV do globalnych DataFrame'ów przy starcie aplikacji."""
    global dataframes, terc_data, simc_data, ulic_data, kody_pocztowe_data, ulic_data_enriched
    logger.info(f"Rozpoczynanie ładowania danych z katalogu: {DATA_DIR}")

    if not os.path.exists(DATA_DIR):
        logger.error(f"Katalog '{DATA_DIR}' nie istnieje. Nie można załadować danych.")
        return

    all_files = os.listdir(DATA_DIR)
    required_files = [TERC_FILENAME, SIMC_FILENAME, ULIC_FILENAME, KODY_POCZTOWE_FILENAME]
    missing_files = [f for f in required_files if f not in all_files]

    if missing_files:
        logger.warning(f"Brak wymaganych plików w katalogu '{DATA_DIR}': {', '.join(missing_files)}")

    loaded_files_count = 0
    for file_name in required_files:
        if file_name in all_files:
            file_path = os.path.join(DATA_DIR, file_name)
            df = None
            try:
                df = pd.read_csv(
                    file_path, delimiter=';', on_bad_lines='warn', encoding='utf-8',
                    dtype=COLUMN_DTYPES, low_memory=False
                )
                logger.info(f"Załadowano {file_name} (UTF-8).")
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(
                        file_path, delimiter=';', on_bad_lines='warn', encoding='latin1',
                        dtype=COLUMN_DTYPES, low_memory=False
                    )
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
                df.columns = df.columns.str.strip()
                dataframes[file_name] = df
                loaded_files_count += 1
                if file_name == TERC_FILENAME: terc_data = df
                elif file_name == SIMC_FILENAME: simc_data = df
                elif file_name == ULIC_FILENAME: ulic_data = df
                elif file_name == KODY_POCZTOWE_FILENAME: kody_pocztowe_data = df
        else:
             logger.warning(f"Plik {file_name} nie został znaleziony w {DATA_DIR}.")


    logger.info(f"Zakończono ładowanie danych. Załadowano {loaded_files_count} z {len(required_files)} wymaganych plików.")

    if ulic_data is not None and simc_data is not None:
        ulic_data_enriched = enrich_ulic_data(ulic_data, simc_data)
        if ulic_data_enriched is not None:
             logger.info("Pomyślnie wzbogacono dane ULIC o nazwy miejscowości.")
        else:
             logger.warning("Nie udało się wzbogacić danych ULIC.")
    else:
        logger.warning("Nie można wzbogacić danych ULIC, ponieważ brakuje danych ULIC lub SIMC.")

    if kody_pocztowe_data is not None:
         try:
            if 'PNA' in kody_pocztowe_data.columns:
                 kody_pocztowe_data['PNA'] = kody_pocztowe_data['PNA'].astype(str)
            else:
                 logger.error(f"Brak kolumny 'PNA' w pliku {KODY_POCZTOWE_FILENAME}")
                 kody_pocztowe_data = None

            if 'MIEJSCOWOŚĆ' in kody_pocztowe_data.columns:
                # Stworzenie 'MIEJSCOWOŚĆ_CLEAN' - kluczowe dla nowego endpointu
                kody_pocztowe_data['MIEJSCOWOŚĆ_CLEAN'] = kody_pocztowe_data['MIEJSCOWOŚĆ'].str.extract(r'\((.*?)\)', expand=False).fillna(kody_pocztowe_data['MIEJSCOWOŚĆ']).str.strip()
            else:
                 logger.error(f"Brak kolumny 'MIEJSCOWOŚĆ' w pliku {KODY_POCZTOWE_FILENAME}")
                 kody_pocztowe_data = None

            logger.info("Przygotowano dane kodów pocztowych.")

         except Exception as e:
            logger.error(f"Błąd podczas przygotowywania danych kodów pocztowych: {e}")
            kody_pocztowe_data = None

# ... (reszta funkcji pomocniczych: enrich_ulic_data, get_terc_codes, etc.) ...
def enrich_ulic_data(ulic_df, simc_df):
    """Wzbogaca dane ULIC nazwami miejscowości z SIMC."""
    if ulic_df is None or simc_df is None:
        logger.warning("Próba wzbogacenia ULIC, ale brakuje danych wejściowych.")
        return None
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
             logger.warning("Brak kolumny 'NAZWA_2' w danych ULIC. Użyto tylko 'NAZWA_1'.")
        else:
             logger.error("Brak kolumny 'NAZWA_1' (i być może 'NAZWA_2') w danych ULIC. Nie można utworzyć pełnej nazwy ulicy.")
             return ulic_df
        merge_cols = ['WOJ', 'POW', 'GMI', 'RODZ_GMI', 'SYM']
        if all(col in ulic_enriched.columns for col in merge_cols) and \
           all(col in simc_to_merge.columns for col in merge_cols):
            ulic_enriched = pd.merge(
                ulic_enriched,
                simc_to_merge,
                on=merge_cols,
                how='left'
            )
            return ulic_enriched
        else:
            missing_ulic = [col for col in merge_cols if col not in ulic_enriched.columns]
            missing_simc = [col for col in merge_cols if col not in simc_to_merge.columns]
            logger.error(f"Nie można połączyć ULIC i SIMC. Brakujące kolumny w ULIC: {missing_ulic}, w SIMC: {missing_simc}")
            return ulic_df
    except Exception as e:
        logger.error(f"Błąd podczas wzbogacania danych ULIC: {e}")
        return None

def get_terc_codes(woj_nazwa, pow_nazwa, gmi_nazwa, miejscowosc_nazwa):
    """Wyszukuje kody TERC dla województwa, powiatu i gminy."""
    if terc_data is None:
        return None, None, None
    terc_woj, terc_pow, terc_gmi = None, None, None
    woj_code, pow_code = None, None
    try:
        if woj_nazwa:
            woj_row = terc_data[terc_data['NAZWA'].str.lower() == woj_nazwa.lower()]
            if not woj_row.empty:
                woj_code = woj_row['WOJ'].iloc[0]
                terc_woj = woj_code
            else: logger.warning(f"Nie znaleziono TERC WOJ dla: {woj_nazwa}")
        if woj_code and pow_nazwa:
            pow_row = terc_data[(terc_data['NAZWA'].str.lower() == pow_nazwa.lower()) & (terc_data['WOJ'] == woj_code) & (terc_data['POW'].notna()) & (terc_data['GMI'].isna())]
            if not pow_row.empty:
                pow_code = pow_row['POW'].iloc[0]
                terc_pow = f"{woj_code}{pow_code}"
            else: logger.warning(f"Nie znaleziono TERC POW dla: {pow_nazwa} w woj. {woj_nazwa}")
        if woj_code and pow_code and gmi_nazwa:
             gmi_row = terc_data[((terc_data['NAZWA'].str.lower() == gmi_nazwa.lower()) | (terc_data['NAZWA'].str.lower() == miejscowosc_nazwa.lower())) & (terc_data['WOJ'] == woj_code) & (terc_data['POW'] == pow_code) & (terc_data['GMI'].notna()) & (terc_data['RODZ'].notna())]
             if not gmi_row.empty:
                 if len(gmi_row) > 1: logger.info(f"Znaleziono wiele TERC GMI dla '{gmi_nazwa}'/'{miejscowosc_nazwa}'. Wybieram pierwszy.")
                 gmi_data = gmi_row.iloc[0]
                 terc_gmi = f"{gmi_data['WOJ']}{gmi_data['POW']}{gmi_data['GMI']}{gmi_data['RODZ']}"
             else: logger.warning(f"Nie znaleziono TERC GMI dla gminy: {gmi_nazwa} lub miejscowości: {miejscowosc_nazwa} w pow. {pow_nazwa}")
    except Exception as e:
        logger.error(f"Błąd podczas wyszukiwania kodów TERC: {e}")
        return None, None, None
    return terc_woj, terc_pow, terc_gmi

def get_simc_code(terc_gmi_full, miejscowosc_nazwa, gmina_nazwa):
    """Wyszukuje kod SIMC dla danej gminy i miejscowości (z fallbackiem na nazwę gminy)."""
    if simc_data is None or not terc_gmi_full or len(terc_gmi_full) != 7:
        logger.warning("Nie można wyszukać SIMC: brak danych SIMC lub nieprawidłowy TERC gminy.")
        return None, None
    woj, pow, gmi, rodz_gmi = terc_gmi_full[:2], terc_gmi_full[2:4], terc_gmi_full[4:6], terc_gmi_full[6]
    sym_code, found_name = None, None
    try:
        matching_simc = simc_data[(simc_data['WOJ'] == woj) & (simc_data['POW'] == pow) & (simc_data['GMI'] == gmi) & (simc_data['RODZ_GMI'] == rodz_gmi) & (simc_data['NAZWA'].str.strip().str.lower() == miejscowosc_nazwa.strip().lower())]
        if not matching_simc.empty:
            if len(matching_simc) > 1: logger.info(f"Znaleziono wiele SIMC dla miejscowości '{miejscowosc_nazwa}'. Wybieram pierwszy.")
            simc_details = matching_simc.iloc[0]
            sym_code = simc_details['SYM']
            found_name = simc_details['NAZWA']
            logger.info(f"Znaleziono SIMC dla miejscowości '{miejscowosc_nazwa}': {sym_code}")
            return sym_code, found_name
        else:
            logger.info(f"Nie znaleziono SIMC dla '{miejscowosc_nazwa}'. Próba dla nazwy gminy '{gmina_nazwa}'...")
            matching_simc_fallback = simc_data[(simc_data['WOJ'] == woj) & (simc_data['POW'] == pow) & (simc_data['GMI'] == gmi) & (simc_data['RODZ_GMI'] == rodz_gmi) & (simc_data['NAZWA'].str.strip().str.lower() == gmina_nazwa.strip().lower())]
            if not matching_simc_fallback.empty:
                if len(matching_simc_fallback) > 1: logger.info(f"Znaleziono wiele SIMC dla nazwy gminy '{gmina_nazwa}'. Wybieram pierwszy.")
                simc_details_fallback = matching_simc_fallback.iloc[0]
                sym_code = simc_details_fallback['SYM']
                found_name = simc_details_fallback['NAZWA']
                logger.info(f"Znaleziono SIMC dla nazwy gminy '{gmina_nazwa}' (fallback): {sym_code}")
                return sym_code, found_name
            else:
                logger.warning(f"Nie znaleziono SIMC ani dla miejscowości '{miejscowosc_nazwa}', ani dla nazwy gminy '{gmina_nazwa}' (TERC GMI: {terc_gmi_full})")
                return None, None
    except Exception as e:
        logger.error(f"Błąd podczas wyszukiwania kodu SIMC: {e}")
        return None, None

def get_ulic_data(terc_gmi_full, simc_code):
    """Wyszukuje dane ULIC dla danego kodu TERC gminy i kodu SIMC."""
    if ulic_data_enriched is None or not terc_gmi_full or len(terc_gmi_full) != 7 or not simc_code:
         logger.warning("Nie można wyszukać ULIC: brak wzbogaconych danych ULIC, nieprawidłowy TERC gminy lub brak kodu SIMC.")
         return pd.DataFrame()
    woj, pow, gmi, rodz_gmi = terc_gmi_full[:2], terc_gmi_full[2:4], terc_gmi_full[4:6], terc_gmi_full[6]
    try:
        matching_ulic = ulic_data_enriched[(ulic_data_enriched['WOJ'] == woj) & (ulic_data_enriched['POW'] == pow) & (ulic_data_enriched['GMI'] == gmi) & (ulic_data_enriched['RODZ_GMI'] == rodz_gmi) & (ulic_data_enriched['SYM'] == simc_code)].copy()
        if not matching_ulic.empty:
            logger.info(f"Znaleziono {len(matching_ulic)} ulic dla SIMC: {simc_code}")
            result_df = matching_ulic[['SYM_UL', 'CECHA', 'NAZWA_ULICY_FULL', 'STAN_NA']].rename(columns={'NAZWA_ULICY_FULL': 'NAZWA_ULICY', 'SYM_UL': 'ULIC_CODE'})
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


# --- Pydantic Models (dla definicji odpowiedzi) ---
class MiejscowosciResponse(BaseModel):
    postal_code: str
    miejscowosci: List[str]

# --- Inicjalizacja FastAPI ---
app = FastAPI(
    title="API TERYT - Uteryterowana Terytowarka",
    description="API do wyszukiwania informacji adresowych na podstawie kodów pocztowych i danych TERYT.",
    version="1.1.0" # Zwiększono wersję
)

# --- Event handler - ładowanie danych przy starcie ---
@app.on_event("startup")
async def startup_event():
    load_data_on_startup()

# --- Endpointy API ---

@app.get("/health", summary="Sprawdza stan API", tags=["Status"])
async def health_check():
    """Zwraca status OK, jeśli API działa."""
    data_loaded = all(df is not None for df in [terc_data, simc_data, ulic_data_enriched, kody_pocztowe_data])
    return {"status": "OK", "data_loaded": data_loaded}

# --- NOWY ENDPOINT ---
@app.get(
    "/miejscowosci/{postal_code}",
    summary="Zwraca listę miejscowości dla podanego kodu pocztowego",
    tags=["Wyszukiwanie"],
    response_model=MiejscowosciResponse # Użycie modelu Pydantic
)
async def get_miejscowosci_by_postal_code(
    postal_code: str = Path(..., description="Kod pocztowy w formacie XX-XXX", regex=r"^\d{2}-\d{3}$")
):
    """
    Na podstawie kodu pocztowego zwraca posortowaną listę unikalnych nazw miejscowości,
    które są przypisane do tego kodu w pliku kodów pocztowych.
    """
    if kody_pocztowe_data is None:
        logger.error(f"Zapytanie o miejscowości dla {postal_code}, ale dane kodów pocztowych nie są załadowane.")
        raise HTTPException(status_code=503, detail="Dane kodów pocztowych nie są załadowane. Spróbuj ponownie później.")

    # Upewnij się, że kolumna MIEJSCOWOŚĆ_CLEAN istnieje
    if 'MIEJSCOWOŚĆ_CLEAN' not in kody_pocztowe_data.columns:
        logger.error(f"Brak wymaganej kolumny 'MIEJSCOWOŚĆ_CLEAN' w danych kodów pocztowych.")
        raise HTTPException(status_code=500, detail="Wewnętrzny błąd serwera: Brak przetworzonej kolumny miejscowości.")


    postal_code = postal_code.strip()
    # Filtruj dane kodów pocztowych
    pasujace_df = kody_pocztowe_data[kody_pocztowe_data['PNA'] == postal_code]

    if pasujace_df.empty:
        logger.info(f"Nie znaleziono miejscowości dla kodu pocztowego: {postal_code}")
        raise HTTPException(status_code=404, detail=f"Nie znaleziono miejscowości dla kodu pocztowego: {postal_code}")

    # Wyodrębnij unikalne, posortowane nazwy miejscowości
    # Używamy MIEJSCOWOŚĆ_CLEAN, która została przygotowana w load_data_on_startup
    lista_miejscowosci = sorted(pasujace_df['MIEJSCOWOŚĆ_CLEAN'].unique())

    logger.info(f"Znaleziono {len(lista_miejscowosci)} miejscowości dla kodu {postal_code}: {', '.join(lista_miejscowosci)}")

    # Zwróć wynik zgodnie z modelem Pydantic
    return MiejscowosciResponse(postal_code=postal_code, miejscowosci=lista_miejscowosci)


@app.get(
    "/lookup/postal_code/{postal_code}",
    summary="Wyszukuje szczegółowe informacje adresowe dla kodu pocztowego",
    tags=["Wyszukiwanie"],
    response_model=Dict[str, Any]
)
async def lookup_postal_code(
    postal_code: str = Path(..., description="Kod pocztowy w formacie XX-XXX", regex=r"^\d{2}-\d{3}$"),
    miejscowosc: Optional[str] = Query(None, description="Opcjonalnie: Nazwa miejscowości do zawężenia wyników (jeśli kod pocztowy obejmuje wiele miejscowości)")
):
    """
    Na podstawie kodu pocztowego zwraca listę pasujących miejscowości lub,
    jeśli podano miejscowość, zwraca szczegółowe dane TERYT (TERC, SIMC, ULIC)
    dla tej kombinacji kodu i miejscowości.
    """
    if kody_pocztowe_data is None:
        raise HTTPException(status_code=503, detail="Dane kodów pocztowych nie są załadowane.")
    if 'MIEJSCOWOŚĆ_CLEAN' not in kody_pocztowe_data.columns:
         raise HTTPException(status_code=500, detail="Wewnętrzny błąd serwera: Brak przetworzonej kolumny miejscowości.")

    postal_code = postal_code.strip()
    pasujace_miejscowosci_df = kody_pocztowe_data[kody_pocztowe_data['PNA'] == postal_code]

    if pasujace_miejscowosci_df.empty:
        raise HTTPException(status_code=404, detail=f"Nie znaleziono miejscowości dla kodu pocztowego: {postal_code}")

    lista_miejscowosci = sorted(pasujace_miejscowosci_df['MIEJSCOWOŚĆ_CLEAN'].unique())

    if not miejscowosc and len(lista_miejscowosci) > 1:
        return {
            "postal_code": postal_code,
            "message": "Podany kod pocztowy obejmuje wiele miejscowości. Użyj endpointu /miejscowosci/{postal_code} aby zobaczyć listę lub podaj parametr 'miejscowosc' w tym zapytaniu, aby uzyskać szczegóły.",
            "available_miejscowosci": lista_miejscowosci # Zwracamy listę dla informacji
        }

    target_miejscowosc = None
    if miejscowosc:
        target_miejscowosc = next((m for m in lista_miejscowosci if m.lower() == miejscowosc.strip().lower()), None)
        if not target_miejscowosc:
             raise HTTPException(status_code=404, detail=f"Miejscowość '{miejscowosc}' nie została znaleziona dla kodu pocztowego {postal_code}. Dostępne: {', '.join(lista_miejscowosci)}")
    elif len(lista_miejscowosci) == 1:
        target_miejscowosc = lista_miejscowosci[0]
    else:
         raise HTTPException(status_code=400, detail="Nie można określić miejscowości. Podaj parametr 'miejscowosc'.")

    dane_miejscowosci_row = pasujace_miejscowosci_df[pasujace_miejscowosci_df['MIEJSCOWOŚĆ_CLEAN'] == target_miejscowosc].iloc[0]
    woj_nazwa = dane_miejscowosci_row.get('WOJEWÓDZTWO')
    pow_nazwa = dane_miejscowosci_row.get('POWIAT')
    gmi_nazwa = dane_miejscowosci_row.get('GMINA')
    ulica_z_kodu = dane_miejscowosci_row.get('ULICA') if pd.notna(dane_miejscowosci_row.get('ULICA')) else None
    numery_z_kodu = dane_miejscowosci_row.get('NUMERY') if pd.notna(dane_miejscowosci_row.get('NUMERY')) else None

    if not all([woj_nazwa, pow_nazwa, gmi_nazwa]):
         missing_info = [name for name, val in [('WOJEWÓDZTWO', woj_nazwa), ('POWIAT', pow_nazwa), ('GMINA', gmi_nazwa)] if not val]
         logger.error(f"Brakujące dane w pliku kodów pocztowych dla {target_miejscowosc} ({postal_code}): {', '.join(missing_info)}")
         raise HTTPException(status_code=500, detail=f"Niekompletne dane w pliku kodów pocztowych dla {target_miejscowosc}. Brakuje: {', '.join(missing_info)}")

    terc_woj, terc_pow, terc_gmi_full = get_terc_codes(woj_nazwa, pow_nazwa, gmi_nazwa, target_miejscowosc)
    sym_code, simc_nazwa_oficjalna = None, None
    if terc_gmi_full:
        sym_code, simc_nazwa_oficjalna = get_simc_code(terc_gmi_full, target_miejscowosc, gmi_nazwa)
    ulic_df = pd.DataFrame()
    if terc_gmi_full and sym_code:
        ulic_df = get_ulic_data(terc_gmi_full, sym_code)

    response = {
        "query": {"postal_code": postal_code, "miejscowosc_input": miejscowosc, "miejscowosc_selected": target_miejscowosc},
        "location_from_postal_code": {"miejscowosc": target_miejscowosc, "wojewodztwo": woj_nazwa, "powiat": pow_nazwa, "gmina": gmi_nazwa, "ulica_suggestion": ulica_z_kodu, "numery_suggestion": numery_z_kodu},
        "teryt_codes": {"terc_woj": terc_woj, "terc_pow": terc_pow, "terc_gmi": terc_gmi_full, "simc": sym_code, "simc_official_name": simc_nazwa_oficjalna},
        "ulic_data": ulic_df.to_dict(orient='records') if not ulic_df.empty else []
    }
    return response

# --- Uruchomienie aplikacji ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)