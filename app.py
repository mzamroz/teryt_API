import pandas as pd
import streamlit as st
import os

# --- Konfiguracja ---
DATA_DIR = './dane' # Katalog z plikami CSV
TERC_FILENAME = 'TERC_Adresowy_2025-04-08.csv'
SIMC_FILENAME = 'SIMC_Adresowy_2025-04-08.csv'
ULIC_FILENAME = 'ULIC_Adresowy_2025-04-08.csv'
KODY_POCZTOWE_FILENAME = 'kody_pocztowe.csv'

# Słownik przechowujący typy danych dla kluczowych kolumn (jako stringi, aby zachować wiodące zera)
COLUMN_DTYPES = {
    'WOJ': str, 'POW': str, 'GMI': str, 'RODZ': str, 'RODZ_GMI': str,
    'SYM': str, 'SYM_UL': str, 'SYMPOD': str, 'PNA': str
}

# --- Ładowanie i przygotowanie danych ---
# Funkcja do ładowania danych z cacheowaniem Streamlit
@st.cache_data
def load_data(data_dir, terc_filename, simc_filename, ulic_filename, kody_pocztowe_filename, column_dtypes):
    """Ładuje pliki CSV do DataFrame'ów, obsługując różne kodowania i błędy."""
    dataframes = {}
    loaded_files = []
    terc_data_local = None
    simc_data_local = None
    ulic_data_local = None
    errors = []
    warnings = []

    if not os.path.exists(data_dir):
        errors.append(f"Katalog '{data_dir}' nie istnieje.")
        return dataframes, loaded_files, terc_data_local, simc_data_local, ulic_data_local, errors, warnings

    all_files = os.listdir(data_dir)
    # warnings.append(f"Znaleziono pliki w katalogu '{data_dir}': {', '.join(all_files)}") # Mniej istotna informacja

    required_files = [terc_filename, simc_filename, ulic_filename, kody_pocztowe_filename]
    missing_files = [f for f in required_files if f not in all_files]

    if missing_files:
        errors.append(f"Brak wymaganych plików w katalogu '{data_dir}': {', '.join(missing_files)}")

    for file_name in all_files:
        if file_name.endswith('.csv'):
            file_path = os.path.join(data_dir, file_name)
            df = None
            try:
                df = pd.read_csv(
                    file_path, delimiter=';', on_bad_lines='warn', encoding='utf-8',
                    dtype=column_dtypes, low_memory=False
                )
                # warnings.append(f"Załadowano {file_name} (UTF-8).")
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(
                        file_path, delimiter=';', on_bad_lines='warn', encoding='latin1',
                        dtype=column_dtypes, low_memory=False
                    )
                    warnings.append(f"Plik {file_name} załadowano używając kodowania 'latin1' zamiast 'utf-8'.")
                except Exception as e_inner:
                    errors.append(f"Nie udało się załadować {file_name} ani z UTF-8, ani z Latin-1: {e_inner}")
                    continue
            except pd.errors.ParserError as e_parser:
                 errors.append(f"Błąd parsowania {file_name}: {e_parser}. Sprawdź strukturę pliku i separator.")
                 continue
            except Exception as e_outer:
                errors.append(f"Nieoczekiwany błąd podczas ładowania {file_name}: {e_outer}")
                continue

            if df is not None:
                df.columns = df.columns.str.strip()
                dataframes[file_name] = df
                loaded_files.append(file_name)
                if file_name == terc_filename: terc_data_local = df
                elif file_name == simc_filename: simc_data_local = df
                elif file_name == ulic_filename: ulic_data_local = df

    return dataframes, loaded_files, terc_data_local, simc_data_local, ulic_data_local, errors, warnings

# Funkcja do wzbogacania danych ULIC (cacheowana)
@st.cache_data
def enrich_ulic_data(ulic_df, simc_df):
    """ обогащает данные ULIC названиями населенных пунктов из SIMC."""
    if ulic_df is None or simc_df is None:
        return None

    # Przygotuj SIMC do merge'a - wybierz potrzebne kolumny i zmień nazwę 'NAZWA'
    simc_to_merge = simc_df[['WOJ', 'POW', 'GMI', 'RODZ_GMI', 'SYM', 'NAZWA']].copy()
    simc_to_merge.rename(columns={'NAZWA': 'NAZWA_MIEJSCOWOSCI'}, inplace=True)

    # Połącz NAZWA_1 i NAZWA_2 w ULIC
    ulic_enriched = ulic_df.copy()
    ulic_enriched['NAZWA_ULICY_FULL'] = ulic_enriched['NAZWA_1'].fillna('') + ' ' + ulic_enriched['NAZWA_2'].fillna('')
    ulic_enriched['NAZWA_ULICY_FULL'] = ulic_enriched['NAZWA_ULICY_FULL'].str.strip()

    # Złącz ULIC z SIMC
    ulic_enriched = pd.merge(
        ulic_enriched,
        simc_to_merge,
        on=['WOJ', 'POW', 'GMI', 'RODZ_GMI', 'SYM'],
        how='left' # Zachowaj wszystkie ulice, nawet jeśli nie znajdą dopasowania w SIMC
    )
    return ulic_enriched


# --- Główna część aplikacji ---
st.set_page_config(layout="wide") # Użyj szerszego layoutu
st.title("Uteryterowana terytowarka v2.2 (z ULIC, fallback SIMC i wyszukiwaniem ulic)")

# Ładowanie danych
dataframes, loaded_files, terc_data, simc_data, ulic_data, load_errors, load_warnings = load_data(
    DATA_DIR, TERC_FILENAME, SIMC_FILENAME, ULIC_FILENAME, KODY_POCZTOWE_FILENAME, COLUMN_DTYPES
)

# Wyświetl błędy i ostrzeżenia ładowania
for warning in load_warnings:
    st.info(warning)
for error in load_errors:
    st.error(error)

# Sprawdzenie, czy kluczowe dane zostały załadowane
if terc_data is None or simc_data is None or ulic_data is None or KODY_POCZTOWE_FILENAME not in dataframes:
    st.error("Nie udało się załadować wszystkich wymaganych plików danych (TERC, SIMC, ULIC, Kody Pocztowe). Aplikacja nie może kontynuować.")
    st.stop()
else:
    st.success("Wszystkie wymagane pliki danych zostały pomyślnie załadowane.")

# Wzbogacanie danych ULIC (wykonywane raz dzięki cache)
ulic_data_enriched = enrich_ulic_data(ulic_data, simc_data)
if ulic_data_enriched is None:
     st.error("Nie udało się wzbogacić danych ULIC o nazwy miejscowości.")
     # Można kontynuować bez wzbogaconych danych, ale wyszukiwanie ulic będzie mniej użyteczne
     # st.stop()


# --- Logika aplikacji ---

# Inicjalizacja zmiennych stanu sesji
if 'miejscowosc' not in st.session_state: st.session_state.miejscowosc = None
if 'wojewodztwo' not in st.session_state: st.session_state.wojewodztwo = None
if 'powiat' not in st.session_state: st.session_state.powiat = None
if 'gmina' not in st.session_state: st.session_state.gmina = None
if 'ulica_z_kodu' not in st.session_state: st.session_state.ulica_z_kodu = None
if 'numery_z_kodu' not in st.session_state: st.session_state.numery_z_kodu = None
if 'terc_woj' not in st.session_state: st.session_state.terc_woj = None
if 'terc_powiat' not in st.session_state: st.session_state.terc_powiat = None
if 'terc_gmina' not in st.session_state: st.session_state.terc_gmina = None
if 'sym_code' not in st.session_state: st.session_state.sym_code = None
if 'matching_ulic_df' not in st.session_state: st.session_state.matching_ulic_df = pd.DataFrame()
if 'last_kod_pocztowy' not in st.session_state: st.session_state.last_kod_pocztowy = ""
if 'street_search_query' not in st.session_state: st.session_state.street_search_query = ""


# Podział na kolumny dla lepszego układu
col1, col2 = st.columns(2)


# Sekcja kodu pocztowego
st.header("Wyszukiwanie po kodzie pocztowym")
kod_pocztowy = st.text_input("Wprowadź kod pocztowy (np. 00-001):", key="kod_pocztowy_input", value=st.session_state.last_kod_pocztowy)

# Resetowanie stanu jeśli wprowadzono nowy kod pocztowy
if kod_pocztowy != st.session_state.last_kod_pocztowy:
    # Resetuj stan, gdy kod pocztowy się zmienia
    st.session_state.miejscowosc = None
    st.session_state.wojewodztwo = None
    st.session_state.powiat = None
    st.session_state.gmina = None
    st.session_state.ulica_z_kodu = None
    st.session_state.numery_z_kodu = None
    st.session_state.terc_woj = None
    st.session_state.terc_powiat = None
    st.session_state.terc_gmina = None
    st.session_state.sym_code = None
    st.session_state.matching_ulic_df = pd.DataFrame()
    st.session_state.last_kod_pocztowy = kod_pocztowy
    # st.experimental_rerun() # Wymuś przeładowanie, aby selectbox się zaktualizował poprawnie

if kod_pocztowy:
    df_kody_pocztowe = dataframes[KODY_POCZTOWE_FILENAME]
    df_kody_pocztowe['PNA'] = df_kody_pocztowe['PNA'].astype(str)
    df_kody_pocztowe['MIEJSCOWOŚĆ_CLEAN'] = df_kody_pocztowe['MIEJSCOWOŚĆ'].str.extract(r'\((.*?)\)', expand=False).fillna(df_kody_pocztowe['MIEJSCOWOŚĆ'])
    pasujace_miejscowosci_df = df_kody_pocztowe[df_kody_pocztowe['PNA'] == kod_pocztowy]

    if not pasujace_miejscowosci_df.empty:
        lista_miejscowosci = sorted(pasujace_miejscowosci_df['MIEJSCOWOŚĆ_CLEAN'].unique())

        selected_miejscowosc_index = 0
        if len(lista_miejscowosci) > 1:
            options = [""] + lista_miejscowosci
            try:
                current_selection_index = options.index(st.session_state.miejscowosc) if st.session_state.miejscowosc in options else 0
            except ValueError: current_selection_index = 0
            selected_miejscowosc = st.selectbox("Wybierz miejscowość:", options, index=current_selection_index, key="sel_miejscowosc")
            if selected_miejscowosc: st.session_state.miejscowosc = selected_miejscowosc
            else: st.session_state.miejscowosc = None # Reset jeśli wybrano pustą opcję

        elif len(lista_miejscowosci) == 1:
            if st.session_state.miejscowosc != lista_miejscowosci[0]: # Ustaw tylko jeśli się zmieniło
                st.session_state.miejscowosc = lista_miejscowosci[0]
                # st.experimental_rerun() # Może być potrzebne do odświeżenia
            st.write(f"Automatycznie wybrano miejscowość: **{st.session_state.miejscowosc}**")
        else:
                st.warning("Nie znaleziono unikalnych nazw miejscowości dla podanego kodu pocztowego.")
                st.session_state.miejscowosc = None

        # Przetwarzaj tylko jeśli miejscowość jest wybrana
        if st.session_state.miejscowosc:
            dane_miejscowosci = pasujace_miejscowosci_df[pasujace_miejscowosci_df['MIEJSCOWOŚĆ_CLEAN'] == st.session_state.miejscowosc].iloc[0]
            # Aktualizuj stan tylko jeśli wartości się zmieniły
            if st.session_state.wojewodztwo != dane_miejscowosci['WOJEWÓDZTWO']: st.session_state.wojewodztwo = dane_miejscowosci['WOJEWÓDZTWO']
            if st.session_state.powiat != dane_miejscowosci['POWIAT']: st.session_state.powiat = dane_miejscowosci['POWIAT']
            if st.session_state.gmina != dane_miejscowosci['GMINA']: st.session_state.gmina = dane_miejscowosci['GMINA']
            ulica_new = dane_miejscowosci['ULICA'] if pd.notna(dane_miejscowosci['ULICA']) else None
            numery_new = dane_miejscowosci['NUMERY'] if pd.notna(dane_miejscowosci['NUMERY']) else None
            if st.session_state.ulica_z_kodu != ulica_new: st.session_state.ulica_z_kodu = ulica_new
            if st.session_state.numery_z_kodu != numery_new: st.session_state.numery_z_kodu = numery_new

            # --- Wyszukiwanie kodów TERC ---
            with st.expander("Dane TERYT (TERC)", expanded=False): # Domyślnie zwinięte
                # Województwo
                terc_woj_new = None
                if st.session_state.wojewodztwo:
                    woj_row = terc_data[terc_data['NAZWA'].str.lower() == st.session_state.wojewodztwo.lower()]
                    if not woj_row.empty:
                        terc_woj_new = woj_row['WOJ'].iloc[0]
                        st.write(f"**Województwo:** {st.session_state.wojewodztwo} (TERC WOJ: {terc_woj_new})")
                    else:
                        st.warning(f"Nie znaleziono kodu TERC dla województwa: {st.session_state.wojewodztwo}")
                if st.session_state.terc_woj != terc_woj_new: st.session_state.terc_woj = terc_woj_new

                # Powiat
                terc_powiat_new = None
                if st.session_state.terc_woj and st.session_state.powiat:
                    pow_row = terc_data[(terc_data['NAZWA'].str.lower() == st.session_state.powiat.lower()) & (terc_data['WOJ'] == st.session_state.terc_woj) & (terc_data['POW'].notna()) & (terc_data['GMI'].isna())]
                    if not pow_row.empty:
                        terc_powiat_new = f"{pow_row['WOJ'].iloc[0]}{pow_row['POW'].iloc[0]}"
                        st.write(f"**Powiat:** {st.session_state.powiat} (TERC POW: {terc_powiat_new})")
                    else:
                        st.warning(f"Nie znaleziono kodu TERC dla powiatu: {st.session_state.powiat} w woj. {st.session_state.wojewodztwo}")
                if st.session_state.terc_powiat != terc_powiat_new: st.session_state.terc_powiat = terc_powiat_new

                # Gmina
                terc_gmina_new = None
                if st.session_state.terc_powiat and st.session_state.gmina:
                    woj_code = st.session_state.terc_powiat[:2]; pow_code = st.session_state.terc_powiat[2:]
                    gmi_row = terc_data[((terc_data['NAZWA'].str.lower() == st.session_state.gmina.lower()) | (terc_data['NAZWA'].str.lower() == st.session_state.miejscowosc.lower())) & (terc_data['WOJ'] == woj_code) & (terc_data['POW'] == pow_code) & (terc_data['GMI'].notna()) & (terc_data['RODZ'].notna())]
                    if not gmi_row.empty:
                        if len(gmi_row) > 1: st.info(f"Znaleziono wiele pasujących gmin dla '{st.session_state.gmina}'/'{st.session_state.miejscowosc}'. Wybieram pierwszą.")
                        gmi_data = gmi_row.iloc[0]
                        terc_gmina_new = f"{gmi_data['WOJ']}{gmi_data['POW']}{gmi_data['GMI']}{gmi_data['RODZ']}"
                        st.write(f"**Gmina:** {st.session_state.gmina} (TERC GMI: {terc_gmina_new})")
                    else:
                        st.warning(f"Nie znaleziono kodu TERC dla gminy: {st.session_state.gmina} lub miejscowości: {st.session_state.miejscowosc} w pow. {st.session_state.powiat}")
                if st.session_state.terc_gmina != terc_gmina_new: st.session_state.terc_gmina = terc_gmina_new


    else: # if not pasujace_miejscowosci_df.empty:
        st.warning("Nie znaleziono miejscowości dla podanego kodu pocztowego.")
        if st.session_state.miejscowosc is not None: # Resetuj tylko jeśli coś było wybrane
            st.session_state.miejscowosc = None; st.session_state.terc_gmina = None; st.session_state.sym_code = None
            # st.experimental_rerun()


# --- Wyszukiwanie kodu SIMC (z fallbackiem) ---
if st.session_state.terc_gmina and st.session_state.miejscowosc:
    with st.expander("Dane TERYT (SIMC)", expanded=False): # Domyślnie zwinięte
        woj = st.session_state.terc_gmina[:2]; pow = st.session_state.terc_gmina[2:4]; gmi = st.session_state.terc_gmina[4:6]; rodz_gmi = st.session_state.terc_gmina[6]
        simc_found = False
        sym_code_new = None

        # Krok 1: Wyszukaj po miejscowości
        matching_simc = simc_data[(simc_data['WOJ'] == woj) & (simc_data['POW'] == pow) & (simc_data['GMI'] == gmi) & (simc_data['RODZ_GMI'] == rodz_gmi) & (simc_data['NAZWA'].str.strip().str.lower() == st.session_state.miejscowosc.strip().lower())]
        if not matching_simc.empty:
            if len(matching_simc) > 1: st.info(f"Znaleziono wiele wpisów SIMC dla miejscowości '{st.session_state.miejscowosc}'. Wyświetlam pierwszy.")
            simc_details = matching_simc.iloc[0]
            sym_code_new = simc_details['SYM']
            st.success(f"Znaleziono kod SIMC (SYM) dla miejscowości **{st.session_state.miejscowosc}**: **{sym_code_new}**")
            st.dataframe(matching_simc[['SYM', 'SYMPOD', 'NAZWA', 'RM', 'MZ', 'WOJ', 'POW', 'GMI', 'RODZ_GMI']])
            simc_found = True
        else:
            # Krok 2: Fallback - Wyszukaj po nazwie gminy
            st.info(f"Nie znaleziono SIMC dla '{st.session_state.miejscowosc}'. Próba dla nazwy gminy '{st.session_state.gmina}'...")
            matching_simc_fallback = simc_data[(simc_data['WOJ'] == woj) & (simc_data['POW'] == pow) & (simc_data['GMI'] == gmi) & (simc_data['RODZ_GMI'] == rodz_gmi) & (simc_data['NAZWA'].str.strip().str.lower() == st.session_state.gmina.strip().lower())]
            if not matching_simc_fallback.empty:
                if len(matching_simc_fallback) > 1: st.info(f"Znaleziono wiele wpisów SIMC dla nazwy gminy '{st.session_state.gmina}'. Wyświetlam pierwszy.")
                simc_details_fallback = matching_simc_fallback.iloc[0]
                sym_code_new = simc_details_fallback['SYM']
                st.success(f"Znaleziono kod SIMC (SYM) dla **nazwy gminy {st.session_state.gmina}**: **{sym_code_new}** (użyto jako fallback)")
                st.dataframe(matching_simc_fallback[['SYM', 'SYMPOD', 'NAZWA', 'RM', 'MZ', 'WOJ', 'POW', 'GMI', 'RODZ_GMI']])
                simc_found = True
            else:
                st.warning(f"Nie znaleziono kodu SIMC ani dla miejscowości '{st.session_state.miejscowosc}', ani dla nazwy gminy '{st.session_state.gmina}' z kodem TERC gminy {st.session_state.terc_gmina}")

        if st.session_state.sym_code != sym_code_new:
                st.session_state.sym_code = sym_code_new
                # st.experimental_rerun() # Może być potrzebne do odświeżenia ULIC

# --- Wyszukiwanie kodów ULIC (dla kodu pocztowego) ---
if st.session_state.sym_code and st.session_state.terc_gmina:
        with st.expander("Dane TERYT (ULIC) - dla wybranej miejscowości/gminy", expanded=False): # Domyślnie zwinięte
            woj = st.session_state.terc_gmina[:2]; pow = st.session_state.terc_gmina[2:4]; gmi = st.session_state.terc_gmina[4:6]; rodz_gmi = st.session_state.terc_gmina[6]
            matching_ulic = pd.DataFrame() # Domyślnie pusty
            if ulic_data_enriched is not None:
                    matching_ulic = ulic_data_enriched[
                    (ulic_data_enriched['WOJ'] == woj) &
                    (ulic_data_enriched['POW'] == pow) &
                    (ulic_data_enriched['GMI'] == gmi) &
                    (ulic_data_enriched['RODZ_GMI'] == rodz_gmi) &
                    (ulic_data_enriched['SYM'] == st.session_state.sym_code)
                ].copy()

            if not matching_ulic.empty:
                st.success(f"Znaleziono {len(matching_ulic)} ulic dla SIMC: {st.session_state.sym_code}:")
                # Zapisz znalezione ulice w stanie sesji (tylko potrzebne kolumny)
                st.session_state.matching_ulic_df = matching_ulic[['SYM_UL', 'CECHA', 'NAZWA_ULICY_FULL', 'STAN_NA']].rename(columns={'NAZWA_ULICY_FULL': 'NAZWA_ULICY'})
                st.dataframe(st.session_state.matching_ulic_df)
            else:
                st.warning(f"Nie znaleziono kodów ULIC dla SIMC: {st.session_state.sym_code} w pliku {ULIC_FILENAME}.")
                if not st.session_state.matching_ulic_df.empty: # Resetuj tylko jeśli coś tam było
                        st.session_state.matching_ulic_df = pd.DataFrame()
                        # st.experimental_rerun()

# --- Wybór ulicy (jeśli znaleziono dla kodu pocztowego) ---
if not st.session_state.matching_ulic_df.empty:
    st.subheader("Wybór ulicy (z wyników dla kodu pocztowego)")
    if st.session_state.ulica_z_kodu:
        st.info(f"Ulica sugerowana na podstawie kodu pocztowego: '{st.session_state.ulica_z_kodu}'. Możesz wybrać inną z listy poniżej.")
    lista_ulic_opcje = [""] + sorted(st.session_state.matching_ulic_df['NAZWA_ULICY'].unique().tolist())
    default_index = 0
    if st.session_state.ulica_z_kodu and st.session_state.ulica_z_kodu in lista_ulic_opcje:
        try: default_index = lista_ulic_opcje.index(st.session_state.ulica_z_kodu)
        except ValueError: default_index = 0
    wybrana_ulica = st.selectbox("Wybierz ulicę z listy TERYT:", lista_ulic_opcje, index=default_index, key="sel_ulica")
    if wybrana_ulica:
        dane_wybranej_ulicy = st.session_state.matching_ulic_df[st.session_state.matching_ulic_df['NAZWA_ULICY'] == wybrana_ulica].iloc[0]
        st.write(f"Wybrano ulicę: **{wybrana_ulica}**")
        st.write(f"**Kod ULIC (SYM_UL):** {int(dane_wybranej_ulicy['SYM_UL']):05d} (z TERYT)")
        st.write(f"**Cecha:** {dane_wybranej_ulicy['CECHA']}")



# Sekcja wyświetlania surowych danych (opcjonalnie, na dole)
st.divider()
with st.expander("Podgląd załadowanych plików CSV"):
    if loaded_files:
        selected_file = st.selectbox("Wybierz plik do wyświetlenia:", loaded_files, key="sel_raw_file")
        if selected_file in dataframes:
            st.write(f"Wyświetlanie DataFrame dla **{selected_file}**:")
            st.dataframe(dataframes[selected_file])
        else:
            st.warning(f"DataFrame dla pliku {selected_file} nie jest dostępny.")
    else:
        st.write("Nie załadowano żadnych plików CSV.")

