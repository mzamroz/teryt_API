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

# --- Ładowanie danych ---
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
    warnings.append(f"Znaleziono pliki w katalogu '{data_dir}': {', '.join(all_files)}")

    required_files = [terc_filename, simc_filename, ulic_filename, kody_pocztowe_filename]
    missing_files = [f for f in required_files if f not in all_files]

    if missing_files:
        errors.append(f"Brak wymaganych plików w katalogu '{data_dir}': {', '.join(missing_files)}")
        # Nie zwracamy od razu, pozwalamy załadować co się da

    for file_name in all_files:
        if file_name.endswith('.csv'):
            file_path = os.path.join(data_dir, file_name)
            df = None # Reset df for each file
            try:
                # Spróbuj UTF-8
                df = pd.read_csv(
                    file_path,
                    delimiter=';',
                    on_bad_lines='warn',
                    encoding='utf-8',
                    dtype=column_dtypes,
                    low_memory=False
                )
                warnings.append(f"Załadowano {file_name} (UTF-8).")
            except UnicodeDecodeError:
                try:
                    # Spróbuj Latin-1
                    df = pd.read_csv(
                        file_path,
                        delimiter=';',
                        on_bad_lines='warn',
                        encoding='latin1',
                        dtype=column_dtypes,
                        low_memory=False
                    )
                    warnings.append(f"Załadowano {file_name} (Latin-1).")
                except Exception as e_inner:
                    errors.append(f"Nie udało się załadować {file_name} ani z UTF-8, ani z Latin-1: {e_inner}")
                    continue # Przejdź do następnego pliku
            except pd.errors.ParserError as e_parser:
                 errors.append(f"Błąd parsowania {file_name}: {e_parser}. Sprawdź strukturę pliku i separator.")
                 continue
            except Exception as e_outer:
                errors.append(f"Nieoczekiwany błąd podczas ładowania {file_name}: {e_outer}")
                continue

            if df is not None:
                 # Usuń potencjalne białe znaki z nazw kolumn
                df.columns = df.columns.str.strip()
                dataframes[file_name] = df
                loaded_files.append(file_name)

                # Przypisz DataFrame do odpowiednich zmiennych
                if file_name == terc_filename:
                    terc_data_local = df
                elif file_name == simc_filename:
                    simc_data_local = df
                elif file_name == ulic_filename:
                    ulic_data_local = df

    return dataframes, loaded_files, terc_data_local, simc_data_local, ulic_data_local, errors, warnings

# --- Główna część aplikacji ---
st.title("Uteryterowana terytowarka v2.1 (z ULIC i fallback SIMC)")

# Ładowanie danych
dataframes, loaded_files, terc_data, simc_data, ulic_data, load_errors, load_warnings = load_data(
    DATA_DIR, TERC_FILENAME, SIMC_FILENAME, ULIC_FILENAME, KODY_POCZTOWE_FILENAME, COLUMN_DTYPES
)

# Wyświetl błędy i ostrzeżenia ładowania
for warning in load_warnings:
    st.info(warning) # Użyj info dla mniej krytycznych komunikatów
for error in load_errors:
    st.error(error)

# Sprawdzenie, czy kluczowe dane zostały załadowane
if terc_data is None or simc_data is None or ulic_data is None or KODY_POCZTOWE_FILENAME not in dataframes:
    st.error("Nie udało się załadować wszystkich wymaganych plików danych (TERC, SIMC, ULIC, Kody Pocztowe). Aplikacja nie może kontynuować.")
    st.stop()
else:
    st.success("Wszystkie wymagane pliki danych zostały pomyślnie załadowane.")


# --- Logika aplikacji ---

# Inicjalizacja zmiennych stanu sesji dla zachowania wartości między interakcjami
if 'miejscowosc' not in st.session_state:
    st.session_state.miejscowosc = None
if 'wojewodztwo' not in st.session_state:
    st.session_state.wojewodztwo = None
if 'powiat' not in st.session_state:
    st.session_state.powiat = None
if 'gmina' not in st.session_state:
    st.session_state.gmina = None
if 'ulica_z_kodu' not in st.session_state:
    st.session_state.ulica_z_kodu = None
if 'numery_z_kodu' not in st.session_state:
    st.session_state.numery_z_kodu = None
if 'terc_woj' not in st.session_state:
    st.session_state.terc_woj = None
if 'terc_powiat' not in st.session_state:
    st.session_state.terc_powiat = None
if 'terc_gmina' not in st.session_state:
    st.session_state.terc_gmina = None
if 'sym_code' not in st.session_state:
    st.session_state.sym_code = None
if 'matching_ulic_df' not in st.session_state:
    st.session_state.matching_ulic_df = pd.DataFrame() # Pusty DataFrame na start

# Sekcja kodu pocztowego
st.header("Wyszukiwanie po kodzie pocztowym")
kod_pocztowy = st.text_input("Wprowadź kod pocztowy (np. 00-001):", key="kod_pocztowy_input")

# Resetowanie stanu jeśli wprowadzono nowy kod pocztowy
if 'last_kod_pocztowy' not in st.session_state:
    st.session_state.last_kod_pocztowy = ""

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


if kod_pocztowy:
    df_kody_pocztowe = dataframes[KODY_POCZTOWE_FILENAME]
    df_kody_pocztowe['PNA'] = df_kody_pocztowe['PNA'].astype(str)
    df_kody_pocztowe['MIEJSCOWOŚĆ_CLEAN'] = df_kody_pocztowe['MIEJSCOWOŚĆ'].str.extract(r'\((.*?)\)', expand=False).fillna(df_kody_pocztowe['MIEJSCOWOŚĆ'])
    pasujace_miejscowosci_df = df_kody_pocztowe[df_kody_pocztowe['PNA'] == kod_pocztowy]

    if not pasujace_miejscowosci_df.empty:
        lista_miejscowosci = sorted(pasujace_miejscowosci_df['MIEJSCOWOŚĆ_CLEAN'].unique())

        selected_miejscowosc_index = 0 # Domyślnie puste
        if len(lista_miejscowosci) > 1:
            # Dodaj pustą opcję na początku
            options = [""] + lista_miejscowosci
            # Sprawdź czy poprzednio wybrana miejscowość jest w opcjach
            try:
                current_selection_index = options.index(st.session_state.miejscowosc) if st.session_state.miejscowosc in options else 0
            except ValueError:
                 current_selection_index = 0

            selected_miejscowosc = st.selectbox(
                "Wybierz miejscowość:",
                options,
                index=current_selection_index, # Ustaw poprzednio wybraną lub pustą
                key="sel_miejscowosc"
                )
            if selected_miejscowosc: # Jeśli użytkownik coś wybrał
                 st.session_state.miejscowosc = selected_miejscowosc

        elif len(lista_miejscowosci) == 1:
            st.session_state.miejscowosc = lista_miejscowosci[0]
            st.write(f"Automatycznie wybrano miejscowość: **{st.session_state.miejscowosc}**")
        else:
             st.warning("Nie znaleziono unikalnych nazw miejscowości dla podanego kodu pocztowego.")
             st.session_state.miejscowosc = None # Reset

        # Przetwarzaj tylko jeśli miejscowość jest wybrana
        if st.session_state.miejscowosc:
            dane_miejscowosci = pasujace_miejscowosci_df[pasujace_miejscowosci_df['MIEJSCOWOŚĆ_CLEAN'] == st.session_state.miejscowosc].iloc[0]
            st.session_state.wojewodztwo = dane_miejscowosci['WOJEWÓDZTWO']
            st.session_state.powiat = dane_miejscowosci['POWIAT']
            st.session_state.gmina = dane_miejscowosci['GMINA'] # Zapisz nazwę gminy
            st.session_state.ulica_z_kodu = dane_miejscowosci['ULICA'] if pd.notna(dane_miejscowosci['ULICA']) else None
            st.session_state.numery_z_kodu = dane_miejscowosci['NUMERY'] if pd.notna(dane_miejscowosci['NUMERY']) else None

            # --- Wyszukiwanie kodów TERC ---
            with st.expander("Dane TERYT (TERC)", expanded=True):
                # Województwo
                woj_row = terc_data[terc_data['NAZWA'].str.lower() == st.session_state.wojewodztwo.lower()]
                if not woj_row.empty:
                    st.session_state.terc_woj = woj_row['WOJ'].iloc[0]
                    st.write(f"**Województwo:** {st.session_state.wojewodztwo} (TERC WOJ: {st.session_state.terc_woj})")
                else:
                    st.warning(f"Nie znaleziono kodu TERC dla województwa: {st.session_state.wojewodztwo}")
                    st.session_state.terc_woj = None

                # Powiat
                if st.session_state.terc_woj:
                    pow_row = terc_data[
                        (terc_data['NAZWA'].str.lower() == st.session_state.powiat.lower()) &
                        (terc_data['WOJ'] == st.session_state.terc_woj) &
                        (terc_data['POW'].notna()) &
                        (terc_data['GMI'].isna())
                    ]
                    if not pow_row.empty:
                        st.session_state.terc_powiat = f"{pow_row['WOJ'].iloc[0]}{pow_row['POW'].iloc[0]}"
                        st.write(f"**Powiat:** {st.session_state.powiat} (TERC POW: {st.session_state.terc_powiat})")
                    else:
                        st.warning(f"Nie znaleziono kodu TERC dla powiatu: {st.session_state.powiat} w woj. {st.session_state.wojewodztwo}")
                        st.session_state.terc_powiat = None
                else:
                     st.session_state.terc_powiat = None


                # Gmina
                if st.session_state.terc_powiat:
                    woj_code = st.session_state.terc_powiat[:2]
                    pow_code = st.session_state.terc_powiat[2:]
                    gmi_row = terc_data[
                        ((terc_data['NAZWA'].str.lower() == st.session_state.gmina.lower()) | (terc_data['NAZWA'].str.lower() == st.session_state.miejscowosc.lower())) &
                        (terc_data['WOJ'] == woj_code) &
                        (terc_data['POW'] == pow_code) &
                        (terc_data['GMI'].notna()) &
                        (terc_data['RODZ'].notna())
                    ]
                    if not gmi_row.empty:
                        if len(gmi_row) > 1:
                            st.info(f"Znaleziono wiele pasujących gmin dla '{st.session_state.gmina}'/'{st.session_state.miejscowosc}'. Wybieram pierwszą z listy.")
                        gmi_data = gmi_row.iloc[0]
                        st.session_state.terc_gmina = f"{gmi_data['WOJ']}{gmi_data['POW']}{gmi_data['GMI']}{gmi_data['RODZ']}"
                        st.write(f"**Gmina:** {st.session_state.gmina} (TERC GMI: {st.session_state.terc_gmina})")
                    else:
                        st.warning(f"Nie znaleziono kodu TERC dla gminy: {st.session_state.gmina} lub miejscowości: {st.session_state.miejscowosc} w pow. {st.session_state.powiat}")
                        st.session_state.terc_gmina = None
                else:
                    st.info("Nie można wyszukać gminy bez kodu TERC powiatu.")
                    st.session_state.terc_gmina = None

                if st.session_state.ulica_z_kodu:
                    st.write(f"**Ulica (z kodu pocztowego):** {st.session_state.ulica_z_kodu}, **Numery:** {st.session_state.numery_z_kodu}")
                else:
                    st.write(f"Dane ulicy nie są powiązane bezpośrednio z tym kodem pocztowym w pliku źródłowym.")

    else: # if not pasujace_miejscowosci_df.empty:
        st.warning("Nie znaleziono miejscowości dla podanego kodu pocztowego.")
        # Reset stanu, jeśli kod pocztowy jest nieprawidłowy
        st.session_state.miejscowosc = None
        st.session_state.terc_gmina = None
        st.session_state.sym_code = None


# --- Wyszukiwanie kodu SIMC (z fallbackiem) ---
if st.session_state.terc_gmina and st.session_state.miejscowosc:
    with st.expander("Dane TERYT (SIMC)", expanded=True):
        woj = st.session_state.terc_gmina[:2]
        pow = st.session_state.terc_gmina[2:4]
        gmi = st.session_state.terc_gmina[4:6]
        rodz_gmi = st.session_state.terc_gmina[6]
        simc_found = False # Flaga informująca czy znaleziono SIMC

        # Krok 1: Wyszukaj w SIMC na podstawie kodów TERC i nazwy miejscowości
        matching_simc = simc_data[
            (simc_data['WOJ'] == woj) &
            (simc_data['POW'] == pow) &
            (simc_data['GMI'] == gmi) &
            (simc_data['RODZ_GMI'] == rodz_gmi) &
            (simc_data['NAZWA'].str.strip().str.lower() == st.session_state.miejscowosc.strip().lower())
        ]

        if not matching_simc.empty:
            if len(matching_simc) > 1:
                 st.info(f"Znaleziono wiele wpisów SIMC dla miejscowości '{st.session_state.miejscowosc}'. Wyświetlam pierwszy.")
            simc_details = matching_simc.iloc[0]
            st.session_state.sym_code = simc_details['SYM']
            st.success(f"Znaleziono kod SIMC (SYM) dla miejscowości **{st.session_state.miejscowosc}**: **{st.session_state.sym_code}**")
            st.write("Szczegóły z rejestru SIMC:")
            st.dataframe(matching_simc[['SYM', 'SYMPOD', 'NAZWA', 'RM', 'MZ', 'WOJ', 'POW', 'GMI', 'RODZ_GMI']])
            simc_found = True
        else:
            # Krok 2: Fallback - Wyszukaj w SIMC używając nazwy gminy, jeśli wyszukiwanie po miejscowości zawiodło
            st.info(f"Nie znaleziono kodu SIMC dla miejscowości '{st.session_state.miejscowosc}'. Próba wyszukania dla nazwy gminy '{st.session_state.gmina}'...")

            matching_simc_fallback_gmina = simc_data[
                (simc_data['WOJ'] == woj) &
                (simc_data['POW'] == pow) &
                (simc_data['GMI'] == gmi) &
                (simc_data['RODZ_GMI'] == rodz_gmi) &
                # Użyj nazwy gminy zapisanej w stanie sesji
                (simc_data['NAZWA'].str.strip().str.lower() == st.session_state.gmina.strip().lower())
            ]

            if not matching_simc_fallback_gmina.empty:
                if len(matching_simc_fallback_gmina) > 1:
                    st.info(f"Znaleziono wiele wpisów SIMC dla nazwy gminy '{st.session_state.gmina}'. Wyświetlam pierwszy.")
                simc_details_fallback = matching_simc_fallback_gmina.iloc[0]
                st.session_state.sym_code = simc_details_fallback['SYM'] # Zapisz znaleziony kod SYM
                st.success(f"Znaleziono kod SIMC (SYM) dla **nazwy gminy {st.session_state.gmina}**: **{st.session_state.sym_code}** (użyto jako fallback)")
                st.write("Szczegóły z rejestru SIMC (fallback):")
                st.dataframe(matching_simc_fallback_gmina[['SYM', 'SYMPOD', 'NAZWA', 'RM', 'MZ', 'WOJ', 'POW', 'GMI', 'RODZ_GMI']])
                simc_found = True
            else:
                st.warning(f"Nie znaleziono kodu SIMC ani dla miejscowości '{st.session_state.miejscowosc}', ani dla nazwy gminy '{st.session_state.gmina}' z kodem TERC gminy {st.session_state.terc_gmina}")
                st.session_state.sym_code = None # Resetuj kod SYM

        if not simc_found:
             st.session_state.sym_code = None # Upewnij się, że jest None jeśli nie znaleziono

# --- Wyszukiwanie kodów ULIC ---
if st.session_state.sym_code and st.session_state.terc_gmina:
     with st.expander("Dane TERYT (ULIC)", expanded=True):
        woj = st.session_state.terc_gmina[:2]
        pow = st.session_state.terc_gmina[2:4]
        gmi = st.session_state.terc_gmina[4:6]
        rodz_gmi = st.session_state.terc_gmina[6]

        # Filtruj ulic_data na podstawie kodów TERC i kodu SIMC (SYM)
        matching_ulic = ulic_data[
            (ulic_data['WOJ'] == woj) &
            (ulic_data['POW'] == pow) &
            (ulic_data['GMI'] == gmi) &
            (ulic_data['RODZ_GMI'] == rodz_gmi) &
            (ulic_data['SYM'] == st.session_state.sym_code) # Użyj kodu SYM ze stanu sesji
        ].copy() # Użyj .copy(), aby uniknąć SettingWithCopyWarning

        if not matching_ulic.empty:
            st.success(f"Znaleziono {len(matching_ulic)} ulic dla miejscowości/gminy powiązanej z SIMC: {st.session_state.sym_code}:")

            # Połącz NAZWA_1 i NAZWA_2 dla pełnej nazwy ulicy
            matching_ulic['NAZWA_ULICY'] = matching_ulic['NAZWA_1'].fillna('') + ' ' + matching_ulic['NAZWA_2'].fillna('')
            matching_ulic['NAZWA_ULICY'] = matching_ulic['NAZWA_ULICY'].str.strip()

            # Zapisz znalezione ulice w stanie sesji
            st.session_state.matching_ulic_df = matching_ulic[['SYM_UL', 'CECHA', 'NAZWA_ULICY', 'STAN_NA']]

            # Wyświetl tabelę z kodami ULIC
            st.dataframe(st.session_state.matching_ulic_df)

        else:
            st.warning(f"Nie znaleziono kodów ULIC dla SIMC: {st.session_state.sym_code} w pliku {ULIC_FILENAME}.")
            st.session_state.matching_ulic_df = pd.DataFrame() # Resetuj DataFrame ulic

# --- Wybór ulicy (jeśli znaleziono) ---
# Ta sekcja powinna być poza expanderem ULIC, aby była widoczna po znalezieniu ulic
if not st.session_state.matching_ulic_df.empty:
    st.subheader("Wybór ulicy")
    if st.session_state.ulica_z_kodu:
        st.info(f"Ulica sugerowana na podstawie kodu pocztowego: '{st.session_state.ulica_z_kodu}'. Możesz wybrać inną z listy poniżej.")

    # Przygotuj opcje dla selectboxa
    lista_ulic_opcje = [""] + sorted(st.session_state.matching_ulic_df['NAZWA_ULICY'].unique().tolist())
    # Ustaw domyślny wybór na ulicę z kodu pocztowego, jeśli pasuje do listy, w przeciwnym razie pusty
    default_index = 0
    if st.session_state.ulica_z_kodu and st.session_state.ulica_z_kodu in lista_ulic_opcje:
        try:
            default_index = lista_ulic_opcje.index(st.session_state.ulica_z_kodu)
        except ValueError:
             default_index = 0 # Fallback na pusty wybór

    wybrana_ulica = st.selectbox(
        "Wybierz ulicę z listy TERYT:",
        lista_ulic_opcje,
        index=default_index, # Ustaw domyślny wybór
        key="sel_ulica"
        )

    if wybrana_ulica:
        dane_wybranej_ulicy = st.session_state.matching_ulic_df[st.session_state.matching_ulic_df['NAZWA_ULICY'] == wybrana_ulica].iloc[0]
        st.write(f"Wybrano ulicę: **{wybrana_ulica}**")
        st.write(f"**Kod ULIC (SYM_UL):** {dane_wybranej_ulicy['SYM_UL']}")
        st.write(f"**Cecha:** {dane_wybranej_ulicy['CECHA']}")


# Sekcja wyświetlania surowych danych (opcjonalnie)
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
