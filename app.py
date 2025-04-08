import pandas as pd
import streamlit as st
import os

# Load files from ./dane into DataFrames
dataframes = {}
data_dir = './dane'
loaded_files = []  # Keep track of successfully loaded files
terc_data = None  # Zmienna do przechowywania danych TERC
simc_data = None  # Zmienna do przechowywania danych SIMC

if os.path.exists(data_dir):
    for file_name in os.listdir(data_dir):
        if file_name.endswith('.csv'):
            file_path = os.path.join(data_dir, file_name)
            try:
                # Attempt to read the CSV file with error handling
                df = pd.read_csv(file_path, delimiter=';', on_bad_lines='skip', encoding='utf-8')
                dataframes[file_name] = df
                loaded_files.append(file_name)  # Add to the list of loaded files
                if file_name == 'TERC_Adresowy_2025-04-08.csv':
                    terc_data = df  # Przypisanie DataFrame do terc_data
                elif file_name == 'SIMC_Adresowy_2025-04-08.csv':  # Dodaj obsługę pliku SIMC
                    simc_data = df  # Przypisanie DataFrame do simc_data
            except pd.errors.ParserError as e:
                st.warning(f"Błąd podczas ładowania {file_name}: {e}. Pomijam ten plik.")
            except UnicodeDecodeError:
                try:
                    # Jeśli 'utf-8' zawiedzie, spróbuj 'latin1'
                    df = pd.read_csv(file_path, delimiter=';', on_bad_lines='skip', encoding='latin1')
                    dataframes[file_name] = df
                    loaded_files.append(file_name)
                    if file_name == 'SIMC_Adresowy_2025-04-08.csv':  # Dodaj obsługę pliku SIMC
                        simc_data = df  # Przypisanie DataFrame do simc_data
                    st.warning(f"Plik {file_name} załadowano używając kodowania 'latin1'.")
                except Exception as e:
                    st.error(f"Nieoczekiwany błąd podczas ładowania {file_name}: {e}")
            except Exception as e:
                st.error(f"Nieoczekiwany błąd podczas ładowania {file_name}: {e}")

# Streamlit app to display DataFrames
st.title("Uteryterowana terytowarka")

# Dodajmy obsługę kodów pocztowych
kod_pocztowy = st.text_input("Wprowadź kod pocztowy:", "")
miejscowosc = None  # Inicjalizacja zmiennej miejscowość

if kod_pocztowy and 'kody_pocztowe.csv' in dataframes:
    df_kody_pocztowe = dataframes['kody_pocztowe.csv']

    # Wyodrębnij nazwy w nawiasach, jeśli istnieją
    df_kody_pocztowe['MIEJSCOWOŚĆ'] = df_kody_pocztowe['MIEJSCOWOŚĆ'].str.extract(r'\((.*?)\)', expand=False).fillna(df_kody_pocztowe['MIEJSCOWOŚĆ'])

    # Filtruj DataFrame po kodzie pocztowym.
    pasujace_miejscowosci = df_kody_pocztowe[df_kody_pocztowe['PNA'] == kod_pocztowy]
    # Pobierz unikalne nazwy miejscowości.
    lista_miejscowosci = pasujace_miejscowosci['MIEJSCOWOŚĆ'].unique()

    if len(lista_miejscowosci) > 0:
        # Jeśli znaleziono więcej niż jedną miejscowość, pozwól użytkownikowi wybrać.
        miejscowosc = st.selectbox("Wybierz miejscowość:", lista_miejscowosci)
    elif len(lista_miejscowosci) == 1:
        miejscowosc = lista_miejscowosci[0]
    else:
        st.warning("Nie znaleziono miejscowości dla podanego kodu pocztowego.")

    if miejscowosc:
        # Filtruj DataFrame po wybranej miejscowości.
        dane_miejscowosci = pasujace_miejscowosci[pasujace_miejscowosci['MIEJSCOWOŚĆ'] == miejscowosc].iloc[0]  # Weź pierwszy rekord

        # Automatycznie wypełnij pola.
        wojewodztwo = dane_miejscowosci['WOJEWÓDZTWO']
        powiat = dane_miejscowosci['POWIAT']
        gmina = dane_miejscowosci['GMINA']
        ulica = dane_miejscowosci['ULICA'] if not pd.isna(dane_miejscowosci['ULICA']) else None
        numery = dane_miejscowosci['NUMERY'] if not pd.isna(dane_miejscowosci['NUMERY']) else None

        terc_woj = None
        terc_powiat = None
        terc_gmina = None

        if terc_data is not None:
            # Znajdź kod TERC województwa
            woj_row = terc_data[terc_data['NAZWA'].str.lower() == wojewodztwo.lower()]
            if not woj_row.empty:
                terc_woj = f"{int(woj_row['WOJ'].iloc[0]):02d}"
            else:
                terc_woj = None

            # Znajdź kod TERC powiatu
            pow_row = terc_data[terc_data['NAZWA'].str.lower() == powiat.lower()]
            if not pow_row.empty:
                terc_powiat = f"{int(pow_row['WOJ'].iloc[0]):02d}{int(pow_row['POW'].iloc[0]):02d}"
            else:
                terc_powiat = None

            # Znajdź kod TERC gminy
            gmi_row = terc_data[terc_data['NAZWA'].str.lower() == gmina.lower()]
            if not gmi_row.empty:
                try:
                    woj = int(gmi_row['WOJ'].iloc[0]) if not pd.isna(gmi_row['WOJ'].iloc[0]) else None
                    powiat = int(gmi_row['POW'].iloc[0]) if not pd.isna(gmi_row['POW'].iloc[0]) else None
                    gmina_code = int(gmi_row['GMI'].iloc[0]) if not pd.isna(gmi_row['GMI'].iloc[0]) else None
                    rodz = int(gmi_row['RODZ'].iloc[0]) if not pd.isna(gmi_row['RODZ'].iloc[0]) else None

                    if None not in (woj, powiat, gmina_code, rodz):
                        terc_gmina = f"{woj:02d}{powiat:02d}{gmina_code:02d}{rodz}"
                    else:
                        terc_gmina = None
                except ValueError:
                    terc_gmina = None
            else:
                terc_gmina = None

            # Jeśli terc_gmina nadal jest None, spróbuj użyć nazwy miejscowości
            if terc_gmina is None:
                gmi_row = terc_data[terc_data['NAZWA'].str.lower() == miejscowosc.lower()]
                if not gmi_row.empty:
                    try:
                        woj = int(gmi_row['WOJ'].iloc[0]) if not pd.isna(gmi_row['WOJ'].iloc[0]) else None
                        powiat = int(gmi_row['POW'].iloc[0]) if not pd.isna(gmi_row['POW'].iloc[0]) else None
                        gmina_code = int(gmi_row['GMI'].iloc[0]) if not pd.isna(gmi_row['GMI'].iloc[0]) else None
                        rodz = int(gmi_row['RODZ'].iloc[0]) if not pd.isna(gmi_row['RODZ'].iloc[0]) else None

                        if None not in (woj, powiat, gmina_code, rodz):
                            terc_gmina = f"{woj:02d}{powiat:02d}{gmina_code:02d}{rodz}"
                        else:
                            terc_gmina = None
                    except ValueError:
                        terc_gmina = None

        st.write(f"Wybrano miejscowość: {miejscowosc}")
        st.write(f"**Województwo:** {wojewodztwo} (TERC: {terc_woj})")
        st.write(f"**Powiat:** {powiat} (TERC: {terc_powiat})")
        st.write(f"**Gmina:** {gmina} (TERC: {terc_gmina})")

        if ulica:
            st.write(f"Ulica: {ulica}, Numery: {numery}")
        else:
            st.write(f"Ulica i numery nie są dostępne w danych.")

# Obsługa kodów SIMC
if simc_data is not None and miejscowosc:
    # Przygotuj wartości z TERC do wyszukiwania w SIMC
    if terc_gmina:
        woj = terc_gmina[:2]
        pow = terc_gmina[2:4]
        gmi = terc_gmina[4:6]
        rodz_gmi = terc_gmina[6]
        
        # Wyszukaj w SIMC na podstawie kodów z TERC
        matching_simc = simc_data[
            (simc_data['WOJ'] == int(woj)) & 
            (simc_data['POW'] == int(pow)) & 
            (simc_data['GMI'] == int(gmi)) & 
            (simc_data['RODZ_GMI'] == int(rodz_gmi)) 
        ]
        
        if not matching_simc.empty:
            st.write("### Znaleziono kod SIMC:")
            # Wyświetl kod SYM dla znalezionej miejscowości
            sym_code = matching_simc.iloc[0]['SYM']
            st.write(f"Kod SIMC (SYM) dla miejscowości {miejscowosc}: **{sym_code}**")
            
            # Wyświetl szczegóły z rejestru SIMC
            st.write("Szczegóły z rejestru SIMC:")
            st.dataframe(matching_simc[['SYM', 'SYMPOD', 'NAZWA', 'WOJ', 'POW', 'GMI', 'RODZ_GMI', 'RM', 'MZ']])
        else:
            st.warning(f"Nie znaleziono kodu SIMC dla miejscowości {miejscowosc} z podanymi kodami TERC")

if loaded_files:  # Check if any files were loaded
    selected_file = st.selectbox("Wybierz plik do wyświetlenia:", loaded_files)
    st.write(f"Wyświetlanie DataFrame dla **{selected_file}**:")
    st.dataframe(dataframes[selected_file])
    
else:
    st.write(
        "Nie załadowano żadnych DataFrames. Upewnij się, że w katalogu './dane' znajdują się pliki CSV i są poprawnie sformatowane."
    )
    st.warning("Katalog './dane' nie istnieje.")

