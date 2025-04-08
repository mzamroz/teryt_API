import streamlit as st
import requests
import pandas as pd
import re # Do walidacji kodu pocztowego

# --- Konfiguracja ---
st.set_page_config(page_title="Wyszukiwarka TERYT", layout="wide")
st.title("Wyszukiwarka Kodów TERYT i Danych Adresowych")

# --- Inicjalizacja Session State ---
# Klucze dla stanu aplikacji
keys_to_init = [
    'current_postal_code', 'localities_list', 'selected_locality',
    'details_data', 'streets_list', 'selected_street_name', 'selected_street_data'
]
for key in keys_to_init:
    if key not in st.session_state:
        st.session_state[key] = None

# --- Wprowadzenie adresu URL API ---
# Użyj kolumn, aby umieścić input obok etykiety dla lepszego wyglądu
col1_api, col2_api = st.columns([1,3])
with col1_api:
    st.write("Adres URL API:") # Etykieta
with col2_api:
    api_base_url = st.text_input(
        "Podstawowy adres URL API FastAPI:", # Ten label jest ukryty dzięki label_visibility
        st.session_state.get("api_url_input", "http://127.0.0.1:8000"), # Zachowaj wpisany URL
        key="api_url_input",
        label_visibility="collapsed" # Ukryj domyślną etykietę
    )

# Funkcja pomocnicza do walidacji kodu pocztowego
def is_valid_postal_code(code):
    """Sprawdza, czy string jest poprawnym kodem pocztowym (XX-XXX)."""
    if not code:
        return False
    return bool(re.match(r"^\d{2}-\d{3}$", code.strip()))

# Funkcja pomocnicza do wykonywania zapytań API
def make_api_request(method, endpoint, params=None, json_data=None, path_params=None):
    """Wykonuje zapytanie do API i obsługuje podstawowe błędy."""
    # Sprawdzenie czy URL API jest wprowadzony odbywa się teraz wewnątrz funkcji
    # Odczytuj URL ze stanu sesji, aby upewnić się, że używamy aktualnego
    current_api_url = st.session_state.get("api_url_input", "").strip()
    if not current_api_url:
        st.error("Proszę wprowadzić adres URL API.")
        return None
    base_url = current_api_url.rstrip('/')
    full_url = base_url + endpoint

    if path_params:
        for key, value in path_params.items():
            full_url = full_url.replace(f"{{{key}}}", str(value))

    try:
        response = requests.request(method, full_url, params=params, json=json_data, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        st.error(f"Nie można połączyć się z API pod adresem: {base_url}. Upewnij się, że API jest uruchomione.")
        return None
    except requests.exceptions.Timeout:
        st.error("Przekroczono limit czasu oczekiwania na odpowiedź API.")
        return None
    except requests.exceptions.HTTPError as e:
        try:
            error_details = e.response.json()
            if e.response.status_code == 400 and isinstance(error_details.get('detail'), dict) and 'available_localities' in error_details['detail']:
                 st.warning(f"Błąd API (Status: {e.response.status_code}): {error_details['detail']['message']}")
                 st.info(f"Dostępne miejscowości: {', '.join(error_details['detail']['available_localities'])}")
            elif e.response.status_code == 404 and endpoint.endswith("/localities"):
                 st.error(f"Nie znaleziono miejscowości dla podanego kodu pocztowego (Status: 404).")
            else:
                 st.error(f"Błąd API (Status: {e.response.status_code}): {error_details.get('detail', e.response.text)}")
        except ValueError:
             st.error(f"Błąd HTTP {e.response.status_code}: {e.response.text}")
        return None
    except requests.exceptions.RequestException as e:
        st.error(f"Wystąpił nieoczekiwany błąd zapytania: {e}")
        return None
    except ValueError: # Błąd dekodowania JSON
        st.error("Nie udało się zdekodować odpowiedzi JSON z API.")
        return None

# --- Funkcje Callback dla zmian w selectboxach ---
def reset_dependent_state(level):
    """Resetuje stan zależny od poziomu zmiany (postal_code, locality, street)."""
    if level <= 'postal_code':
        #st.session_state.localities_list = None
        st.session_state.selected_locality = None
    if level <= 'locality':
        st.session_state.details_data = None
        st.session_state.streets_list = None
        st.session_state.selected_street_name = None
        st.session_state.selected_street_data = None

def fetch_localities():
    """Pobiera miejscowości dla kodu pocztowego."""
    reset_dependent_state('postal_code') # Resetuj wszystko poniżej kodu pocztowego
    postal_code = st.session_state.get('postal_code_input', '').strip()
    if not is_valid_postal_code(postal_code):
        st.warning("Wprowadź poprawny kod pocztowy (XX-XXX), aby wyszukać.")
        st.session_state.current_postal_code = None
        # Wyczyść listę, jeśli kod jest niepoprawny
        st.session_state.localities_list = None
        return

    st.session_state.current_postal_code = postal_code
    with st.spinner(f"Pobieranie miejscowości dla {postal_code}..."):
        endpoint = f"/postal_codes/{postal_code}/localities"
        response = make_api_request("GET", endpoint)
        if response and response.get("localities"):
            st.session_state.localities_list = sorted(response["localities"]) # Sortuj alfabetycznie
        else:
            # Błąd (np. 404) został już obsłużony w make_api_request
            # Ustawiamy pustą listę, aby ukryć selectbox, jeśli nie było błędu krytycznego
            if response is not None: # Jeśli nie było błędu połączenia itp.
                st.session_state.localities_list = []
            else: # Jeśli był błąd krytyczny, ustaw None, aby nic się nie renderowało
                 st.session_state.localities_list = None


def locality_changed():
    """Obsługuje zmianę wybranej miejscowości."""
    reset_dependent_state('locality') # Resetuj stan ulicy i szczegółów
    # Odczytaj wartość bezpośrednio z klucza widgetu
    st.session_state.selected_locality = st.session_state.get('locality_selector')
    if not st.session_state.selected_locality or not st.session_state.current_postal_code:
        return # Nic nie rób, jeśli miejscowość nie jest wybrana lub brakuje kodu

    postal_code = st.session_state.current_postal_code
    locality = st.session_state.selected_locality

    with st.spinner(f"Pobieranie danych dla {locality} ({postal_code})..."):
        details_endpoint = f"/postal_codes/{postal_code}/details"
        params = {'locality': locality}
        details_response = make_api_request("GET", details_endpoint, params=params)

        if details_response:
            st.session_state.details_data = details_response
            streets = details_response.get("streets", [])
            if streets:
                # Sortuj ulice alfabetycznie wg nazwy
                st.session_state.streets_list = sorted(streets, key=lambda x: x.get('street_name', ''))
            else:
                st.session_state.streets_list = [] # Pusta lista, jeśli brak ulic
        else:
            # Błąd został już obsłużony, ale resetujemy stan
            st.session_state.details_data = None
            st.session_state.streets_list = None

def street_changed():
    """Obsługuje zmianę wybranej ulicy."""
    # Odczytaj wartość bezpośrednio z klucza widgetu
    st.session_state.selected_street_name = st.session_state.get('street_selector')
    st.session_state.selected_street_data = None # Resetuj na wszelki wypadek
    if st.session_state.selected_street_name and st.session_state.streets_list:
        # Znajdź pełne dane wybranej ulicy
        for street_data in st.session_state.streets_list:
            if street_data.get('street_name') == st.session_state.selected_street_name:
                st.session_state.selected_street_data = street_data
                break

# --- Sekcja 1: Sprawdź status API (Opcjonalna) ---
with st.expander("1. Sprawdź status API"):
    if st.button("Sprawdź status"):
        with st.spinner("Sprawdzanie statusu API..."):
            health_data = make_api_request("GET", "/health")
            if health_data:
                status = health_data.get("status", "Nieznany")
                data_loaded = health_data.get("data_loaded", False)
                detail = health_data.get("detail", "Brak szczegółów.")
                if status == "OK": st.success(f"Status API: {status} - {detail}")
                elif status == "WARN": st.warning(f"Status API: {status} - {detail}")
                else: st.error(f"Status API: {status} - {detail}")

# --- Sekcja 2: Wyszukiwanie Adresu ---
st.header("2. Wyszukaj kody TERYT i dane adresowe")

# Użyj kolumn dla lepszego układu
col1, col2 = st.columns([1, 2])

with col1:
    postal_code_input = st.text_input(
        "Kod pocztowy (format XX-XXX):",
        # Użyj wartości ze stanu sesji, aby zachować wpisany kod
        value=st.session_state.get('postal_code_input', ''),
        key='postal_code_input',
        help="Wpisz kod pocztowy i kliknij 'Wyszukaj'."
    )
    st.button("Wyszukaj", key="btn_fetch_localities", on_click=fetch_localities)

# --- Logika wyświetlania selectboxów i wyników ---

# Selectbox Miejscowości
# Renderuj, jeśli lista miejscowości została zainicjowana (nawet jeśli jest pusta po błędzie 404)
if st.session_state.localities_list is not None:
    if st.session_state.localities_list: # Renderuj selectbox tylko jeśli lista nie jest pusta
        # Oblicz index tylko jeśli selected_locality istnieje i jest w liście
        current_locality_index = None
        # Sprawdź, czy wybrana miejscowość nadal jest na liście (na wypadek zmiany kodu pocztowego)
        if st.session_state.selected_locality and st.session_state.selected_locality in st.session_state.localities_list:
             current_locality_index = st.session_state.localities_list.index(st.session_state.selected_locality)
        else:
            # Jeśli poprzednio wybrana miejscowość nie pasuje do nowej listy, zresetuj wybór
            st.session_state.selected_locality = None

        st.selectbox(
            "Wybierz miejscowość:",
            options=st.session_state.localities_list, # Użyj explicit 'options'
            key='locality_selector', # Klucz widgetu do odczytu stanu
            on_change=locality_changed, # Callback wywoływany przy zmianie
            index=current_locality_index, # Ustawia domyślnie wybraną wartość (lub None)
            placeholder="-- Wybierz --" # Tekst zastępczy
        )
    # Komunikat o braku miejscowości (404) jest obsługiwany w make_api_request i fetch_localities
    # Jeśli lista jest pusta (localities_list == []), nic się tu nie renderuje, co jest OK.

# Selectbox Ulicy
# Renderuj tylko jeśli wybrano miejscowość i lista ulic została pobrana (nie jest None)
if st.session_state.selected_locality and st.session_state.streets_list is not None:
    if st.session_state.streets_list: # Renderuj selectbox tylko jeśli lista ulic nie jest pusta
        street_names = [s.get('street_name', 'Brak nazwy') for s in st.session_state.streets_list]
        # Oblicz index tylko jeśli selected_street_name istnieje i jest w liście
        current_street_index = None
        if st.session_state.selected_street_name and st.session_state.selected_street_name in street_names:
            current_street_index = street_names.index(st.session_state.selected_street_name)
        else:
             # Jeśli poprzednio wybrana ulica nie pasuje do nowej listy, zresetuj wybór
            st.session_state.selected_street_name = None
            st.session_state.selected_street_data = None # Ważne, aby wyniki też zniknęły

        st.selectbox(
            "Wybierz ulicę:",
            options=street_names, # Użyj explicit 'options'
            key='street_selector', # Klucz widgetu
            on_change=street_changed, # Callback
            index=current_street_index, # Ustawia domyślnie wybraną wartość (lub None)
            placeholder="-- Wybierz --" # Tekst zastępczy
        )
    elif st.session_state.details_data: # Jeśli mamy dane miejscowości, ale brak ulic (streets_list == [])
         st.info(f"Miejscowość '{st.session_state.selected_locality}' nie posiada zarejestrowanych ulic w systemie ULIC.")
         # Wyświetl częściowe wyniki, jeśli ulica nie jest (i nie może być) wybrana
         if not st.session_state.selected_street_data: # Upewnij się, że ulica nie jest wybrana
            st.subheader("Wyniki wyszukiwania (bez ulicy):")
            # Sprawdź czy details_data istnieje
            if st.session_state.details_data:
                details = st.session_state.details_data
                loc_info = details.get("location_from_postal_code", {})
                teryt = details.get("teryt_codes", {})
                st.write(f"- **Kod pocztowy:** {st.session_state.current_postal_code}")
                st.write(f"- **Miejscowość:** {loc_info.get('locality', 'N/A')}")
                st.write(f"- **Gmina:** {loc_info.get('municipality_name', 'N/A')}")
                st.write(f"- **Powiat:** {loc_info.get('county_name', 'N/A')}")
                st.write(f"- **Województwo:** {loc_info.get('voivodeship_name', 'N/A')}")
                st.divider()
                st.write(f"- **TERC Gmina (pełny):** {teryt.get('terc_municipality', 'Nie znaleziono')}")
                st.write(f"- **SIMC (Miejscowość):** {teryt.get('simc', 'Nie znaleziono')}")
                st.write(f"- **SIMC Nazwa Oficjalna:** {teryt.get('simc_official_name', 'Nie znaleziono')}")
                st.write(f"- **ULIC (Ulica):** Brak (miejscowość bez ulic)")


# Wyświetlanie końcowych wyników
# Renderuj tylko jeśli wybrano ulicę (selected_street_data ma wartość)
if st.session_state.selected_street_data:
    st.subheader("Wyniki wyszukiwania:")
    # Upewnij się, że details_data istnieje (powinno, jeśli mamy selected_street_data)
    if st.session_state.details_data:
        details = st.session_state.details_data
        street_info = st.session_state.selected_street_data
        loc_info = details.get("location_from_postal_code", {})
        teryt = details.get("teryt_codes", {})

        st.write(f"- **Kod pocztowy:** {st.session_state.current_postal_code}")
        st.write(f"- **Miejscowość:** {loc_info.get('locality', 'N/A')}")
        st.write(f"- **Ulica:** {street_info.get('street_name', 'N/A')} ({street_info.get('feature_type', '')})")
        st.write(f"- **Gmina:** {loc_info.get('municipality_name', 'N/A')}")
        st.write(f"- **Powiat:** {loc_info.get('county_name', 'N/A')}")
        st.write(f"- **Województwo:** {loc_info.get('voivodeship_name', 'N/A')}")
        st.divider()
        st.write(f"- **TERC Województwo:** {teryt.get('terc_voivodeship', 'Nie znaleziono')}")
        st.write(f"- **TERC Powiat:** {teryt.get('terc_county', 'Nie znaleziono')}")
        st.write(f"- **TERC Gmina (pełny):** {teryt.get('terc_municipality', 'Nie znaleziono')}")
        st.write(f"- **SIMC (Miejscowość):** {teryt.get('simc', 'Nie znaleziono')}")
        st.write(f"- **SIMC Nazwa Oficjalna:** {teryt.get('simc_official_name', 'Nie znaleziono')}")
        st.write(f"- **ULIC (Ulica):** {street_info.get('ulic_code', 'Nie znaleziono')}")
        st.write(f"- **ULIC Stan na:** {street_info.get('valid_as_of', 'N/A')}")
    else:
        # Ten przypadek nie powinien wystąpić, jeśli logika callbacków jest poprawna
        st.warning("Wystąpił niespodziewany błąd: Brak danych szczegółowych mimo wybranej ulicy.")

# Wyświetlanie Session State (Debugging)
with st.expander("Debug: Session State"):
    st.json({key: value for key, value in st.session_state.items()})

# --- Stopka ---
st.markdown("---")
st.caption("teryt")
