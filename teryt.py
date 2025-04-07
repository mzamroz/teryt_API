from zeep import Client
from zeep.transports import Transport
from zeep.wsse.username import UsernameToken
from requests import Session

# Ścieżka do pliku WSDL
wsdl_url = "terytws1.wsdl"

# Dane logowania
username = "maciej.zamroz@esv.pl"
password = "mJEr8bGfl"

# Tworzenie klienta SOAP z WS-Security
session = Session()
transport = Transport(session=session)
wsse = UsernameToken(username, password)  # Dodanie WS-Security z UsernameToken
client = Client(wsdl=wsdl_url, transport=transport, wsse=wsse)

try:
    # Wywołanie metody CzyZalogowany
    response = client.service.PobierzListeWojewodztw('2025-04-01')
    print("Response:", response)
except Exception as e:
    print("Błąd podczas wywoływania API:", e)