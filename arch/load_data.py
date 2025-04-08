import duckdb
import os
from zeep import Client
from zeep.transports import Transport
from requests import Session

file_name = 'spispna-cz1.txt'

conn = duckdb.connect('kody_pocztowe.duckdb')

# Iteracja po plikach w folderze

if file_name.endswith('.txt'):
    
    # Wczytanie danych z pliku do tabeli w DuckDB
    table_name = table_name = os.path.splitext(file_name)[0].replace("-", "_")
    conn.execute(f"""
        CREATE TABLE {table_name} AS 
        SELECT * FROM read_csv_auto('{file_name}')
    """)
    print(f"Wczytano dane z pliku {file_name} do tabeli {table_name}")

# Zamknięcie połączenia
conn.close()


wsdl_url = "terytws1.wsdl"

username = "MarekWodawski"
password = "eh54BV23tj32"

session = Session()
session.auth = (username, password)  
transport = Transport(session=session)
client = Client(wsdl=wsdl_url, transport=transport)


try:
    response = client.service.CzyZalogowany()
    print("Czy zalogowany:", response)
except Exception as e:
    print("Błąd podczas wywoływania API:", e)