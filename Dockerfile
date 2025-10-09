FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY dane/ ./dane/

# Variables d'environnement
ENV DATA_DIR="./dane"
ENV TERC_FILENAME="TERC_Adresowy_2025-04-08.csv"
ENV SIMC_FILENAME="SIMC_Adresowy_2025-04-08.csv"
ENV ULIC_FILENAME="ULIC_Adresowy_2025-04-08.csv"
ENV KODY_POCZTOWE_FILENAME="kody_pocztowe.csv"

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "5555"]
