import os
import json
import pandas as pd
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ===================== Autenticar com Google APIs =====================
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
credentials_info = json.loads(json_secret)
scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=scopes)
drive_service = build("drive", "v3", credentials=credentials)
sheets_service = build("sheets", "v4", credentials=credentials)

# ===================== Headers da API Conta Azul =====================
headers = {
    'X-Authorization': 'c4f0e05e-2d04-4a6b-8605-2aedb558d809',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0'
}

# ===================== Colunas a serem extraídas =====================
colunas_base = [
    "id",
    "description",
    "dueDate",
    "expectedPaymentDate",
    "lastAcquittanceDate",
    "unpaid",
    "paid",
    "status",
    "financialEvent.id",
    "financialEvent.categoryDescriptions",
    "financialEvent.negotiator.id",
    "financialEvent.negotiator.name"
]

# ===================== Coleta paginada da API =====================
page = 1
page_size = 1000
all_items = []

while True:
    url = f"https://services.contaazul.com/finance-pro-reader/v1/installment-view?page={page}&page_size={page_size}"
    payload = json.dumps({
        "quickFilter": "ALL",
        "search": "",
        "type": "EXPENSE"
    })

    response = requests.post(url, headers=headers, data=payload)
    data = response.json()
    items = data.get("items", [])
    if not items:
        break
    all_items.extend(items)
    page += 1

# ===================== Normalização dos dados =====================
def extract_fields(item, campos):
    flat_item = {}
    for campo in campos:
        partes = campo.split('.')
        valor = item
        for parte in partes:
            valor = valor.get(parte, {}) if isinstance(valor, dict) else {}
        flat_item[campo] = valor if valor != {} else None
    return flat_item

dados_formatados = [extract_fields(item, colunas_base) for item in all_items]
df = pd.DataFrame(dados_formatados)

# ===================== Buscar ID da planilha no Google Drive =====================
folder_id = "10UEE_tenpCEyJ_6dt2r1_iw7Vnpo9zm2"
sheet_name = "Financeiro_contas_a_pagar_Dagaz"

query = f"name='{sheet_name}' and mimeType='application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents and trashed=false"
results = drive_service.files().list(q=query, spaces='drive', fields="files(id, name)").execute()
files = results.get("files", [])

if not files:
    raise Exception(f"Planilha '{sheet_name}' não encontrada na pasta do Drive.")

spreadsheet_id = files[0]['id']

# ===================== Limpar conteúdo anterior da planilha =====================
sheets_service.spreadsheets().values().clear(
    spreadsheetId=spreadsheet_id,
    range="A:Z"
).execute()

# ===================== Atualizar dados na planilha =====================
values = [df.columns.tolist()] + df.fillna("").values.tolist()
sheets_service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    range="A1",
    valueInputOption="RAW",
    body={"values": values}
).execute()

print("✅ Planilha Google atualizada com sucesso.")
