import os
import json
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from googleapiclient.discovery import build
from google.oauth2 import service_account

# ===================== Autenticação Google =====================
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
credentials_info = json.loads(json_secret)
credentials = service_account.Credentials.from_service_account_info(
    credentials_info,
    scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
)

drive_service = build("drive", "v3", credentials=credentials)
sheets_service = build("sheets", "v4", credentials=credentials)

# ===================== Buscar arquivos no Drive =====================
folder_id = "10UEE_tenpCEyJ_6dt2r1_iw7Vnpo9zm2"
sheet_input_name = "Financeiro_contas_a_receber_Dagaz"
sheet_output_name = "Detalhe_centro_recebimento"

def get_file_id(name):
    query = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    result = drive_service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
    files = result.get("files", [])
    if not files:
        raise FileNotFoundError(f"Arquivo '{name}' não encontrado na pasta especificada.")
    return files[0]["id"]

input_sheet_id = get_file_id(sheet_input_name)
output_sheet_id = get_file_id(sheet_output_name)

# ===================== Leitura do Google Sheets diretamente para o Pandas =====================
sheet_range = "A:Z"
result = sheets_service.spreadsheets().values().get(
    spreadsheetId=input_sheet_id,
    range=sheet_range
).execute()

values = result.get('values', [])
df_base = pd.DataFrame(values[1:], columns=values[0])
ids = df_base["financialEvent.id"].dropna().unique()

print(f"📥 Planilha carregada com {len(ids)} IDs únicos.")

# ===================== Configuração da API Conta Azul =====================
headers = {
    'X-Authorization': 'c4f0e05e-2d04-4a6b-8605-2aedb558d809',
    'User-Agent': 'Mozilla/5.0'
}

# ===================== Função para extrair todos os campos aninhados =====================
def extract_fields(item):
    resultado = []
    base_id = item.get("id")
    categories = item.get("categoriesRatio", [])

    for cat in categories:
        linha = {"id": base_id}
        for k, v in cat.items():
            if k == "costCentersRatio":
                for i, centro in enumerate(v):
                    for ck, cv in centro.items():
                        linha[f"categoriesRatio.costCentersRatio.{i}.{ck}"] = cv
            else:
                linha[f"categoriesRatio.{k}"] = v
        resultado.append(linha)

    return resultado

# ===================== Coleta paralela dos detalhes via API =====================
def fetch_detail(fid):
    url = f"https://services.contaazul.com/contaazul-bff/finance/v1/financial-events/{fid}/summary"
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return extract_fields(response.json())
        else:
            print(f"❌ Erro no ID {fid}: {response.status_code}")
    except Exception as e:
        print(f"⚠️ Falha no ID {fid}: {e}")
    return None

print("🚀 Iniciando requisições paralelas...")

todos_detalhes = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_detail, fid) for fid in ids]
    for f in as_completed(futures):
        resultado = f.result()
        if resultado:
            todos_detalhes.extend(resultado)

print(f"✅ Coleta finalizada com {len(todos_detalhes)} registros.")

# ===================== Enviar dados ao Google Sheets =====================
df_detalhes = pd.DataFrame(todos_detalhes)

# Limpar conteúdo anterior da planilha
sheets_service.spreadsheets().values().clear(
    spreadsheetId=output_sheet_id,
    range="A:Z"
).execute()

# Enviar os dados
values = [df_detalhes.columns.tolist()] + df_detalhes.fillna("").astype(str).values.tolist()
sheets_service.spreadsheets().values().update(
    spreadsheetId=output_sheet_id,
    range="A1",
    valueInputOption="RAW",
    body={"values": values}
).execute()

print("📊 Dados atualizados na planilha com sucesso.")
