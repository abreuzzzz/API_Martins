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
folder_id = "1NmHSga-UCUycinn2RMKviwM1XX_Mr5AR"
sheet_input_name = "Financeiro_contas_a_receber_Martins"
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

# ===================== Leitura do Google Sheets =====================
sheet_range = "A:Z"
result = sheets_service.spreadsheets().values().get(
    spreadsheetId=input_sheet_id,
    range=sheet_range
).execute()

values = result.get('values', [])
if not values:
    raise ValueError("A planilha está vazia.")

# Preencher linhas menores que o cabeçalho com ""
num_cols = len(values[0])
values_fixed = [row + [""]*(num_cols - len(row)) if len(row) < num_cols else row for row in values[1:]]

df_base = pd.DataFrame(values_fixed, columns=values[0])
ids = df_base["financialEvent.id"].dropna().unique()
print(f"📥 Planilha carregada com {len(ids)} IDs únicos.")

# ===================== Configuração da API Conta Azul =====================
headers = {
    'X-Authorization': 'ed702bf5-4807-4266-b187-02dd7a7a8705',
    'User-Agent': 'Mozilla/5.0'
}

# ===================== Função para extrair campos aninhados =====================
def extract_fields(item):
    resultado = []
    base_id = item.get("id") or ""
    observation = item.get("observation", "") or ""
    attachments = item.get("attachments", [])
    tem_attachments = "Sim" if attachments and len(attachments) > 0 else "Não"

    if "desconsiderar anexo" in observation.lower():
        tem_attachments = "Sim"

    categories = item.get("categoriesRatio", [])
    for cat in categories:
        linha = {"id": base_id, "tem_attachments": tem_attachments, "observation": observation}
        for k, v in cat.items():
            if k == "costCentersRatio":
                for i, centro in enumerate(v):
                    for ck, cv in centro.items():
                        linha[f"categoriesRatio.costCentersRatio.{i}.{ck}"] = cv
            else:
                linha[f"categoriesRatio.{k}"] = v
        resultado.append(linha)

    if not categories:
        # Garantir linha mínima mesmo sem categoriesRatio
        resultado.append({"id": base_id, "tem_attachments": tem_attachments, "observation": observation})

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
ids_falhos = []

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(fetch_detail, fid): fid for fid in ids}
    for f in as_completed(futures):
        resultado = f.result()
        if resultado:
            todos_detalhes.extend(resultado)
        else:
            ids_falhos.append(futures[f])

print(f"✅ Coleta finalizada com {len(todos_detalhes)} registros.")
if ids_falhos:
    print(f"⚠️ IDs com falha na coleta: {ids_falhos}")

# ===================== Transformar em DataFrame com cabeçalhos fixos =====================
cabecalhos = [
    "id",
    "categoriesRatio.negative",
    "categoriesRatio.grossValue",
    "categoriesRatio.operationType",
    "categoriesRatio.type",
    "categoriesRatio.category",
    "categoriesRatio.value",
    "categoriesRatio.categoryId",
    "tem_attachments",
    "observation"
]

df_detalhes = pd.DataFrame(todos_detalhes)
df_detalhes = df_detalhes.reindex(columns=cabecalhos, fill_value="")

# ===================== Limpar conteúdo anterior da planilha =====================
sheets_service.spreadsheets().values().clear(
    spreadsheetId=output_sheet_id,
    range="A:Z"
).execute()

# ===================== Enviar dados ao Google Sheets em lotes =====================
batch_size = 1000
data_values = [df_detalhes.columns.tolist()] + df_detalhes.fillna("").astype(str).values.tolist()

for i in range(0, len(data_values)-1, batch_size):
    batch_data = data_values[i+1:i+1+batch_size]  # +1 para pular cabeçalho
    start_row = i + 2  # linha 1 é o cabeçalho

    try:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=output_sheet_id,
            range=f"A{start_row}",
            valueInputOption="RAW",
            body={"values": batch_data}
        ).execute()
        print(f"📊 Lote {i//batch_size + 1} enviado: linhas {start_row} a {start_row + len(batch_data) - 1}")
    except Exception as e:
        print(f"❌ Erro ao enviar lote {i//batch_size + 1}: {e}")
        mini_batch_size = 500
        for j in range(0, len(batch_data), mini_batch_size):
            mini_batch = batch_data[j:j + mini_batch_size]
            mini_start_row = start_row + j
            try:
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=output_sheet_id,
                    range=f"A{mini_start_row}",
                    valueInputOption="RAW",
                    body={"values": mini_batch}
                ).execute()
                print(f"📊 Mini-lote enviado: linhas {mini_start_row} a {mini_start_row + len(mini_batch) - 1}")
            except Exception as mini_e:
                print(f"❌ Erro crítico no mini-lote: {mini_e}")

print("📊 Dados atualizados na planilha com sucesso.")
