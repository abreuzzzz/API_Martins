import os
import json
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ===================== Autenticar com Google APIs =====================
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
credentials_info = json.loads(json_secret)
scopes = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=scopes)
drive_service = build("drive", "v3", credentials=credentials)
sheets_service = build("sheets", "v4", credentials=credentials)

# ===================== Configurações =====================
export_url = "https://services.contaazul.com/finance-pro-reports/api/v1/installment-view/export"
headers = {
    'x-authorization': 'ed702bf5-4807-4266-b187-02dd7a7a8705',
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0'
}

# Lista de status para processar
status_list = ["ACQUITTED", "PARTIAL", "PENDING", "LOST"]

# ===================== Baixar e consolidar arquivos XLSX =====================
print("🔄 Iniciando download dos arquivos XLSX para cada status...")

all_dataframes = []

for status_atual in status_list:
    print(f"\n📥 Baixando dados para status: {status_atual}")

    payload = json.dumps({
        "dueDateFrom": None,
        "dueDateTo": None,
        "quickFilter": "ALL",
        "search": "",
        "status": [status_atual],
        "type": "REVENUE"
    })

    try:
        response = requests.post(export_url, headers=headers, data=payload)
        response.raise_for_status()

        # Ler o arquivo XLSX da resposta
        xlsx_content = BytesIO(response.content)
        df = pd.read_excel(xlsx_content)

        # Adicionar coluna de status
        df['status'] = status_atual

        print(f"  ✅ {len(df)} registros baixados para {status_atual}")

        all_dataframes.append(df)

    except requests.exceptions.RequestException as e:
        print(f"  ⚠️ Erro ao baixar dados para {status_atual}: {e}")
        continue
    except Exception as e:
        print(f"  ⚠️ Erro ao processar arquivo XLSX para {status_atual}: {e}")
        continue

# ===================== Consolidar todos os DataFrames =====================
if not all_dataframes:
    raise Exception("❌ Nenhum dado foi baixado com sucesso!")

print(f"\n🔄 Consolidando {len(all_dataframes)} arquivos...")
df_consolidado = pd.concat(all_dataframes, ignore_index=True)

# Remover duplicatas baseadas no ID (se existir coluna 'id')
if 'id' in df_consolidado.columns:
    df_consolidado = df_consolidado.drop_duplicates(subset=['id'], keep='first')
    print(f"📋 Total de registros únicos após remoção de duplicatas: {len(df_consolidado)}")
else:
    print(f"📋 Total de registros consolidados: {len(df_consolidado)}")

# ===================== Atualizar status PENDING para OVERDUE =====================
print(f"\n🔄 Verificando status PENDING com data vencida...")

# Calcular data de ontem
ontem = datetime.now() - timedelta(days=1)
ontem = ontem.replace(hour=0, minute=0, second=0, microsecond=0)

# Nome da coluna de data de vencimento (ajuste se necessário)
col_vencimento = "Data de vencimento"

if col_vencimento in df_consolidado.columns:
    # Converter coluna de vencimento para datetime
    df_consolidado[col_vencimento] = pd.to_datetime(df_consolidado[col_vencimento], format='%d/%m/%Y', errors='coerce', dayfirst=True)

    # Contar quantos serão atualizados
    mask_update = (df_consolidado['status'] == 'PENDING') & (df_consolidado[col_vencimento] <= ontem)
    total_atualizados = mask_update.sum()

    # Atualizar status
    df_consolidado.loc[mask_update, 'status'] = 'OVERDUE'

    print(f"  ✅ {total_atualizados} registros PENDING atualizados para OVERDUE")
else:
    print(f"  ⚠️ AVISO: Coluna '{col_vencimento}' não encontrada!")
    print(f"  Colunas disponíveis: {df_consolidado.columns.tolist()}")

# ===================== Criar nova coluna com valor calculado =====================
print(f"\n🔄 Criando coluna 'Valor Calculado'...")

# Nomes das colunas para contas a receber
col_recebido = "Valor total recebido da parcela (R$)"
col_aberto = "Valor da parcela em aberto (R$)"

# Garantir que as colunas existam
if col_recebido not in df_consolidado.columns or col_aberto not in df_consolidado.columns:
    print(f"  ⚠️ AVISO: Colunas esperadas não encontradas!")
    print(f"  Colunas disponíveis: {df_consolidado.columns.tolist()}")
else:
    # Criar a nova coluna baseada nas condições
    def calcular_valor(row):
        if row['status'] == 'ACQUITTED':
            # Se ACQUITTED, considerar apenas valor recebido
            return row[col_recebido]
        elif row['status'] == 'PARTIAL':
            # Se PARTIAL, somar valor recebido + valor em aberto
            return row[col_recebido] + row[col_aberto]
        else:
            # Para outros status (PENDING, OVERDUE, LOST), considerar valor em aberto
            return row[col_aberto]

    df_consolidado['Valor Calculado'] = df_consolidado.apply(calcular_valor, axis=1)
    print(f"  ✅ Coluna 'Valor Calculado' criada com sucesso!")

# ===================== Converter colunas datetime para string =====================
print(f"\n🔄 Convertendo colunas de data para string...")

# Identificar colunas de tipo datetime
datetime_columns = df_consolidado.select_dtypes(include=['datetime64']).columns.tolist()

# Converter cada coluna datetime para string no formato desejado
for col in datetime_columns:
    df_consolidado[col] = df_consolidado[col].dt.strftime('%d/%m/%Y')
    print(f"  ✅ Coluna '{col}' convertida para string")

# ===================== Renomear colunas conforme especificação =====================
print(f"\n🔄 Renomeando colunas...")

# Dicionário de mapeamento: nome_antigo -> nome_novo
colunas_renomear = {
    "Data de vencimento": "dueDate",
    "Data de competência": "financialEvent.competenceDate",
    "Valor Calculado": "paid",
    "Centro de Custo 1": "categoriesRatio.costCentersRatio.0.costCenter",
    "Categoria 1": "categoriesRatio.category",
    "Descrição": "description",
    "Nome do cliente": "financialEvent.negotiator.name",
    "Data do último pagamento": "lastAcquittanceDate"
}

# Renomear apenas as colunas que existem no DataFrame
colunas_renomeadas = {}
for col_antiga, col_nova in colunas_renomear.items():
    if col_antiga in df_consolidado.columns:
        colunas_renomeadas[col_antiga] = col_nova
        print(f"  ✅ '{col_antiga}' → '{col_nova}'")
    else:
        print(f"  ⚠️ Coluna '{col_antiga}' não encontrada")

df_consolidado.rename(columns=colunas_renomeadas, inplace=True)

# ===================== Buscar ID da planilha no Google Drive =====================
folder_id = "1NmHSga-UCUycinn2RMKviwM1XX_Mr5AR"
sheet_name = "Financeiro_contas_a_receber_Martins"

query = f"name='{sheet_name}' and mimeType='application/vnd.google-apps.spreadsheet' and '{folder_id}' in parents and trashed=false"
results = drive_service.files().list(q=query, spaces='drive', fields="files(id, name)").execute()
files = results.get("files", [])

if not files:
    raise Exception(f"Planilha '{sheet_name}' não encontrada na pasta do Drive.")

spreadsheet_id = files[0]['id']

# ===================== Limpar conteúdo anterior da planilha =====================
print(f"\n🧹 Limpando planilha '{sheet_name}'...")
sheets_service.spreadsheets().values().clear(
    spreadsheetId=spreadsheet_id,
    range="A:BA"
).execute()

# ===================== Atualizar dados na planilha =====================
print(f"📤 Atualizando planilha com {len(df_consolidado)} registros...")
values = [df_consolidado.columns.tolist()] + df_consolidado.fillna("").values.tolist()
sheets_service.spreadsheets().values().update(
    spreadsheetId=spreadsheet_id,
    range="A1",
    valueInputOption="USER_ENTERED",
    body={"values": values}
).execute()

print(f"\n✅ Planilha Google '{sheet_name}' atualizada com sucesso!")
print(f"📊 Total de registros: {len(df_consolidado)}")
print(f"📊 Registros por status (após ajustes):")
for status in status_list + ['OVERDUE']:
    count = len(df_consolidado[df_consolidado['status'] == status])
    if count > 0:
        print(f"  - {status}: {count} registros")
