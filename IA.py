import json
import pandas as pd
from openai import OpenAI
import os
from datetime import datetime
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials

deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")
client = OpenAI(api_key=deepseek_api_key, base_url="https://api.deepseek.com")

# URL da planilha Google Sheets exportada como CSV
sheet_id = "1VUF9gi6WIuWaT8gK_P9pIocOwByuwPF49owrIJZ0g9M"
sheet_csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"

SHEET_ID2 = "1tDumkItOssgKRvBuI1dEpRoiyoGvBd1-MqixK-zzsKU"  # ID da planilha de destino

# Ler a planilha
df = pd.read_csv(sheet_csv_url)

# Limpar valores monetários
def limpar_valores(col):
    return (
        col.astype(str)
           .str.replace(r"[^\d,.-]", "", regex=True)
           .str.replace(".", "", regex=False)
           .str.replace(",", ".", regex=False)
           .pipe(pd.to_numeric, errors="coerce")
    )

df['unpaid'] = limpar_valores(df['unpaid'])
df['paid'] = limpar_valores(df['paid'])
df['categoriesRatio.value'] = limpar_valores(df['categoriesRatio.value'])

# Converter coluna de data
# Conversão de datas com parsing manual para evitar problemas de formatação
def parse_data_segura(coluna):
    datas = pd.to_datetime(
        coluna.apply(lambda x: '-'.join(x.split('-')[:3]) if isinstance(x, str) and '-' in x else None),
        format='%Y-%m-%d',
        errors='coerce'
    )
    return datas

df['lastAcquittanceDate'] = parse_data_segura(df['lastAcquittanceDate'])
df['dueDate'] = parse_data_segura(df['dueDate'])

# Filtrar apenas registros do ano corrente
ano_corrente = datetime.today().year
df = df[df['lastAcquittanceDate'].dt.year == ano_corrente]

# Criar colunas auxiliares
df['AnoMes'] = df['lastAcquittanceDate'].dt.to_period('M')
df['Trimestre'] = df['lastAcquittanceDate'].dt.to_period('Q')
df['AnoMes_Caixa'] = df['lastAcquittanceDate'].dt.to_period('M')
df['Trimestre_Caixa'] = df['lastAcquittanceDate'].dt.to_period('Q')

# Resumo trimestral: valores pagos e pendentes por tipo
resumo_trimestral = df.groupby(['Trimestre', 'tipo'])[['categoriesRatio.value', 'unpaid']].sum().unstack(fill_value=0)

# Variação mensal por categoria
resumo_mensal_categoria = df.groupby(['AnoMes', 'categoriesRatio.category'])['categoriesRatio.value'].sum().unstack(fill_value=0)
variacao_mensal_pct = resumo_mensal_categoria.pct_change().fillna(0)
categorias_com_alta = (variacao_mensal_pct > 0.3).apply(lambda row: row[row > 0.3].to_dict(), axis=1).to_dict()

# Valores totais
total_recebido = df[df['tipo'] == 'Receita']['categoriesRatio.value'].sum()
total_pago = df[df['tipo'] == 'Despesa']['categoriesRatio.value'].sum()
total_pendente_despesa = df[
    (df['tipo'] == 'Despesa') & (df['status'] == 'OVERDUE')
]['categoriesRatio.value'].sum()
total_pendente_receita = df[
    (df['tipo'] == 'Receita') & (df['status'] == 'OVERDUE')
]['categoriesRatio.value'].sum()
saldo_liquido = total_recebido - total_pago
top_categorias = df['categoriesRatio.category'].value_counts().head(3).to_dict()

# ================= CÁLCULOS COMPLEMENTARES ===================

# Filtrar transações realizadas
hoje = pd.to_datetime(datetime.today().date())
df_realizadas = df[df['lastAcquittanceDate'] <= hoje].copy()

df_realizadas['valor_ajustado'] = df_realizadas.apply(
    lambda row: abs(row['categoriesRatio.value']) if row['tipo'] == 'Receita' else -abs(row['categoriesRatio.value']),
    axis=1
)

# Fluxo de Caixa
fluxo_caixa = df_realizadas.groupby('AnoMes_Caixa')['valor_ajustado'].sum().reset_index()
fluxo_caixa['saldo_acumulado'] = fluxo_caixa['valor_ajustado'].cumsum()

# Receitas e despesas por mês
df_receitas = df_realizadas[df_realizadas['tipo'].str.lower() == 'Receita']
df_despesas = df_realizadas[df_realizadas['tipo'].str.lower() == 'Despesa']

receitas_mensais = df_receitas.groupby('AnoMes')['categoriesRatio.value'].sum().reset_index()
despesas_mensais = df_despesas.groupby('AnoMes')['categoriesRatio.value'].sum().reset_index()

# Rentabilidade
rentabilidade = pd.merge(
    receitas_mensais,
    despesas_mensais,
    on='AnoMes',
    how='outer',
    suffixes=('_receita', '_despesa')
).fillna(0)

rentabilidade['lucro'] = rentabilidade['categoriesRatio.value_receita'] - rentabilidade['categoriesRatio.value_despesa']
rentabilidade['margem_lucro'] = rentabilidade['lucro'] / rentabilidade['categoriesRatio.value_receita'].replace(0, pd.NA)

# Pendências e vencidos
df_pendentes = df[(df['unpaid'] > 0) & (df['dueDate'] <= hoje) & (df['status'] == 'OVERDUE')]
pendentes_por_tipo = df_pendentes.groupby('tipo')['unpaid'].sum().to_dict()

# Inadimplência
total_vencido = df_pendentes[df_pendentes['tipo'] == 'Receita']['categoriesRatio.value'].sum()
inadimplencia = total_vencido / total_recebido if total_recebido else 0

# Prompt detalhado
prompt = f"""
Você é um analista financeiro sênior. Recebi um extrato financeiro com as seguintes informações agregadas:

1. Visão geral:
- Total recebido (entradas): R$ {total_recebido:,.2f}
- Total pago (saídas): R$ {total_pago:,.2f}
- Receita pendente (Receita): R$ {total_pendente_receita:,.2f}
- Despesa pendente (Despesa): R$ {total_pendente_despesa:,.2f}
- Saldo líquido (entradas - saídas): R$ {saldo_liquido:,.2f}

2. Top 3 categorias mais frequentes: {top_categorias}

3. Resumo trimestral (valores pagos e pendentes por tipo de transação):
{resumo_trimestral.to_string()}

4. Categorias com aumentos mensais significativos (acima de 30% de um mês para o outro):
{categorias_com_alta}

5. Fluxo de caixa mensal, me de também o mês a mês:
{fluxo_caixa.to_string(index=False)}

6. Rentabilidade mensal (lucro e margem de lucro):
{rentabilidade[['AnoMes', 'lucro', 'margem_lucro']].to_string(index=False)}

7. Pendências vencidas por tipo:
{pendentes_por_tipo}

8. Inadimplência (proporção de valores vencidos sobre receitas realizadas): {inadimplencia:.2%}

9. faça um resumo executivo.

Por favor, me forneça:
- Insights sobre a saúde financeira e tendências.
- Sinais de alerta (pendências, desequilíbrios).
- Oportunidades de otimização (redução de custos ou melhoria na previsibilidade).
- Recomendações práticas com base no histórico recente (por trimestre e por categoria).

Seja objetivo, claro e direto.
"""

# Chamar a IA
response = client.chat.completions.create(
    model="deepseek-chat",
    messages=[
        {"role": "system", "content": "Você é um analista financeiro experiente."},
        {"role": "user", "content": prompt}
    ],
    temperature=1.0
)

# Mostrar insights
#print("=== INSIGHTS GERADOS ===")
#print(response.choices[0].message.content)

# Credenciais do serviço
json_secret = os.getenv("GDRIVE_SERVICE_ACCOUNT")
creds_dict = json.loads(json_secret)
creds = Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])

# Acessar a planilha
gc = gspread.authorize(creds)
spreadsheet = gc.open_by_key("1tDumkItOssgKRvBuI1dEpRoiyoGvBd1-MqixK-zzsKU")
worksheet = spreadsheet.get_worksheet(0)  # primeira aba

# Limpar todo o conteúdo anterior
worksheet.clear()

# Processar conteúdo da IA
conteudo_ia = response.choices[0].message.content

blocos = conteudo_ia.split("####")
dados = []

for bloco in blocos:
    bloco = bloco.strip()
    if not bloco:
        continue
    if bloco.startswith("**"):
        titulo = bloco.split("**")[1]
        resultado = bloco.split("**", 2)[-1].strip()
        dados.append([titulo, resultado])

# Escrever na planilha
worksheet.update(dados, "A1")
