import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from urllib.parse import quote

# CONFIGURAÇÃO
GUILD_NAME = "Digit One"
PROJECT_ID = "tibia-analytics"
DATASET_ID = "tibia_data"
TABLE_ID = "guild_members_history"

def fetch_guild_data(guild_name):
    # 1. URL: Usa %20 para espaços (igual ao seu exemplo curl)
    safe_name = quote(guild_name)
    url = f"https://api.tibiadata.com/v4/guild/{safe_name}"
    
    # 2. HEADERS: Importante para simular um navegador/curl e evitar bloqueio
    headers = {
        'accept': 'application/json',
        'User-Agent': 'TibiaAnalyticsBot/1.0' 
    }
    
    print(f"Buscando dados da guild: {guild_name} (URL: {url})...")
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Erro na API: {response.status_code}")
        return []

    data = response.json()
    
    # Validação de Segurança
    if 'guild' not in data or 'members' not in data['guild']:
        print("Guild não encontrada ou chave 'members' ausente.")
        return []

    members_data = []
    extraction_date = datetime.now().date()
    
    # A LISTA DE MEMBROS É PLANA (DIRETA)
    # Não existe mais agrupamento por Rank, o rank é um atributo de cada membro.
    raw_members_list = data['guild']['members']
    
    print(f"Processando {len(raw_members_list)} membros encontrados...")

    for member in raw_members_list:
        members_data.append({
            "guild_name": data['guild']['name'],
            "rank": member.get('rank', 'Unknown'),         # Pega direto do membro
            "character_name": member.get('name', 'Unknown'), # Pega direto do membro
            "vocation": member.get('vocation', 'None'),
            "level": member.get('level', 0),
            "joined_date": member.get('joined', None),
            "status": member.get('status', 'offline'),
            "extraction_date": extraction_date
        })
            
    return members_data

def save_to_bigquery(data):
    if not data:
        print("Nenhum dado para salvar.")
        return

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    df = pd.DataFrame(data)
    
    # Tratamento de Datas
    df['joined_date'] = pd.to_datetime(df['joined_date'], errors='coerce').dt.date
    df['extraction_date'] = pd.to_datetime(df['extraction_date']).dt.date

    # Configuração do Job (Append)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("guild_name", "STRING"),
            bigquery.SchemaField("rank", "STRING"),
            bigquery.SchemaField("character_name", "STRING"),
            bigquery.SchemaField("vocation", "STRING"),
            bigquery.SchemaField("level", "INTEGER"),
            bigquery.SchemaField("joined_date", "DATE"),
            bigquery.SchemaField("status", "STRING"),
            bigquery.SchemaField("extraction_date", "DATE"),
        ]
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Sucesso! {len(df)} membros carregados no BigQuery.")

# EXECUÇÃO
if __name__ == "__main__":
    members = fetch_guild_data(GUILD_NAME)
    save_to_bigquery(members)
