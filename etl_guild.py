import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from urllib.parse import quote

# CONFIGURAÇÃO: Agora é uma lista com as duas guilds
GUILDS_TO_TRACK = [
    "Digit One", 
    "Digit One Academy"
]

PROJECT_ID = "tibia-analytics"
DATASET_ID = "tibia_data"
TABLE_ID = "guild_members_history"

def fetch_guild_data(guild_name):
    # 1. URL: Usa a função quote para tratar espaços (ex: Digit%20One)
    safe_name = quote(guild_name)
    url = f"https://api.tibiadata.com/v4/guild/{safe_name}"
    
    # 2. HEADERS: Simula um navegador para evitar bloqueio
    headers = {
        'accept': 'application/json',
        'User-Agent': 'TibiaAnalyticsBot/1.0' 
    }
    
    print(f"Buscando dados da guild: {guild_name}...")
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Erro na API ({guild_name}): {response.status_code}")
        return []

    data = response.json()
    
    # Validação de Segurança
    if 'guild' not in data or 'members' not in data['guild']:
        print(f"Aviso: Guild '{guild_name}' não encontrada ou sem membros.")
        return []

    members_data = []
    extraction_date = datetime.now().date()
    
    # A LISTA DE MEMBROS É PLANA (DIRETA)
    raw_members_list = data['guild']['members']
    
    print(f" > {guild_name}: {len(raw_members_list)} membros encontrados.")

    for member in raw_members_list:
        members_data.append({
            "guild_name": data['guild']['name'], # O nome oficial que vem da API
            "rank": member.get('rank', 'Unknown'),
            "character_name": member.get('name', 'Unknown'),
            "vocation": member.get('vocation', 'None'),
            "level": member.get('level', 0),
            "joined_date": member.get('joined', None),
            "status": member.get('status', 'offline'),
            "extraction_date": extraction_date
        })
            
    return members_data

def save_to_bigquery(data):
    if not data:
        print("Nenhum dado para salvar no total.")
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

    # Envia tudo de uma vez
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Sucesso Total! {len(df)} linhas carregadas no BigQuery (Somando todas as guilds).")

# EXECUÇÃO PRINCIPAL
if __name__ == "__main__":
    all_guilds_data = []
    
    # Loop: Passa por cada guild da lista
    for guild in GUILDS_TO_TRACK:
        guild_members = fetch_guild_data(guild)
        all_guilds_data.extend(guild_members) # Junta na lista principal
        
    # Salva tudo de uma vez só no final
    save_to_bigquery(all_guilds_data)
