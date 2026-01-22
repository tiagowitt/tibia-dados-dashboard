import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import os

# CONFIGURAÇÃO
GUILD_NAME = "Digit One"  # <--- COLOQUE O NOME DA GUILD AQUI (Exato, com espaços)
PROJECT_ID = "tibia-analytics"
DATASET_ID = "tibia_data"
TABLE_ID = "guild_members_history"

def fetch_guild_data(guild_name):
    # URL da API (TibiaData v4)
    url = f"https://api.tibiadata.com/v4/guild/{guild_name.replace(' ', '+')}"
    
    print(f"Buscando dados da guild: {guild_name}...")
    response = requests.get(url)
    
    if response.status_code != 200:
        print(f"Erro na API: {response.status_code}")
        return []

    data = response.json()
    
    # Verifica se a guild existe
    if 'guild' not in data or 'members' not in data['guild']:
        print("Guild não encontrada ou sem membros.")
        return []

    members_data = []
    extraction_date = datetime.now().date()

    # O JSON vem agrupado por Rank (Líder, Vice, Membro, etc)
    for rank_group in data['guild']['members']:
        rank_name = rank_group['rank_title']
        
        for char in rank_group['characters']:
            members_data.append({
                "guild_name": data['guild']['name'],
                "rank": rank_name,
                "character_name": char['name'],
                "vocation": char['vocation'],
                "level": char['level'],
                "joined_date": char['joined'],
                "status": char['status'], # Online/Offline
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
    
    # Converte colunas de data para o formato correto do BigQuery
    df['joined_date'] = pd.to_datetime(df['joined_date']).dt.date
    df['extraction_date'] = pd.to_datetime(df['extraction_date']).dt.date

    # Configuração do Job (Append para criar histórico)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", # Mantém o histórico dia a dia
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
