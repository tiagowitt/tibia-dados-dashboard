import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import glob
from datetime import datetime

# --- CONFIGURAÇÕES ---
# Nome do projeto no Google Cloud
PROJECT_ID = 'tibia-analytics'
# Nome do Dataset criado no BigQuery
DATASET_ID = 'tibia_data'
# Nome da Tabela a ser criada/atualizada
TABLE_ID = 'ranking_history'
# Caminho da pasta onde estão os arquivos parquet no seu repositório
PARQUET_FOLDER = 'dados/ranking/Collabra' 

def load_data_to_bigquery():
    # --- TRAVA DE SEGURANÇA CONTRA REPROCESSAMENTO DESNECESSÁRIO ---
    # Se o arquivo do dia de hoje não existir localmente, significa que
    # o workflow anterior não gerou novos dados (bloqueado pela trava de redundância).
    data_hoje = datetime.now().date()
    arquivo_hoje = f"ranking_Collabra_{data_hoje}.parquet"
    caminho_arquivo_hoje = os.path.join(PARQUET_FOLDER, arquivo_hoje)

    if not os.path.exists(caminho_arquivo_hoje):
        print(f"[AVISO] O arquivo de hoje ({arquivo_hoje}) não foi gerado nesta rodada.")
        print("A base do BigQuery já está atualizada. Abortando processo de carga para poupar processamento.")
        return
    # -----------------------------------------------------------------

    # 1. Autenticação
    # Se estiver usando o 'google-github-actions/auth', o client pega automático:
    client = bigquery.Client(project=PROJECT_ID)

    # 2. Ler os arquivos Parquet
    # Procura todos os arquivos .parquet na pasta especificada
    files = glob.glob(f"{PARQUET_FOLDER}/*.parquet")
    
    if not files:
        print("Nenhum arquivo parquet encontrado.")
        return

    print(f"Encontrados {len(files)} arquivos. Iniciando leitura...")
    
    # Lê todos os arquivos e junta em um único DataFrame
    df_list = []
    for file in files:
        try:
            # Lê o arquivo parquet
            df_temp = pd.read_parquet(file)
            df_list.append(df_temp)
        except Exception as e:
            print(f"Erro ao ler o arquivo {file}: {e}")

    if not df_list:
        print("Nenhum dado válido para processar.")
        return

    full_df = pd.concat(df_list, ignore_index=True)
    
    print(f"Total de linhas para processar: {len(full_df)}")

    # 4. Configuração do Job de Carga
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    
    job_config = bigquery.LoadJobConfig(
        # WRITE_TRUNCATE: Substitui a tabela inteira com base no histórico completo do Git.
        write_disposition="WRITE_TRUNCATE", 
        
        # Detecta o esquema automaticamente baseado no DataFrame
        autodetect=True,
    )

    # 5. Enviar para o BigQuery
    try:
        job = client.load_table_from_dataframe(
            full_df, table_ref, job_config=job_config
        )
        job.result()  # Espera o job terminar
        
        print(f"Sucesso! {job.output_rows} linhas carregadas na tabela {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}.")
        
    except Exception as e:
        print(f"Erro ao enviar para o BigQuery: {e}")

if __name__ == "__main__":
    load_data_to_bigquery()
