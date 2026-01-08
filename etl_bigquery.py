import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import glob

# --- CONFIGURAÇÕES ---
# Nome do projeto no Google Cloud
PROJECT_ID = 'seu-projeto-id'
# Nome do Dataset criado no BigQuery
DATASET_ID = 'tibia_data'
# Nome da Tabela a ser criada/atualizada
TABLE_ID = 'ranking_history'
# Caminho da pasta onde estão os arquivos parquet no seu repositório
PARQUET_FOLDER = 'data/' 

def load_data_to_bigquery():
    # 1. Autenticação
    # O GitHub Actions vai injetar a chave via variável de ambiente, 
    # ou podemos usar a autenticação padrão se configurado no passo anterior do YAML.
    # Aqui assumimos que o ambiente já está autenticado ou buscamos a credencial.
    
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
    
    # 3. Tratamento Básico (Opcional)
    # Garante que tipos de dados complexos não quebrem o envio (ex: datas)
    # full_df['date'] = pd.to_datetime(full_df['date'])

    print(f"Total de linhas para processar: {len(full_df)}")

    # 4. Configuração do Job de Carga
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    
    job_config = bigquery.LoadJobConfig(
        # WRITE_TRUNCATE: Substitui a tabela inteira. 
        # Use WRITE_APPEND se quiser apenas adicionar novos dados e tem certeza que não duplicará.
        write_disposition="WRITE_TRUNCATE", 
        
        # Detecta o esquema automaticamente baseado no DataFrame
        autodetect=True,
        
        # Opção para particionar a tabela (Opcional, mas recomendado para performance)
        # time_partitioning=bigquery.TimePartitioning(
        #     type_=bigquery.TimePartitioningType.DAY,
        #     field="data_extracao" # Nome da coluna de data no seu parquet
        # )
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
