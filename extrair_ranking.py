import requests
import pandas as pd
import time
from datetime import datetime
import os

# --- CONFIGURAÇÕES ---
# Não usamos mais lista fixa. O script buscará todos os mundos online.

# Configurações do Ranking
RANK_CATEGORY = "experience"
RANK_PROFESSION = "all"

# Estrutura de Pastas
ROOT_FOLDER = "dados"
FOLDER_RANKING = "ranking"
FOLDER_KILLSTATS = "kill_statistics"

# --- FUNÇÕES AUXILIARES DE REQUISIÇÃO ---

def get_api_data(url):
    """Função genérica com retries simples"""
    try:
        response = requests.get(url, timeout=20) # Aumentei um pouco o timeout para segurança
        if response.status_code != 200:
            print(f"   [API Status {response.status_code}] URL: {url}")
            return None
        return response.json()
    except Exception as e:
        print(f"   [Erro Conexão] {e}")
        return None

# --- NOVA FUNÇÃO: OBTER LISTA DE MUNDOS ---
def get_active_worlds():
    print("--- Buscando lista de mundos ativos... ---")
    url = "https://api.tibiadata.com/v4/worlds"
    data = get_api_data(url)
    
    world_list = []
    if data and 'worlds' in data and 'regular_worlds' in data['worlds']:
        for world in data['worlds']['regular_worlds']:
            world_list.append(world['name'])
            
    print(f"Total de mundos encontrados: {len(world_list)}")
    return world_list

# --- LÓGICA DE EXTRAÇÃO: HIGHSCORES (Com a correção do Rank 1000) ---
def process_highscores(world):
    print(f"   > [Ranking XP] Iniciando: {world}")
    all_players = []
    page = 1
    
    # Loop Infinito com trava
    while True:
        url = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/{page}"
        data = get_api_data(url)
        
        if not data or 'highscores' not in data:
            break
            
        page_list = data['highscores'].get('highscore_list', [])
        
        # Se a lista vier vazia, acabou
        if not page_list:
            break
        
        all_players.extend(page_list)
        
        # TRAVA DE SEGURANÇA: Top 1000 (Página 20)
        if page >= 20:
            break
            
        page += 1
        time.sleep(0.5) 

    if all_players:
        df = pd.DataFrame(all_players)
        df['extraction_date'] = datetime.now().date()
        df['world'] = world
        df['type'] = 'highscores'
        
        target_folder = os.path.join(ROOT_FOLDER, FOLDER_RANKING, world)
        os.makedirs(target_folder, exist_ok=True)
        
        filename = f"highscores_{world}_{datetime.now().date()}.parquet"
        filepath = os.path.join(target_folder, filename)
        
        df.to_parquet(filepath, index=False)
        print(f"   > [Ranking XP] Finalizado: {len(df)} linhas.")
    else:
        print(f"   > [Ranking XP] Falha ou dados vazios.")

# --- LÓGICA DE EXTRAÇÃO: KILL STATISTICS ---
def process_kill_statistics(world):
    url = f"https://api.tibiadata.com/v4/killstatistics/{world}"
    data = get_api_data(url)
    
    if not data or 'killstatistics' not in data:
        return

    entries = data['killstatistics'].get('entries', [])
    
    if entries:
        df = pd.DataFrame(entries)
        df['extraction_date'] = datetime.now().date()
        df['world'] = world
        df['type'] = 'kill_statistics'

        target_folder = os.path.join(ROOT_FOLDER, FOLDER_KILLSTATS, world)
        os.makedirs(target_folder, exist_ok=True)
        
        filename = f"killstatistics_{world}_{datetime.now().date()}.parquet"
        filepath = os.path.join(target_folder, filename)
        
        df.to_parquet(filepath, index=False)
        print(f"   > [Kill Stats] Salvo: {len(df)} registros.")

# --- ORQUESTRADOR ---
def main():
    if not os.path.exists(ROOT_FOLDER):
        os.makedirs(ROOT_FOLDER)

    # 1. Pega a lista dinâmica
    worlds_to_process = get_active_worlds()
    
    # Backup caso a API de worlds falhe
    if not worlds_to_process:
        print("AVISO CRÍTICO: Não foi possível obter lista de mundos. Usando fallback.")
        worlds_to_process = ["Antica", "Venebra"] 

    # 2. Itera sobre todos
    for i, world_name in enumerate(worlds_to_process):
        print(f"\n[{i+1}/{len(worlds_to_process)}] Processando: {world_name}")
        
        process_highscores(world_name)
        process_kill_statistics(world_name)
        
        # Pausa leve entre mundos para não sobrecarregar
        time.sleep(1) 

    print("\n--- FIM DA ROTINA GLOBAL ---")

if __name__ == "__main__":
    main()
