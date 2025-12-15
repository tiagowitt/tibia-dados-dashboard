import requests
import pandas as pd
import time
from datetime import datetime
import os

# --- CONFIGURAÇÕES ---
# Não precisamos mais da lista manual de mundos!
# O script vai buscar isso sozinho.

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
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"   [Erro API] {e}")
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

# --- LÓGICA DE EXTRAÇÃO: HIGHSCORES ---
def process_highscores(world):
    print(f"   > [Ranking XP] Iniciando...")
    all_players = []
    
    # Pega página 1
    url_p1 = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/1"
    data = get_api_data(url_p1)
    
    if not data or 'highscores' not in data:
        print(f"   > [Ranking XP] Falha ao obter dados. Pulando.")
        return

    page_1_list = data['highscores'].get('highscore_list', [])
    all_players.extend(page_1_list)
    
    try:
        total_pages = data.get('highscores', {}).get('highscores_page', {}).get('total_pages', 1)
    except:
        total_pages = 1
        
    # Limite de segurança: Se tiver mais de 20 páginas (Top 1000), corta.
    # Isso evita ficar preso eternamente em mundos muito populosos se algo bugar.
    if total_pages > 20:
        total_pages = 20

    # Loop páginas restantes
    if total_pages > 1:
        for page in range(2, total_pages + 1):
            time.sleep(0.5) # Pausa leve
            url_page = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/{page}"
            p_data = get_api_data(url_page)
            
            if p_data and 'highscores' in p_data:
                all_players.extend(p_data['highscores'].get('highscore_list', []))

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
        print(f"   > [Ranking XP] Salvo: {len(df)} linhas.")

# --- LÓGICA DE EXTRAÇÃO: KILL STATISTICS ---
def process_kill_statistics(world):
    print(f"   > [Kill Stats] Iniciando...")
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
        print(f"   > [Kill Stats] Salvo: {len(df)} linhas.")

# --- ORQUESTRADOR ---
def main():
    if not os.path.exists(ROOT_FOLDER):
        os.makedirs(ROOT_FOLDER)

    # 1. Busca a lista dinâmica de todos os mundos
    worlds_to_process = get_active_worlds()
    
    # 2. Loop principal
    for i, world_name in enumerate(worlds_to_process):
        print(f"\n[{i+1}/{len(worlds_to_process)}] Processando: {world_name}")
        
        process_highscores(world_name)
        time.sleep(1)
        process_kill_statistics(world_name)
        
        # Pausa entre mundos (importante para escala global)
        time.sleep(1.5) 

    print("\n--- FIM DA ROTINA GLOBAL ---")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
