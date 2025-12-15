import requests
import pandas as pd
import time
from datetime import datetime
import os

# --- CONFIGURAÇÕES ---
WORLDS = [
    "Collabra", "Issobra", "Venebra", "Descubra",
    "Etebra", "Luzibra", "Ustebra", "Yubra"
]

# Configurações do Ranking
RANK_CATEGORY = "experience"
RANK_PROFESSION = "all"

# Pasta raiz principal
ROOT_FOLDER = "dados"
FOLDER_RANKING = "ranking"
FOLDER_KILLSTATS = "kill_statistics"

# --- FUNÇÕES AUXILIARES DE REQUISIÇÃO ---

def get_api_data(url):
    """Função genérica para chamar a API com tratamento de erro"""
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Erro na requisição ({url}): {e}")
        return None

# --- LÓGICA DE EXTRAÇÃO: HIGHSCORES (RANKING) ---

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
        
    # Loop páginas restantes
    if total_pages > 1:
        for page in range(2, total_pages + 1):
            time.sleep(0.5) # Rate limiting
            url_page = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/{page}"
            p_data = get_api_data(url_page)
            
            if p_data and 'highscores' in p_data:
                all_players.extend(p_data['highscores'].get('highscore_list', []))

    # Salvar
    if all_players:
        df = pd.DataFrame(all_players)
        df['extraction_date'] = datetime.now().date()
        df['world'] = world
        df['type'] = 'highscores'
        
        # --- DEFINIÇÃO DA PASTA DE RANKING ---
        # Caminho final: dados/ranking/Collabra
        target_folder = os.path.join(ROOT_FOLDER, FOLDER_RANKING, world)
        os.makedirs(target_folder, exist_ok=True)
        
        filename = f"highscores_{world}_{datetime.now().date()}.parquet"
        filepath = os.path.join(target_folder, filename)
        
        df.to_parquet(filepath, index=False)
        print(f"   > [Ranking XP] Salvo em: {filepath}")

# --- LÓGICA DE EXTRAÇÃO: KILL STATISTICS ---

def process_kill_statistics(world):
    print(f"   > [Kill Stats] Iniciando...")
    
    url = f"https://api.tibiadata.com/v4/killstatistics/{world}"
    data = get_api_data(url)
    
    if not data or 'killstatistics' not in data:
        print(f"   > [Kill Stats] Falha ao obter dados.")
        return

    entries = data['killstatistics'].get('entries', [])
    
    if entries:
        df = pd.DataFrame(entries)
        df['extraction_date'] = datetime.now().date()
        df['world'] = world
        df['type'] = 'kill_statistics'

        # --- DEFINIÇÃO DA PASTA DE KILL STATS ---
        # Caminho final: dados/kill_statistics/Collabra
        target_folder = os.path.join(ROOT_FOLDER, FOLDER_KILLSTATS, world)
        os.makedirs(target_folder, exist_ok=True)
        
        filename = f"killstatistics_{world}_{datetime.now().date()}.parquet"
        filepath = os.path.join(target_folder, filename)
        
        df.to_parquet(filepath, index=False)
        print(f"   > [Kill Stats] Salvo em: {filepath}")
    else:
        print(f"   > [Kill Stats] Nenhum registro encontrado hoje.")

# --- ORQUESTRADOR PRINCIPAL ---

def main():
    # Garante que a raiz existe
    if not os.path.exists(ROOT_FOLDER):
        os.makedirs(ROOT_FOLDER)

    print(f"--- INICIANDO ROTINA COMPLETA ({datetime.now()}) ---")

    for world_name in WORLDS:
        print(f"\nProcessing World: {world_name}")
        
        # 1. Processa Ranking
        process_highscores(world_name)
        
        # Pausa entre tipos de requisição
        time.sleep(1)
        
        # 2. Processa Kill Statistics
        process_kill_statistics(world_name)
        
        # Pausa entre mundos
        time.sleep(1) 

    print("\n--- FIM DA ROTINA ---")

if __name__ == "__main__":
    main()
