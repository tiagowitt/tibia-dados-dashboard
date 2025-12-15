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

CATEGORY = "experience"
PROFESSION = "all"

# Pasta raiz
OUTPUT_FOLDER = "dados"

def get_highscore_data(world, category, profession, page_num):
    """Busca uma página específica da API"""
    url = f"https://api.tibiadata.com/v4/highscores/{world}/{category}/{profession}/{page_num}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"[{world}] Erro pag {page_num}: {e}")
        return None

def process_world(world):
    """Lógica de extração para um único mundo"""
    print(f"--- Iniciando extração: {world} ---")
    all_players = []
    
    # 1. Pega página 1
    data = get_highscore_data(world, CATEGORY, PROFESSION, 1)
    if not data or 'highscores' not in data:
        print(f"[{world}] Falha ao obter dados iniciais. Pulando...")
        return

    page_1_list = data['highscores'].get('highscore_list', [])
    all_players.extend(page_1_list)
    
    try:
        total_pages = data.get('highscores', {}).get('highscores_page', {}).get('total_pages', 1)
    except:
        total_pages = 1
        
    print(f"[{world}] Total páginas: {total_pages}")

    # 2. Loop páginas restantes
    if total_pages > 1:
        for page in range(2, total_pages + 1):
            time.sleep(0.5) # Pausa amigável para a API
            p_data = get_highscore_data(world, CATEGORY, PROFESSION, page)
            if p_data and 'highscores' in p_data:
                all_players.extend(p_data['highscores'].get('highscore_list', []))
            else:
                print(f"[{world}] Erro na página {page}")

    # 3. Salvar Arquivo na Pasta do Mundo
    if all_players:
        df = pd.DataFrame(all_players)
        
        df['extraction_date'] = datetime.now().date()
        df['world'] = world
        df['category'] = CATEGORY
        
        # --- MUDANÇA AQUI: Cria subpasta para o mundo ---
        world_folder = os.path.join(OUTPUT_FOLDER, world)
        os.makedirs(world_folder, exist_ok=True)
        
        filename = f"highscores_{world}_{datetime.now().date()}.parquet"
        filepath = os.path.join(world_folder, filename)
        
        df.to_parquet(filepath, index=False)
        print(f"[{world}] Salvo em: {filepath} ({len(df)} registros)")
    else:
        print(f"[{world}] Nenhum dado encontrado.")

def main():
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    for world_name in WORLDS:
        process_world(world_name)
        time.sleep(1) 

if __name__ == "__main__":
    main()
