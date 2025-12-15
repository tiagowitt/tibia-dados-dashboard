import requests
import pandas as pd
import time
from datetime import datetime
import os

# --- CONFIGURAÇÕES ---
WORLD = "Collabra"       # Mude para o mundo que quiser
CATEGORY = "experience"
PROFESSION = "all"

# Caminho relativo simples para o GitHub Actions
OUTPUT_FOLDER = "dados"

def get_highscore_data(world, category, profession, page_num):
    url = f"https://api.tibiadata.com/v4/highscores/{world}/{category}/{profession}/{page_num}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Erro pag {page_num}: {e}")
        return None

def main():
    print(f"--- Iniciando extração: {WORLD} ---")
    all_players = []
    
    # Pega página 1
    data = get_highscore_data(WORLD, CATEGORY, PROFESSION, 1)
    if not data or 'highscores' not in data:
        return

    page_1_list = data['highscores'].get('highscore_list', [])
    all_players.extend(page_1_list)
    
    try:
        total_pages = data.get('highscores', {}).get('highscores_page', {}).get('total_pages', 1)
    except:
        total_pages = 1
        
    print(f"Total paginas: {total_pages}")

    # Loop paginas
    if total_pages > 1:
        for page in range(2, total_pages + 1):
            time.sleep(0.5) # Respeitando a API
            p_data = get_highscore_data(WORLD, CATEGORY, PROFESSION, page)
            if p_data and 'highscores' in p_data:
                all_players.extend(p_data['highscores'].get('highscore_list', []))

    # Salvar
    if all_players:
        df = pd.DataFrame(all_players)
        df['extraction_date'] = datetime.now().date()
        df['world'] = WORLD
        
        if not os.path.exists(OUTPUT_FOLDER):
            os.makedirs(OUTPUT_FOLDER)

        filename = f"highscores_{WORLD}_{datetime.now().date()}.parquet"
        filepath = os.path.join(OUTPUT_FOLDER, filename)
        
        df.to_parquet(filepath, index=False)
        print(f"Salvo: {filepath} com {len(df)} registros")

if __name__ == "__main__":
    main()
