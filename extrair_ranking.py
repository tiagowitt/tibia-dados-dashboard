import requests
import pandas as pd
import time
from datetime import datetime
import os

# --- CONFIGURAÇÕES ---
RANK_CATEGORY = "experience"
RANK_PROFESSION = "all"
ROOT_FOLDER = "dados"
FOLDER_RANKING = "ranking"
FOLDER_KILLSTATS = "kill_statistics"

# --- FUNÇÃO BLINDADA COM RETRY ---
def get_api_data(url, max_retries=5):
    """
    Tenta buscar dados. Se der erro 503/429 (bloqueio), espera e tenta de novo.
    """
    # Cabeçalho para simular um navegador e evitar bloqueios simples
    headers = {
        'User-Agent': 'TibiaAnalyticsBot/1.0 (Contact: admin@seuemail.com)',
        'accept': 'application/json'
    }

    # Tentativas
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=25)
            
            # Sucesso
            if response.status_code == 200:
                return response.json()
            
            # Erros de "Calma lá" (Rate Limit ou Servidor Ocupado)
            elif response.status_code in [429, 500, 502, 503, 504]:
                wait_time = (attempt + 1) * 3  # Espera progressiva: 3s, 6s, 9s...
                print(f"   [Aviso] Erro {response.status_code}. Tentativa {attempt+1}/{max_retries}. Esperando {wait_time}s...")
                time.sleep(wait_time)
                continue # Tenta de novo
            
            # Erro definitivo (ex: 404 Not Found)
            else:
                print(f"   [Erro Fatal] Status {response.status_code} na URL: {url}")
                return None

        except Exception as e:
            print(f"   [Erro Conexão] {e}. Tentando novamente...")
            time.sleep(3)
    
    print("   [Falha] Esgotadas todas as tentativas.")
    return None

# --- OBTER LISTA DE MUNDOS ---
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

# --- HIGHSCORES ---
def process_highscores(world):
    print(f"   > [Ranking XP] Iniciando: {world}")
    all_players = []
    page = 1
    
    while True:
        url = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/{page}"
        data = get_api_data(url)
        
        if not data or 'highscores' not in data:
            break
            
        page_list = data['highscores'].get('highscore_list', [])
        
        if not page_list:
            break
        
        all_players.extend(page_list)
        
        # TRAVA DE SEGURANÇA: Top 1000
        if page >= 20:
            break
            
        page += 1
        # Aumentei levemente o tempo entre páginas para evitar bloqueio sequencial
        time.sleep(1) 

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
        print(f"   > [Ranking XP] ATENÇÃO: Nenhum dado coletado.")

# --- KILL STATISTICS ---
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

    worlds_to_process = get_active_worlds()
    
    if not worlds_to_process:
        print("AVISO CRÍTICO: Não foi possível obter lista de mundos. Usando fallback.")
        worlds_to_process = ["Antica", "Venebra"] 

    for i, world_name in enumerate(worlds_to_process):
        print(f"\n[{i+1}/{len(worlds_to_process)}] Processando: {world_name}")
        
        process_highscores(world_name)
        process_kill_statistics(world_name)
        
        # Pausa maior entre mundos (respira fundo antes do próximo)
        time.sleep(2) 

    print("\n--- FIM DA ROTINA GLOBAL ---")

if __name__ == "__main__":
    main()
