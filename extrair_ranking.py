import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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

# --- CONFIGURAÇÃO AVANÇADA DE SESSÃO ---
# Cria uma conexão persistente que sabe lidar com erros 503 automaticamente
def create_session():
    session = requests.Session()
    # Configura a estratégia de insistência (Retry)
    retry = Retry(
        total=5,  # Tenta até 5 vezes se der erro
        backoff_factor=1,  # Espera progressiva: 1s, 2s, 4s, 8s, 16s
        status_forcelist=[429, 500, 502, 503, 504], # Lista de erros "perdoáveis"
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

# Inicializa a sessão global
http = create_session()

# --- FUNÇÃO DE REQUISIÇÃO ---
def get_api_data(url):
    """Busca dados usando a sessão persistente e headers corretos."""
    headers = {
        'User-Agent': 'TibiaAnalyticsBot/2.0 (GitHub Actions)',
        'accept': 'application/json'
    }

    try:
        # Timeout de 30s para evitar travamentos eternos
        response = http.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        
        # Se chegou aqui, é um erro que o Retry não resolveu (ex: 404)
        print(f"   [Erro API] Status {response.status_code} para URL: {url}", flush=True)
        return None

    except Exception as e:
        print(f"   [Erro Conexão] {e}", flush=True)
        return None

# --- OBTER LISTA DE MUNDOS ---
def get_active_worlds():
    print("--- Buscando lista de mundos ativos... ---", flush=True)
    url = "https://api.tibiadata.com/v4/worlds"
    data = get_api_data(url)
    
    world_list = []
    if data and 'worlds' in data and 'regular_worlds' in data['worlds']:
        for world in data['worlds']['regular_worlds']:
            world_list.append(world['name'])
            
    print(f"Total de mundos encontrados: {len(world_list)}", flush=True)
    return world_list

# --- RANKING (HIGHSCORES) ---
def process_highscores(world):
    print(f"   > [Ranking XP] Iniciando: {world}", flush=True)
    all_players = []
    page = 1
    
    # Loop de Páginas
    while True:
        url = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/{page}"
        data = get_api_data(url)
        
        # Se não vier dados, para o loop
        if not data or 'highscores' not in data:
            break
            
        page_list = data['highscores'].get('highscore_list', [])
        
        if not page_list:
            break
        
        all_players.extend(page_list)
        
        # TRAVA DE SEGURANÇA: Top 1000 (Página 20)
        if page >= 20:
            break
            
        page += 1
        # Pequena pausa entre páginas para não sobrecarregar
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
        print(f"   > [Ranking XP] Sucesso: {len(df)} linhas salvas.", flush=True)
    else:
        print(f"   > [Ranking XP] AVISO: Nenhum dado coletado.", flush=True)

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
        print(f"   > [Kill Stats] Sucesso: {len(df)} linhas salvas.", flush=True)

# --- ROTINA PRINCIPAL ---
def main():
    if not os.path.exists(ROOT_FOLDER):
        os.makedirs(ROOT_FOLDER)

    worlds_to_process = get_active_worlds()
    
    # Fallback caso a API de mundos falhe
    if not worlds_to_process:
        print("AVISO: Usando lista de emergência (API de mundos falhou).", flush=True)
        worlds_to_process = ["Antica", "Venebra", "Ferobra", "Lobera"] 

    for i, world_name in enumerate(worlds_to_process):
        print(f"\n[{i+1}/{len(worlds_to_process)}] Processando: {world_name}", flush=True)
        
        process_highscores(world_name)
        process_kill_statistics(world_name)
        
        # PAUSA ESTRATÉGICA ENTRE MUNDOS
        # Isso evita que o IP seja bloqueado por excesso de velocidade
        time.sleep(2) 

    print("\n--- FIM DA ROTINA GLOBAL ---", flush=True)

if __name__ == "__main__":
    main()
