import requests
import pandas as pd
import time
from datetime import datetime
import os

# --- CONFIGURAÇÕES ---
# Lista fixa de mundos
WORLDS = [
    "Collabra", "Issobra", "Venebra", "Descubra",
    "Etebra", "Luzibra", "Ustebra", "Yubra"
]

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
        # Se der erro 404 ou 500, não quebra o script, apenas retorna None
        if response.status_code != 200:
            print(f"   [API Status {response.status_code}] URL: {url}")
            return None
        return response.json()
    except Exception as e:
        print(f"   [Erro Conexão] {e}")
        return None

# --- LÓGICA DE EXTRAÇÃO: HIGHSCORES (Corrigida para pegar até Rank 1000) ---
def process_highscores(world):
    print(f"   > [Ranking XP] Iniciando: {world}")
    all_players = []
    page = 1
    
    # Loop Infinito com trava: Continua pegando páginas até a 20 ou acabar
    while True:
        url = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/{page}"
        data = get_api_data(url)
        
        # Se a API falhar ou não trouxer highscores, para o loop
        if not data or 'highscores' not in data:
            break
            
        page_list = data['highscores'].get('highscore_list', [])
        
        # SE A LISTA ESTIVER VAZIA: Significa que as páginas acabaram
        if not page_list:
            break
        
        # Adiciona os jogadores encontrados
        all_players.extend(page_list)
        
        # TRAVA DE SEGURANÇA:
        # Para na página 20 (Top 1000). 
        if page >= 20:
            break
            
        # Prepara para a próxima página
        page += 1
        time.sleep(0.5) # Pausa para não bloquear o IP

    # Salva o resultado final
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
        print(f"   > [Ranking XP] Finalizado: {len(df)} jogadores extraídos.")
    else:
        print(f"   > [Ranking XP] Nenhum dado encontrado para {world}.")

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

    for i, world_name in enumerate(WORLDS):
        print(f"\n[{i+1}/{len(WORLDS)}] Processando: {world_name}")
        
        process_highscores(world_name)
        process_kill_statistics(world_name)
        
        time.sleep(1) 

    print("\n--- FIM DA ROTINA ---")

if __name__ == "__main__":
    main()
