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

# --- CONFIGURAÇÃO DE SESSÃO ROBUSTA ---
def create_session():
    session = requests.Session()
    retry = Retry(
        total=3,  # Tenta 3 vezes internamente para erros de conexão (DNS, recusado)
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

http = create_session()

# --- FUNÇÃO DE REQUISIÇÃO (COM TIMEOUT DE 2 MINUTOS) ---
# Alterado o padrão para 120 segundos (2 minutos)
def get_api_data(url, timeout_val=120):
    headers = {
        'User-Agent': 'TibiaAnalyticsBot/3.0 (RetrySystem)',
        'accept': 'application/json'
    }
    try:
        # Se demorar mais que 120s (2 min), gera erro de Timeout
        response = http.get(url, headers=headers, timeout=timeout_val)
        
        if response.status_code == 200:
            return response.json()
        return None 
    
    except requests.exceptions.Timeout:
        print(f"   [TIMEOUT] O servidor demorou mais de {timeout_val}s. Adicionando ao retry.", flush=True)
        return None
    except Exception as e:
        print(f"   [Erro Conexão] {e}", flush=True)
        return None

# --- OBTER MUNDOS ---
def get_active_worlds():
    print("--- Buscando lista de mundos ativos... ---", flush=True)
    url = "https://api.tibiadata.com/v4/worlds"
    # Aqui usamos o timeout padrão de 120s
    data = get_api_data(url)
    
    world_list = []
    if data and 'worlds' in data and 'regular_worlds' in data['worlds']:
        for world in data['worlds']['regular_worlds']:
            world_list.append(world['name'])
    
    print(f"Total de mundos encontrados: {len(world_list)}", flush=True)
    return world_list

# --- LÓGICA DE SALVAMENTO AUXILIAR ---
def save_parquet(df, world, folder_type):
    target_folder = os.path.join(ROOT_FOLDER, folder_type, world)
    os.makedirs(target_folder, exist_ok=True)
    
    filename = f"{folder_type}_{world}_{datetime.now().date()}.parquet"
    filepath = os.path.join(target_folder, filename)
    
    # Se o arquivo já existe (Retry Global ou Local), faz append
    if os.path.exists(filepath):
        try:
            df_existing = pd.read_parquet(filepath)
            df_final = pd.concat([df_existing, df], ignore_index=True)
            df_final.drop_duplicates(inplace=True) # Garante que não duplique dados
            df_final.to_parquet(filepath, index=False)
            return True, len(df_final)
        except Exception as e:
            print(f"   [Erro ao Atualizar Arquivo] {e}", flush=True)
            return False, 0
    else:
        df.to_parquet(filepath, index=False)
        return True, len(df)

# --- PROCESSAMENTO DO RANKING ---
def process_highscores(world):
    print(f"   > [Ranking XP] Iniciando: {world}", flush=True)
    
    valid_data = []
    failed_pages = [] # Lista para guardar as páginas que deram erro/timeout
    
    # 1. TENTATIVA INICIAL (Varredura 1 a 20)
    for page in range(1, 21):
        url = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/{page}"
        # Timeout explícito de 120s (2 min)
        data = get_api_data(url, timeout_val=120) 
        
        # Se deu Timeout ou Erro -> Guarda na lista para tentar depois
        if data is None:
            failed_pages.append(page)
            continue 

        # Se a página existe mas não tem lista, ou acabou o ranking -> Para
        if 'highscores' not in data:
            break
        page_list = data['highscores'].get('highscore_list', [])
        if not page_list:
            break 
        
        valid_data.extend(page_list)
        time.sleep(0.5)

    # 2. RETRY LOCAL (Tenta recuperar imediatamente o que falhou nesse mundo)
    if failed_pages:
        print(f"   > [Retry Local] Tentando recuperar {len(failed_pages)} páginas em {world}...", flush=True)
        still_failed = []
        
        for page in failed_pages:
            url = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/{page}"
            # Tenta de novo com 120s
            data = get_api_data(url, timeout_val=120) 
            
            if data and 'highscores' in data:
                page_list = data['highscores'].get('highscore_list', [])
                if page_list:
                    valid_data.extend(page_list)
                    print(f"     [Recuperado] Pagina {page} salva!", flush=True)
                else:
                    pass # Página vazia
            else:
                print(f"     [Falha Persistente] Pagina {page} ainda com erro.", flush=True)
                still_failed.append(page)
            
            time.sleep(1) 
        
        failed_pages = still_failed # Atualiza a lista para mandar pro Global

    # Salva o que conseguimos coletar neste mundo
    if valid_data:
        df = pd.DataFrame(valid_data)
        df['extraction_date'] = datetime.now().date()
        df['world'] = world
        df['type'] = 'highscores'
        
        saved, count = save_parquet(df, world, FOLDER_RANKING)
        if saved:
            msg_pendentes = f"(Pendentes para Global: {failed_pages})" if failed_pages else ""
            print(f"   > [Ranking XP] Salvo: {len(df)} linhas. {msg_pendentes}", flush=True)
    else:
        print(f"   > [Ranking XP] AVISO: Nenhum dado coletado.", flush=True)

    # Retorna o que sobrou de erro para tentar no final de tudo
    return [(world, p) for p in failed_pages]

# --- KILL STATISTICS ---
def process_kill_statistics(world):
    url = f"https://api.tibiadata.com/v4/killstatistics/{world}"
    # Timeout explícito de 120s (2 min)
    data = get_api_data(url, timeout_val=120)
    
    if data and 'killstatistics' in data:
        entries = data['killstatistics'].get('entries', [])
        if entries:
            df = pd.DataFrame(entries)
            df['extraction_date'] = datetime.now().date()
            df['world'] = world
            df['type'] = 'kill_statistics'
            save_parquet(df, world, FOLDER_KILLSTATS)
            print(f"   > [Kill Stats] Sucesso: {len(df)} linhas.", flush=True)

# --- RETRY GLOBAL (Última chance) ---
def run_global_retry(global_failures):
    if not global_failures:
        return

    print("\n" + "="*50)
    print(f"INICIANDO RETRY GLOBAL ({len(global_failures)} falhas pendentes)")
    print("Isso ocorre após tentar todos os mundos.")
    print("="*50, flush=True)
    
    for world, page in global_failures:
        print(f" > Tentando resgatar: {world} (Pag {page})...", flush=True)
        url = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/{page}"
        
        # Aumentamos o delay e o timeout para ter mais chance (140 segundos aqui)
        time.sleep(3) 
        data = get_api_data(url, timeout_val=140) # 140 segundos de tolerância na última tentativa
        
        if data and 'highscores' in data:
            page_list = data['highscores'].get('highscore_list', [])
            if page_list:
                df = pd.DataFrame(page_list)
                df['extraction_date'] = datetime.now().date()
                df['world'] = world
                df['type'] = 'highscores'
                
                # O save_parquet usa 'append', então vai adicionar ao arquivo existente sem apagar o anterior
                saved, total_rows = save_parquet(df, world, FOLDER_RANKING)
                print(f"   [SUCESSO] {world} Pag {page} RECUPERADA! Total linhas no arquivo: {total_rows}", flush=True)
            else:
                print(f"   [Vazio] Pagina retornou vazia.", flush=True)
        else:
            print(f"   [FALHA FINAL] Não foi possível recuperar {world} Pag {page}.", flush=True)

# --- MAIN ---
def main():
    if not os.path.exists(ROOT_FOLDER):
        os.makedirs(ROOT_FOLDER)

    worlds = get_active_worlds()
    if not worlds:
        worlds = ["Antica", "Bona"] 

    all_global_failures = []

    for i, world in enumerate(worlds):
        print(f"\n[{i+1}/{len(worlds)}] Processando: {world}", flush=True)
        
        # O retorno 'failures' são as páginas que falharam na 1ª tentativa E no retry local
        failures = process_highscores(world)
        all_global_failures.extend(failures)
        
        process_kill_statistics(world)
        time.sleep(1) 

    # Se sobrou algo na lista global, tenta agora
    run_global_retry(all_global_failures)
    
    print("\n--- FIM DA ROTINA ---", flush=True)

if __name__ == "__main__":
    main()
