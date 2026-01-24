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
        total=3,  # Tenta 3 vezes imediato (interno do request)
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

http = create_session()

# --- FUNÇÃO DE REQUISIÇÃO ---
def get_api_data(url):
    headers = {
        'User-Agent': 'TibiaAnalyticsBot/3.0 (RetrySystem)',
        'accept': 'application/json'
    }
    try:
        response = http.get(url, headers=headers, timeout=25)
        if response.status_code == 200:
            return response.json()
        return None # Retorna None em caso de erro definitivo (após retries da sessão)
    except Exception as e:
        print(f"   [Erro Conexão] {e}", flush=True)
        return None

# --- OBTER MUNDOS ---
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

# --- LÓGICA DE SALVAMENTO AUXILIAR ---
def save_parquet(df, world, folder_type):
    target_folder = os.path.join(ROOT_FOLDER, folder_type, world)
    os.makedirs(target_folder, exist_ok=True)
    
    filename = f"{folder_type}_{world}_{datetime.now().date()}.parquet"
    filepath = os.path.join(target_folder, filename)
    
    # Se o arquivo já existe (caso do Retry Global), adiciona os dados (append)
    if os.path.exists(filepath):
        # Lê o existente, junta com o novo e salva
        try:
            df_existing = pd.read_parquet(filepath)
            df_final = pd.concat([df_existing, df], ignore_index=True)
            # Remove duplicatas caso haja sobreposição acidental
            df_final.drop_duplicates(inplace=True)
            df_final.to_parquet(filepath, index=False)
            return True, len(df_final)
        except Exception as e:
            print(f"   [Erro ao Atualizar Arquivo] {e}", flush=True)
            return False, 0
    else:
        df.to_parquet(filepath, index=False)
        return True, len(df)

# --- PROCESSAMENTO DO RANKING (COM LÓGICA DE RETRY) ---
def process_highscores(world):
    print(f"   > [Ranking XP] Iniciando: {world}", flush=True)
    
    valid_data = []
    failed_pages = [] # Lista para guardar as páginas que deram erro
    
    # 1. TENTATIVA INICIAL (Varredura 1 a 20)
    # Usamos range fixo para garantir que se a pag 4 falhar, tentamos a 5
    for page in range(1, 21):
        url = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/{page}"
        data = get_api_data(url)
        
        # Cenário A: Erro de Conexão/API -> Marca para tentar depois e CONTINUA
        if data is None:
            print(f"     [Falha] Pagina {page} falhou. Adicionada à fila de retry.", flush=True)
            failed_pages.append(page)
            continue 

        # Cenário B: Dados vazios ou Fim da Lista -> Para o loop (não é erro)
        if 'highscores' not in data:
            break
        page_list = data['highscores'].get('highscore_list', [])
        if not page_list:
            break # Fim real do ranking
        
        # Sucesso
        valid_data.extend(page_list)
        time.sleep(0.5) # Pausa suave

    # 2. RETRY LOCAL (Nível Mundo)
    # Tenta recuperar imediatamente as páginas que falharam antes de sair do mundo
    if failed_pages:
        print(f"   > [Retry Local] Tentando recuperar {len(failed_pages)} páginas em {world}...", flush=True)
        still_failed = []
        
        for page in failed_pages:
            url = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/{page}"
            data = get_api_data(url)
            
            if data and 'highscores' in data:
                page_list = data['highscores'].get('highscore_list', [])
                if page_list:
                    valid_data.extend(page_list)
                    print(f"     [Recuperado] Pagina {page} salva com sucesso!", flush=True)
                else:
                    # Se voltou vazio no retry, talvez a página não exista mesmo
                    pass 
            else:
                print(f"     [Falha Novamente] Pagina {page} ainda com erro.", flush=True)
                still_failed.append(page)
            
            time.sleep(1) # Pausa maior no retry
        
        failed_pages = still_failed # Atualiza lista para o global

    # Salva o que conseguimos
    if valid_data:
        df = pd.DataFrame(valid_data)
        df['extraction_date'] = datetime.now().date()
        df['world'] = world
        df['type'] = 'highscores'
        
        saved, count = save_parquet(df, world, FOLDER_RANKING)
        if saved:
            print(f"   > [Ranking XP] Parcial: {len(df)} linhas salvas. (Pendentes: {failed_pages})", flush=True)
    else:
        print(f"   > [Ranking XP] AVISO: Nenhum dado coletado.", flush=True)

    # Retorna as páginas que continuam com erro para o Retry Global
    return [(world, p) for p in failed_pages]

# --- KILL STATISTICS (Simples, sem paginação complexa) ---
def process_kill_statistics(world):
    url = f"https://api.tibiadata.com/v4/killstatistics/{world}"
    data = get_api_data(url)
    
    if data and 'killstatistics' in data:
        entries = data['killstatistics'].get('entries', [])
        if entries:
            df = pd.DataFrame(entries)
            df['extraction_date'] = datetime.now().date()
            df['world'] = world
            df['type'] = 'kill_statistics'
            save_parquet(df, world, FOLDER_KILLSTATS)
            print(f"   > [Kill Stats] Sucesso: {len(df)} linhas.", flush=True)

# --- FUNÇÃO DE RETRY GLOBAL (Nível Final) ---
def run_global_retry(global_failures):
    if not global_failures:
        return

    print("\n" + "="*40)
    print(f"INICIANDO RETRY GLOBAL ({len(global_failures)} falhas pendentes)")
    print("="*40, flush=True)
    
    for world, page in global_failures:
        print(f" > Tentando resgatar: {world} (Pag {page})...", flush=True)
        url = f"https://api.tibiadata.com/v4/highscores/{world}/{RANK_CATEGORY}/{RANK_PROFESSION}/{page}"
        
        # Tenta com mais insistência ou paciência
        time.sleep(2) 
        data = get_api_data(url)
        
        if data and 'highscores' in data:
            page_list = data['highscores'].get('highscore_list', [])
            if page_list:
                # Prepara DataFrame
                df = pd.DataFrame(page_list)
                df['extraction_date'] = datetime.now().date()
                df['world'] = world
                df['type'] = 'highscores'
                
                # Salva (Modo Append automático na função save_parquet)
                saved, total_rows = save_parquet(df, world, FOLDER_RANKING)
                print(f"   [SUCESSO] {world} Pag {page} recuperada e adicionada ao arquivo! Total linhas: {total_rows}", flush=True)
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
        worlds = ["Antica", "Bona"] # Fallback

    all_global_failures = []

    for i, world in enumerate(worlds):
        print(f"\n[{i+1}/{len(worlds)}] Processando: {world}", flush=True)
        
        # Processa e coleta falhas persistentes
        failures = process_highscores(world)
        all_global_failures.extend(failures)
        
        process_kill_statistics(world)
        
        time.sleep(1.5) # Pausa entre mundos

    # Fim da execução principal -> Tenta o Retry Global
    run_global_retry(all_global_failures)
    
    print("\n--- FIM DA ROTINA ---", flush=True)

if __name__ == "__main__":
    main()
