import requests
import json
import os
import time

# Pasta onde salvaremos o dicionário
OUTPUT_FOLDER = "dados"
BOSS_FILE = "bosses.json"

def get_api_data(url, params=None):
    try:
        headers = {'User-Agent': 'TibiaDataCollector/1.0'}
        response = requests.get(url, params=params, headers=headers, timeout=20)
        if response.status_code != 200:
            print(f"Erro {response.status_code} na URL: {url}")
            return None
        return response.json()
    except Exception as e:
        print(f"Erro de conexão: {e}")
        return None

def main():
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    print("--- Iniciando atualização da Base de Bosses (TibiaWiki) ---")
    
    wiki_api_url = "https://tibia.fandom.com/api.php"
    
    # Mapeamento: Categoria na Wiki -> Nome limpo para o Dashboard
    categories = {
        "Category:Bane_Bosses": "Bane",
        "Category:Archfoe_Bosses": "Archfoe",
        "Category:Nemesis_Bosses": "Nemesis"
    }
    
    boss_db = {}
    
    for wiki_cat, label in categories.items():
        print(f"   > Baixando categoria: {label}...")
        
        params = {
            "action": "query",
            "list": "categorymembers",
            "cmtitle": wiki_cat,
            "cmlimit": "500",
            "format": "json"
        }
        
        data = get_api_data(wiki_api_url, params)
        
        if data and "query" in data and "categorymembers" in data["query"]:
            members = data["query"]["categorymembers"]
            for member in members:
                boss_name = member["title"]
                boss_db[boss_name] = label
        
        time.sleep(1)

    # Salva o dicionário em um arquivo JSON leve
    file_path = os.path.join(OUTPUT_FOLDER, BOSS_FILE)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(boss_db, f, ensure_ascii=False, indent=4)
        
    print(f"--- Sucesso! {len(boss_db)} bosses salvos em {file_path} ---")

if __name__ == "__main__":
    main()
