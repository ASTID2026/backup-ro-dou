import base64
import requests
import io
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Configurações
TCU_URL = "https://sites.tcu.gov.br/dados-abertos/jurisprudencia/arquivos/acordao-completo/acordao-completo-2026.csv"
GITHUB_OWNER = "ASTID2026"
GITHUB_REPO = "Ro-Dou"
FILE_PATH = "acordao/acordao-completo-2026-comaer.csv"
BRANCH = "main"

def sync_and_filter_tcu_acordaos():
    github_token = Variable.get("GITHUB_TOKEN")
    last_tcu_size = Variable.get("LAST_TCU_COMPLETO_SIZE", default_var="0")
    
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    print("Verificando arquivo no TCU...")
    tcu_response = requests.head(TCU_URL)
    tcu_response.raise_for_status()
    current_tcu_size = tcu_response.headers.get('Content-Length', '0')
    
    print(f"Tamanho anterior mapeado: {last_tcu_size} bytes")
    print(f"Tamanho atual no TCU: {current_tcu_size} bytes")

    if current_tcu_size != last_tcu_size:
        print("Nova versão detectada no TCU. Iniciando download...")
        
        download_response = requests.get(TCU_URL)
        download_response.raise_for_status()
        
        print("Download concluído. Iniciando filtragem dos dados...")
        
        # Lê o CSV em memória ajustando o separador para '|' e lendo como texto
        df = pd.read_csv(
            io.BytesIO(download_response.content), 
            sep='|', 
            quotechar='"',
            encoding='utf-8', 
            on_bad_lines='skip',
            dtype=str
        )
        
        # Remove espaços em branco invisíveis antes ou depois do nome das colunas
        df.columns = df.columns.str.strip()
        print(f"Colunas lidas do arquivo: {df.columns.tolist()}")
        
        # Cria a máscara validando apenas as colunas ENTIDADE e INTERESSADOS
        termo_busca = 'comando da aeronáutica'
        
        # Verifica se as colunas existem no arquivo antes de aplicar o filtro
        if 'ENTIDADE' in df.columns and 'INTERESSADOS' in df.columns:
            mask = (
                df['ENTIDADE'].fillna('').str.contains(termo_busca, case=False) |
                df['INTERESSADOS'].fillna('').str.contains(termo_busca, case=False)
            )
            df_filtrado = df[mask]
        else:
            raise ValueError(f"Colunas não encontradas. As colunas disponíveis são: {df.columns.tolist()}")
        
        print(f"Total de registros originais: {len(df)}. Registros após filtro: {len(df_filtrado)}.")
        
        # Converte o DataFrame filtrado de volta para CSV na memória (usando o separador |)
        csv_buffer = io.StringIO()
        df_filtrado.to_csv(csv_buffer, index=False, sep='|', quoting=3, escapechar='\\')
                
        # Prepara para envio ao GitHub
        encoded_content = base64.b64encode(csv_buffer.getvalue().encode('utf-8')).decode('utf-8')
        
        # Pega o SHA do arquivo atual no GitHub (necessário para sobrescrever)
        github_api_url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{FILE_PATH}"
        check_repo = requests.get(github_api_url, headers=headers)
        file_sha = check_repo.json().get("sha") if check_repo.status_code == 200 else None

        payload = {
            "message": f"Atualização de acórdãos (Filtro COMAER) - {datetime.now().strftime('%Y-%m-%d')}",
            "content": encoded_content,
            "branch": BRANCH
        }
        if file_sha:
            payload["sha"] = file_sha
            
        print("Enviando commit do arquivo filtrado para o repositório...")
        put_response = requests.put(github_api_url, headers=headers, json=payload)
        put_response.raise_for_status()
        
        # Atualiza a variável no Airflow
        Variable.set("LAST_TCU_COMPLETO_SIZE", current_tcu_size)
        print("Sincronização e filtragem concluídas com sucesso!")
        
    else:
        print("O arquivo no TCU não mudou. Nenhuma atualização necessária.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sync_acordaos_comaer_github',
    default_args=default_args,
    description='Filtra acórdãos por entidade/interessados e sincroniza com GitHub',
    schedule_interval='@daily',
    start_date=datetime(2026, 7, 27),
    catchup=False,
    tags=['tcu', 'ro-dou', 'github', 'filtragem'],
) as dag:

    sync_task = PythonOperator(
        task_id='filter_and_update_acordaos',
        python_callable=sync_and_filter_tcu_acordaos
    )