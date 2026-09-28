import os
import requests
import base64
import json
import holidays
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable

# Configurações
DIRETORIO_TMP = "/tmp/bca_downloads"
GITHUB_REPO = "ASTID2026/DIRETORIO_BCA"
ANO = 2026

def executar_download_bca(**kwargs):
    # Usando data_interval_end para pegar a data real de acionamento da DAG
    data_alvo = kwargs['data_interval_end'].date()
    feriados_br = holidays.Brazil(years=ANO)
    
    # Validação de fim de semana ou feriado
    if data_alvo.weekday() >= 5 or data_alvo in feriados_br:
        raise AirflowSkipException(f"Data {data_alvo} é fim de semana ou feriado. Download cancelado.")

    os.makedirs(DIRETORIO_TMP, exist_ok=True)
    
    # Formatação da data
    dia = str(data_alvo.day).zfill(2)
    mes = str(data_alvo.month).zfill(2)
    ano_str = str(data_alvo.year)
    data_ddmmaaaa = data_alvo.strftime("%d%m%Y")
    
    # Formato usado no final do arquivo bca_n°_dd-mm-aaaa
    data_tracos = data_alvo.strftime("%d-%m-%Y") 
    
    # Padrão Regex: procura "bca_", seguido de qualquer coisa (.*?), seguido da data
    padrao_bca = re.compile(rf"bca_.*?_{data_tracos}", re.IGNORECASE)
    
    nome_arquivo = f"bca_{data_ddmmaaaa}.pdf"
    
    url_base = "http://www.cendoc.intraer/sisbca/index.php"
    
    payload = {
        'dia': dia,
        'mes': mes,
        'ano': ano_str,
        'acao': 'pesquisar'
    }
    
    print(f"Iniciando busca do BCA para a data: {data_tracos}")
    
    try:
        session = requests.Session()
        
        response_busca = session.post(url_base, data=payload, timeout=20)
        response_busca.raise_for_status()
        
        soup = BeautifulSoup(response_busca.text, 'html.parser')
        
        links_download = soup.find_all('a', href=lambda href: href and "download.php" in href)
        link_tag = None
        
        for link in links_download:
            href_link = link.get('href', '')
            
            # Valida se o link atende ao padrão flexível do regex (ex: bca_157_14-09-2026)
            if padrao_bca.search(href_link):
                link_tag = link
                break
        
        if not link_tag:
            raise AirflowSkipException(f"O boletim do dia {data_tracos} ainda não foi publicado. O site retornou documentos antigos. Cancelando task.")
            
        url_download = link_tag['href']
        
        # Tratamento caso o href retorne um link relativo
        if not url_download.startswith("http"):
             url_download = "http://www.cendoc.intraer/sisbca/" + url_download.lstrip('/')
             
        print(f"Link dinâmico capturado: {url_download}")
             
        # Baixa o arquivo do link capturado
        response_pdf = session.get(url_download, stream=True, timeout=20)
        
        if response_pdf.status_code == 200:
            caminho_completo = os.path.join(DIRETORIO_TMP, nome_arquivo)
            with open(caminho_completo, 'wb') as arquivo:
                for chunk in response_pdf.iter_content(chunk_size=8192):
                    arquivo.write(chunk)
            print(f"Sucesso! Arquivo {nome_arquivo} salvo.")
            return caminho_completo 
        else:
            raise Exception(f"Falha ao baixar o PDF. Status code: {response_pdf.status_code}")
            
    except requests.exceptions.ConnectionError:
        raise Exception("Erro de conexão. Verifique se o Worker possui acesso à rede INTRAER.")

def enviar_para_github(**kwargs):
    ti = kwargs['ti']
    caminho_arquivo = ti.xcom_pull(task_ids='baixar_bca_intraer')
    
    if not caminho_arquivo or not os.path.exists(caminho_arquivo):
        raise Exception("Arquivo local não encontrado. A task de download falhou silenciosamente.")

    nome_arquivo = os.path.basename(caminho_arquivo)
    github_token = Variable.get("GITHUB_TOKEN_BCA", default_var="COLOQUE_SEU_TOKEN_AQUI")
    
    if github_token == "COLOQUE_SEU_TOKEN_AQUI":
         raise Exception("Token do GitHub não configurado nas Variáveis do Airflow!")

    url_api = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{nome_arquivo}"
    
    with open(caminho_arquivo, "rb") as f:
        conteudo_b64 = base64.b64encode(f.read()).decode("utf-8")
        
    headers = {
        "Authorization": f"Bearer {github_token}",
        "Accept": "application/vnd.github.v3+json"
    }
    
    response_get = requests.get(url_api, headers=headers)
    sha = response_get.json().get("sha") if response_get.status_code == 200 else None

    payload = {
        "message": f"Airflow: Adiciona BCA {nome_arquivo}",
        "content": conteudo_b64
    }
    if sha:
        payload["sha"] = sha
        
    print(f"Fazendo upload de {nome_arquivo} para {GITHUB_REPO}...")
    response_put = requests.put(url_api, headers=headers, data=json.dumps(payload))
    
    if response_put.status_code in (200, 201):
        print("Upload realizado com sucesso!")
        os.remove(caminho_arquivo) 
    else:
        raise Exception(f"Falha ao enviar pro Git: {response_put.json()}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'download_diario_bca_dinamico',
    default_args=default_args,
    description='Pesquisa o BCA por data na Intraer, extrai o link de download e envia para o GitHub',
    schedule_interval='0 10 * * 1-5', 
    catchup=False,
    tags=['intraer', 'bca', 'github', 'scraping']
) as dag:

    task_baixar_bca = PythonOperator(
        task_id='baixar_bca_intraer',
        python_callable=executar_download_bca,
        provide_context=True,
    )

    task_git_push = PythonOperator(
        task_id='push_para_github',
        python_callable=enviar_para_github,
        provide_context=True,
    )

    task_baixar_bca >> task_git_push