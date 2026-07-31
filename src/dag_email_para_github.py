from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import imaplib
import email
from github import Github
from email.header import decode_header

# ==========================================
# CONFIGURAÇÕES FIXAS
# ==========================================
IMAP_SERVER = "imap.mail.intraer" # Mude se não for Gmail
REPOSITORIO_NOME = "ASTID2026/Ro-Dou"

def conectar_email(email_user, email_pass):
    """Conecta ao servidor IMAP e retorna a caixa de entrada."""
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login("astid.cenciar", "n8Z0vylaexorUYVN3n2W")
    mail.select("inbox")
    return mail

def enviar_para_github(nome_arquivo, conteudo_arquivo, github_token):
    """Envia o arquivo lido para o repositório do GitHub."""
    g = Github(github_token)
    repo = g.get_repo(REPOSITORIO_NOME)
    
    caminho_no_github = f"csv_recebidos/{nome_arquivo}"
    mensagem_commit = f"Adicionando arquivo {nome_arquivo} recebido por email via Airflow"

    try:
        # Tenta criar o arquivo no GitHub
        repo.create_file(caminho_no_github, mensagem_commit, conteudo_arquivo, branch="main")
        print(f"Sucesso: {nome_arquivo} enviado para o GitHub!")
    except Exception as e:
        print(f"Erro ao enviar para o GitHub. (O arquivo já existe?): {e}")

def extrair_emails_e_comitar(**kwargs):
    """Função principal que será chamada pelo PythonOperator no Airflow."""
    
    # Resgata as credenciais cadastradas na interface do Airflow (Admin > Variables)
    email_user = Variable.get("EMAIL_USUARIO")
    email_pass = Variable.get("EMAIL_SENHA_APP")
    github_token = Variable.get("GITHUB_TOKEN")

    mail = conectar_email(email_user, email_pass)
    
    # Busca apenas por e-mails NÃO LIDOS (UNSEEN)
    status, mensagens = mail.search(None, "UNSEEN")
    ids_emails = mensagens[0].split()

    if not ids_emails:
        print("Nenhum e-mail novo encontrado na caixa de entrada.")
        mail.logout()
        return

    for email_id in ids_emails:
        status, dados_msg = mail.fetch(email_id, "(RFC822)")
        for response_part in dados_msg:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                
                # Verifica se o e-mail tem anexos
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_maintype() == "multipart":
                            continue
                        if part.get("Content-Disposition") is None:
                            continue
                        
                        nome_arquivo = part.get_filename()
                        
                        # Se houver um arquivo e ele for CSV
                        if nome_arquivo and nome_arquivo.lower().endswith('.csv'):
                            nome_arquivo, encoding = decode_header(nome_arquivo)[0]
                            if isinstance(nome_arquivo, bytes):
                                nome_arquivo = nome_arquivo.decode(encoding if encoding else 'utf-8')
                            
                            print(f"Processando CSV encontrado: {nome_arquivo}")
                            conteudo_arquivo = part.get_payload(decode=True)
                            
                            # Envia para o GitHub usando o token resgatado
                            enviar_para_github(nome_arquivo, conteudo_arquivo, github_token)

    mail.logout()

# ==========================================
# DEFINIÇÃO DA DAG
# ==========================================
default_args = {
    'owner': 'seu_nome',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Criando a DAG que rodará a cada hora
with DAG(
    'extracao_csv_email_para_github',
    default_args=default_args,
    description='Extrai anexos CSV de emails não lidos e envia para o GitHub',
    schedule_interval='@hourly', # Pode ser alterado para uma cron expression (ex: '0 8 * * *' para todo dia às 8h)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['automacao', 'github', 'email'],
) as dag:

    # Definindo a Task usando o PythonOperator
    task_processar_emails = PythonOperator(
        task_id='buscar_e_enviar_csvs',
        python_callable=extrair_emails_e_comitar,
        provide_context=True
    )

    # A DAG tem apenas uma task, então não precisamos definir dependências (ex: task1 >> task2)
    task_processar_emails