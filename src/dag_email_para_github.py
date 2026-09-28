from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import imaplib
import email
from github import Github
from email.header import decode_header
import pandas as pd
import io
import csv  # <-- IMPORTANTE: Adicionado para usar a constante csv.QUOTE_NONE

# ==========================================
# CONFIGURAÇÕES FIXAS
# ==========================================
IMAP_SERVER = "imap.mail.intraer" # Mude se não for Gmail
REPOSITORIO_NOME = "ASTID2026/Ro-Dou"

def conectar_email(email_user, email_pass):
    """Conecta ao servidor IMAP e retorna a caixa de entrada."""
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    # Usando as variáveis passadas em vez de credenciais expostas no código
    mail.login("astid.cenciar", "n8Z0vylaexorUYVN3n2W")
    mail.select("inbox")
    return mail

def enviar_para_github(nome_arquivo, conteudo_arquivo, github_token, pasta_destino):
    """Envia o arquivo lido para o repositório do GitHub na pasta especificada."""
    g = Github(github_token)
    repo = g.get_repo(REPOSITORIO_NOME)
    
    caminho_no_github = f"{pasta_destino}/{nome_arquivo}"
    mensagem_commit = f"Adicionando arquivo {nome_arquivo} via Airflow"

    try:
        # Tenta criar o arquivo no GitHub
        repo.create_file(caminho_no_github, mensagem_commit, conteudo_arquivo, branch="main")
        print(f"Sucesso: {nome_arquivo} enviado para o GitHub na pasta {pasta_destino}!")
    except Exception as e:
        print(f"Erro ao enviar {nome_arquivo} para o GitHub. (O arquivo já existe?): {e}")

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
        mail.logout()
        # Levantamos uma exceção para forçar a falha da tarefa, 
        # acionando as repetições (retries) configuradas na DAG.
        raise ValueError("Nenhum e-mail novo encontrado na caixa de entrada. Acionando nova tentativa em 30 min.")

    # Pega as datas atuais para usar no nome do arquivo e no corpo do texto
    data_atual = datetime.now()
    data_nome_arquivo = data_atual.strftime("%d%m%Y")
    data_corpo_arquivo = data_atual.strftime("%d/%m/%Y")

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
                        
                        nome_anexo = part.get_filename()
                        
                        # Se houver um arquivo e ele for CSV
                        if nome_anexo and nome_anexo.lower().endswith('.csv'):
                            nome_anexo, encoding = decode_header(nome_anexo)[0]
                            if isinstance(nome_anexo, bytes):
                                nome_anexo = nome_anexo.decode(encoding if encoding else 'utf-8')
                            
                            print(f"Processando CSV encontrado no e-mail: {nome_anexo}")
                            conteudo_arquivo_bytes = part.get_payload(decode=True)
                            
                            try:
                                # 1. Lê o CSV original em um DataFrame do Pandas
                                df = pd.read_csv(io.BytesIO(conteudo_arquivo_bytes))
                                
                                # 2. Suprime as 3 primeiras colunas (se o arquivo tiver mais de 3 colunas)
                                if len(df.columns) > 3:
                                    df_modificado = df.iloc[:, 3:].copy()
                                else:
                                    # Se tiver 3 ou menos, remove tudo deixando um df vazio (segurança)
                                    df_modificado = pd.DataFrame()
                                
                                # 3. Insere a data da extração no corpo do arquivo (nova coluna)
                                if not df_modificado.empty:
                                    df_modificado['Data_Extracao'] = data_corpo_arquivo
                                
                                # 4. Prepara o novo CSV modificado em memória
                                csv_buffer = io.StringIO()
                                
                                df_modificado.to_csv(
                                    csv_buffer, 
                                    index=False, 
                                    sep=';',                   # Separador padrão do Excel no Windows em Português
                                    lineterminator='\r\n',     # Quebra de linha padrão do Windows
                                    quoting=csv.QUOTE_NONE,    # Força a não utilizar aspas duplas
                                    escapechar='\\'            # Caractere de escape necessário quando as aspas são removidas
                                )
                                
                                # Recomendado: forçar encoding como latin1 ou utf-8-sig para garantir leitura correta de acentos no Excel Windows
                                csv_bytes = csv_buffer.getvalue().encode('utf-8-sig')
                                
                                # 5. Prepara o novo arquivo Excel (.xlsx) em memória
                                excel_buffer = io.BytesIO()
                                df_modificado.to_excel(excel_buffer, index=False, engine='openpyxl')
                                excel_bytes = excel_buffer.getvalue()
                                
                                # 6. Define os novos nomes padronizados dos arquivos (CSV e Excel)
                                nome_arquivo_padronizado = f"extracao_dou_{data_nome_arquivo}.csv"
                                nome_excel_padronizado = f"extracao_dou_{data_nome_arquivo}.xlsx"

                                # 7. Envia o CSV modificado para a pasta "csv_recebidos"
                                enviar_para_github(nome_arquivo_padronizado, csv_bytes, github_token, "csv_recebidos")
                                
                                # 8. Envia o Excel modificado para a pasta "excell"
                                enviar_para_github(nome_excel_padronizado, excel_bytes, github_token, "excell")

                            except Exception as e:
                                print(f"Erro ao processar os dados do arquivo {nome_anexo}: {e}")

    mail.logout()

# ==========================================
# DEFINIÇÃO DA DAG
# ==========================================
default_args = {
    'owner': 'seu_nome',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,                            # Tenta repetir até 10 vezes caso falhe/não ache o arquivo
    'retry_delay': timedelta(minutes=30),     # Aguarda exatamente 30 minutos entre cada repetição
}

# Criando a DAG que rodará às 10h de Seg a Sex
with DAG(
    'extracao_csv_email_para_github',
    default_args=default_args,
    description='Extrai anexos CSV de emails, remove 3 primeiras colunas e envia como CSV e Excel para o GitHub',
    schedule_interval='0 10 * * MON-FRI',     # Executa às 10:00 da manhã, de Segunda a Sexta
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['automacao', 'github', 'email', 'pandas'],
) as dag:

    # Definindo a Task usando o PythonOperator
    task_processar_emails = PythonOperator(
        task_id='buscar_processar_e_enviar_arquivos',
        python_callable=extrair_emails_e_comitar,
        provide_context=True
    )

    task_processar_emails