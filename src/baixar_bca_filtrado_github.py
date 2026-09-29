import os
import requests
import base64
import json
import re
import holidays
import sqlite3
import fitz  # PyMuPDF (mantido para leitura e extração do texto original)
import pdfkit # Adicionado para conversão HTML -> PDF
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.utils.email import send_email

# Configurações do Sistema
DIRETORIO_TMP = "/tmp/bca_downloads"
GITHUB_REPO = "ASTID2026/DIRETORIO_BCA"
ANO = 2026
DB_PATH = "/tmp/bca_registros.db"

def executar_download_bca(**kwargs):
    data_alvo = kwargs['data_interval_end'].date()
    feriados_br = holidays.Brazil(years=ANO)
    
    if data_alvo.weekday() >= 5 or data_alvo in feriados_br:
        raise AirflowSkipException(f"Data {data_alvo} é fim de semana ou feriado. Download cancelado.")

    os.makedirs(DIRETORIO_TMP, exist_ok=True)
    
    dia, mes, ano_str = str(data_alvo.day).zfill(2), str(data_alvo.month).zfill(2), str(data_alvo.year)
    data_ddmmaaaa, data_tracos = data_alvo.strftime("%d%m%Y"), data_alvo.strftime("%d-%m-%Y") 
    
    padrao_bca = re.compile(rf"bca_.*?_{data_tracos}", re.IGNORECASE)
    nome_arquivo = f"bca_{data_ddmmaaaa}.pdf"
    url_base = "http://www.cendoc.intraer/sisbca/index.php"
    
    payload = {'dia': dia, 'mes': mes, 'ano': ano_str, 'acao': 'pesquisar'}
    
    print(f"Iniciando busca do BCA para a data: {data_tracos}")
    
    try:
        session = requests.Session()
        response_busca = session.post(url_base, data=payload, timeout=20)
        response_busca.raise_for_status()
        
        soup = BeautifulSoup(response_busca.text, 'html.parser')
        links_download = soup.find_all('a', href=lambda href: href and "download.php" in href)
        link_tag = next((link for link in links_download if padrao_bca.search(link.get('href', ''))), None)
        
        if not link_tag:
            raise AirflowSkipException(f"BCA não publicado para o dia {data_tracos}.")
            
        url_download = link_tag['href']
        if not url_download.startswith("http"):
             url_download = "http://www.cendoc.intraer/sisbca/" + url_download.lstrip('/')
             
        response_pdf = session.get(url_download, stream=True, timeout=20)
        if response_pdf.status_code == 200:
            caminho_completo = os.path.join(DIRETORIO_TMP, nome_arquivo)
            with open(caminho_completo, 'wb') as arquivo:
                for chunk in response_pdf.iter_content(chunk_size=8192):
                    arquivo.write(chunk)
            
            tamanho_minimo = int(Variable.get("TAMANHO_BCA_FILTRO", default_var=0))
            tamanho_atual = os.path.getsize(caminho_completo)
            
            if tamanho_minimo > 0 and tamanho_atual < tamanho_minimo:
                os.remove(caminho_completo)
                raise AirflowSkipException(
                    f"BCA encontrado, mas tamanho atual ({tamanho_atual} bytes) é inferior ao limite "
                    f"mínimo configurado ({tamanho_minimo} bytes). Aguardando atualização completa..."
                )
            
            print(f"Sucesso! Arquivo {nome_arquivo} salvo com {tamanho_atual} bytes.")
            return caminho_completo 
        else:
            raise Exception(f"Falha ao baixar o PDF. Status code: {response_pdf.status_code}")
            
    except requests.exceptions.ConnectionError:
        raise Exception("Erro de conexão. Verifique se o Worker possui acesso à rede INTRAER.")

def extrair_e_filtrar_bca(**kwargs):
    ti = kwargs['ti']
    caminho_arquivo = ti.xcom_pull(task_ids='baixar_bca_intraer')
    data_bca = kwargs['data_interval_end'].strftime("%d-%m-%Y")
    
    if not caminho_arquivo or not os.path.exists(caminho_arquivo):
        raise Exception("Arquivo local não encontrado para processamento.")

    FILTROS_MONITORAMENTO = Variable.get(
        "FILTROS_MONITORAMENTO_BCA",
        deserialize_json=True
    )

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS publicacoes_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo_alvo TEXT, 
            termo_encontrado TEXT, 
            data_bca TEXT,
            UNIQUE(termo_encontrado, data_bca)
        )
    ''')
    conn.commit()

    doc = fitz.open(caminho_arquivo)
    alvos_encontrados = []
    
    texto_completo = ""
    for pagina in doc:
        texto_pagina = pagina.get_text()
        texto_pagina = re.sub(r'Fl\.\s*n[º°]\s*\d+', '', texto_pagina)
        texto_pagina = re.sub(r'\(Continuação do Boletim.*?\)', '', texto_pagina)
        texto_completo += texto_pagina + "\n"

    padrao_divisao = r'(?m)^(?=(?:\d+\s*[-–.)]\s*[A-ZÇÃÕÁÉÍÓÚÊÔ ]+\s*)?(?:PORTARIA|DESPACHO|DECISÃO|ATO|AVISO|ORDEM DE SERVIÇO|INSTRUÇÃO NORMATIVA)\b)'
    blocos_materias = re.split(padrao_divisao, texto_completo)

    patentes = r'(?:Tenente[- ]?Brigadeiro|Ten Brig|Major[- ]?Brigadeiro|Maj Brig|Brigadeiro|Brig|Coronel|Cel|Major|Maj|Capit[ãa]o|Cap|Tenente|Ten|1º Ten|2º Ten|1[º°]\s*Ten|2[º°]\s*Ten|Suboficial|SO|Sargento|Sgt|1S|2S|3S|1º Sgt|2º Sgt|3º Sgt|Cabo|Cb|Soldado|S1|S2|Taifeiro|TM|T1|T2)'
    re_inicio_pessoa = re.compile(rf'^\s*(?:[A-Z0-9º°.-]+\s+)?{patentes}\b', re.IGNORECASE)
    re_conclusao = re.compile(r'^\s*(?:Autorizar|A missão|As despesas|Art\.|Ficam|Fica|Determinar|Publique-se|Cumpra-se|A\s+missão|As\s+despesas)\b', re.IGNORECASE)

    materias_extraidas = []
    termos_busca_texto = []
    for chave in ['nomes', 'sarams', 'oms']:
        termos_busca_texto.extend([str(t).strip().upper() for t in FILTROS_MONITORAMENTO.get(chave, []) if str(t).strip()])

    for bloco in blocos_materias:
        bloco_limpo = bloco.strip()
        if not bloco_limpo:
            continue
            
        bloco_upper = bloco_limpo.upper()
        
        # Correção: Busca os termos ignorando espaços duplos ou quebras de linha
        termos_presentes = []
        for t in termos_busca_texto:
            padrao_busca = r'\s+'.join(re.escape(p) for p in t.split())
            if re.search(padrao_busca, bloco_upper):
                termos_presentes.append(t)
        
        if termos_presentes:
            linhas = bloco_limpo.split('\n')
            linhas_filtradas = []
            buffer_pessoa = []
            
            def processar_buffer():
                if not buffer_pessoa: return []
                texto_pessoa = " ".join(buffer_pessoa).upper()
                
                # Valida usando o mesmo padrão flexível
                for t in termos_presentes:
                    padrao = r'\s+'.join(re.escape(p) for p in t.split())
                    if re.search(padrao, texto_pessoa):
                        return buffer_pessoa
                
                if re.search(r'\b\d{6,7}\b', texto_pessoa):
                    return []
                return buffer_pessoa

            for linha in linhas:
                linha_strip = linha.strip()
                
                if not linha_strip:
                    if buffer_pessoa:
                        linhas_filtradas.extend(processar_buffer())
                        buffer_pessoa = []
                    linhas_filtradas.append("") 
                    continue
                    
                if re_inicio_pessoa.match(linha_strip) or re_conclusao.match(linha_strip):
                    if buffer_pessoa:
                        linhas_filtradas.extend(processar_buffer())
                        buffer_pessoa = []
                    
                    if re_inicio_pessoa.match(linha_strip):
                        buffer_pessoa.append(linha_strip)
                    else:
                        linhas_filtradas.append(linha_strip)
                else:
                    if buffer_pessoa:
                        buffer_pessoa.append(linha_strip)
                    else:
                        linhas_filtradas.append(linha_strip)
                        
            if buffer_pessoa:
                linhas_filtradas.extend(processar_buffer())
            
            texto_final = "\n".join(linhas_filtradas)
            texto_final = re.sub(r'(?<!\n)\n(?!\n)', ' ', texto_final)
            texto_final = re.sub(r'(?is)SEÇÃO\s+[IVXLC]+\s*[-–].*?\(Sem alteração\)', '', texto_final)
            texto_final = re.sub(r'(?i)Fl\.\s*n[º°]\s*\d+', '', texto_final)
            
            termos_presentes.sort(key=len, reverse=True)
            
            # Grifa usando Regex flexível (trata quebras de linha/espaços no nome)
            for termo in termos_presentes:
                padrao_highlight = r'\s+'.join(re.escape(p) for p in termo.split())
                texto_final = re.sub(
                    rf'({padrao_highlight})', 
                    r'<mark style="background-color: yellow; padding: 2px 4px; border-radius: 3px; font-weight: bold; color: black;">\1</mark>', 
                    texto_final, 
                    flags=re.IGNORECASE
                )
            
            texto_final = re.sub(r'\n{2,}', '<br><br>', texto_final).strip()
            termos_card_html = " ".join([f"<mark style='background-color: yellow; padding: 2px 4px; border-radius: 3px; color: black;'>{t}</mark>" for t in termos_presentes])

            materias_extraidas.append(
                f"<div style='background-color: #ffffff; border-left: 5px solid #004B87; border-radius: 4px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); font-family: Arial, sans-serif;'>\n"
                f"  <div style='color: #d9534f; font-size: 13px; text-transform: uppercase; margin-bottom: 15px; border-bottom: 1px solid #eeeeee; padding-bottom: 8px;'>\n"
                f"      <strong>⚠️ ALVOS NESTE BLOCO: {termos_card_html}</strong>\n"
                f"  </div>\n"
                f"  <div style='text-align: justify; line-height: 1.6; font-size: 14px; color: #333333;'>\n"
                f"      {texto_final}\n"
                f"  </div>\n"
                f"</div>"
            )

    # Identificação dos alvos encontrados (para salvar no BD e resumo)
    for num_pagina, pagina in enumerate(doc):
        texto_pagina = pagina.get_text()
        texto_limpo = " ".join(texto_pagina.split()).upper()

        for nome in FILTROS_MONITORAMENTO.get('nomes', []):
            nome_limpo = str(nome).strip().upper()
            if nome_limpo and re.compile(r'\s+'.join(re.escape(p) for p in nome_limpo.split())).search(texto_limpo):
                alvos_encontrados.append({'tipo': 'Nome', 'termo': nome.strip()})

        for saram in FILTROS_MONITORAMENTO.get('sarams', []):
            saram_limpo = str(saram).strip().upper()
            if saram_limpo and re.compile(rf"\b{re.escape(saram_limpo)}[\-\/]?\d*\b").search(texto_limpo):
                alvos_encontrados.append({'tipo': 'SARAM', 'termo': saram.strip()})

        for om in FILTROS_MONITORAMENTO.get('oms', []):
            om_limpa = str(om).strip().upper()
            if om_limpa and re.compile(rf"\b{re.escape(om_limpa)}\b").search(texto_limpo):
                alvos_encontrados.append({'tipo': 'Unidade (OM)', 'termo': om.strip()})

    doc.close()

    if not alvos_encontrados:
        conn.close()
        os.remove(caminho_arquivo)
        raise AirflowSkipException("Nenhum dos alvos monitorados foi encontrado no boletim de hoje. Push cancelado.")

    caminho_filtrado_pdf = os.path.join(DIRETORIO_TMP, f"BCA_{data_bca}_Filtrado.pdf")
    caminho_relatorio_html = os.path.join(DIRETORIO_TMP, f"Relatorio_BCA_{data_bca}.html")

    texto_final_cards = "".join(materias_extraidas)
    termos_unicos = sorted(list(set([alvo['termo'] for alvo in alvos_encontrados])))
    termos_destacados_html = " ".join([
        f'<span style="background-color: yellow; padding: 4px 8px; border-radius: 4px; font-weight: bold; color: black; display: inline-block; margin: 4px 4px 4px 0;">{t}</span>' 
        for t in termos_unicos
    ])

    corpo_email_completo = f"""
    <!DOCTYPE html>
    <html>
        <head>
            <meta charset="utf-8">
        </head>
        <body style="font-family: Arial, sans-serif; background-color: #f4f4f9; margin: 0; padding: 20px;">
            <div style="max-width: 800px; margin: 0 auto; background-color: #f4f4f9;">
                
                <div style="background-color: #004B87; color: #ffffff; padding: 25px; text-align: center; border-radius: 8px 8px 0 0;">
                    <h2 style="margin: 0; font-size: 22px; font-weight: normal;">Boletim do Comando da Aeronáutica (BCA)</h2>
                    <p style="margin: 8px 0 0 0; font-size: 14px; color: #e0e0e0; text-transform: uppercase;">Relatório de Alertas Monitorados</p>
                </div>
                
                <div style="background-color: #ffffff; padding: 30px; border-left: 1px solid #dddddd; border-right: 1px solid #dddddd;">
                    <p style="color: #333333; font-size: 15px; border-bottom: 1px dashed #cccccc; padding-bottom: 15px;">
                        Data de Publicação: <strong>{data_bca}</strong>
                    </p>
                    
                    <div style="background-color: #fcf8e3; border-left: 4px solid #faebcc; padding: 15px; margin-bottom: 25px; border-radius: 4px;">
                        <p style="color: #8a6d3b; font-size: 14px; margin-top: 0; margin-bottom: 10px;">
                            <strong>Termos de busca encontrados nesta edição:</strong>
                        </p>
                        <div>
                            {termos_destacados_html}
                        </div>
                    </div>

                    <p style="color: #555555; font-size: 14px; margin-bottom: 25px;">
                        Confira os recortes das matérias abaixo (os termos estão <mark style="background-color: yellow; padding: 2px 4px; border-radius: 3px; font-weight: bold; color: black;">destacados</mark>):
                    </p>
                    
                    {texto_final_cards}

                </div>
                
                <div style="background-color: #eeeeee; text-align: center; padding: 20px; font-size: 12px; color: #777777; border-radius: 0 0 8px 8px; border: 1px solid #dddddd; border-top: none;">
                    Este é um relatório automático gerado pelo Apache Airflow.<br><br>
                    <strong>Nota:</strong> Este documento foi gerado automaticamente a partir das matérias filtradas.
                </div>
                
            </div>
        </body>
    </html>
    """

    # 1. Salva o HTML
    with open(caminho_relatorio_html, "w", encoding="utf-8") as f_html:
        f_html.write(corpo_email_completo)

    # 2. Converte o HTML diretamente em PDF para que fiquem idênticos
    opcoes_pdf = {
        'encoding': 'UTF-8',
        'enable-local-file-access': None,
        'no-outline': None,
        'margin-top': '0mm',
        'margin-right': '0mm',
        'margin-bottom': '0mm',
        'margin-left': '0mm'
    }
    pdfkit.from_string(corpo_email_completo, caminho_filtrado_pdf, options=opcoes_pdf)

    novos_registros = []
    for alvo in alvos_encontrados:
        try:
            cursor.execute('''
                INSERT INTO publicacoes_v2 (tipo_alvo, termo_encontrado, data_bca)
                VALUES (?, ?, ?)
            ''', (alvo['tipo'], alvo['termo'], data_bca))
            
            if cursor.rowcount == 1:
                novos_registros.append(alvo)
        except sqlite3.Error:
            pass

    conn.commit()
    conn.close()

    if novos_registros:
        print("========== ALVOS ENCONTRADOS ==========")
        for reg in novos_registros:
            print(f"TIPO: {reg['tipo']} | TERMO ACHADO: {reg['termo']}")
        print("=======================================")
    
    return [caminho_filtrado_pdf, caminho_relatorio_html]

def enviar_email_bca(**kwargs):
    ti = kwargs['ti']
    arquivos = ti.xcom_pull(task_ids='extrair_e_filtrar_bca')
    data_bca = kwargs['data_interval_end'].strftime("%d-%m-%Y")
    
    if not arquivos:
        raise Exception("Nenhum arquivo encontrado para envio de e-mail.")

    if isinstance(arquivos, str):
        arquivos = [arquivos]

    arquivo_pdf = next((f for f in arquivos if f.endswith('.pdf')), None)
    arquivo_html = next((f for f in arquivos if f.endswith('.html')), None)
    
    email_dest = Variable.get("email_rec_bca", default_var=None)
    
    if not email_dest:
        print("Aviso: Variável 'email_rec_bca' não configurada. E-mail não será enviado.")
        return

    conteudo_html = ""
    if arquivo_html and os.path.exists(arquivo_html):
        with open(arquivo_html, 'r', encoding='utf-8') as f:
            conteudo_html = f.read()

    anexos = [arquivo_pdf] if arquivo_pdf and os.path.exists(arquivo_pdf) else None

    send_email(
        to=email_dest,
        subject=f"Monitoramento BCA - {data_bca}",
        html_content=conteudo_html,
        files=anexos
    )
    
    print(f"E-mail de notificação enviado com sucesso para: {email_dest}")

def enviar_para_github(**kwargs):
    ti = kwargs['ti']
    arquivos_para_enviar = ti.xcom_pull(task_ids='extrair_e_filtrar_bca')
    
    if not arquivos_para_enviar:
        raise Exception("Nenhum arquivo encontrado. A task anterior pode ter falhado ou pulado.")

    if isinstance(arquivos_para_enviar, str):
        arquivos_para_enviar = [arquivos_para_enviar]

    github_token = Variable.get("GITHUB_TOKEN_BCA", default_var="COLOQUE_SEU_TOKEN_AQUI")
    
    if github_token == "COLOQUE_SEU_TOKEN_AQUI":
         raise Exception("Token do GitHub não configurado nas Variáveis do Airflow!")

    headers = {
        "Authorization": f"Bearer {github_token}", 
        "Accept": "application/vnd.github.v3+json"
    }

    for caminho_arquivo in arquivos_para_enviar:
        if not os.path.exists(caminho_arquivo):
            continue

        nome_arquivo = os.path.basename(caminho_arquivo)
        url_api = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{nome_arquivo}"
        
        with open(caminho_arquivo, "rb") as f:
            conteudo_b64 = base64.b64encode(f.read()).decode("utf-8")
            
        response_get = requests.get(url_api, headers=headers)
        sha = response_get.json().get("sha") if response_get.status_code == 200 else None

        payload = {
            "message": f"Airflow: Adiciona {nome_arquivo}", 
            "content": conteudo_b64
        }
        if sha: payload["sha"] = sha
            
        print(f"Fazendo upload de {nome_arquivo} para {GITHUB_REPO}...")
        response_put = requests.put(url_api, headers=headers, data=json.dumps(payload))
        
        if response_put.status_code in (200, 201):
            print(f"Upload de {nome_arquivo} realizado com sucesso!")
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
    'download_processamento_bca',
    default_args=default_args,
    description='Pesquisa BCA, extrai HTML formatado e converte para PDF idêntico.',
    schedule_interval='0 * * * 1-5', 
    catchup=False,
    tags=['intraer', 'bca', 'scraping', 'alertas']
) as dag:

    task_baixar_bca = PythonOperator(
        task_id='baixar_bca_intraer',
        python_callable=executar_download_bca,
        provide_context=True,
    )

    task_filtrar_bca = PythonOperator(
        task_id='extrair_e_filtrar_bca',
        python_callable=extrair_e_filtrar_bca,
        provide_context=True,
    )
    
    task_enviar_email = PythonOperator(
        task_id='enviar_email_bca',
        python_callable=enviar_email_bca,
        provide_context=True,
    )

    task_git_push = PythonOperator(
        task_id='push_para_github',
        python_callable=enviar_para_github,
        provide_context=True,
    )

    task_baixar_bca >> task_filtrar_bca >> task_enviar_email >> task_git_push