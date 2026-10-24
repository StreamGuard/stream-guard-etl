import re
import csv
import io
import json
import math
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from contextlib import closing

import boto3
from dotenv import load_dotenv


# Relaciona as colunas da captura aos tipos e unidades das métricas.
METRICAS = {
    "uso_cpu": ("USO_CPU", "%"),
    "carga_1m": ("CARGA_CPU_1M", "load"),
    "carga_5m": ("CARGA_CPU_5M", "load"),
    "carga_15m": ("CARGA_CPU_15M", "load"),
    "uso_ram": ("USO_RAM", "%"),
    "ram_disponivel": ("RAM_DISPONIVEL", "bytes"),
    "uso_disco": ("USO_DISCO", "%"),
    "taxa_leitura_disco": ("TAXA_LEITURA_DISCO", "bytes/s"),
    "taxa_escrita_disco": ("TAXA_ESCRITA_DISCO", "bytes/s"),
    "taxa_envio_rede": ("TAXA_ENVIO_REDE", "bytes/s"),
    "taxa_recebimento_rede": ("TAXA_RECEBIMENTO_REDE", "bytes/s"),
}
CAMPOS_PROCESSOS = [
    "servidor_id", "timestamp", "pid", "nome", "status", "uso_cpu", "ram_bytes"
]


# Valida identificadores e quantidades inteiras.
def validar_inteiro(texto, minimo):
    valor = int(texto)
    if valor < minimo:
        raise ValueError(f"Inteiro menor que {minimo}: {texto}")
    return valor


# Rejeita números negativos, infinitos e percentuais gerais acima de 100.
def validar_numero(texto, percentual=False):
    valor = float(texto)
    if not math.isfinite(valor) or valor < 0:
        raise ValueError(f"Número inválido: {texto}")
    if percentual and valor > 100:
        raise ValueError(f"Percentual geral acima de 100: {texto}")
    return valor


# Cria uma tabela temporária em disco para comparar registros.
def preparar_banco(conexao):
    conexao.execute("""
        CREATE TABLE registros (
            categoria TEXT NOT NULL,
            identidade TEXT NOT NULL,
            dados TEXT NOT NULL,
            origem TEXT NOT NULL,
            conflito INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (categoria, identidade)
        )
    """)


# Identifica medições novas, repetidas ou com valores conflitantes.
def registrar(conexao, categoria, identidade, dados, origem):
    chave = json.dumps(identidade, ensure_ascii=False)
    conteudo = json.dumps(dados, ensure_ascii=False)
    anterior = conexao.execute(
        "SELECT dados FROM registros WHERE categoria = ? AND identidade = ?",
        (categoria, chave),
    ).fetchone()

    # Guarda a primeira ocorrência dessa identidade.
    if anterior is None:
        conexao.execute(
            "INSERT INTO registros (categoria, identidade, dados, origem) VALUES (?, ?, ?, ?)",
            (categoria, chave, conteudo, origem),
        )
        return "novo"

    # Uma cópia idêntica não precisa ser gravada novamente.
    if anterior[0] == conteudo:
        return "repetido"

    # Marca conflitos para excluir essa identidade da saída válida.
    conexao.execute(
        "UPDATE registros SET conflito = 1 WHERE categoria = ? AND identidade = ?",
        (categoria, chave),
    )
    return "conflito"


# Valida a linha bruta e prepara os registros no formato da Silver.
def transformar_linha(categoria, linha):
    if None in linha or any(valor is None for valor in linha.values()):
        raise ValueError("Quantidade de campos diferente do cabeçalho")

    servidor = validar_inteiro(linha["servidor_id"], 1)
    horario = datetime.strptime(linha["timestamp"], "%Y-%m-%d %H:%M:%S")
    timestamp = horario.strftime("%Y-%m-%d %H:%M:%S")

    # Preserva os campos do processo; sua CPU pode ultrapassar 100%.
    if categoria == "processos":
        pid = validar_inteiro(linha["pid"], 0)
        nome = linha["nome"].strip()
        status = linha["status"].strip()
        if not nome or not status:
            raise ValueError("Nome ou estado do processo vazio")
        cpu = validar_numero(linha["uso_cpu"])
        ram = validar_inteiro(linha["ram_bytes"], 0)
        dados = [servidor, timestamp, pid, nome, status, cpu, ram]
        return [([servidor, timestamp, pid], dados)]

    # Transforma cada métrica preenchida em uma medição separada.
    registros = []
    for campo, (tipo, unidade) in METRICAS.items():
        texto = linha[campo].strip()
        # Vazio significa ausência de medição; zero continua sendo válido.
        if not texto:
            continue
        if campo == "ram_disponivel":
            valor = validar_inteiro(texto, 0)
        else:
            valor = validar_numero(texto, campo in {"uso_cpu", "uso_ram", "uso_disco"})
        dados = [servidor, timestamp, tipo, valor, unidade]
        registros.append(([servidor, timestamp, tipo], dados))

    if not registros:
        raise ValueError("Linha sem nenhuma medição")
    return registros


# Lê o CSV linha por linha e registra problemas no relatório de qualidade.
def processar_csv(conexao, categoria, arquivo, origem, rejeitados, contagem):
    leitor = csv.DictReader(arquivo)
    esperados = CAMPOS_PROCESSOS if categoria == "processos" else [
        "servidor_id", "timestamp", *METRICAS
    ]
    # Confere nomes e quantidade de colunas antes de tratar os dados.
    if leitor.fieldnames is None or len(leitor.fieldnames) != len(esperados) or set(leitor.fieldnames) != set(esperados):
        rejeitados.writerow([origem, 1, "Cabeçalho incompatível", ""])
        contagem["invalidos"] += 1
        return

    for linha in leitor:
        try:
            registros = transformar_linha(categoria, linha)
        # Separa a linha inválida sem interromper as demais.
        except (ValueError, KeyError, TypeError, OverflowError) as erro:
            rejeitados.writerow([
                origem, leitor.line_num, str(erro), json.dumps(linha, ensure_ascii=False)
            ])
            contagem["invalidos"] += 1
            continue

        for identidade, dados in registros:
            resultado = registrar(conexao, categoria, identidade, dados, origem)
            contagem[resultado] += 1
            if resultado == "conflito":
                rejeitados.writerow([
                    origem, leitor.line_num,
                    "Valores diferentes para a mesma identidade; medição excluída da Silver",
                    json.dumps(dados, ensure_ascii=False),
                ])
    conexao.commit()


# Exporta somente registros sem conflito para um CSV tratado.
def exportar_csv(conexao, categoria, caminho, headers):
    total = 0
    with caminho.open("w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.writer(arquivo)
        escritor.writerow(headers)
        consulta = conexao.execute(
            "SELECT dados FROM registros WHERE categoria = ? AND conflito = 0 ORDER BY identidade",
            (categoria,),
        )
        for (dados,) in consulta:
            escritor.writerow(json.loads(dados))
            total += 1
    return total


def listar_arquivos_prontos(s3, bucket):
    # Primeiro identifica todos os lotes concluídos; a paginação pode ter várias respostas.
    paginas = s3.get_paginator("list_objects_v2")
    concluidos = {}
    for pagina in paginas.paginate(Bucket=bucket, Prefix="bronze/lotes-concluidos/"):
        for objeto in pagina.get("Contents", []):
            chave = objeto["Key"]
            if not chave.endswith(".json"):
                continue
            resposta = s3.get_object(Bucket=bucket, Key=chave)
            with closing(resposta["Body"]) as corpo:
                marcador = json.loads(corpo.read())
            lote = marcador["lote"]
            if chave != f"bronze/lotes-concluidos/{lote}.json":
                raise ValueError(f"Nome de marcador incompatível: {chave}")
            objetos = marcador["objetos"]
            if not objetos:
                raise ValueError(f"Marcador sem arquivos: {chave}")
            for item in objetos:
                arquivo = item["chave"]
                padrao = r"bronze/(monitoramento_v2_\d+|processos_\d+)/" + re.escape(lote) + r"\.csv"
                if not re.fullmatch(padrao, arquivo):
                    raise ValueError(f"Objeto inesperado no marcador: {arquivo}")
                # Confere todos os arquivos antes de liberar qualquer parte do lote.
                tamanho = s3.head_object(Bucket=bucket, Key=arquivo)["ContentLength"]
                if tamanho != item["bytes"]:
                    raise ValueError(f"Tamanho diferente do marcador: {arquivo}")
                concluidos[arquivo] = tamanho

    for pagina in paginas.paginate(Bucket=bucket, Prefix="bronze/"):
        for objeto in pagina.get("Contents", []):
            chave = objeto["Key"]
            # Reconhece os históricos antigos pelo nome SHA-256 do remetente anterior.
            antigo = re.fullmatch(r"bronze/(monitoramento_v2_\d+|processos_\d+)/[0-9a-f]{64}\.csv", chave)
            novo = chave in concluidos
            if not antigo and not novo:
                continue
            if novo and objeto["Size"] != concluidos[chave]:
                raise ValueError(f"Objeto alterado após a conferência: {chave}")
            categoria = "metricas" if chave.startswith("bronze/monitoramento_v2_") else "processos"
            yield categoria, chave


def main(s3=None, bucket=None):
    # Carrega bucket, perfil e região do arquivo .env.
    load_dotenv(Path(__file__).resolve().parent / ".env")
    bucket = bucket or os.getenv("S3_BUCKET")
    if not bucket:
        raise ValueError("Configure S3_BUCKET no .env.")

    # Usa as credenciais do perfil configurado para acessar a AWS.
    if s3 is None:
        sessao = boto3.Session(
            profile_name=os.getenv("AWS_PROFILE") or None,
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )
        # Cria o cliente que lista, lê e envia arquivos ao bucket.
        s3 = sessao.client("s3")
    # Identifica o relatório desta execução com um horário em UTC.
    execucao = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    contagem = dict(novo=0, repetido=0, conflito=0, invalidos=0, arquivos=0)

    # Remove os arquivos temporários ao terminar ou ocorrer um erro.
    with tempfile.TemporaryDirectory() as temporario:
        pasta = Path(temporario)
        with closing(sqlite3.connect(pasta / "registros.db")) as conexao:
            preparar_banco(conexao)
            caminho_rejeitados = pasta / "rejeitados.csv"
            with caminho_rejeitados.open("w", newline="", encoding="utf-8") as arquivo:
                rejeitados = csv.writer(arquivo)
                rejeitados.writerow(["origem", "linha", "motivo", "dados"])
                # Lê históricos antigos e apenas lotes novos com envio concluído.
                for categoria, chave in listar_arquivos_prontos(s3, bucket):
                    print(f"Lendo: {chave}")
                    resposta = s3.get_object(Bucket=bucket, Key=chave)
                    with io.TextIOWrapper(resposta["Body"], encoding="utf-8-sig", newline="") as entrada:
                        processar_csv(conexao, categoria, entrada, chave, rejeitados, contagem)
                    contagem["arquivos"] += 1

            if contagem["arquivos"] == 0:
                raise ValueError("Nenhum CSV reconhecido na Bronze; execute enviar-bronze.py primeiro.")

            # Define os cabeçalhos dos dois arquivos tratados.
            saidas = {
                "metricas": ["servidor_id", "timestamp", "tipo", "valor", "unidade"],
                "processos": CAMPOS_PROCESSOS,
            }
            for categoria, headers in saidas.items():
                caminho = pasta / f"{categoria}.csv"
                total = exportar_csv(conexao, categoria, caminho, headers)
                if total:
                    chave = f"silver/atual/{categoria}.csv"
                    # Publica a saída atual, substituindo a anterior nesse caminho.
                    s3.upload_file(str(caminho), bucket, chave)
                    print(f"Silver: {total} registros em {chave}")
                else:
                    print(f"Sem registros válidos de {categoria}; destino anterior não foi alterado.")

            # Salva rejeições e conflitos em um relatório por execução.
            s3.upload_file(
                str(caminho_rejeitados), bucket, f"silver/qualidade/{execucao}/rejeitados.csv"
            )
    print("Resumo:", contagem)

if __name__ == "__main__":
    main()
