import csv
import math
import os
import sqlite3
import tempfile
from contextlib import closing
from datetime import datetime
from pathlib import Path

import boto3
from dotenv import load_dotenv


def agrupar_periodo(timestamp, minutos):
    # Agrupa pelo horário registrado; não converte o fuso da captura.
    horario = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    minuto = (horario.minute // minutos) * minutos
    return horario.replace(minute=minuto, second=0).strftime("%Y-%m-%d %H:%M:%S")


def baixar_silver(s3, bucket, chave, destino):
    # Baixa uma cópia local da Silver para produzir a Gold nesta execução.
    s3.download_file(bucket, chave, str(destino))


def carregar_metricas(conexao, caminho, janela):
    # Mantém cada medição e acrescenta sua janela de análise.
    conexao.execute("""
        CREATE TABLE metricas (
            servidor_id INTEGER, timestamp TEXT, tipo TEXT,
            valor REAL, unidade TEXT, periodo TEXT,
            PRIMARY KEY (servidor_id, timestamp, tipo)
        )
    """)
    total = 0
    with caminho.open(newline="", encoding="utf-8-sig") as arquivo:
        leitor = csv.DictReader(arquivo)
        if leitor.fieldnames != ["servidor_id", "timestamp", "tipo", "valor", "unidade"]:
            raise ValueError("Cabeçalho inesperado em metricas.csv da Silver.")
        for linha in leitor:
            valor = float(linha["valor"])
            if not math.isfinite(valor) or valor < 0:
                raise ValueError("Valor inválido na Silver; interrompendo a Gold.")
            conexao.execute(
                "INSERT INTO metricas VALUES (?, ?, ?, ?, ?, ?)",
                (int(linha["servidor_id"]), linha["timestamp"], linha["tipo"],
                 valor, linha["unidade"], agrupar_periodo(linha["timestamp"], janela)),
            )
            total += 1
    return total


def carregar_processos(conexao, caminho, janela):
    # O PID 0 representa ociosidade no Windows e não entra no resumo de consumo.
    conexao.execute("""
        CREATE TABLE processos (
            servidor_id INTEGER, timestamp TEXT, pid INTEGER, nome TEXT,
            uso_cpu REAL, ram_bytes INTEGER, periodo TEXT,
            PRIMARY KEY (servidor_id, timestamp, pid)
        )
    """)
    total = 0
    ignorados = 0
    with caminho.open(newline="", encoding="utf-8-sig") as arquivo:
        leitor = csv.DictReader(arquivo)
        esperados = ["servidor_id", "timestamp", "pid", "nome", "status", "uso_cpu", "ram_bytes"]
        if leitor.fieldnames != esperados:
            raise ValueError("Cabeçalho inesperado em processos.csv da Silver.")
        for linha in leitor:
            if int(linha["pid"]) == 0:
                ignorados += 1
                continue
            cpu = float(linha["uso_cpu"])
            ram = int(linha["ram_bytes"])
            if not math.isfinite(cpu) or cpu < 0 or ram < 0:
                raise ValueError("Consumo de processo inválido na Silver.")
            conexao.execute(
                "INSERT INTO processos VALUES (?, ?, ?, ?, ?, ?, ?)",
                (int(linha["servidor_id"]), linha["timestamp"], int(linha["pid"]),
                 linha["nome"], cpu, ram, agrupar_periodo(linha["timestamp"], janela)),
            )
            total += 1
    return total, ignorados


def exportar_consulta(conexao, caminho, consulta):
    # Exporta o resultado linha por linha, sem carregar uma lista completa na RAM.
    cursor = conexao.execute(consulta)
    with caminho.open("w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.writer(arquivo)
        escritor.writerow([coluna[0] for coluna in cursor.description])
        escritor.writerows(cursor)


def main(s3=None, bucket=None, janela=None):
    # Reutiliza as configurações AWS dos scripts anteriores.
    load_dotenv(Path(__file__).resolve().parent / ".env")
    bucket = bucket or os.getenv("S3_BUCKET")
    if not bucket:
        raise ValueError("Configure S3_BUCKET no .env.")

    # A janela de análise é diferente do intervalo de captura definido no MySQL.
    janela = int(os.getenv("GOLD_JANELA_MINUTOS", "5")) if janela is None else janela
    if janela not in (1, 5, 10, 15, 20, 30, 60):
        raise ValueError("GOLD_JANELA_MINUTOS deve ser 1, 5, 10, 15, 20, 30 ou 60.")

    if s3 is None:
        sessao = boto3.Session(
            profile_name=os.getenv("AWS_PROFILE") or None,
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )
        s3 = sessao.client("s3")

    # Arquivos e banco temporários são removidos somente após fechar a conexão.
    with tempfile.TemporaryDirectory() as temporario:
        pasta = Path(temporario)
        metricas = pasta / "metricas.csv"
        processos = pasta / "processos.csv"
        baixar_silver(s3, bucket, "silver/atual/metricas.csv", metricas)
        baixar_silver(s3, bucket, "silver/atual/processos.csv", processos)

        with closing(sqlite3.connect(pasta / "gold.db")) as conexao:
            total_metricas = carregar_metricas(conexao, metricas, janela)
            total_processos, ignorados = carregar_processos(conexao, processos, janela)
            if not total_metricas:
                raise ValueError("Silver sem métricas; a Gold anterior não será substituída.")

            # Preserva os detalhes para correlações e outras análises em R.
            detalhes = pasta / "metricas_detalhadas.csv"
            exportar_consulta(conexao, detalhes, """
                SELECT servidor_id, timestamp, tipo, valor, unidade
                FROM metricas ORDER BY servidor_id, timestamp, tipo
            """)

            # Calcula estatísticas das amostras, sem misturar métricas ou unidades.
            resumo = pasta / "resumo_metricas.csv"
            exportar_consulta(conexao, resumo, """
                SELECT servidor_id, periodo AS inicio_periodo, tipo, unidade,
                       COUNT(*) AS quantidade_amostras,
                       MIN(timestamp) AS primeira_amostra,
                       MAX(timestamp) AS ultima_amostra,
                       AVG(valor) AS media_amostral,
                       MIN(valor) AS minimo, MAX(valor) AS maximo
                FROM metricas GROUP BY servidor_id, periodo, tipo, unidade
                ORDER BY servidor_id, periodo, tipo, unidade
            """)

            # Resume consumo observado; não soma RAM nem CPU ao longo do tempo.
            resumo_processos = pasta / "resumo_processos.csv"
            exportar_consulta(conexao, resumo_processos, """
                SELECT servidor_id, periodo AS inicio_periodo, pid, nome,
                       COUNT(*) AS quantidade_amostras,
                       MIN(timestamp) AS primeira_amostra,
                       MAX(timestamp) AS ultima_amostra,
                       AVG(uso_cpu) AS cpu_media_amostral,
                       MAX(uso_cpu) AS cpu_maxima,
                       AVG(ram_bytes) AS ram_media_bytes,
                       MAX(ram_bytes) AS ram_maxima_bytes
                FROM processos GROUP BY servidor_id, periodo, pid, nome
                ORDER BY servidor_id, periodo, pid, nome
            """)

        # Publica somente depois que todas as saídas foram calculadas com sucesso.
        for caminho in (detalhes, resumo, resumo_processos):
            chave = f"gold/atual/{caminho.name}"
            s3.upload_file(str(caminho), bucket, chave)
            print(f"Gold publicada: s3://{bucket}/{chave}")

        print(f"Janela: {janela} minutos")
        print(f"Medições: {total_metricas}; processos: {total_processos}; PID 0 excluídos: {ignorados}")

if __name__ == "__main__":
    main()