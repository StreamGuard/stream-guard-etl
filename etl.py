import io
import json
import os
import re
import tempfile
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

from tratamento import (
    CAMPOS_METRICAS, CAMPOS_PROCESSOS, TAMANHO_BLOCO,
    deduplicar, exportar_csv, ler_bronze, transformar_quadro,
)


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


def main(s3=None, bucket=None, tamanho_bloco=TAMANHO_BLOCO):
    load_dotenv(Path(__file__).resolve().parent / ".env")
    bucket = bucket or os.getenv("S3_BUCKET")
    if not bucket:
        raise ValueError("Configure S3_BUCKET no .env.")
    if tamanho_bloco <= 0:
        raise ValueError("O tamanho do bloco deve ser positivo.")
    if s3 is None:
        sessao = boto3.Session(
            profile_name=os.getenv("AWS_PROFILE") or None,
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )
        s3 = sessao.client("s3")

    execucao = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    contagem = dict(novo=0, repetido=0, conflito=0, invalidos=0, arquivos=0)
    partes = {"metricas": [], "processos": []}
    qualidade = []
    for categoria, chave in listar_arquivos_prontos(s3, bucket):
        print(f"Lendo: {chave}")
        resposta = s3.get_object(Bucket=bucket, Key=chave)
        with io.TextIOWrapper(resposta["Body"], encoding="utf-8-sig", newline="") as entrada:
            for brutos in ler_bronze(entrada, categoria, chave, qualidade,
                                     contagem["arquivos"], tamanho_bloco):
                tratados = transformar_quadro(brutos, categoria, qualidade)
                if not tratados.empty:
                    partes[categoria].append(tratados)
        contagem["arquivos"] += 1
    if not contagem["arquivos"]:
        raise ValueError("Nenhum CSV reconhecido na Bronze; execute enviar-bronze.py primeiro.")

    with tempfile.TemporaryDirectory() as temporario:
        pasta = Path(temporario)
        for categoria, colunas in (("metricas", CAMPOS_METRICAS), ("processos", CAMPOS_PROCESSOS)):
            quadro = pd.concat(partes[categoria], ignore_index=True) if partes[categoria] else pd.DataFrame()
            partes[categoria].clear()
            saida, contadores = deduplicar(quadro, categoria, qualidade)
            for nome, quantidade in contadores.items():
                contagem[nome] += quantidade
            if not saida.empty:
                caminho = pasta / f"{categoria}.csv"
                exportar_csv(saida, caminho, colunas)
                chave = f"silver/atual/{categoria}.csv"
                s3.upload_file(str(caminho), bucket, chave)
                print(f"Silver: {len(saida)} registros em {chave}")
            else:
                print(f"Sem registros válidos de {categoria}; destino anterior não foi alterado.")
            del quadro, saida
        contagem["invalidos"] = sum(r["_categoria"] == "invalido" for r in qualidade)
        relatorio = pd.DataFrame(qualidade, columns=[
            "origem", "linha", "motivo", "dados", "_arquivo", "_campo", "_categoria",
        ])
        relatorio = relatorio.sort_values(["_arquivo", "linha", "_campo"], kind="stable")
        caminho = pasta / "rejeitados.csv"
        exportar_csv(relatorio, caminho, ["origem", "linha", "motivo", "dados"])
        s3.upload_file(str(caminho), bucket, f"silver/qualidade/{execucao}/rejeitados.csv")
    print("Resumo:", contagem)


if __name__ == "__main__":
    main()
