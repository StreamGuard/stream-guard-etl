import os
import tempfile
from pathlib import Path

import boto3
from dotenv import load_dotenv

from tratamento import CAMPOS_METRICAS, TAMANHO_BLOCO, exportar_csv, ler_silver


def resumir_metricas(metricas):
    return metricas.groupby(
        ["servidor_id", "inicio_periodo", "tipo", "unidade"], sort=True, dropna=False
    ).agg(
        quantidade_amostras=("valor", "size"),
        primeira_amostra=("timestamp", "min"),
        ultima_amostra=("timestamp", "max"),
        media_amostral=("valor", "mean"),
        minimo=("valor", "min"),
        maximo=("valor", "max"),
    ).reset_index()


def resumir_processos(processos):
    return processos.groupby(
        ["servidor_id", "inicio_periodo", "pid", "nome"], sort=True, dropna=False
    ).agg(
        quantidade_amostras=("uso_cpu", "size"),
        primeira_amostra=("timestamp", "min"),
        ultima_amostra=("timestamp", "max"),
        cpu_media_amostral=("uso_cpu", "mean"),
        cpu_maxima=("uso_cpu", "max"),
        ram_media_bytes=("ram_bytes", "mean"),
        ram_maxima_bytes=("ram_bytes", "max"),
    ).reset_index()


def main(s3=None, bucket=None, janela=None, tamanho_bloco=TAMANHO_BLOCO):
    load_dotenv(Path(__file__).resolve().parent / ".env")
    bucket = bucket or os.getenv("S3_BUCKET")
    if not bucket:
        raise ValueError("Configure S3_BUCKET no .env.")
    janela = int(os.getenv("GOLD_JANELA_MINUTOS", "5")) if janela is None else janela
    if janela not in (1, 5, 10, 15, 20, 30, 60):
        raise ValueError("GOLD_JANELA_MINUTOS deve ser 1, 5, 10, 15, 20, 30 ou 60.")
    if tamanho_bloco <= 0:
        raise ValueError("O tamanho do bloco deve ser positivo.")
    if s3 is None:
        sessao = boto3.Session(
            profile_name=os.getenv("AWS_PROFILE") or None,
            region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        )
        s3 = sessao.client("s3")

    with tempfile.TemporaryDirectory() as temporario:
        pasta = Path(temporario)
        for nome in ("metricas.csv", "processos.csv"):
            s3.download_file(bucket, f"silver/atual/{nome}", str(pasta / nome))
        metricas, _ = ler_silver(pasta / "metricas.csv", "metricas", janela, tamanho_bloco)
        processos, ignorados = ler_silver(pasta / "processos.csv", "processos", janela, tamanho_bloco)
        if metricas.empty:
            raise ValueError("Silver sem métricas; a Gold anterior não será substituída.")

        detalhes = metricas.sort_values(["servidor_id", "timestamp", "tipo"])[CAMPOS_METRICAS]
        resumo = resumir_metricas(metricas)
        resumo_processos = resumir_processos(processos)
        for quadro, nome in (
            (detalhes, "metricas_detalhadas.csv"),
            (resumo, "resumo_metricas.csv"),
            (resumo_processos, "resumo_processos.csv"),
        ):
            exportar_csv(quadro, pasta / nome, list(quadro.columns))

        # Todas as saídas são calculadas antes da primeira publicação.
        for nome in ("metricas_detalhadas.csv", "resumo_metricas.csv", "resumo_processos.csv"):
            chave = f"gold/atual/{nome}"
            s3.upload_file(str(pasta / nome), bucket, chave)
            print(f"Gold publicada: s3://{bucket}/{chave}")
        print(f"Janela: {janela} minutos")
        print(f"Medições: {len(metricas)}; processos: {len(processos)}; PID 0 excluídos: {ignorados}")


if __name__ == "__main__":
    main()
