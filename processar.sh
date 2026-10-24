#!/usr/bin/env bash
set -euo pipefail

# Executa na pasta do projeto.
cd "$(dirname "$0")"

# Impede dois processamentos simultâneos.
exec 9>.processamento.lock
flock -n 9 || exit 0

# Verifica se existem arquivos de captura na Bronze.
# Ausência de dados é diferente de falha de acesso à AWS.
if .venv/bin/python - <<'PY'
import os
import sys
import boto3
from dotenv import load_dotenv

load_dotenv(".env")

sessao = boto3.Session(
    profile_name=os.getenv("AWS_PROFILE") or None,
    region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1")
)
s3 = sessao.client("s3")

for pagina in s3.get_paginator("list_objects_v2").paginate(
    Bucket=os.environ["S3_BUCKET"], Prefix="bronze/"
):
    for objeto in pagina.get("Contents", []):
        chave = objeto["Key"]
        if chave.startswith(("bronze/monitoramento_v2_", "bronze/processos_")) and chave.endswith(".csv"):
            sys.exit(0)

sys.exit(10)
PY
then
    echo 'Arquivos de captura encontrados. Iniciando Silver e Gold.'
else
    codigo=$?
    if [[ "$codigo" == 10 ]]; then
        echo 'Bronze sem capturas. Aguardando o primeiro lote.'
        exit 0
    fi
    echo 'Falha ao consultar a Bronze. Confira o erro acima.' >&2
    exit "$codigo"
fi

# Gera a Gold somente se o tratamento da Silver terminar sem erro.
.venv/bin/python -u etl.py
.venv/bin/python -u gerar-gold.py
