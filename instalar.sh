#!/usr/bin/env bash
set -euo pipefail

RAIZ="$(cd "$(dirname "$0")" && pwd)"
PYTHON_ETL="${PYTHON_ETL:-python3}"
USUARIO="$(id -un)"
GRUPO="$(id -gn 2>/dev/null || id -g)"
MODO="${1:-}"

if (( $# > 1 )) || [[ -n "$MODO" && "$MODO" != "--verificar" && "$MODO" != "--validar-config" ]]; then
    echo 'Uso: bash instalar.sh [--verificar|--validar-config]' >&2
    exit 2
fi

for arquivo in etl.py gerar-gold.py tratamento.py requirements.txt processar.sh; do
    [[ -f "$RAIZ/$arquivo" ]] || { echo "Arquivo ausente: $arquivo" >&2; exit 1; }
done
if [[ "$RAIZ" =~ [[:space:]] || "$RAIZ" == *%* || "$RAIZ" == *\\* ]]; then
    echo 'Para o serviço systemd, use um caminho Linux sem espaços, barras invertidas ou %.' >&2
    exit 1
fi

renderizar_servico() {
    cat <<SERVICE
[Unit]
Description=Processamento Silver e Gold do StreamGuard
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
User=$USUARIO
Group=$GRUPO
WorkingDirectory=$RAIZ
ExecStart=/usr/bin/bash $RAIZ/processar.sh
SERVICE
}

renderizar_timer() {
    cat <<'TIMER'
[Unit]
Description=Agendamento da ETL do StreamGuard

[Timer]
OnBootSec=2min
OnUnitInactiveSec=5min
Unit=streamguard-etl.service

[Install]
WantedBy=timers.target
TIMER
}

# Exibe os arquivos planejados sem instalar, acessar a AWS ou alterar serviços.
if [[ "$MODO" == "--verificar" ]]; then
    renderizar_servico
    renderizar_timer
    exit 0
fi

"$PYTHON_ETL" - "$RAIZ/.env" <<'PY'
from pathlib import Path
import re
import sys

caminho = Path(sys.argv[1])
if not caminho.is_file():
    raise SystemExit('Crie o .env da ETL com S3_BUCKET e AWS_DEFAULT_REGION antes de instalar.')

config = {}
for linha in caminho.read_text(encoding='utf-8-sig').splitlines():
    linha = linha.strip()
    if not linha or linha.startswith('#') or '=' not in linha:
        continue
    chave, valor = linha.split('=', 1)
    valor = valor.strip()
    if len(valor) >= 2 and valor[0] == valor[-1] and valor[0] in ('"', "'"):
        valor = valor[1:-1]
    else:
        valor = re.split(r'\s+#', valor, maxsplit=1)[0].rstrip()
    config[chave.strip()] = valor

bucket = config.get('S3_BUCKET', '')
if bucket == 'NOME_DO_SEU_BUCKET' or not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', bucket):
    raise SystemExit('Preencha S3_BUCKET no .env da ETL com um nome válido, sem s3://.')
if not re.fullmatch(r'[a-z]{2}(?:-[a-z]+)+-\d+', config.get('AWS_DEFAULT_REGION', '')):
    raise SystemExit('Preencha AWS_DEFAULT_REGION no .env da ETL com uma região válida.')
if 'AWS_PROFILE' in config:
    raise SystemExit('Na EC2, comente AWS_PROFILE no .env da ETL para utilizar a função IAM.')
janela = config.get('GOLD_JANELA_MINUTOS', '5')
if janela not in {'1', '5', '10', '15', '20', '30', '60'}:
    raise SystemExit('GOLD_JANELA_MINUTOS inválido no .env da ETL.')
print('Configuração da ETL validada localmente; acesso à AWS não foi testado.')
PY

if [[ "$MODO" == "--validar-config" ]]; then
    exit 0
fi
if [[ $EUID -eq 0 ]]; then
    echo 'Execute como usuário normal; somente a instalação dos serviços utiliza sudo.' >&2
    exit 1
fi

sudo -n true
cd "$RAIZ"
[[ -x .venv/bin/python ]] || "$PYTHON_ETL" -m venv .venv
.venv/bin/python -m pip install -r requirements.txt

renderizar_servico | sudo tee /etc/systemd/system/streamguard-etl.service >/dev/null
renderizar_timer | sudo tee /etc/systemd/system/streamguard-etl.timer >/dev/null
sudo systemctl daemon-reload
sudo systemctl enable --now streamguard-etl.timer
echo 'ETL agendada no novo repositório. O timer executará operações no S3.'
echo 'Acompanhe com: sudo journalctl -u streamguard-etl.service -n 50 --no-pager'
