# StreamGuard ETL

Processamento independente de métricas e processos do StreamGuard no Amazon S3:

```text
Bronze → validação e deduplicação → Silver → agregações → Gold
```

A transformação utiliza SQLite temporário em disco. A execução não depende da captura ou da API Web.

## Componentes

- `etl.py`: lê a Bronze, valida os registros, elimina duplicações, rejeita conflitos e publica a Silver e o relatório de qualidade.
- `gerar-gold.py`: lê a Silver e produz dados detalhados e resumos por servidor e janela de tempo.
- `processar.sh`: executa Silver e Gold em sequência, com trava contra processamento simultâneo.
- `instalar.sh`: prepara o ambiente Python e registra o serviço e o timer systemd.

## Instalação e configuração

Requer Python 3.10 ou superior. As dependências externas são `boto3` e `python-dotenv`; SQLite pertence à biblioteca padrão.

No PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

No Linux, use `python3 -m venv .venv` e `.venv/bin/python -m pip install -r requirements.txt`.

Crie um `.env` na raiz com base em `.env.example`, preservando configurações existentes:

| Variável | Uso |
|---|---|
| `S3_BUCKET` | Bucket de leitura e publicação, sem `s3://`. |
| `AWS_DEFAULT_REGION` | Região AWS; padrão `us-east-1`. |
| `AWS_PROFILE` | Perfil local opcional. Na EC2, omita para utilizar a função IAM. |
| `GOLD_JANELA_MINUTOS` | Janela Gold; padrão 5 minutos. |

As janelas aceitas são 1, 5, 10, 15, 20, 30 e 60 minutos. Os scripts carregam o `.env` deste repositório; variáveis já exportadas têm precedência. Não são necessárias credenciais de login da aplicação Web.

## Execução

Na raiz do repositório:

```powershell
.\.venv\Scripts\python.exe etl.py
.\.venv\Scripts\python.exe gerar-gold.py
```

Execute a Gold somente após o sucesso da Silver. No Linux, substitua o executável por `.venv/bin/python`, ou utilize `bash processar.sh` para coordenar a sequência. O script requer `flock`.

Esses comandos leem e publicam arquivos no S3 configurado.

## Contratos e tratamento

A Bronze utiliza os prefixos `bronze/monitoramento_v2_<id>/` e `bronze/processos_<id>/`. Novos lotes são liberados pelos marcadores em `bronze/lotes-concluidos/`, após conferir os tamanhos dos objetos. Arquivos históricos reconhecidos pelo nome SHA-256 também são aceitos.

| Camada | Arquivos |
|---|---|
| `silver/atual/` | `metricas.csv`, `processos.csv` |
| `gold/atual/` | `metricas_detalhadas.csv`, `resumo_metricas.csv`, `resumo_processos.csv` |
| `silver/qualidade/<execucao>/` | `rejeitados.csv` |

As identidades são `(servidor_id, timestamp, tipo)` para métricas e `(servidor_id, timestamp, pid)` para processos. Cópias idênticas são descartadas; conflitos excluem a identidade inteira da Silver, inclusive entre arquivos.

Campos de métricas vazios representam ausência; zero continua válido. Linhas com valores inválidos são rejeitadas integralmente. Percentuais gerais devem estar entre 0 e 100; CPU de processo pode ultrapassar 100%. PID 0 permanece na Silver e não integra o resumo Gold de processos.

Timestamps seguem `YYYY-MM-DD HH:MM:SS`, sem conversão de fuso. As unidades são `%`, `load`, `bytes` e `bytes/s`. As agregações separam servidores, métricas, unidades e janelas; CPU e RAM de processos não são somadas ao longo do tempo.

## Agendamento na EC2

O instalador requer Ubuntu, systemd, Python com suporte a `venv` e permissão de instalação. Utilize um caminho sem espaços, barras invertidas ou `%`.

Para conferir a configuração sem instalar serviços ou acessar a AWS:

```bash
bash instalar.sh --verificar
bash instalar.sh --validar-config
```

A primeira opção exibe as unidades planejadas; a segunda valida o `.env`. `PYTHON_ETL` seleciona o Python do instalador; o padrão é `python3`.

`bash instalar.sh` instala as dependências e registra `streamguard-etl.service` e `streamguard-etl.timer`, executados pelo usuário da instalação. O serviço utiliza este repositório como diretório de trabalho. O timer usa `OnBootSec=2min` e `OnUnitInactiveSec=5min`.

Consulte a execução com `sudo journalctl -u streamguard-etl.service -n 50 --no-pager`. A instalação conjunta de API/MySQL é coordenada pelo instalador da captura, com os três repositórios lado a lado.

## Limitações

A ETL relê toda a Bronze e publica arquivos sequencialmente. Silver sem registros válidos não substitui saídas anteriores; Gold exige ambos os CSVs Silver. A instalação, as permissões IAM e os consumidores dos arquivos precisam ser homologados na infraestrutura real.
