"""ETL do StreamGuard em uma única função Lambda.

Fluxo, a cada execução (agendada a cada 5 minutos):
    Bronze (lotes novos) -> Silver (um arquivo por empresa e dia) -> Gold (CSV + JSON por empresa)

Layout no bucket:
    bronze/<empresa>/monitoramento_v2_<id>/<lote>.csv
    bronze/<empresa>/processos_<id>/<lote>.csv
    bronze/<empresa>/lotes-concluidos/<AAAA-MM-DD>/<lote>.json     (marcador de lote completo)
    silver/<empresa>/<dia>/metricas.csv, processos.csv
    silver/qualidade/<execucao>/resumo.json                        (uma por execução)
    silver/qualidade/<execucao>/<empresa>/rejeitados.csv           (só se houve rejeição)
    gold/<empresa>/atual/...                                        (sobrescrita)
    gold/<empresa>/historico/<dia>/...                              (5 min, um dia por pasta)
    estado/processamento.json                                       (ponto de retomada)
"""
import io
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

from tratamento import (
    CAMPOS_METRICAS, CAMPOS_PROCESSOS, CHAVES, FORMATO_DATA, TAMANHO_BLOCO,
    deduplicar, ler_bronze, transformar_quadro,
)

ARQUIVOS_GOLD = ("resumo_metricas", "resumo_processos")
HORAS_ATUAL_METRICAS = 48     # resolução de 5 min
HORAS_ATUAL_PROCESSOS = 6
DIAS_RESUMO_1H = 7
DIAS_RESUMO_6H = 30
MARGEM_SEGUNDOS = 120         # recua o ponto de retomada; reprocessar um lote não duplica nada


# ---------------------------------------------------------------- acesso ao S3
def criar_s3():
    import boto3
    return boto3.client("s3")


def listar(s3, bucket, prefixo, delimitador=None):
    """Devolve (objetos, subpastas) de um prefixo, percorrendo todas as páginas."""
    objetos, pastas = [], []
    argumentos = {"Bucket": bucket, "Prefix": prefixo}
    if delimitador:
        argumentos["Delimiter"] = delimitador
    for pagina in s3.get_paginator("list_objects_v2").paginate(**argumentos):
        objetos += pagina.get("Contents", [])
        pastas += [p["Prefix"] for p in pagina.get("CommonPrefixes", [])]
    return objetos, pastas


def ler_bytes(s3, bucket, chave):
    return s3.get_object(Bucket=bucket, Key=chave)["Body"].read()


def ler_json(s3, bucket, chave):
    try:
        return json.loads(ler_bytes(s3, bucket, chave))
    except s3.exceptions.NoSuchKey:
        return None


def gravar(s3, bucket, chave, conteudo, tipo):
    if isinstance(conteudo, str):
        conteudo = conteudo.encode("utf-8")
    s3.put_object(Bucket=bucket, Key=chave, Body=conteudo, ContentType=tipo)


def gravar_csv_json(s3, bucket, base, quadro):
    """Grava o mesmo quadro em CSV (fica no Data Lake) e em JSON (o Node lê este)."""
    saida = quadro.copy()
    gravar(s3, bucket, f"{base}.csv", saida.to_csv(index=False, lineterminator="\r\n"), "text/csv")
    # servidor_id vai como texto no JSON: é o formato que as dashboards já usavam ao ler o CSV.
    saida["servidor_id"] = saida["servidor_id"].astype(str)
    gravar(s3, bucket, f"{base}.json", saida.to_json(orient="records", force_ascii=False), "application/json")


def ler_csv_s3(s3, bucket, chave):
    try:
        corpo = ler_bytes(s3, bucket, chave)
    except s3.exceptions.NoSuchKey:
        return None
    return pd.read_csv(io.BytesIO(corpo), encoding="utf-8-sig")


# ---------------------------------------------------------------- datas
def inicio_da_janela(timestamps, minutos):
    datas = pd.to_datetime(timestamps, format=FORMATO_DATA, errors="coerce")
    return datas.dt.floor(f"{minutos}min").dt.strftime(FORMATO_DATA)


def dias_utc(agora, quantidade):
    return [(agora - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(quantidade)]


# ---------------------------------------------------------------- Bronze -> dados tratados
def ler_lote(s3, bucket, marcador, qualidade, contagem, empresa, nome_lote):
    """Lê os CSVs de um lote e devolve {'metricas': [...], 'processos': [...]} já tratados."""
    partes = {"metricas": [], "processos": []}
    if type(marcador.get("empresa_id")) is not int or str(marcador["empresa_id"]) != empresa:
        raise ValueError("Empresa do marcador difere do caminho")
    if marcador.get("lote") != nome_lote:
        raise ValueError("Nome do lote difere do marcador")
    identidade = re.search(r"_empresa_([1-9][0-9]*)_servidor_([1-9][0-9]*)_", nome_lote)
    if not identidade or identidade.group(1) != empresa:
        raise ValueError("Nome do lote sem identidade compatível")
    servidor = identidade.group(2)
    objetos = marcador.get("objetos")
    if not isinstance(objetos, list) or not objetos:
        raise ValueError("Marcador sem arquivos")
    arquivos = []
    vistos = set()
    for item in objetos:
        chave = item["chave"]
        padrao = rf"bronze/{empresa}/(monitoramento_v2|processos)_{servidor}/{re.escape(nome_lote)}\.csv"
        tipo = re.fullmatch(padrao, chave)
        if not tipo or chave in vistos:
            raise ValueError("Objeto fora da identidade do lote ou repetido")
        vistos.add(chave)
        corpo = ler_bytes(s3, bucket, chave)
        if type(item.get("bytes")) is not int or item["bytes"] < 0 or len(corpo) != item["bytes"]:
            raise ValueError("Tamanho do objeto difere do marcador")
        categoria = "metricas" if tipo.group(1) == "monitoramento_v2" else "processos"
        arquivos.append((chave, categoria, corpo.decode("utf-8-sig")))
    # Nenhuma parte do lote é incorporada antes de conferir todos os objetos.
    for chave, categoria, texto in arquivos:
        for brutos in ler_bronze(io.StringIO(texto, newline=""), categoria, chave, qualidade,
                                 contagem["arquivos"], TAMANHO_BLOCO):
            ids = pd.to_numeric(brutos["servidor_id"], errors="coerce")
            if (ids.gt(0) & ids.ne(int(servidor))).any():
                raise ValueError("Servidor do CSV difere da identidade do lote")
            antes = len(qualidade)
            tratados = transformar_quadro(brutos, categoria, qualidade)
            contagem["lidas"] += len(brutos)
            contagem["rejeitadas_tratamento"] += len(qualidade) - antes
            if not tratados.empty:
                partes[categoria].append(tratados)
        contagem["arquivos"] += 1
    return partes



def tipar(quadro, categoria):
    """Os quadros chegam com colunas genéricas; aqui ficam com os tipos certos."""
    colunas = CAMPOS_METRICAS if categoria == "metricas" else CAMPOS_PROCESSOS
    quadro = quadro[colunas].copy()
    quadro["servidor_id"] = quadro["servidor_id"].astype("int64")
    if categoria == "metricas":
        quadro["valor"] = quadro["valor"].astype("float64")
    else:
        quadro["pid"] = quadro["pid"].astype("int64")
        quadro["uso_cpu"] = quadro["uso_cpu"].astype("float64")
        quadro["ram_bytes"] = quadro["ram_bytes"].astype("int64")
    return quadro


# ---------------------------------------------------------------- Silver (empresa + dia)
def atualizar_silver(s3, bucket, empresa, dia, categoria, novos, qualidade):
    """Junta as linhas novas ao arquivo do dia. Linha que já existe é ignorada (vale a primeira)."""
    chave = f"silver/{empresa}/{dia}/{categoria}.csv"
    colunas = CAMPOS_METRICAS if categoria == "metricas" else CAMPOS_PROCESSOS
    chaves = CHAVES[categoria]
    existente = ler_csv_s3(s3, bucket, chave)
    if existente is not None:
        existente = tipar(existente, categoria)
        ja_existe = pd.MultiIndex.from_frame(novos[chaves]).isin(pd.MultiIndex.from_frame(existente[chaves]))
        novos = novos.loc[~ja_existe]
        combinado = pd.concat([existente, novos], ignore_index=True)
    else:
        combinado = novos
    if not len(novos):
        return combinado
    combinado = combinado.sort_values(chaves, kind="stable")
    gravar(s3, bucket, chave, combinado.to_csv(index=False, lineterminator="\r\n"), "text/csv")
    return combinado


# ---------------------------------------------------------------- Gold
def resumir_metricas(metricas, janela):
    metricas = metricas.assign(inicio_periodo=inicio_da_janela(metricas["timestamp"], janela))
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


def resumir_processos(processos, janela):
    processos = processos.loc[processos["pid"] != 0]  # PID 0 (ocioso) não entra no Gold
    processos = processos.assign(inicio_periodo=inicio_da_janela(processos["timestamp"], janela))
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


def reagrupar(resumo5, minutos):
    """Junta janelas de 5 min em janelas maiores. A média é ponderada pelo número de amostras."""
    if resumo5.empty:
        return resumo5
    quadro = resumo5.assign(
        inicio_periodo=inicio_da_janela(resumo5["inicio_periodo"], minutos),
        soma=resumo5["media_amostral"] * resumo5["quantidade_amostras"],
    )
    agrupado = quadro.groupby(["servidor_id", "inicio_periodo", "tipo", "unidade"], sort=True).agg(
        quantidade_amostras=("quantidade_amostras", "sum"),
        primeira_amostra=("primeira_amostra", "min"),
        ultima_amostra=("ultima_amostra", "max"),
        soma=("soma", "sum"),
        minimo=("minimo", "min"),
        maximo=("maximo", "max"),
    ).reset_index()
    agrupado["media_amostral"] = agrupado["soma"] / agrupado["quantidade_amostras"]
    return agrupado[["servidor_id", "inicio_periodo", "tipo", "unidade", "quantidade_amostras",
                     "primeira_amostra", "ultima_amostra", "media_amostral", "minimo", "maximo"]]


def recortar(quadro, horas, agora):
    """Recorta pelo relógio da execução, no mesmo horário brasileiro dos dados."""
    if quadro.empty:
        return quadro
    datas = pd.to_datetime(quadro["inicio_periodo"], format=FORMATO_DATA)
    referencia = pd.Timestamp(agora.astimezone(timezone(timedelta(hours=-3))).replace(tzinfo=None))
    return quadro.loc[(datas >= referencia - pd.Timedelta(hours=horas)) & (datas <= referencia)]



def ler_historico(s3, bucket, empresa, arquivo, dias, em_memoria):
    """Junta os resumos de 5 min dos dias pedidos; usa o que acabou de ser calculado, sem reler."""
    partes = []
    for dia in dias:
        quadro = em_memoria.get((dia, arquivo))
        if quadro is None:
            quadro = ler_csv_s3(s3, bucket, f"gold/{empresa}/historico/{dia}/{arquivo}.csv")
        if quadro is not None and len(quadro):
            partes.append(quadro)
    return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()


def atualizar_gold(s3, bucket, empresa, dados_dia, janela, agora):
    """dados_dia: {dia: {'metricas': quadro, 'processos': quadro}} dos dias que mudaram."""
    em_memoria = {}
    for dia, quadros in dados_dia.items():
        base = f"gold/{empresa}/historico/{dia}"
        if len(quadros["metricas"]):
            resumo = resumir_metricas(quadros["metricas"], janela)
            em_memoria[(dia, "resumo_metricas")] = resumo
            gravar_csv_json(s3, bucket, f"{base}/resumo_metricas", resumo)
        if len(quadros["processos"]):
            resumo = resumir_processos(quadros["processos"], janela)
            em_memoria[(dia, "resumo_processos")] = resumo
            gravar_csv_json(s3, bucket, f"{base}/resumo_processos", resumo)

    # A pasta "atual" é refeita a partir do histórico (5 min), do dia mais novo para o mais antigo.
    _, pastas = listar(s3, bucket, f"gold/{empresa}/historico/", "/")
    dias = sorted((p.rstrip("/").split("/")[-1] for p in pastas), reverse=True)

    hoje = agora.astimezone(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d")
    limite = (agora.astimezone(timezone(timedelta(hours=-3))) - timedelta(days=30)).strftime("%Y-%m-%d")
    dias = [dia for dia in dias if limite <= dia <= hoje]
    metricas5 = ler_historico(s3, bucket, empresa, "resumo_metricas", dias, em_memoria)
    if metricas5.empty:
        metricas5 = resumir_metricas(pd.DataFrame(columns=CAMPOS_METRICAS), janela)
    base = f"gold/{empresa}/atual"
    gravar_csv_json(s3, bucket, f"{base}/resumo_metricas",
                    recortar(metricas5, HORAS_ATUAL_METRICAS, agora).sort_values(["servidor_id", "inicio_periodo", "tipo"]))
    gravar_csv_json(s3, bucket, f"{base}/resumo_metricas_1h",
                    reagrupar(recortar(metricas5, DIAS_RESUMO_1H * 24, agora), 60))
    gravar_csv_json(s3, bucket, f"{base}/resumo_metricas_6h",
                    reagrupar(recortar(metricas5, DIAS_RESUMO_6H * 24, agora), 360))
    processos = ler_historico(s3, bucket, empresa, "resumo_processos", dias, em_memoria)
    if processos.empty:
        processos = resumir_processos(pd.DataFrame(columns=CAMPOS_PROCESSOS), janela)
    gravar_csv_json(s3, bucket, f"{base}/resumo_processos",
                    recortar(processos, HORAS_ATUAL_PROCESSOS, agora).sort_values(["servidor_id", "inicio_periodo", "pid"]))



# ---------------------------------------------------------------- limpeza
def apagar_prefixo(s3, bucket, prefixo):
    objetos, _ = listar(s3, bucket, prefixo)
    for i in range(0, len(objetos), 1000):
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": o["Key"]} for o in objetos[i:i + 1000]]})


def limpar_antigos(s3, bucket, empresa, hoje, retencao_dias):
    hoje = hoje.astimezone(timezone(timedelta(hours=-3)))
    limite = (hoje - timedelta(days=retencao_dias)).strftime("%Y-%m-%d")
    for raiz in (f"silver/{empresa}/", f"gold/{empresa}/historico/"):
        _, pastas = listar(s3, bucket, raiz, "/")
        for pasta in pastas:
            nome = pasta.rstrip("/").split("/")[-1]
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", nome) and nome < limite:
                apagar_prefixo(s3, bucket, pasta)


# ---------------------------------------------------------------- execução
def lotes_novos(s3, bucket, ate, agora, dias_busca):
    """Marcadores de lote (por empresa) concluídos desde a última execução."""
    _, pastas = listar(s3, bucket, "bronze/", "/")
    empresas = sorted(p.split("/")[1] for p in pastas if re.fullmatch(r"bronze/\d+/", p))
    novos = {e: [] for e in empresas}
    for empresa in empresas:
        quantidade = max(dias_busca, (agora.date() - ate.date()).days + 1) if ate is not None else dias_busca
        for dia in dias_utc(agora, quantidade):
            objetos, _ = listar(s3, bucket, f"bronze/{empresa}/lotes-concluidos/{dia}/")
            for objeto in objetos:
                if objeto["Key"].endswith(".json") and (ate is None or objeto["LastModified"] >= ate):
                    novos[empresa].append(objeto["Key"])
    return novos


def processar_empresa(s3, bucket, empresa, marcadores, janela, qualidade, contagem, agora):
    """Lotes da empresa -> Silver dos dias afetados -> Gold. Devolve a pasta de dias alterados."""
    partes = {"metricas": [], "processos": []}
    for chave in sorted(marcadores):
        marcador = json.loads(ler_bytes(s3, bucket, chave))
        lote = ler_lote(s3, bucket, marcador, qualidade, contagem, empresa, chave.rsplit("/", 1)[-1][:-5])
        for categoria in partes:
            partes[categoria] += lote[categoria]
        contagem["lotes"] += 1

    dados_dia = {}
    for categoria, colunas in (("metricas", CAMPOS_METRICAS), ("processos", CAMPOS_PROCESSOS)):
        if not partes[categoria]:
            continue
        quadro = pd.concat(partes[categoria], ignore_index=True)
        saida, _ = deduplicar(quadro, categoria, qualidade)   # repetidos dentro dos lotes novos
        if saida.empty:
            continue
        saida = tipar(saida, categoria)
        for dia, doDia in saida.groupby(saida["timestamp"].str[:10]):
            combinado = atualizar_silver(s3, bucket, empresa, dia, categoria, doDia, qualidade)
            dados_dia.setdefault(dia, {"metricas": pd.DataFrame(), "processos": pd.DataFrame()})
            dados_dia[dia][categoria] = combinado
    atualizar_gold(s3, bucket, empresa, dados_dia, janela, agora)


def gravar_rejeitados(s3, bucket, execucao, qualidade_por_empresa):
    for empresa, qualidade in qualidade_por_empresa.items():
        if not qualidade:
            continue
        relatorio = pd.DataFrame(qualidade, columns=[
            "origem", "linha", "motivo", "dados", "_arquivo", "_campo", "_categoria"])
        relatorio = relatorio.sort_values(["_arquivo", "linha", "_campo"], kind="stable")
        texto = relatorio[["origem", "linha", "motivo", "dados"]].to_csv(index=False, lineterminator="\r\n")
        gravar(s3, bucket, f"silver/qualidade/{execucao}/{empresa}/rejeitados.csv", texto, "text/csv")


def executar(s3=None, bucket=None, event=None, agora=None):
    event = event or {}
    bucket = bucket or os.environ["S3_BUCKET"]
    s3 = s3 or criar_s3()
    janela = int(os.getenv("GOLD_JANELA_MINUTOS", "5"))
    if janela not in (1, 5, 10, 15, 20, 30, 60):
        raise ValueError("GOLD_JANELA_MINUTOS deve ser 1, 5, 10, 15, 20, 30 ou 60.")
    retencao = int(os.getenv("RETENCAO_DIAS", "90"))

    agora = agora or datetime.now(timezone.utc)
    cronometro = time.monotonic()
    execucao = agora.strftime("%Y%m%dT%H%M%S%fZ")

    estado = ler_json(s3, bucket, "estado/processamento.json") or {}
    ate = datetime.fromisoformat(estado["ate"]) if estado.get("ate") else None
    # "dias" no evento permite reprocessar o passado: {"dias": 7}
    dias_busca = int(event.get("dias", 2))
    if event.get("dias"):
        ate = None

    resumo = {"execucao": execucao, "inicio": agora.isoformat(), "lotes": 0, "linhas_validas": 0,
              "linhas_rejeitadas": 0, "situacao": "concluida", "observacao": "", "empresas": {}}
    qualidade_por_empresa = {}
    houve_erro = False

    for empresa, marcadores in lotes_novos(s3, bucket, ate, agora, dias_busca).items():
        qualidade = qualidade_por_empresa.setdefault(empresa, [])
        contagem = dict(arquivos=0, lidas=0, rejeitadas_tratamento=0, lotes=0)
        try:
            processar_empresa(s3, bucket, empresa, marcadores, janela, qualidade, contagem, agora)
            limpar_antigos(s3, bucket, empresa, agora, retencao)
            rejeitadas = len(qualidade)
            validas = max(contagem["lidas"] - contagem["rejeitadas_tratamento"], 0)
            resumo["empresas"][empresa] = {"lotes": contagem["lotes"], "linhas_validas": validas,
                                           "linhas_rejeitadas": rejeitadas}
            resumo["lotes"] += contagem["lotes"]
            resumo["linhas_validas"] += validas
            resumo["linhas_rejeitadas"] += rejeitadas
        except Exception as erro:  # uma empresa com problema não impede as outras
            houve_erro = True
            resumo["situacao"] = "falhou"
            resumo["observacao"] += f"empresa {empresa}: {type(erro).__name__}: {erro}; "
            print(f"Falha na empresa {empresa}: {erro}")

    gravar_rejeitados(s3, bucket, execucao, qualidade_por_empresa)
    resumo["duracao_s"] = round(time.monotonic() - cronometro)
    gravar(s3, bucket, f"silver/qualidade/{execucao}/resumo.json",
           json.dumps(resumo, ensure_ascii=False), "application/json")

    # Só avança o ponto de retomada se tudo deu certo; senão a próxima execução tenta de novo.
    if not houve_erro:
        gravar(s3, bucket, "estado/processamento.json",
               json.dumps({"ate": (agora - timedelta(seconds=MARGEM_SEGUNDOS)).isoformat()}), "application/json")
    print("Resumo:", json.dumps(resumo, ensure_ascii=False))
    return resumo


def lambda_handler(event, context):
    return executar(event=event)
