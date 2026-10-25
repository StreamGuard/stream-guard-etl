import csv
import json
import math
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

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
CAMPOS_METRICAS = ["servidor_id", "timestamp", "tipo", "valor", "unidade"]
CAMPOS_PROCESSOS = ["servidor_id", "timestamp", "pid", "nome", "status", "uso_cpu", "ram_bytes"]
CHAVES = {
    "metricas": ["servidor_id", "timestamp", "tipo"],
    "processos": ["servidor_id", "timestamp", "pid"],
}
FORMATO_DATA = "%Y-%m-%d %H:%M:%S"
TAMANHO_BLOCO = 10_000


def validar_inteiro(texto, minimo=None):
    valor = int(texto)
    if minimo is not None and valor < minimo:
        raise ValueError(f"Inteiro menor que {minimo}: {texto}")
    return valor


def validar_numero(texto, percentual=False):
    valor = float(texto)
    if not math.isfinite(valor) or valor < 0:
        raise ValueError(f"Número inválido: {texto}")
    if percentual and valor > 100:
        raise ValueError(f"Percentual geral acima de 100: {texto}")
    return valor


def converter_inteiros(serie, minimo=None):
    textos = serie.fillna("").astype(str).str.strip()
    # O caminho comum mantém a precisão; int() cobre sintaxes e inteiros maiores.
    simples = textos.str.fullmatch(r"[+-]?[0-9]{1,18}")
    valores = pd.to_numeric(textos.where(simples), errors="coerce", dtype_backend="numpy_nullable").astype(object)
    if minimo is not None:
        valores.loc[simples & valores.lt(minimo).fillna(False)] = pd.NA
    for indice, texto in textos.loc[~simples & textos.ne("")].items():
        try:
            valores.at[indice] = validar_inteiro(texto, minimo)
        except (ValueError, TypeError, OverflowError):
            valores.at[indice] = pd.NA
    return valores


def converter_numeros(serie, percentual=False):
    textos = serie.fillna("").astype(str).str.strip()
    numeros = pd.to_numeric(textos, errors="coerce")
    reconhecidos = numeros.notna()
    valores = pd.Series(np.nan, index=serie.index, dtype="float64")
    # astype usa a conversão de float do Python, preservando a precisão da captura.
    valores.loc[reconhecidos] = textos.loc[reconhecidos].astype("float64")
    for indice, texto in textos.loc[~reconhecidos & textos.ne("")].items():
        try:
            valores.at[indice] = validar_numero(texto, percentual)
        except (ValueError, TypeError, OverflowError):
            pass
    invalidos = ~np.isfinite(valores) | valores.lt(0)
    if percentual:
        invalidos |= valores.gt(100)
    return valores.mask(invalidos)


def converter_timestamps(serie):
    padrao = r"[0-9]{4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}"
    serie = serie.fillna("").astype(str)
    simples = serie.str.fullmatch(padrao)
    datas = pd.to_datetime(serie.where(simples), format=FORMATO_DATA, errors="coerce")
    valores = datas.dt.strftime(FORMATO_DATA).astype(object)
    # Preserva datas aceitas por strptime fora do intervalo datetime64[ns].
    for indice, texto in serie.loc[datas.isna()].items():
        try:
            valores.at[indice] = datetime.strptime(texto, FORMATO_DATA).strftime(FORMATO_DATA)
        except (ValueError, TypeError, OverflowError):
            valores.at[indice] = pd.NA
    return valores


def json_dados(valores):
    return json.dumps(
        [v.item() if isinstance(v, np.generic) else v for v in valores],
        ensure_ascii=False,
    )


def exportar_csv(quadro, caminho, colunas):
    quadro.to_csv(caminho, columns=colunas, index=False, encoding="utf-8", lineterminator="\r\n")


def rejeitar(qualidade, origem, linha, motivo, dados, arquivo, campo=-1, categoria="invalido"):
    qualidade.append({
        "origem": origem, "linha": linha, "motivo": motivo, "dados": dados,
        "_arquivo": arquivo, "_campo": campo, "_categoria": categoria,
    })


def ler_bronze(arquivo, categoria, origem, qualidade, ordem, tamanho=TAMANHO_BLOCO):
    leitor = csv.DictReader(arquivo)
    esperados = CAMPOS_PROCESSOS if categoria == "processos" else ["servidor_id", "timestamp", *METRICAS]
    if leitor.fieldnames is None or len(leitor.fieldnames) != len(esperados) or set(leitor.fieldnames) != set(esperados):
        rejeitar(qualidade, origem, 1, "Cabeçalho incompatível", "", ordem)
        return
    linhas, numeros = [], []
    for linha in leitor:
        if None in linha or any(valor is None for valor in linha.values()):
            rejeitar(qualidade, origem, leitor.line_num, "Quantidade de campos diferente do cabeçalho",
                     json.dumps(linha, ensure_ascii=False), ordem)
            continue
        linhas.append(linha)
        numeros.append(leitor.line_num)
        if len(linhas) >= tamanho:
            quadro = pd.DataFrame.from_records(linhas, columns=leitor.fieldnames)
            quadro["_linha"] = numeros
            quadro["_origem"] = origem
            quadro["_arquivo"] = ordem
            yield quadro
            linhas, numeros = [], []
    if linhas:
        quadro = pd.DataFrame.from_records(linhas, columns=leitor.fieldnames)
        quadro["_linha"] = numeros
        quadro["_origem"] = origem
        quadro["_arquivo"] = ordem
        yield quadro


def motivo_invalido(categoria, linha):
    # Somente as linhas já rejeitadas pelo DataFrame passam pelo diagnóstico.
    try:
        validar_inteiro(linha["servidor_id"], 1)
        datetime.strptime(linha["timestamp"], FORMATO_DATA)
        if categoria == "processos":
            validar_inteiro(linha["pid"], 0)
            if not linha["nome"].strip() or not linha["status"].strip():
                raise ValueError("Nome ou estado do processo vazio")
            validar_numero(linha["uso_cpu"])
            validar_inteiro(linha["ram_bytes"], 0)
        else:
            mediu = False
            for campo in METRICAS:
                texto = linha[campo].strip()
                if not texto:
                    continue
                mediu = True
                if campo == "ram_disponivel":
                    validar_inteiro(texto, 0)
                else:
                    validar_numero(texto, campo in {"uso_cpu", "uso_ram", "uso_disco"})
            if not mediu:
                raise ValueError("Linha sem nenhuma medição")
    except (ValueError, KeyError, TypeError, OverflowError) as erro:
        return str(erro)
    raise ValueError("Linha válida rejeitada pela conversão vetorizada")


def transformar_quadro(brutos, categoria, qualidade):
    metadados = ["_linha", "_origem", "_arquivo"]
    campos = [c for c in brutos.columns if c not in metadados]
    tratados = brutos[metadados].copy()
    tratados["servidor_id"] = converter_inteiros(brutos["servidor_id"], 1)
    tratados["timestamp"] = converter_timestamps(brutos["timestamp"])
    validos = tratados["servidor_id"].notna() & tratados["timestamp"].notna()
    if categoria == "processos":
        tratados["pid"] = converter_inteiros(brutos["pid"], 0)
        tratados["nome"] = brutos["nome"].str.strip()
        tratados["status"] = brutos["status"].str.strip()
        tratados["uso_cpu"] = converter_numeros(brutos["uso_cpu"])
        tratados["ram_bytes"] = converter_inteiros(brutos["ram_bytes"], 0)
        validos &= tratados[["pid", "uso_cpu", "ram_bytes"]].notna().all(axis=1)
        validos &= tratados["nome"].ne("") & tratados["status"].ne("")
    else:
        mediu = pd.Series(False, index=brutos.index)
        for campo in METRICAS:
            presente = brutos[campo].str.strip().ne("")
            mediu |= presente
            tratados[campo] = (converter_inteiros(brutos[campo], 0) if campo == "ram_disponivel"
                               else converter_numeros(brutos[campo], campo in {"uso_cpu", "uso_ram", "uso_disco"}))
            validos &= ~presente | tratados[campo].notna()
        validos &= mediu
    for indice in brutos.index[~validos]:
        linha = brutos.loc[indice, campos].to_dict()
        rejeitar(qualidade, brutos.at[indice, "_origem"], int(brutos.at[indice, "_linha"]),
                 motivo_invalido(categoria, linha), json.dumps(linha, ensure_ascii=False),
                 int(brutos.at[indice, "_arquivo"]))
    tratados = tratados.loc[validos].copy()
    if categoria == "processos":
        tratados["_campo"] = 0
        return tratados[[*CAMPOS_PROCESSOS, *metadados, "_campo"]]
    if tratados.empty:
        return pd.DataFrame(columns=[*CAMPOS_METRICAS, *metadados, "_campo"])
    tratados[list(METRICAS)] = tratados[list(METRICAS)].astype(object)
    longo = tratados.melt(id_vars=["servidor_id", "timestamp", *metadados],
                         value_vars=list(METRICAS), var_name="_nome", value_name="valor")
    longo = longo.loc[longo["valor"].notna()].copy()
    longo["tipo"] = longo["_nome"].map({c: t for c, (t, _) in METRICAS.items()})
    longo["unidade"] = longo["_nome"].map({c: u for c, (_, u) in METRICAS.items()})
    longo["_campo"] = longo["_nome"].map({c: i for i, c in enumerate(METRICAS)})
    return longo.sort_values(["_arquivo", "_linha", "_campo"])[[*CAMPOS_METRICAS, *metadados, "_campo"]]


def deduplicar(quadro, categoria, qualidade):
    colunas = CAMPOS_METRICAS if categoria == "metricas" else CAMPOS_PROCESSOS
    if quadro.empty:
        return pd.DataFrame(columns=colunas), dict(novo=0, repetido=0, conflito=0)
    chaves = CHAVES[categoria]
    duplicadas = quadro.duplicated(chaves)
    primeiras = quadro.loc[~duplicadas]
    repetidos = conflitos = 0
    if duplicadas.any():
        payload = [c for c in colunas if c not in chaves]
        atuais = quadro.loc[duplicadas, payload].reset_index(drop=True)
        referencia = primeiras.set_index(chaves)[payload].reindex(
            pd.MultiIndex.from_frame(quadro.loc[duplicadas, chaves])
        ).reset_index(drop=True)
        iguais = atuais.eq(referencia).all(axis=1)
        campo_float = "valor" if categoria == "metricas" else "uso_cpu"
        zeros = atuais[campo_float].eq(0) & referencia[campo_float].eq(0)
        sinais_atuais = np.signbit(atuais[campo_float].where(zeros, 0).astype(float))
        sinais_primeiros = np.signbit(referencia[campo_float].where(zeros, 0).astype(float))
        iguais &= sinais_atuais.eq(sinais_primeiros)
        repetidos, conflitos = int(iguais.sum()), int((~iguais).sum())
        conflitantes = quadro.loc[duplicadas].loc[(~iguais).to_numpy()]
        for linha in conflitantes.itertuples(index=False, name=None):
            registro = dict(zip(quadro.columns, linha))
            rejeitar(qualidade, registro["_origem"], int(registro["_linha"]),
                     "Valores diferentes para a mesma identidade; medição excluída da Silver",
                     json_dados([registro[c] for c in colunas]), int(registro["_arquivo"]),
                     int(registro["_campo"]), "conflito")
        chaves_conflito = pd.MultiIndex.from_frame(conflitantes[chaves])
        primeiras_validas = primeiras.loc[
            ~pd.MultiIndex.from_frame(primeiras[chaves]).isin(chaves_conflito)
        ]
    else:
        primeiras_validas = primeiras
    saida = primeiras_validas[colunas].copy()
    # A ordenação histórica da Silver é lexicográfica sobre a identidade JSON.
    saida["_identidade"] = [json_dados(valores) for valores in saida[chaves].itertuples(index=False, name=None)]
    saida = saida.sort_values("_identidade").drop(columns="_identidade")
    return saida, dict(novo=len(primeiras), repetido=repetidos, conflito=conflitos)


def ler_silver(caminho, categoria, janela, tamanho=TAMANHO_BLOCO):
    colunas = CAMPOS_METRICAS if categoria == "metricas" else CAMPOS_PROCESSOS
    partes, ignorados = [], 0
    try:
        leitor = pd.read_csv(caminho, dtype=str, na_filter=False, chunksize=tamanho, encoding="utf-8-sig", engine="python")
    except pd.errors.EmptyDataError as erro:
        raise ValueError(f"Cabeçalho inesperado em {Path(caminho).name} da Silver.") from erro
    with leitor:
        for quadro in leitor:
            if list(quadro.columns) != colunas:
                raise ValueError(f"Cabeçalho inesperado em {Path(caminho).name} da Silver.")
            if categoria == "processos":
                quadro["pid"] = converter_inteiros(quadro["pid"])
                if quadro["pid"].isna().any():
                    raise ValueError("PID inválido na Silver.")
                ociosos = quadro["pid"].eq(0)
                ignorados += int(ociosos.sum())
                quadro = quadro.loc[~ociosos].copy()
            quadro["servidor_id"] = converter_inteiros(quadro["servidor_id"])
            quadro["timestamp"] = converter_timestamps(quadro["timestamp"])
            if categoria == "metricas":
                quadro["valor"] = converter_numeros(quadro["valor"])
                obrigatorias = ["servidor_id", "timestamp", "valor"]
            else:
                quadro["uso_cpu"] = converter_numeros(quadro["uso_cpu"])
                quadro["ram_bytes"] = converter_inteiros(quadro["ram_bytes"], 0)
                obrigatorias = ["servidor_id", "timestamp", "pid", "uso_cpu", "ram_bytes"]
            if quadro[obrigatorias].isna().any(axis=None):
                raise ValueError("Valor inválido na Silver; interrompendo a Gold.")
            inteiros = ["servidor_id"] + (["pid", "ram_bytes"] if categoria == "processos" else [])
            for campo in inteiros:
                if ((quadro[campo] < -(2**63)) | (quadro[campo] > 2**63 - 1)).any():
                    raise OverflowError("Inteiro da Silver fora do intervalo de 64 bits.")
                quadro[campo] = quadro[campo].astype("int64")
            campo_float = "valor" if categoria == "metricas" else "uso_cpu"
            quadro.loc[quadro[campo_float].eq(0), campo_float] = 0.0
            datas = pd.to_datetime(quadro["timestamp"], format=FORMATO_DATA, errors="coerce")
            quadro["inicio_periodo"] = datas.dt.floor(f"{janela}min").dt.strftime(FORMATO_DATA)
            for indice in quadro.index[datas.isna()]:
                data = datetime.strptime(quadro.at[indice, "timestamp"], FORMATO_DATA)
                quadro.at[indice, "inicio_periodo"] = data.replace(
                    minute=(data.minute // janela) * janela, second=0
                ).strftime(FORMATO_DATA)
            partes.append(quadro)
    dados = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame(columns=[*colunas, "inicio_periodo"])
    if dados.duplicated(CHAVES[categoria]).any():
        raise ValueError("Identidade duplicada na Silver; interrompendo a Gold.")
    return dados, ignorados
