from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
import streamlit as st


PROCESSED_BASE = Path("D:/data_quality/data/processed")


def _discover_processed_files() -> List[str]:
    files: List[Path] = []
    single = PROCESSED_BASE / "dic_fic_uc_processed.parquet"
    if single.exists():
        files.append(single)

    for folder_name in ("dic_fic_uc_processed", "dic_fic_uc"):
        folder = PROCESSED_BASE / folder_name
        if folder.exists() and folder.is_dir():
            files.extend(sorted(folder.rglob("*.parquet")))

    unique: List[str] = []
    seen = set()
    for p in files:
        sp = str(p)
        if sp not in seen:
            seen.add(sp)
            unique.append(sp)
    return unique


def _resolve_col(schema: Dict[str, object], candidates: List[str]) -> Optional[str]:
    low = {k.lower(): k for k in schema.keys()}
    for c in candidates:
        if c.lower() in low:
            return low[c.lower()]
    return None


def _resolve_col_fuzzy(schema: Dict[str, object], include_all: List[str], include_any: Optional[List[str]] = None) -> Optional[str]:
    for c in schema.keys():
        lc = c.lower()
        if not all(tok in lc for tok in include_all):
            continue
        if include_any and not any(tok in lc for tok in include_any):
            continue
        return c
    return None


def _regional_expr(schema: Dict[str, object]) -> Optional[pl.Expr]:
    col_reg_total = _resolve_col(schema, ["REGIONAL_TOTAL"])
    if col_reg_total:
        return pl.col(col_reg_total).cast(pl.Utf8).fill_null("SEM_REGIONAL").str.to_uppercase().alias("REGIONAL_TOTAL")

    col_sigla = _resolve_col(schema, ["SIGLA_REGIONAL"])
    if not col_sigla:
        return None

    sig = pl.col(col_sigla).cast(pl.Utf8).fill_null("").str.to_uppercase()
    return (
        pl.when(sig == "NRT")
        .then(pl.lit("NRT"))
        .when(sig == "CSL")
        .then(pl.lit("CSL"))
        .when(sig == "NRO")
        .then(pl.lit("NRO"))
        .when(sig == "LES")
        .then(pl.lit("LES"))
        .when(sig == "OES")
        .then(pl.lit("OES"))
        .when(sig == "L")
        .then(pl.lit("NRT"))
        .when(sig == "P")
        .then(pl.lit("CSL"))
        .when(sig == "M")
        .then(pl.lit("NRO"))
        .when(sig == "C")
        .then(pl.lit("LES"))
        .when(sig == "V")
        .then(pl.lit("OES"))
        .when(sig == "COPEL")
        .then(pl.lit("COPEL"))
        .otherwise(pl.lit("SEM_REGIONAL"))
        .alias("REGIONAL_TOTAL")
    )


def _to_datetime_expr(col_name: str) -> pl.Expr:
    c = pl.col(col_name)
    s = c.cast(pl.Utf8).fill_null("")
    return pl.coalesce(
        [
            c.cast(pl.Datetime, strict=False),
            s.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S%.f", strict=False),
            s.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False),
            s.str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.f", strict=False),
            s.str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S%.f", strict=False),
            s.str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S", strict=False),
            s.str.strptime(pl.Datetime, "%Y-%m-%d %H:%M", strict=False),
            s.str.strptime(pl.Datetime, "%d/%m/%Y %H:%M", strict=False),
        ]
    )


def _build_metrics(
    files: List[str],
    forced_cols: Optional[Dict[str, str]] = None,
    sample_only_nrt: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    schema = dict(pl.scan_parquet(files).collect_schema())
    col_uc = forced_cols.get("uc") if forced_cols else None
    col_intrp = forced_cols.get("intrp") if forced_cols else None
    col_ini = forced_cols.get("ini") if forced_cols else None
    col_fim = forced_cols.get("fim") if forced_cols else None
    reg_total_forced = forced_cols.get("reg_total") if forced_cols else None
    reg_sigla_forced = forced_cols.get("reg_sigla") if forced_cols else None

    col_uc = col_uc or _resolve_col(schema, ["NUM_UC_UCI"]) or _resolve_col_fuzzy(schema, ["num", "uc"])
    col_intrp = col_intrp or _resolve_col(schema, ["NUM_INTRP_INIC_MANOBRA_UCI", "NUM_INTRP_PERCEBIDA"]) or _resolve_col_fuzzy(
        schema, ["num", "intrp"]
    )
    col_ini = col_ini or _resolve_col(
        schema,
        ["DTHR_INICIO_INTRP_UC", "DTHR_INICIO_INTRP_PERCEBIDO", "DTHR_INICIO_INTRP_PERCEBIDA"],
    ) or _resolve_col_fuzzy(
        schema, ["inicio"], ["intrp", "intr", "interrup"]
    )
    col_fim = col_fim or _resolve_col(
        schema,
        [
            "DATA_HORA_FIM_INTR",
            "DATA_HORA_FIM_INTR_PERCEBIDO",
            "DATA_HORA_FIM_INTR_PERCEBIDA",
        ],
    ) or _resolve_col_fuzzy(
        schema, ["fim"], ["intrp", "intr", "interrup"]
    )

    if reg_total_forced and reg_total_forced in schema:
        reg_expr = pl.col(reg_total_forced).cast(pl.Utf8).fill_null("SEM_REGIONAL").str.to_uppercase().alias("REGIONAL_TOTAL")
    elif reg_sigla_forced and reg_sigla_forced in schema:
        sig = pl.col(reg_sigla_forced).cast(pl.Utf8).fill_null("").str.to_uppercase()
        reg_expr = (
            pl.when(sig == "L")
            .then(pl.lit("NRT"))
            .when(sig == "P")
            .then(pl.lit("CSL"))
            .when(sig == "M")
            .then(pl.lit("NRO"))
            .when(sig == "C")
            .then(pl.lit("LES"))
            .when(sig == "V")
            .then(pl.lit("OES"))
            .when(sig == "COPEL")
            .then(pl.lit("COPEL"))
            .otherwise(pl.lit("SEM_REGIONAL"))
            .alias("REGIONAL_TOTAL")
        )
    else:
        reg_expr = _regional_expr(schema)
    if not col_uc or not col_ini or not col_fim or reg_expr is None:
        return pl.DataFrame(), pl.DataFrame()

    lf = (
        pl.scan_parquet(files)
        .select(
            [
                reg_expr,
                pl.col(col_uc).cast(pl.Utf8).alias("NUM_UC_UCI"),
                (pl.col(col_intrp).cast(pl.Utf8).alias("NUM_INTRP") if col_intrp else pl.lit("").alias("NUM_INTRP")),
                _to_datetime_expr(col_ini).alias("DT_INI"),
                _to_datetime_expr(col_fim).alias("DT_FIM"),
            ]
        )
        .filter(
            pl.col("NUM_UC_UCI").is_not_null()
            & (pl.col("NUM_UC_UCI") != "")
            & pl.col("DT_INI").is_not_null()
            & pl.col("DT_FIM").is_not_null()
            & (pl.col("DT_FIM") >= pl.col("DT_INI"))
            & (pl.col("REGIONAL_TOTAL") != "COPEL")
        )
        .sort(["REGIONAL_TOTAL", "NUM_UC_UCI", "DT_INI", "DT_FIM"])
        .with_columns(
            [
                ((pl.col("DT_FIM") - pl.col("DT_INI")).dt.total_seconds()).alias("DUR_SEC"),
                pl.col("DT_FIM").shift(1).over(["REGIONAL_TOTAL", "NUM_UC_UCI"]).alias("PREV_FIM"),
            ]
        )
        .with_columns(
            [
                ((pl.col("DT_INI") - pl.col("PREV_FIM")).dt.total_seconds()).alias("GAP_PREV_SEC"),
                pl.col("DUR_SEC").shift(1).over(["REGIONAL_TOTAL", "NUM_UC_UCI"]).alias("PREV_DUR_SEC"),
                (pl.col("DT_INI").dt.second() > 0).alias("INI_TEM_SEGUNDO"),
                (pl.col("DT_FIM").dt.second() > 0).alias("FIM_TEM_SEGUNDO"),
                pl.col("DT_INI").dt.truncate("1m").alias("DT_INI_MIN"),
                pl.col("DT_FIM").dt.truncate("1m").alias("DT_FIM_MIN"),
            ]
        )
        .with_columns(
            [
                ((pl.col("DT_FIM_MIN") - pl.col("DT_INI_MIN")).dt.total_seconds()).alias("DUR_SEC_MIN"),
                pl.col("DT_FIM_MIN").shift(1).over(["REGIONAL_TOTAL", "NUM_UC_UCI"]).alias("PREV_FIM_MIN"),
            ]
        )
        .with_columns(
            [
                ((pl.col("DT_INI_MIN") - pl.col("PREV_FIM_MIN")).dt.total_seconds()).alias("GAP_PREV_SEC_MIN"),
                pl.col("DUR_SEC_MIN").shift(1).over(["REGIONAL_TOTAL", "NUM_UC_UCI"]).alias("PREV_DUR_SEC_MIN"),
            ]
        )
        .with_columns(
            [
                (pl.col("INI_TEM_SEGUNDO") | pl.col("FIM_TEM_SEGUNDO")).alias("TEM_PRECISAO_SEGUNDO"),
                (
                    pl.col("GAP_PREV_SEC").is_not_null()
                    & (pl.col("GAP_PREV_SEC") >= 0)
                    & (pl.col("GAP_PREV_SEC") < 60)
                ).alias("GAP_LT_60S"),
                (
                    pl.col("GAP_PREV_SEC_MIN").is_not_null()
                    & (pl.col("GAP_PREV_SEC_MIN") >= 0)
                    & (pl.col("GAP_PREV_SEC_MIN") < 60)
                ).alias("GAP_LT_60S_MIN"),
                (pl.col("DUR_SEC") < 60).alias("DUR_LT_60S"),
                (pl.col("DUR_SEC_MIN") < 60).alias("DUR_LT_60S_MIN"),
            ]
        )
        .with_columns(
            [
                (
                    pl.col("GAP_LT_60S")
                    & pl.col("DUR_LT_60S")
                    & (pl.col("PREV_DUR_SEC") < 60)
                ).fill_null(False).alias("MERGE_CRITERIO"),
                (
                    pl.col("GAP_LT_60S_MIN")
                    & pl.col("DUR_LT_60S_MIN")
                    & (pl.col("PREV_DUR_SEC_MIN") < 60)
                ).fill_null(False).alias("MERGE_CRITERIO_MINUTO"),
            ]
        )
    )

    resumo = (
        lf.group_by("REGIONAL_TOTAL")
        .agg(
            [
                pl.len().alias("REGISTROS"),
                pl.col("TEM_PRECISAO_SEGUNDO").sum().alias("COM_SEGUNDOS"),
                pl.col("DUR_LT_60S").sum().alias("DURACAO_LT_60S"),
                pl.col("GAP_LT_60S").sum().alias("GAP_LT_60S"),
                pl.col("MERGE_CRITERIO").sum().alias("MERGE_CRITERIO"),
                pl.col("MERGE_CRITERIO_MINUTO").sum().alias("MERGE_CRITERIO_MINUTO"),
            ]
        )
        .with_columns(
            [
                ((pl.col("COM_SEGUNDOS") / pl.col("REGISTROS")) * 100).round(2).alias("PCT_COM_SEGUNDOS"),
                ((pl.col("DURACAO_LT_60S") / pl.col("REGISTROS")) * 100).round(2).alias("PCT_DUR_LT_60S"),
                ((pl.col("GAP_LT_60S") / pl.col("REGISTROS")) * 100).round(2).alias("PCT_GAP_LT_60S"),
                ((pl.col("MERGE_CRITERIO") / pl.col("REGISTROS")) * 100).round(2).alias("PCT_MERGE"),
                ((pl.col("MERGE_CRITERIO_MINUTO") / pl.col("REGISTROS")) * 100).round(2).alias("PCT_MERGE_MINUTO"),
                (pl.col("MERGE_CRITERIO") - pl.col("MERGE_CRITERIO_MINUTO")).alias("DELTA_MERGE_SEGUNDO"),
            ]
        )
        .sort("PCT_MERGE", descending=True)
        .collect()
    )

    amostra_lf = lf.filter(pl.col("MERGE_CRITERIO") == True)
    if sample_only_nrt:
        amostra_lf = amostra_lf.filter(
            (pl.col("REGIONAL_TOTAL") == "NRT") | (pl.col("REGIONAL_TOTAL") == "L")
        )

    amostra_merge = (
        amostra_lf.select(
            [
                "REGIONAL_TOTAL",
                "NUM_UC_UCI",
                "NUM_INTRP",
                "DT_INI",
                "DT_FIM",
                "DUR_SEC",
                "GAP_PREV_SEC",
                "PREV_DUR_SEC",
                "TEM_PRECISAO_SEGUNDO",
                "MERGE_CRITERIO",
                "MERGE_CRITERIO_MINUTO",
            ]
        )
        .head(500)
        .collect()
    )
    return resumo, amostra_merge


def render_nrt_precision_panel() -> None:
    st.subheader("Verificacao de Precisao de Segundos")
    st.caption(
        "Analisa impacto da precisao de segundos na regra de merge (<1 minuto) por UC, "
        "considerando todas as regionais. Compara criterio atual vs timestamps truncados para minuto."
    )

    files = _discover_processed_files()
    if not files:
        st.warning("Nenhum processed parquet encontrado.")
        return

    schema_map = dict(pl.scan_parquet(files).collect_schema())
    all_cols = sorted(schema_map.keys())

    with st.expander("Mapeamento manual de colunas (opcional)", expanded=False):
        st.caption("Use apenas se a deteccao automatica falhar.")
        col_uc_m = st.selectbox("Coluna UC", options=[""] + all_cols, index=0, key="_dq_nrt_m_uc")
        col_intrp_m = st.selectbox("Coluna interrupcao", options=[""] + all_cols, index=0, key="_dq_nrt_m_intrp")
        col_ini_m = st.selectbox("Coluna inicio", options=[""] + all_cols, index=0, key="_dq_nrt_m_ini")
        col_fim_m = st.selectbox("Coluna fim", options=[""] + all_cols, index=0, key="_dq_nrt_m_fim")
        col_reg_total_m = st.selectbox("Coluna REGIONAL_TOTAL", options=[""] + all_cols, index=0, key="_dq_nrt_m_reg_total")
        col_reg_sigla_m = st.selectbox("Coluna SIGLA_REGIONAL", options=[""] + all_cols, index=0, key="_dq_nrt_m_reg_sigla")
        sample_only_nrt = st.checkbox(
            "Limitar amostra de registros para NRT",
            value=False,
            key="_dq_nrt_sample_only_nrt",
        )

    forced_cols = {
        "uc": col_uc_m,
        "intrp": col_intrp_m,
        "ini": col_ini_m,
        "fim": col_fim_m,
        "reg_total": col_reg_total_m,
        "reg_sigla": col_reg_sigla_m,
    }
    forced_cols = {k: v for k, v in forced_cols.items() if v}

    if st.button("Rodar verificacao de precisao (todas regionais)", key="_dq_nrt_precision_run"):
        with st.spinner("Calculando metricas de precisao temporal..."):
            resumo, amostra = _build_metrics(
                files,
                forced_cols=forced_cols if forced_cols else None,
                sample_only_nrt=bool(sample_only_nrt),
            )
            st.session_state["_dq_nrt_precision_resumo"] = resumo
            st.session_state["_dq_nrt_precision_amostra"] = amostra

            # Diagnostico para facilitar ajuste de nomes de coluna.
            try:
                schema = dict(pl.scan_parquet(files).collect_schema())
                col_uc = _resolve_col(schema, ["NUM_UC_UCI"]) or _resolve_col_fuzzy(schema, ["num", "uc"])
                col_intrp = _resolve_col(schema, ["NUM_INTRP_INIC_MANOBRA_UCI", "NUM_INTRP_PERCEBIDA"]) or _resolve_col_fuzzy(
                    schema, ["num", "intrp"]
                )
                col_ini = _resolve_col(
                    schema,
                    ["DTHR_INICIO_INTRP_UC", "DTHR_INICIO_INTRP_PERCEBIDO", "DTHR_INICIO_INTRP_PERCEBIDA"],
                ) or _resolve_col_fuzzy(
                    schema, ["inicio"], ["intrp", "intr", "interrup"]
                )
                col_fim = _resolve_col(
                    schema,
                    ["DATA_HORA_FIM_INTR", "DATA_HORA_FIM_INTR_PERCEBIDO", "DATA_HORA_FIM_INTR_PERCEBIDA"],
                ) or _resolve_col_fuzzy(
                    schema, ["fim"], ["intrp", "intr", "interrup"]
                )
                col_reg = _resolve_col(schema, ["REGIONAL_TOTAL", "SIGLA_REGIONAL"])
                st.session_state["_dq_nrt_precision_diag"] = {
                    "NUM_UC_UCI": col_uc or "",
                    "NUM_INTRP": col_intrp or "",
                    "INICIO": col_ini or "",
                    "FIM": col_fim or "",
                    "REGIONAL": col_reg or "",
                    "TOTAL_COLUNAS": len(schema),
                }
            except Exception:
                st.session_state["_dq_nrt_precision_diag"] = {}

    resumo = st.session_state.get("_dq_nrt_precision_resumo")
    amostra = st.session_state.get("_dq_nrt_precision_amostra")

    if isinstance(resumo, pl.DataFrame):
        if resumo.is_empty():
            st.info("Nao foi possivel calcular com deteccao automatica. Veja o diagnostico abaixo.")
            diag = st.session_state.get("_dq_nrt_precision_diag", {})
            if isinstance(diag, dict) and diag:
                st.json(diag)
            return
        st.write("Comparativo por regional")
        st.dataframe(resumo, use_container_width=True)

        total_merge_atual = int(resumo["MERGE_CRITERIO"].sum()) if resumo.height else 0
        total_merge_min = int(resumo["MERGE_CRITERIO_MINUTO"].sum()) if resumo.height else 0
        delta_total = total_merge_atual - total_merge_min
        st.info(
            f"Total geral: {total_merge_atual} merges pelo criterio atual vs "
            f"{total_merge_min} com truncamento em minuto (delta={delta_total})."
        )
        diag = st.session_state.get("_dq_nrt_precision_diag", {})
        if isinstance(diag, dict) and diag:
            with st.expander("Diagnostico de colunas usadas"):
                st.json(diag)

    if isinstance(amostra, pl.DataFrame):
        st.write("Amostra de registros afetados pela regra de merge (<1 minuto)")
        if amostra.is_empty():
            st.success("Nao houve casos de merge no escopo analisado.")
        else:
            st.dataframe(amostra, use_container_width=True)
