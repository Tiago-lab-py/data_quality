from __future__ import annotations

import math
from statistics import median
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import polars as pl
import streamlit as st


PROCESSED_BASE = Path("D:/data_quality/data/processed")


def _discover_processed_files() -> List[str]:
    candidates: List[Path] = []

    single_file = PROCESSED_BASE / "dic_fic_uc_processed.parquet"
    if single_file.exists():
        candidates.append(single_file)

    for folder_name in ("dic_fic_uc_processed", "dic_fic_uc"):
        folder = PROCESSED_BASE / folder_name
        if folder.exists() and folder.is_dir():
            candidates.extend(sorted(folder.rglob("*.parquet")))

    unique: List[str] = []
    seen = set()
    for p in candidates:
        sp = str(p)
        if sp not in seen:
            seen.add(sp)
            unique.append(sp)
    return unique


def _norm(text: str) -> str:
    return text.lower().strip()


def _find_col(schema: Dict[str, object], names: List[str]) -> Optional[str]:
    lowered = {_norm(k): k for k in schema.keys()}
    for n in names:
        col = lowered.get(_norm(n))
        if col:
            return col
    return None


def _resolve_regional_col(schema: Dict[str, object]) -> Optional[str]:
    direct = _find_col(
        schema,
        [
            "sigla_regional",
            "sg_regional",
            "regional",
            "sigla_region",
            "regiao_sigla",
        ],
    )
    if direct:
        return direct
    for c in schema.keys():
        lc = _norm(c)
        if "regional" in lc and ("sigla" in lc or "sg_" in lc):
            return c
    return None


def _resolve_date_col(schema: Dict[str, object]) -> Optional[str]:
    direct = _find_col(
        schema,
        [
            "data_referencia",
            "dt_referencia",
            "data",
            "data_inicio",
            "dt_inicio",
            "dt_movimento",
            "competencia",
            "mes_referencia",
        ],
    )
    if direct:
        return direct
    for c in schema.keys():
        lc = _norm(c)
        if "data" in lc or "dt_" in lc or "compet" in lc or "mes" in lc:
            return c
    return None


def _regional_expr(regional_col: str) -> pl.Expr:
    txt = pl.col(regional_col).cast(pl.Utf8).fill_null("")
    trimmed = txt.str.replace_all(r"\s+", "")
    return (
        pl.when(trimmed == "")
        .then(pl.lit("SEM_SIGLA"))
        .otherwise(pl.col(regional_col).cast(pl.Utf8).str.to_uppercase())
        .alias("sigla_regional")
    )


def _month_expr(date_col: str, dtype: object) -> Optional[pl.Expr]:
    dtext = str(dtype).lower()
    c = pl.col(date_col)
    if "date" in dtext or "datetime" in dtext:
        return c.cast(pl.Datetime, strict=False).dt.strftime("%Y-%m").alias("mes_ref")

    txt = c.cast(pl.Utf8).fill_null("")
    ym_hyphen = txt.str.slice(0, 7)
    ym_compact = pl.concat_str([txt.str.slice(0, 4), pl.lit("-"), txt.str.slice(4, 2)])
    return (
        pl.when(txt.str.contains(r"^\d{4}-\d{2}"))
        .then(ym_hyphen)
        .when(txt.str.contains(r"^\d{8}$"))
        .then(ym_compact)
        .when(txt.str.contains(r"^\d{6}$"))
        .then(ym_compact)
        .otherwise(pl.lit(None))
        .alias("mes_ref")
    )


def _collect_summary(files: List[str], regional_col: str) -> Tuple[pl.DataFrame, int, int]:
    lf = pl.scan_parquet(files)
    reg = _regional_expr(regional_col)
    total_df = (
        lf.select(reg)
        .group_by("sigla_regional")
        .agg(pl.len().alias("registros"))
        .sort("registros", descending=True)
        .collect()
    )
    total = int(total_df["registros"].sum()) if total_df.height else 0
    if total_df.height:
        sem_val = total_df.filter(pl.col("sigla_regional") == "SEM_SIGLA")["registros"].sum()
        sem_sigla = int(sem_val or 0)
    else:
        sem_sigla = 0
    with_pct = (
        total_df.with_columns(
            pl.when(pl.lit(total) > 0)
            .then((pl.col("registros") / pl.lit(total)) * 100)
            .otherwise(0.0)
            .alias("pct_registros")
        )
        if total_df.height
        else total_df
    )
    return with_pct, total, sem_sigla


def _collect_monthly(files: List[str], regional_col: str, date_col: str, date_dtype: object) -> pl.DataFrame:
    month = _month_expr(date_col, date_dtype)
    if month is None:
        return pl.DataFrame({"sigla_regional": [], "mes_ref": [], "registros": []})

    lf = pl.scan_parquet(files)
    reg = _regional_expr(regional_col)
    monthly = (
        lf.select([reg, month])
        .filter(pl.col("mes_ref").is_not_null())
        .group_by(["sigla_regional", "mes_ref"])
        .agg(pl.len().alias("registros"))
        .sort(["sigla_regional", "mes_ref"])
        .collect()
    )
    return monthly


def _mad(values: List[float], med: float) -> float:
    devs = [abs(v - med) for v in values]
    return float(median(devs)) if devs else 0.0


def _detect_anomalies(monthly: pl.DataFrame, pct_threshold: float, robust_z_threshold: float) -> Tuple[pl.DataFrame, pl.DataFrame]:
    if monthly.is_empty():
        empty = pl.DataFrame(
            {
                "sigla_regional": [],
                "mes_ref": [],
                "registros": [],
                "registros_mes_anterior": [],
                "pct_variacao": [],
                "robust_z": [],
                "anomalia": [],
            }
        )
        return empty, empty

    rows = monthly.sort(["sigla_regional", "mes_ref"]).to_dicts()
    grouped: Dict[str, List[Dict[str, object]]] = {}
    for r in rows:
        grouped.setdefault(str(r["sigla_regional"]), []).append(r)

    out_rows: List[Dict[str, object]] = []
    for regional, series in grouped.items():
        vals = [float(item["registros"]) for item in series]
        med = float(median(vals)) if vals else 0.0
        mad = _mad(vals, med)
        prev: Optional[float] = None
        for item in series:
            cur = float(item["registros"])
            pct_var = None
            if prev is not None and prev > 0:
                pct_var = ((cur - prev) / prev) * 100.0

            robust_z = None
            if mad > 0:
                robust_z = 0.6745 * (cur - med) / mad

            is_anom = False
            if pct_var is not None and abs(pct_var) >= pct_threshold:
                is_anom = True
            if robust_z is not None and abs(robust_z) >= robust_z_threshold:
                is_anom = True

            out_rows.append(
                {
                    "sigla_regional": regional,
                    "mes_ref": str(item["mes_ref"]),
                    "registros": int(cur),
                    "registros_mes_anterior": int(prev) if prev is not None else None,
                    "pct_variacao": round(float(pct_var), 4) if pct_var is not None else None,
                    "robust_z": round(float(robust_z), 4) if robust_z is not None else None,
                    "anomalia": is_anom,
                }
            )
            prev = cur

    full = pl.DataFrame(out_rows).sort(["mes_ref", "sigla_regional"])
    latest_month = str(monthly["mes_ref"].max())
    latest = full.filter((pl.col("mes_ref") == latest_month) & (pl.col("anomalia") == True)).sort(
        ["pct_variacao", "robust_z"], descending=True
    )
    return full, latest


def _psi_for_latest(monthly: pl.DataFrame) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    if monthly.is_empty():
        return None, None, None

    months = sorted(set(monthly["mes_ref"].to_list()))
    if len(months) < 2:
        return None, None, None

    latest = months[-1]
    prev = months[-2]
    latest_df = monthly.filter(pl.col("mes_ref") == latest)
    prev_df = monthly.filter(pl.col("mes_ref") == prev)

    all_regs = sorted(set(latest_df["sigla_regional"].to_list()) | set(prev_df["sigla_regional"].to_list()))
    latest_map = {str(r["sigla_regional"]): float(r["registros"]) for r in latest_df.to_dicts()}
    prev_map = {str(r["sigla_regional"]): float(r["registros"]) for r in prev_df.to_dicts()}
    sum_latest = sum(latest_map.values())
    sum_prev = sum(prev_map.values())
    if sum_latest <= 0 or sum_prev <= 0:
        return None, latest, prev

    eps = 1e-8
    psi = 0.0
    for reg in all_regs:
        a = (latest_map.get(reg, 0.0) / sum_latest) + eps
        e = (prev_map.get(reg, 0.0) / sum_prev) + eps
        psi += (a - e) * math.log(a / e)
    return float(psi), latest, prev


def render_regional_quality_panel() -> None:
    st.subheader("Analises por sigla_regional")
    st.caption("Volume por regional e deteccao de anomalias no processed.")

    files = _discover_processed_files()
    if not files:
        st.warning("Nao encontrei arquivos parquet processed para analise.")
        return

    schema = pl.scan_parquet(files).collect_schema()
    schema_map = dict(schema)
    regional_col = _resolve_regional_col(schema_map)
    if regional_col is None:
        st.error("Nao encontrei coluna de sigla_regional no processed.")
        return

    date_col = _resolve_date_col(schema_map)
    date_dtype = schema_map.get(date_col) if date_col else None

    with st.expander("Configuracao de analise regional", expanded=False):
        top_n = st.number_input("Top regionais no grafico", min_value=5, max_value=100, value=20, step=5, key="_dq_reg_top_n")
        pct_threshold = st.number_input(
            "Limiar de variacao mensal (%) para alerta",
            min_value=5.0,
            max_value=500.0,
            value=35.0,
            step=5.0,
            key="_dq_reg_pct_threshold",
        )
        robust_z_threshold = st.number_input(
            "Limiar de robust z-score para alerta",
            min_value=1.5,
            max_value=10.0,
            value=3.5,
            step=0.5,
            key="_dq_reg_robust_z_threshold",
        )

    with st.spinner("Calculando distribuicao por regional..."):
        summary, total_registros, sem_sigla = _collect_summary(files, regional_col)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total de registros", f"{total_registros:,}".replace(",", "."))
    c2.metric("Regionais detectadas", summary.height)
    c3.metric("SEM_SIGLA", f"{sem_sigla:,}".replace(",", "."))

    st.write("Quantidade de registros por sigla_regional")
    top_n_int = int(top_n)
    top_summary = summary.head(top_n_int)
    st.dataframe(top_summary, use_container_width=True)

    try:
        chart_df = top_summary.select(["sigla_regional", "registros"]).to_pandas().set_index("sigla_regional")
        st.bar_chart(chart_df["registros"])
    except Exception:
        pass

    if not date_col or date_dtype is None:
        st.info("Nao encontrei coluna de data confiavel para analise mensal de anomalias.")
        return

    with st.spinner("Calculando serie mensal por regional..."):
        monthly = _collect_monthly(files, regional_col, date_col, date_dtype)

    if monthly.is_empty():
        st.info("Nao foi possivel montar serie mensal por regional.")
        return

    mensal_total = monthly.group_by("mes_ref").agg(pl.col("registros").sum().alias("registros")).sort("mes_ref")
    st.write("Evolucao mensal do total de registros")
    try:
        mensal_chart = mensal_total.to_pandas().set_index("mes_ref")
        st.line_chart(mensal_chart["registros"])
    except Exception:
        st.dataframe(mensal_total, use_container_width=True)

    pct_thr = float(pct_threshold)
    z_thr = float(robust_z_threshold)
    _, latest_anom = _detect_anomalies(monthly, pct_thr, z_thr)

    psi, latest_month, prev_month = _psi_for_latest(monthly)
    if psi is not None and latest_month and prev_month:
        level = "Estavel"
        if psi >= 0.25:
            level = "Drift alto"
        elif psi >= 0.10:
            level = "Drift moderado"
        st.caption(f"PSI distribuicao regional ({prev_month} -> {latest_month}): {psi:.4f} [{level}]")

    st.write("Alertas de anomalia por regional (ultimo mes)")
    if latest_anom.is_empty():
        st.success("Sem anomalias no ultimo mes com os limiares atuais.")
    else:
        st.dataframe(latest_anom, use_container_width=True)
