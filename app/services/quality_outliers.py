from __future__ import annotations

from functools import reduce
import operator
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
import streamlit as st


PROCESSED_BASE = Path("D:/data_quality/data/processed")
RESULT_KEY = "_dq_outlier_result"


def _is_numeric_dtype(dtype: object) -> bool:
    text = str(dtype).lower()
    return text.startswith(("int", "uint", "float", "decimal"))


def _empty_outlier_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "coluna": [],
            "nao_nulos": [],
            "outliers": [],
            "outliers_inferiores": [],
            "outliers_superiores": [],
            "pct_outliers": [],
            "limite_inferior": [],
            "limite_superior": [],
            "q1": [],
            "q3": [],
            "iqr": [],
            "min": [],
            "max": [],
            "media": [],
            "desvio_padrao": [],
        }
    )


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


def _run_iqr_outlier_test(files: List[str], iqr_factor: float, min_non_null: int) -> pl.DataFrame:
    lf = pl.scan_parquet(files)
    schema = lf.collect_schema()
    numeric_cols = [c for c, dt in schema.items() if _is_numeric_dtype(dt)]
    if not numeric_cols:
        return _empty_outlier_df()

    stats_exprs = []
    for c in numeric_cols:
        stats_exprs.extend(
            [
                pl.col(c).count().alias(f"{c}__count"),
                pl.col(c).null_count().alias(f"{c}__nulls"),
                pl.col(c).quantile(0.25).alias(f"{c}__q1"),
                pl.col(c).quantile(0.75).alias(f"{c}__q3"),
                pl.col(c).min().alias(f"{c}__min"),
                pl.col(c).max().alias(f"{c}__max"),
                pl.col(c).mean().alias(f"{c}__mean"),
                pl.col(c).std().alias(f"{c}__std"),
            ]
        )

    stats = lf.select(stats_exprs).collect().to_dicts()[0]

    outlier_exprs = []
    valid_cols: List[str] = []
    meta: Dict[str, Dict[str, Optional[float]]] = {}
    for c in numeric_cols:
        count = stats.get(f"{c}__count", 0) or 0
        nulls = stats.get(f"{c}__nulls", 0) or 0
        non_null = int(count) - int(nulls)
        if non_null < min_non_null:
            continue

        q1 = stats.get(f"{c}__q1")
        q3 = stats.get(f"{c}__q3")
        if q1 is None or q3 is None:
            continue

        iqr = q3 - q1
        lower = q1 - (iqr_factor * iqr)
        upper = q3 + (iqr_factor * iqr)

        if iqr == 0:
            expr_any = (
                pl.when(pl.col(c).is_not_null() & (pl.col(c) != q1))
                .then(1)
                .otherwise(0)
                .sum()
                .alias(f"{c}__outliers")
            )
            expr_low = (
                pl.when(pl.col(c).is_not_null() & (pl.col(c) < q1))
                .then(1)
                .otherwise(0)
                .sum()
                .alias(f"{c}__outliers_low")
            )
            expr_high = (
                pl.when(pl.col(c).is_not_null() & (pl.col(c) > q1))
                .then(1)
                .otherwise(0)
                .sum()
                .alias(f"{c}__outliers_high")
            )
        else:
            expr_any = (
                pl.when(pl.col(c).is_not_null() & ((pl.col(c) < lower) | (pl.col(c) > upper)))
                .then(1)
                .otherwise(0)
                .sum()
                .alias(f"{c}__outliers")
            )
            expr_low = (
                pl.when(pl.col(c).is_not_null() & (pl.col(c) < lower))
                .then(1)
                .otherwise(0)
                .sum()
                .alias(f"{c}__outliers_low")
            )
            expr_high = (
                pl.when(pl.col(c).is_not_null() & (pl.col(c) > upper))
                .then(1)
                .otherwise(0)
                .sum()
                .alias(f"{c}__outliers_high")
            )

        outlier_exprs.extend([expr_any, expr_low, expr_high])
        valid_cols.append(c)
        meta[c] = {
            "non_null": float(non_null),
            "q1": float(q1),
            "q3": float(q3),
            "iqr": float(iqr),
            "lower": float(lower),
            "upper": float(upper),
            "min": float(stats.get(f"{c}__min")) if stats.get(f"{c}__min") is not None else None,
            "max": float(stats.get(f"{c}__max")) if stats.get(f"{c}__max") is not None else None,
            "mean": float(stats.get(f"{c}__mean")) if stats.get(f"{c}__mean") is not None else None,
            "std": float(stats.get(f"{c}__std")) if stats.get(f"{c}__std") is not None else None,
        }

    if not outlier_exprs:
        return _empty_outlier_df()

    outliers = lf.select(outlier_exprs).collect().to_dicts()[0]

    rows = []
    for c in valid_cols:
        non_null = int(meta[c]["non_null"] or 0)
        out_count = int(outliers.get(f"{c}__outliers", 0) or 0)
        out_low = int(outliers.get(f"{c}__outliers_low", 0) or 0)
        out_high = int(outliers.get(f"{c}__outliers_high", 0) or 0)
        pct = (out_count / non_null) * 100 if non_null else 0.0
        rows.append(
            {
                "coluna": c,
                "nao_nulos": non_null,
                "outliers": out_count,
                "outliers_inferiores": out_low,
                "outliers_superiores": out_high,
                "pct_outliers": round(pct, 4),
                "limite_inferior": meta[c]["lower"],
                "limite_superior": meta[c]["upper"],
                "q1": meta[c]["q1"],
                "q3": meta[c]["q3"],
                "iqr": meta[c]["iqr"],
                "min": meta[c]["min"],
                "max": meta[c]["max"],
                "media": meta[c]["mean"],
                "desvio_padrao": meta[c]["std"],
            }
        )

    return pl.DataFrame(rows).sort("pct_outliers", descending=True)


def render_outlier_consistency_panel() -> None:
    st.subheader("Consistencia de Dados - Outliers")
    st.caption("Teste IQR no dic_fic_uc_processed.parquet (todas as particoes encontradas).")

    files = _discover_processed_files()
    if not files:
        st.warning("Nenhum arquivo parquet de processed encontrado para dic_fic_uc.")
        return

    with st.expander("Configurar teste de outliers", expanded=False):
        st.write(f"Arquivos detectados: {len(files)}")
        iqr_factor = st.slider("Fator IQR", min_value=1.0, max_value=5.0, value=1.5, step=0.1, key="_dq_iqr_factor")
        min_non_null = st.number_input(
            "Minimo de nao nulos por coluna",
            min_value=1000,
            max_value=100000000,
            value=5000,
            step=1000,
            key="_dq_iqr_min_non_null",
        )
        top_n = st.number_input("Top colunas para exibir", min_value=5, max_value=200, value=30, step=5, key="_dq_iqr_top_n")
        alert_pct = st.number_input(
            "Alerta de percentual de outlier (%)",
            min_value=0.0,
            max_value=100.0,
            value=1.0,
            step=0.1,
            key="_dq_iqr_alert_pct",
        )

        if st.button("Executar teste de outliers", key="_dq_iqr_run"):
            with st.spinner("Calculando outliers por coluna..."):
                result = _run_iqr_outlier_test(files=files, iqr_factor=float(iqr_factor), min_non_null=int(min_non_null))
                st.session_state[RESULT_KEY] = result

    result = st.session_state.get(RESULT_KEY)
    if not isinstance(result, pl.DataFrame):
        return

    if result.is_empty():
        st.info("Nenhuma coluna numerica valida para teste com os filtros atuais.")
        return

    top_n = int(st.session_state.get("_dq_iqr_top_n", 30))
    alert_pct = float(st.session_state.get("_dq_iqr_alert_pct", 1.0))

    total_cols = result.height
    flagged = result.filter(pl.col("pct_outliers") >= alert_pct).height
    st.write(f"Colunas analisadas: {total_cols} | Colunas em alerta (>= {alert_pct:.2f}%): {flagged}")

    shown = result.head(top_n)
    st.dataframe(shown, use_container_width=True)

    csv_bytes = shown.write_csv().encode("utf-8")
    st.download_button(
        "Baixar top outliers (CSV)",
        data=csv_bytes,
        file_name="outliers_dic_fic_uc_processed_top.csv",
        mime="text/csv",
        key="_dq_iqr_download_csv",
    )

    try:
        low_total = int(shown["outliers_inferiores"].sum())
        high_total = int(shown["outliers_superiores"].sum())
        st.caption(f"No top exibido: inferiores={low_total:,} | superiores={high_total:,}".replace(",", "."))
    except Exception:
        pass


def _duration_candidate_columns(files: List[str]) -> List[str]:
    lf = pl.scan_parquet(files)
    schema = lf.collect_schema()
    cols: List[str] = []
    for c, dt in schema.items():
        name = c.lower()
        if not _is_numeric_dtype(dt):
            continue
        if "duracao" in name or "duração" in name or "duration" in name:
            cols.append(c)
    return cols


def _negative_duration_counts(files: List[str], duration_cols: List[str]) -> pl.DataFrame:
    if not duration_cols:
        return pl.DataFrame({"coluna": [], "negativos": []})

    lf = pl.scan_parquet(files)
    exprs = [
        pl.when(pl.col(c).is_not_null() & (pl.col(c) < 0)).then(1).otherwise(0).sum().alias(c)
        for c in duration_cols
    ]
    row = lf.select(exprs).collect().to_dicts()[0]
    out = [{"coluna": c, "negativos": int(row.get(c, 0) or 0)} for c in duration_cols]
    return pl.DataFrame(out).sort("negativos", descending=True)


def _negative_duration_sample(files: List[str], duration_cols: List[str], limit_rows: int) -> pl.DataFrame:
    if not duration_cols:
        return pl.DataFrame()

    lf = pl.scan_parquet(files)
    conditions = [(pl.col(c).is_not_null() & (pl.col(c) < 0)) for c in duration_cols]
    any_negative = reduce(operator.or_, conditions)
    return lf.filter(any_negative).limit(limit_rows).collect()


def render_negative_duration_panel() -> None:
    st.subheader("Consistencia - Duracao Negativa")
    st.caption("Lista registros do processed com valores de duracao menores que zero.")

    files = _discover_processed_files()
    if not files:
        st.warning("Nenhum parquet de processed encontrado.")
        return

    duration_cols = _duration_candidate_columns(files)
    if not duration_cols:
        st.info("Nao encontrei colunas de duracao numericas (duracao/duração/duration).")
        return

    st.write(f"Colunas de duracao detectadas: {len(duration_cols)}")
    selected_cols = st.multiselect(
        "Colunas de duracao para checagem",
        options=duration_cols,
        default=duration_cols,
        key="_dq_negative_duration_cols",
    )
    limit_rows = st.number_input(
        "Maximo de linhas para listar",
        min_value=10,
        max_value=50000,
        value=1000,
        step=10,
        key="_dq_negative_duration_limit",
    )

    if st.button("Listar dados com duracao negativa", key="_dq_negative_duration_run"):
        with st.spinner("Buscando registros com duracao negativa..."):
            cols = selected_cols or duration_cols
            counts = _negative_duration_counts(files, cols)
            sample = _negative_duration_sample(files, cols, int(limit_rows))
            st.session_state["_dq_negative_duration_counts"] = counts
            st.session_state["_dq_negative_duration_sample"] = sample

    counts = st.session_state.get("_dq_negative_duration_counts")
    sample = st.session_state.get("_dq_negative_duration_sample")

    if isinstance(counts, pl.DataFrame) and not counts.is_empty():
        total_neg = int(counts["negativos"].sum())
        st.write(f"Total de ocorrencias negativas nas colunas selecionadas: {total_neg:,}".replace(",", "."))
        st.dataframe(counts, use_container_width=True)
        if total_neg == 0:
            st.info(
                "Sem duracao negativa. Isso pode ocorrer mesmo com outliers, "
                "quando eles estao acima do limite superior."
            )
    elif isinstance(counts, pl.DataFrame):
        st.success("Nao foram encontradas ocorrencias negativas nas colunas selecionadas.")

    if isinstance(sample, pl.DataFrame) and not sample.is_empty():
        st.write(f"Amostra de registros com duracao negativa (ate {sample.height} linhas):")
        st.dataframe(sample, use_container_width=True)

