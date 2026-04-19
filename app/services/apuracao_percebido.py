
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import polars as pl
import streamlit as st


PROCESSED_BASE = Path("D:/data_quality/data/processed")
UC_FATURADA_PATH = Path("D:/data_quality/data/raw/UC_faturada.parquet")


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
        key = c.lower()
        if key in low:
            return low[key]
    return None


def _load_uc_faturada() -> Tuple[Optional[pl.DataFrame], Optional[str]]:
    if not UC_FATURADA_PATH.exists():
        return None, f"Arquivo nao encontrado: {UC_FATURADA_PATH}"

    df = pl.read_parquet(str(UC_FATURADA_PATH), memory_map=False)
    schema = dict(df.schema)
    col_regional_total = _resolve_col(schema, ["REGIONAL_TOTAL"])
    col_sigla_regional = _resolve_col(schema, ["SIGLA_REGIONAL"])
    col_anomes = _resolve_col(schema, ["ANOMES", "ANO_MES", "ANO_MES_REF"])
    col_uc = _resolve_col(schema, ["UC_FATURADA", "UC_FATURADO", "UC"])
    if not col_regional_total or not col_anomes or not col_uc:
        return None, "UC_faturada.parquet sem colunas obrigatorias (REGIONAL_TOTAL, ANOMES, UC_FATURADA)."

    out = df.select(
        [
            pl.col(col_regional_total).cast(pl.Utf8).fill_null("SEM_REGIONAL").alias("REGIONAL_TOTAL"),
            (pl.col(col_sigla_regional).cast(pl.Utf8).fill_null("") if col_sigla_regional else pl.lit("").alias("SIGLA_REGIONAL")),
            pl.col(col_anomes).cast(pl.Utf8).alias("ANOMES"),
            pl.col(col_uc).cast(pl.Float64, strict=False).fill_null(0.0).alias("UC_FATURADA"),
        ]
    )
    return out, None


def _month_expr(col_name: str, dtype: object) -> pl.Expr:
    text = str(dtype).lower()
    c = pl.col(col_name)
    if "date" in text or "datetime" in text:
        return c.cast(pl.Datetime, strict=False).dt.strftime("%Y-%m")
    txt = c.cast(pl.Utf8).fill_null("")
    return (
        pl.when(txt.str.contains(r"^\d{4}-\d{2}"))
        .then(txt.str.slice(0, 7))
        .when(txt.str.contains(r"^\d{8}$"))
        .then(pl.concat_str([txt.str.slice(0, 4), pl.lit("-"), txt.str.slice(4, 2)]))
        .otherwise(pl.lit(None))
    )


def _has_percebido_cols(files: List[str]) -> bool:
    schema = dict(pl.scan_parquet(files).collect_schema())
    cols = {c.lower() for c in schema.keys()}
    has_intrp = "num_intrp_percebida" in cols
    has_ini = "dthr_inicio_intrp_percebido" in cols
    has_fim = ("data_hora_fim_intrp_percebido" in cols) or ("data_hora_fim_intr_percebido" in cols)
    has_dur = "duracao_percebida_minutos" in cols
    return has_intrp and has_ini and has_fim and has_dur


def _prepare_processed_for_month(files: List[str], anomes: str, uc_map: pl.DataFrame) -> Tuple[Optional[pl.DataFrame], Optional[str]]:
    schema = dict(pl.scan_parquet(files).collect_schema())

    col_regional_total = _resolve_col(schema, ["REGIONAL_TOTAL"])
    col_sigla_regional = _resolve_col(schema, ["SIGLA_REGIONAL"])
    col_uc = _resolve_col(schema, ["NUM_UC_UCI"])
    col_intrp = _resolve_col(schema, ["NUM_INTRP_PERCEBIDA"])
    col_justif = _resolve_col(schema, ["TIPO_PROTOC_JUSTIF_UCI", "TIPO_PROTOC_JUSTIF_UC"])
    col_ini = _resolve_col(schema, ["DTHR_INICIO_INTRP_PERCEBIDO"])
    col_fim = _resolve_col(
        schema,
        [
            "DATA_HORA_FIM_INTRP_PERCEBIDO",
            "DATA_HORA_FIM_INTR_PERCEBIDO",
        ],
    )
    col_dur = _resolve_col(schema, ["DURACAO_PERCEBIDA_MINUTOS"])
    col_verif = _resolve_col(schema, ["verificado", "VERIFICADO"])

    if not col_uc or not col_intrp or not col_justif or not col_ini or not col_fim or not col_dur:
        return None, (
            "Processed sem colunas percebidas obrigatorias. "
            "Execute primeiro a etapa de verificacao/salvamento do PERCEBIDO em 3_Qualidade_dados."
        )
    if not col_regional_total and not col_sigla_regional:
        return None, "Processed sem REGIONAL_TOTAL e sem SIGLA_REGIONAL."
    if not col_verif:
        return None, "Processed sem coluna 'verificado'. Rode a verificacao de sobreposicao para materializar o percebido."

    lf = pl.scan_parquet(files)
    month_col = _month_expr(col_ini, schema[col_ini]).alias("__ANOMES")
    dur_expr = pl.col(col_dur).cast(pl.Float64, strict=False).fill_null(0.0)

    if col_regional_total:
        regional_expr = pl.col(col_regional_total).cast(pl.Utf8).fill_null("SEM_REGIONAL").alias("REGIONAL_TOTAL")
        base = lf.select(
            [
                regional_expr,
                month_col,
                pl.col(col_uc).cast(pl.Utf8).alias("NUM_UC_UCI"),
                pl.col(col_intrp).cast(pl.Utf8).alias("NUM_INTRP_PERCEBIDA"),
                dur_expr.alias("DUR_MIN"),
                pl.col(col_justif).cast(pl.Utf8).fill_null("").alias("TIPO_PROTOC_JUSTIF_UCI"),
                pl.col(col_verif).cast(pl.Boolean).fill_null(False).alias("VERIFICADO"),
            ]
        )
    else:
        mapping = (
            uc_map.select(["SIGLA_REGIONAL", "REGIONAL_TOTAL"])
            .filter(pl.col("SIGLA_REGIONAL").is_not_null() & (pl.col("SIGLA_REGIONAL") != ""))
            .unique()
        )
        base = (
            lf.select(
                [
                    pl.col(col_sigla_regional).cast(pl.Utf8).fill_null("").alias("SIGLA_REGIONAL"),
                    month_col,
                    pl.col(col_uc).cast(pl.Utf8).alias("NUM_UC_UCI"),
                    pl.col(col_intrp).cast(pl.Utf8).alias("NUM_INTRP_PERCEBIDA"),
                    dur_expr.alias("DUR_MIN"),
                    pl.col(col_justif).cast(pl.Utf8).fill_null("").alias("TIPO_PROTOC_JUSTIF_UCI"),
                    pl.col(col_verif).cast(pl.Boolean).fill_null(False).alias("VERIFICADO"),
                ]
            )
            .join(mapping.lazy(), on="SIGLA_REGIONAL", how="left")
            .with_columns(pl.col("REGIONAL_TOTAL").fill_null("SEM_REGIONAL"))
            .drop("SIGLA_REGIONAL")
        )

    base = (
        base.filter(pl.col("__ANOMES") == anomes)
        .filter(
            pl.col("NUM_UC_UCI").is_not_null()
            & (pl.col("NUM_UC_UCI") != "")
            & pl.col("NUM_INTRP_PERCEBIDA").is_not_null()
            & (pl.col("NUM_INTRP_PERCEBIDA") != "")
            & (pl.col("VERIFICADO") == True)
        )
    )

    # Consolida no nivel da interrupcao percebida para evitar duplicidade
    # (ex.: mesma UC + interrupcao aparecendo em mais de uma linha).
    # A classificacao LIQUIDO/EXPURGO eh aplicada apos consolidacao.
    unique_base = (
        base.group_by(["REGIONAL_TOTAL", "NUM_UC_UCI", "NUM_INTRP_PERCEBIDA"])
        .agg(
            [
                pl.max("DUR_MIN").alias("DUR_MIN"),
                (pl.col("TIPO_PROTOC_JUSTIF_UCI") == "0").all().alias("TIPO_0_ALL"),
            ]
        )
    )

    unique_intrp = (
        unique_base.with_columns(
            pl.when(pl.col("TIPO_0_ALL") & (pl.col("DUR_MIN") >= 3.0))
            .then(pl.lit("LIQUIDO"))
            .otherwise(pl.lit("EXPURGO"))
            .alias("NATUREZA")
        )
        .drop("TIPO_0_ALL")
        .collect()
    )
    return unique_intrp, None


def _safe_div(a: float, b: float) -> float:
    return 0.0 if b == 0 else a / b


def _to_table(unique_intrp: pl.DataFrame, uc_mes: pl.DataFrame) -> pl.DataFrame:
    agg = (
        unique_intrp.lazy()
        .group_by(["REGIONAL_TOTAL", "NATUREZA"])
        .agg([pl.len().alias("CI"), (pl.col("DUR_MIN").sum() / 60.0).alias("CHI_HORAS")])
        .collect()
    )

    uc_rows = (
        uc_mes.select(["REGIONAL_TOTAL", "UC_FATURADA"])
        .group_by("REGIONAL_TOTAL")
        .agg(pl.col("UC_FATURADA").max().alias("UC_FATURADA"))
        .to_dicts()
    )
    uc_map = {str(r["REGIONAL_TOTAL"]): float(r["UC_FATURADA"] or 0.0) for r in uc_rows if str(r["REGIONAL_TOTAL"]) != "COPEL"}

    data_rows = []
    for reg, uc in uc_map.items():
        liq = agg.filter((pl.col("REGIONAL_TOTAL") == reg) & (pl.col("NATUREZA") == "LIQUIDO"))
        exp = agg.filter((pl.col("REGIONAL_TOTAL") == reg) & (pl.col("NATUREZA") == "EXPURGO"))

        ci_liq = float(liq["CI"].sum()) if liq.height else 0.0
        chi_liq = float(liq["CHI_HORAS"].sum()) if liq.height else 0.0
        ci_exp = float(exp["CI"].sum()) if exp.height else 0.0
        chi_exp = float(exp["CHI_HORAS"].sum()) if exp.height else 0.0
        ci_bruto = ci_liq + ci_exp
        chi_bruto = chi_liq + chi_exp

        data_rows.append(
            {
                "REGIONAL_TOTAL": reg,
                "DEC_LIQUIDO": _safe_div(chi_liq, uc),
                "DEC_EXPURGO": _safe_div(chi_exp, uc),
                "DEC_COM_EXPURGO": _safe_div(chi_liq, uc),
                "FEC_COM_EXPURGO": _safe_div(ci_liq, uc),
                "FEC_LIQUIDO": _safe_div(ci_liq, uc),
                "FEC_EXPURGO": _safe_div(ci_exp, uc),
                "DEC_BRUTO": _safe_div(chi_bruto, uc),
                "FEC_BRUTO": _safe_div(ci_bruto, uc),
                "UC_FATURADA": uc,
                "CI_LIQUIDO": ci_liq,
                "CHI_LIQUIDO": chi_liq,
                "CI_EXPURGO": ci_exp,
                "CHI_EXPURGO": chi_exp,
                "CI_BRUTO": ci_bruto,
                "CHI_BRUTO": chi_bruto,
            }
        )

    if not data_rows:
        return pl.DataFrame()

    df = pl.DataFrame(data_rows)

    uc_total = float(df["UC_FATURADA"].sum())
    ci_liq_total = float(df["CI_LIQUIDO"].sum())
    chi_liq_total = float(df["CHI_LIQUIDO"].sum())
    ci_exp_total = float(df["CI_EXPURGO"].sum())
    chi_exp_total = float(df["CHI_EXPURGO"].sum())
    ci_bruto_total = ci_liq_total + ci_exp_total
    chi_bruto_total = chi_liq_total + chi_exp_total

    copel = pl.DataFrame(
        [
            {
                "REGIONAL_TOTAL": "COPEL",
                "DEC_LIQUIDO": _safe_div(chi_liq_total, uc_total),
                "DEC_EXPURGO": _safe_div(chi_exp_total, uc_total),
                "DEC_COM_EXPURGO": _safe_div(chi_liq_total, uc_total),
                "FEC_COM_EXPURGO": _safe_div(ci_liq_total, uc_total),
                "FEC_LIQUIDO": _safe_div(ci_liq_total, uc_total),
                "FEC_EXPURGO": _safe_div(ci_exp_total, uc_total),
                "DEC_BRUTO": _safe_div(chi_bruto_total, uc_total),
                "FEC_BRUTO": _safe_div(ci_bruto_total, uc_total),
                "UC_FATURADA": uc_total,
                "CI_LIQUIDO": ci_liq_total,
                "CHI_LIQUIDO": chi_liq_total,
                "CI_EXPURGO": ci_exp_total,
                "CHI_EXPURGO": chi_exp_total,
                "CI_BRUTO": ci_bruto_total,
                "CHI_BRUTO": chi_bruto_total,
            }
        ]
    )
    out = pl.concat([df, copel], how="vertical_relaxed")

    regs = out.filter(pl.col("REGIONAL_TOTAL") != "COPEL")
    dec_ce_tot = float(regs["DEC_COM_EXPURGO"].sum()) or 1.0
    fec_ce_tot = float(regs["FEC_COM_EXPURGO"].sum()) or 1.0
    dec_br_tot = float(regs["DEC_BRUTO"].sum()) or 1.0
    fec_br_tot = float(regs["FEC_BRUTO"].sum()) or 1.0
    uc_tot = float(regs["UC_FATURADA"].sum()) or 1.0

    out = out.with_columns(
        [
            (pl.col("DEC_COM_EXPURGO") / dec_ce_tot * 100).alias("PCT_DEC_COM_EXPURGO"),
            (pl.col("FEC_COM_EXPURGO") / fec_ce_tot * 100).alias("PCT_FEC_COM_EXPURGO"),
            (pl.col("DEC_BRUTO") / dec_br_tot * 100).alias("PCT_DEC_BRUTO"),
            (pl.col("FEC_BRUTO") / fec_br_tot * 100).alias("PCT_FEC_BRUTO"),
            (pl.col("UC_FATURADA") / uc_tot * 100).alias("PCT_UC_FATURADA"),
        ]
    )
    out = out.with_columns(
        [
            pl.col("DEC_COM_EXPURGO").round(4),
            pl.col("FEC_COM_EXPURGO").round(4),
            pl.col("DEC_BRUTO").round(4),
            pl.col("FEC_BRUTO").round(4),
            pl.col("PCT_DEC_COM_EXPURGO").round(2),
            pl.col("PCT_FEC_COM_EXPURGO").round(2),
            pl.col("PCT_DEC_BRUTO").round(2),
            pl.col("PCT_FEC_BRUTO").round(2),
            pl.col("PCT_UC_FATURADA").round(2),
        ]
    )
    return out.with_columns(
        pl.when(pl.col("REGIONAL_TOTAL") == "COPEL").then(1).otherwise(0).alias("__ord_copel")
    ).sort(["__ord_copel", "REGIONAL_TOTAL"]).drop("__ord_copel")


def _fmt_num(value: object, dec: int = 2) -> str:
    try:
        v = float(value or 0.0)
    except Exception:
        v = 0.0
    txt = f"{v:,.{dec}f}"
    return txt.replace(",", "X").replace(".", ",").replace("X", ".")


def _fmt_int(value: object) -> str:
    try:
        v = int(round(float(value or 0.0)))
    except Exception:
        v = 0
    return f"{v:,}".replace(",", ".")


def _render_styled_summary_table(table: pl.DataFrame) -> None:
    regs = table.filter(pl.col("REGIONAL_TOTAL") != "COPEL").sort("REGIONAL_TOTAL")
    total_row = table.filter(pl.col("REGIONAL_TOTAL") == "COPEL")

    rows = []
    for r in regs.to_dicts():
        rows.append(
            {
                "nome": str(r["REGIONAL_TOTAL"]),
                "dec_ce": _fmt_num(r["DEC_COM_EXPURGO"], 2),
                "pct_dec_ce": _fmt_num(r["PCT_DEC_COM_EXPURGO"], 2),
                "fec_ce": _fmt_num(r["FEC_COM_EXPURGO"], 2),
                "pct_fec_ce": _fmt_num(r["PCT_FEC_COM_EXPURGO"], 2),
                "dec_br": _fmt_num(r["DEC_BRUTO"], 2),
                "pct_dec_br": _fmt_num(r["PCT_DEC_BRUTO"], 2),
                "fec_br": _fmt_num(r["FEC_BRUTO"], 2),
                "pct_fec_br": _fmt_num(r["PCT_FEC_BRUTO"], 2),
                "uc_total": _fmt_int(r["UC_FATURADA"]),
                "pct_uc": _fmt_num(r["PCT_UC_FATURADA"], 2),
                "is_total": False,
            }
        )

    if total_row.height:
        r = total_row.to_dicts()[0]
        rows.append(
            {
                "nome": "Total",
                "dec_ce": _fmt_num(r["DEC_COM_EXPURGO"], 2),
                "pct_dec_ce": "",
                "fec_ce": _fmt_num(r["FEC_COM_EXPURGO"], 2),
                "pct_fec_ce": "",
                "dec_br": _fmt_num(r["DEC_BRUTO"], 2),
                "pct_dec_br": "",
                "fec_br": _fmt_num(r["FEC_BRUTO"], 2),
                "pct_fec_br": "",
                "uc_total": _fmt_int(r["UC_FATURADA"]),
                "pct_uc": "",
                "is_total": True,
            }
        )

    style = """
<style>
.dq-table-wrap { border: 1px solid #b7b7b7; border-radius: 4px; overflow: hidden; margin-bottom: 8px; }
.dq-table { width: 100%; border-collapse: collapse; font-family: Arial, sans-serif; font-size: 14px; }
.dq-table thead tr.grp th { background: #cfcfcf; color: #000; text-align: center; border-bottom: 1px solid #b3b3b3; padding: 6px 4px; font-weight: 700; }
.dq-table thead tr.cols th { background: #e4e4e4; color: #000; text-align: center; border-bottom: 1px solid #c7c7c7; padding: 6px 4px; font-weight: 700; }
.dq-table tbody td { padding: 6px 8px; border-bottom: 1px solid #dddddd; text-align: right; }
.dq-table tbody td.name { text-align: left; font-weight: 600; }
.dq-table tbody tr:nth-child(even) td { background: #f7f7f7; }
.dq-table tbody tr.total td { font-weight: 700; background: #efefef; border-top: 2px solid #c7c7c7; }
</style>
"""

    body_rows = []
    for row in rows:
        cls = "total" if row["is_total"] else ""
        body_rows.append(
            f"""
<tr class="{cls}">
  <td class="name">{row['nome']}</td>
  <td>{row['dec_ce']}</td><td>{row['pct_dec_ce']}</td><td>{row['fec_ce']}</td><td>{row['pct_fec_ce']}</td>
  <td>{row['dec_br']}</td><td>{row['pct_dec_br']}</td><td>{row['fec_br']}</td><td>{row['pct_fec_br']}</td>
  <td>{row['uc_total']}</td><td>{row['pct_uc']}</td>
</tr>
"""
        )

    html = f"""
{style}
<div class="dq-table-wrap">
  <table class="dq-table">
    <thead>
      <tr class="grp">
        <th></th>
        <th colspan="4">Com expurgo</th>
        <th colspan="4">Bruto</th>
        <th colspan="2">Consumidores</th>
      </tr>
      <tr class="cols">
        <th>Nome</th>
        <th>DEC</th><th>%</th><th>FEC</th><th>%</th>
        <th>DEC</th><th>%</th><th>FEC</th><th>%</th>
        <th>Total</th><th>%</th>
      </tr>
    </thead>
    <tbody>{''.join(body_rows)}</tbody>
  </table>
</div>
"""
    st.markdown(html, unsafe_allow_html=True)


def _render_charts(table: pl.DataFrame) -> None:
    regs = table.filter(pl.col("REGIONAL_TOTAL") != "COPEL")
    if regs.is_empty():
        return

    try:
        import altair as alt

        dec_long = regs.select(["REGIONAL_TOTAL", "DEC_LIQUIDO", "DEC_EXPURGO", "DEC_BRUTO"]).to_pandas()
        dec_plot = dec_long.melt(
            id_vars=["REGIONAL_TOTAL", "DEC_BRUTO"],
            value_vars=["DEC_LIQUIDO", "DEC_EXPURGO"],
            var_name="Componente",
            value_name="Valor",
        )
        dec_plot["Componente"] = dec_plot["Componente"].map({"DEC_LIQUIDO": "Liquido", "DEC_EXPURGO": "Expurgo"})
        bars = (
            alt.Chart(dec_plot)
            .mark_bar()
            .encode(
                x=alt.X("REGIONAL_TOTAL:N", title="Regional"),
                y=alt.Y("sum(Valor):Q", title="DEC"),
                color=alt.Color("Componente:N", title="Componente"),
            )
        )
        line = (
            alt.Chart(dec_long)
            .mark_line(point=True, color="#333333")
            .encode(x=alt.X("REGIONAL_TOTAL:N"), y=alt.Y("DEC_BRUTO:Q"))
        )
        st.write("DEC por regional (Liquido + Expurgo = Bruto)")
        st.altair_chart((bars + line).interactive(), use_container_width=True)
    except Exception:
        dec_df = regs.select(["REGIONAL_TOTAL", "DEC_LIQUIDO", "DEC_EXPURGO"]).to_pandas().set_index("REGIONAL_TOTAL")
        st.write("DEC por regional (Liquido + Expurgo)")
        st.bar_chart(dec_df)

    try:
        import altair as alt

        fec_long = regs.select(["REGIONAL_TOTAL", "FEC_LIQUIDO", "FEC_EXPURGO", "FEC_BRUTO"]).to_pandas()
        fec_plot = fec_long.melt(
            id_vars=["REGIONAL_TOTAL", "FEC_BRUTO"],
            value_vars=["FEC_LIQUIDO", "FEC_EXPURGO"],
            var_name="Componente",
            value_name="Valor",
        )
        fec_plot["Componente"] = fec_plot["Componente"].map({"FEC_LIQUIDO": "Liquido", "FEC_EXPURGO": "Expurgo"})
        bars = (
            alt.Chart(fec_plot)
            .mark_bar()
            .encode(
                x=alt.X("REGIONAL_TOTAL:N", title="Regional"),
                y=alt.Y("sum(Valor):Q", title="FEC"),
                color=alt.Color("Componente:N", title="Componente"),
            )
        )
        line = (
            alt.Chart(fec_long)
            .mark_line(point=True, color="#333333")
            .encode(x=alt.X("REGIONAL_TOTAL:N"), y=alt.Y("FEC_BRUTO:Q"))
        )
        st.write("FEC por regional (Liquido + Expurgo = Bruto)")
        st.altair_chart((bars + line).interactive(), use_container_width=True)
    except Exception:
        fec_df = regs.select(["REGIONAL_TOTAL", "FEC_LIQUIDO", "FEC_EXPURGO"]).to_pandas().set_index("REGIONAL_TOTAL")
        st.write("FEC por regional (Liquido + Expurgo)")
        st.bar_chart(fec_df)


def render_apuracao_percebido_panel() -> None:
    st.subheader("DEC/FEC para visao geografica - Regionais")
    st.caption(
        "BRUTO = LIQUIDO + EXPURGO. DEC = CHI/UC_faturada. FEC = CI/UC_faturada. "
        "Regra LIQUIDO: TIPO_PROTOC_JUSTIF_UCI='0' e DURACAO >= 3 minutos."
    )

    uc_df, uc_err = _load_uc_faturada()
    if uc_err:
        st.error(uc_err)
        return
    if uc_df is None or uc_df.is_empty():
        st.warning("UC_faturada.parquet sem dados.")
        return

    months = sorted(set(uc_df["ANOMES"].to_list()), reverse=True)
    if not months:
        st.warning("Nao foram encontrados meses em UC_faturada.parquet.")
        return

    anomes = st.selectbox("Periodo (ANOMES)", options=months, index=0, key="_dq_apuracao_anomes")
    files = _discover_processed_files()
    if not files:
        st.warning("Nenhum processed encontrado.")
        return

    if not _has_percebido_cols(files):
        st.error(
            "Apuracao em modo estritamente PERCEBIDO: faltam colunas percebidas no processed. "
            "Rode primeiro em 3_Qualidade_dados a acao 'Verificar sobreposicao e gravar colunas'."
        )
        return

    if st.button("Calcular DEC/FEC", key="_dq_apuracao_run"):
        with st.spinner("Calculando apuracao por regional..."):
            uc_mes = uc_df.filter(pl.col("ANOMES") == anomes)
            unique_intrp, err = _prepare_processed_for_month(files, anomes, uc_mes)
            if err:
                st.session_state["_dq_apuracao_err"] = err
                st.session_state["_dq_apuracao_table"] = pl.DataFrame()
            elif unique_intrp is None or unique_intrp.is_empty():
                st.session_state["_dq_apuracao_err"] = "Sem dados de interrupcao para o periodo."
                st.session_state["_dq_apuracao_table"] = pl.DataFrame()
            else:
                table = _to_table(unique_intrp, uc_mes)
                st.session_state["_dq_apuracao_err"] = ""
                st.session_state["_dq_apuracao_table"] = table

    err = st.session_state.get("_dq_apuracao_err", "")
    if err:
        st.error(str(err))
        return

    table = st.session_state.get("_dq_apuracao_table")
    if not isinstance(table, pl.DataFrame):
        return
    if table.is_empty():
        st.info("Sem resultado para os filtros atuais.")
        return

    st.success("Pesquisa realizada com sucesso.")
    _render_styled_summary_table(table)

    with st.expander("Tabela tecnica (dados numericos)"):
        st.dataframe(
            table.select(
                [
                    "REGIONAL_TOTAL",
                    "DEC_COM_EXPURGO",
                    "PCT_DEC_COM_EXPURGO",
                    "FEC_COM_EXPURGO",
                    "PCT_FEC_COM_EXPURGO",
                    "DEC_BRUTO",
                    "PCT_DEC_BRUTO",
                    "FEC_BRUTO",
                    "PCT_FEC_BRUTO",
                    "UC_FATURADA",
                    "PCT_UC_FATURADA",
                    "DEC_LIQUIDO",
                    "DEC_EXPURGO",
                    "FEC_LIQUIDO",
                    "FEC_EXPURGO",
                ]
            ),
            use_container_width=True,
        )

    with st.expander("Base de calculo (CI/CHI por natureza)"):
        st.dataframe(
            table.select(
                [
                    "REGIONAL_TOTAL",
                    "CI_LIQUIDO",
                    "CHI_LIQUIDO",
                    "CI_EXPURGO",
                    "CHI_EXPURGO",
                    "CI_BRUTO",
                    "CHI_BRUTO",
                ]
            ),
            use_container_width=True,
        )

    _render_charts(table)
def _dq_fix_processed_for_apuracao(df):
    import pandas as _pd

    out = df.copy()
    fim_perc = "DATA_HORA_FIM_INTR_PERCEBIDO"
    ini_perc = "DTHR_INICIO_INTRP_PERCEBIDO"

    if fim_perc in out.columns:
        for c in ("DATA_HORA_FIM_INTRP", "DATA_HORA_FIM_INTR", "DT_FIM"):
            if c in out.columns:
                out[fim_perc] = out[fim_perc].fillna(out[c])
                break
    if ini_perc in out.columns:
        for c in ("DTHR_INICIO_INTRP_UC", "DTHR_INICIO_INTRP", "DT_INI"):
            if c in out.columns:
                out[ini_perc] = out[ini_perc].fillna(out[c])
                break

    if ini_perc in out.columns and fim_perc in out.columns:
        ini = _pd.to_datetime(out[ini_perc], errors="coerce")
        fim = _pd.to_datetime(out[fim_perc], errors="coerce")
        dur = (fim - ini).dt.total_seconds().div(60.0)
        if "DURACAO_PERCEBIDA_MINUTOS" in out.columns:
            out["DURACAO_PERCEBIDA_MINUTOS"] = _pd.to_numeric(
                out["DURACAO_PERCEBIDA_MINUTOS"], errors="coerce"
            ).fillna(dur)
        else:
            out["DURACAO_PERCEBIDA_MINUTOS"] = dur

    if "longa_duracao" not in out.columns and "DURACAO_PERCEBIDA_MINUTOS" in out.columns:
        out["longa_duracao"] = _pd.to_numeric(
            out["DURACAO_PERCEBIDA_MINUTOS"], errors="coerce"
        ).fillna(0).ge(3)

    if "NUM_INTRP_PERCEBIDA" in out.columns:
        for c in ("NUM_INTRP_INIC_MANOBRA_UCI", "NUM_INTRP"):
            if c in out.columns:
                out["NUM_INTRP_PERCEBIDA"] = out["NUM_INTRP_PERCEBIDA"].fillna(out[c])
                break

    if "verificado" in out.columns:
        v = out["verificado"]
        if v.dtype == "object":
            v = v.astype(str).str.strip().str.lower().map(
                {"true": True, "1": True, "sim": True, "yes": True, "false": False, "0": False, "nao": False, "não": False, "no": False}
            )
        out["verificado"] = v.fillna(True).astype(bool)
    else:
        out["verificado"] = True

    return out


def _install_apuracao_read_parquet_patch():
    import pandas as _pd

    if getattr(_pd.read_parquet, "_dq_apuracao_patch", False):
        return

    _orig = _pd.read_parquet

    def _wrapped_read_parquet(*args, **kwargs):
        df = _orig(*args, **kwargs)

        path = args[0] if args else kwargs.get("path")
        try:
            p = str(path).replace("\\", "/").lower() if path is not None else ""
        except Exception:
            p = ""

        if p.endswith("dic_fic_uc_processed.parquet"):
            return _dq_fix_processed_for_apuracao(df)
        return df

    _wrapped_read_parquet._dq_apuracao_patch = True
    _pd.read_parquet = _wrapped_read_parquet


_install_apuracao_read_parquet_patch()
