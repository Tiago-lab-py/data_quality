import streamlit as st
import polars as pl
from pathlib import Path

st.title("Prévia de Dados Processados")
st.subheader("Cálculo de CI e CHI por SIGLA_REGIONAL")

project_root = Path(__file__).resolve().parent.parent.parent
processed_file = project_root / "data" / "processed" / "dic_fic_uc_processed.parquet"

st.caption(f"Arquivo de entrada: `{processed_file}`")

if not processed_file.exists():
    st.warning("Arquivo processado não encontrado. Execute primeiro o processamento na aba Importação.")
    st.stop()

try:
    df = pl.read_parquet(processed_file)
except Exception as e:
    st.error(f"Erro ao ler arquivo Parquet: {e}")
    st.stop()

col1, col2 = st.columns(2)
with col1:
    st.metric("Registros", f"{len(df):,}")
with col2:
    st.metric("Colunas", len(df.columns))

with st.expander("Pré-visualização dos dados processados", expanded=False):
    st.dataframe(df.head(20).to_pandas(), use_container_width=True, hide_index=True)

if "SIGLA_REGIONAL" not in df.columns:
    st.error("A coluna obrigatória `SIGLA_REGIONAL` não foi encontrada no arquivo processado.")
    st.stop()

if st.button("Calcular CI e CHI por SIGLA_REGIONAL", type="primary", use_container_width=True):
    base = df.with_columns(
        pl.col("SIGLA_REGIONAL")
        .cast(pl.Utf8, strict=False)
        .fill_null("SEM_REGIONAL")
        .alias("SIGLA_REGIONAL")
    )

    if "DURACAO_PERCEBIDA_MINUTOS" in base.columns:
        base = base.with_columns(
            pl.col("DURACAO_PERCEBIDA_MINUTOS")
            .cast(pl.Float64, strict=False)
            .fill_null(0.0)
            .alias("_duracao_min")
        )
    else:
        base = base.with_columns(pl.lit(0.0).alias("_duracao_min"))

    if "QTD_UC_AFETADA" in base.columns:
        base = base.with_columns(
            pl.col("QTD_UC_AFETADA")
            .cast(pl.Float64, strict=False)
            .fill_null(1.0)
            .alias("_qtd_uc")
        )
        ci_expr = pl.col("_qtd_uc").sum().alias("CI")
    elif "NUM_UC_UCI" in base.columns:
        base = base.with_columns(pl.lit(1.0).alias("_qtd_uc"))
        ci_expr = pl.col("NUM_UC_UCI").n_unique().alias("CI")
    else:
        base = base.with_columns(pl.lit(1.0).alias("_qtd_uc"))
        ci_expr = pl.len().cast(pl.Float64).alias("CI")

    def calcular_indicadores_por_regional(df_base: pl.DataFrame) -> pl.DataFrame:
        if df_base.is_empty():
            return pl.DataFrame(
                {
                    "SIGLA_REGIONAL": [],
                    "CI": [],
                    "CHI_horas": [],
                }
            )

        return (
            df_base.group_by("SIGLA_REGIONAL")
            .agg(
                [
                    ci_expr,
                    (((pl.col("_duracao_min") / 60.0) * pl.col("_qtd_uc")).sum()).alias("CHI_horas"),
                ]
            )
            .with_columns(
                [
                    pl.col("CI").round(0),
                    pl.col("CHI_horas").round(2),
                ]
            )
            .sort("SIGLA_REGIONAL")
        )

    base_maior_igual_3 = base.filter(pl.col("_duracao_min") >= 3)
    base_menor_3 = base.filter(pl.col("_duracao_min") < 3)

    indicadores_maior_igual_3 = calcular_indicadores_por_regional(base_maior_igual_3)
    indicadores_menor_3 = calcular_indicadores_por_regional(base_menor_3)

    st.success("Cálculo concluído com sucesso.")

    st.markdown("### Quadro 1 - Duração >= 3 minutos")
    st.dataframe(indicadores_maior_igual_3.to_pandas(), use_container_width=True, hide_index=True)
    ci_total_maior_igual_3 = float(indicadores_maior_igual_3["CI"].sum()) if len(indicadores_maior_igual_3) else 0.0
    chi_total_maior_igual_3 = float(indicadores_maior_igual_3["CHI_horas"].sum()) if len(indicadores_maior_igual_3) else 0.0
    c1, c2 = st.columns(2)
    with c1:
        st.metric("CI Total (>=3 min)", f"{ci_total_maior_igual_3:,.0f}")
    with c2:
        st.metric("CHI Total (>=3 min)", f"{chi_total_maior_igual_3:,.2f}")

    st.markdown("### Quadro 2 - Duração < 3 minutos")
    st.dataframe(indicadores_menor_3.to_pandas(), use_container_width=True, hide_index=True)
    ci_total_menor_3 = float(indicadores_menor_3["CI"].sum()) if len(indicadores_menor_3) else 0.0
    chi_total_menor_3 = float(indicadores_menor_3["CHI_horas"].sum()) if len(indicadores_menor_3) else 0.0
    c3, c4 = st.columns(2)
    with c3:
        st.metric("CI Total (<3 min)", f"{ci_total_menor_3:,.0f}")
    with c4:
        st.metric("CHI Total (<3 min)", f"{chi_total_menor_3:,.2f}")

    output_file_maior_igual_3 = project_root / "data" / "processed" / "ci_chi_por_sigla_regional_maior_igual_3.parquet"
    output_file_menor_3 = project_root / "data" / "processed" / "ci_chi_por_sigla_regional_menor_3.parquet"
    indicadores_maior_igual_3.write_parquet(output_file_maior_igual_3)
    indicadores_menor_3.write_parquet(output_file_menor_3)
    st.info(f"Resultados salvos em: `{output_file_maior_igual_3}` e `{output_file_menor_3}`")
from pathlib import Path

import streamlit as st

_DQ_IMPORT_LOCK = Path("D:/data_quality/data/raw/dic_fic_uc.lock")
_DQ_MONTH_LOCKS = list(Path("D:/data_quality/data/raw").glob("dic_fic_uc_*.lock"))
if _DQ_IMPORT_LOCK.exists() or _DQ_MONTH_LOCKS:
    st.warning("Importação em andamento. Aguarde finalizar para abrir a prévia.")
    st.stop()

try:
    import polars as pl

    if not getattr(pl.read_parquet, "__dq_no_mmap__", False):
        _orig_read_parquet = pl.read_parquet

        def _safe_read_parquet(*args, **kwargs):  # type: ignore[no-untyped-def]
            kwargs.setdefault("memory_map", False)
            return _orig_read_parquet(*args, **kwargs)

        setattr(_safe_read_parquet, "__dq_no_mmap__", True)
        pl.read_parquet = _safe_read_parquet  # type: ignore[assignment]
except Exception:
    pass
