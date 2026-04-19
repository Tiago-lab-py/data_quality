from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple

import polars as pl
import streamlit as st


PROCESSED_BASE = Path("D:/data_quality/data/processed")
RESULT_KEY = "_dq_intrp_check_result"
SAMPLE_KEY = "_dq_intrp_contained_sample"


def _discover_processed_files() -> List[Path]:
    files: List[Path] = []
    single = PROCESSED_BASE / "dic_fic_uc_processed.parquet"
    if single.exists():
        files.append(single)

    for folder_name in ("dic_fic_uc_processed", "dic_fic_uc"):
        folder = PROCESSED_BASE / folder_name
        if folder.exists() and folder.is_dir():
            files.extend(sorted(folder.rglob("*.parquet")))

    unique: List[Path] = []
    seen = set()
    for p in files:
        sp = str(p)
        if sp not in seen:
            seen.add(sp)
            unique.append(p)
    return unique


def _resolve_columns(df_cols: List[str]) -> Optional[Tuple[str, str, str, str]]:
    lower_map = {c.lower(): c for c in df_cols}
    required = [
        "num_uc_uci",
        "num_intrp_inic_manobra_uci",
        "dthr_inicio_intrp_uc",
        "data_hora_fim_intrp",
    ]
    resolved = []
    alt = {
        "data_hora_fim_intrp": ["data_hora_fim_intrp", "data_hora_fim_intr", "dt_fim_intrp_uc"],
    }
    for r in required:
        col = lower_map.get(r)
        if not col:
            if r in alt:
                for a in alt[r]:
                    col = lower_map.get(a)
                    if col:
                        break
        if not col:
            return None
        resolved.append(col)
    return resolved[0], resolved[1], resolved[2], resolved[3]


def _mark_file(path: Path) -> Dict[str, object]:
    df = pl.read_parquet(str(path), memory_map=False)
    resolved = _resolve_columns(df.columns)
    if resolved is None:
        return {
            "arquivo": str(path),
            "status": "colunas_ausentes",
            "total": 0,
            "verificado_true": 0,
            "contido_true": 0,
            "sobreposto_true": 0,
            "conta_unico_true": 0,
        }

    col_cliente, col_intrp, col_ini, col_fim = resolved
    processed = (
        df.with_row_index("__rid")
        .with_columns(
            [
                pl.col(col_cliente).cast(pl.Utf8).alias("__cliente"),
                pl.col(col_intrp).cast(pl.Utf8).alias("__intrp"),
                pl.col(col_ini).cast(pl.Datetime, strict=False).alias("__ini"),
                pl.col(col_fim).cast(pl.Datetime, strict=False).alias("__fim"),
            ]
        )
        .sort(["__cliente", "__ini", "__fim", "__intrp"])
        .with_columns(
            [
                (
                    pl.col("__cliente").is_not_null()
                    & pl.col("__intrp").is_not_null()
                    & pl.col("__ini").is_not_null()
                    & pl.col("__fim").is_not_null()
                    & (pl.col("__fim") >= pl.col("__ini"))
                ).alias("__valido"),
                pl.col("__fim").cum_max().shift(1).over("__cliente").alias("__prev_max_fim"),
                pl.col("__ini").reverse().cum_min().shift(1).reverse().over("__cliente").alias("__next_min_ini"),
                pl.col("__fim").shift(1).over("__cliente").alias("__prev_fim"),
                (
                    (pl.col("__fim") - pl.col("__ini")).dt.total_seconds() / 60.0
                ).alias("__dur_min"),
            ]
        )
        .with_columns(
            [
                (
                    (pl.col("__ini") - pl.col("__prev_fim")).dt.total_seconds() / 60.0
                ).alias("__gap_prev_min"),
                pl.col("__dur_min").shift(1).over("__cliente").alias("__prev_dur_min"),
            ]
        )
        .with_columns(
            [
                pl.col("__valido").fill_null(False).alias("verificado"),
                (
                    pl.col("__valido")
                    & (pl.col("__fim") <= pl.col("__prev_max_fim"))
                )
                .fill_null(False)
                .alias("Contido_outra_intrp"),
                (
                    pl.col("__valido")
                    & (
                        (pl.col("__ini") <= pl.col("__prev_max_fim"))
                        | (pl.col("__fim") >= pl.col("__next_min_ini"))
                    )
                )
                .fill_null(False)
                .alias("Sobreposicao_tempo"),
                (
                    pl.col("__valido")
                    & (pl.col("__ini") <= pl.col("__prev_max_fim"))
                )
                .fill_null(False)
                .alias("__sobrepoe_anterior"),
                (
                    pl.col("__valido")
                    & pl.col("__gap_prev_min").is_not_null()
                    & (pl.col("__gap_prev_min") >= 0.0)
                    & (pl.col("__gap_prev_min") < 1.0)
                    & pl.col("__dur_min").is_not_null()
                    & pl.col("__prev_dur_min").is_not_null()
                    & (pl.col("__dur_min") < 1.0)
                    & (pl.col("__prev_dur_min") < 1.0)
                )
                .fill_null(False)
                .alias("__merge_percebido"),
            ]
        )
        .with_columns(
            [
                (
                    pl.col("verificado") & (~pl.col("__sobrepoe_anterior"))
                ).alias("Conta_desligamento_unico"),
                (
                    pl.when(pl.col("verificado"))
                    .then(
                        (pl.col("verificado") & (~pl.col("__sobrepoe_anterior")))
                        .cast(pl.Int64)
                        .cum_sum()
                        .over("__cliente")
                    )
                    .otherwise(None)
                ).alias("Grupo_desligamento_uc"),
                (
                    pl.when(pl.col("verificado"))
                    .then(
                        (~pl.col("__merge_percebido")).cast(pl.Int64).cum_sum().over("__cliente")
                    )
                    .otherwise(None)
                ).alias("__grupo_percebido"),
            ]
        )
        .with_columns(
            [
                pl.col("__ini").min().over(["__cliente", "__grupo_percebido"]).alias("__ini_percebido"),
                pl.col("__fim").max().over(["__cliente", "__grupo_percebido"]).alias("__fim_percebido"),
                pl.col("__intrp").first().over(["__cliente", "__grupo_percebido"]).alias("__intrp_percebida"),
            ]
        )
        .with_columns(
            [
                pl.when(pl.col("verificado"))
                .then(pl.col("__intrp_percebida"))
                .otherwise(pl.col(col_intrp).cast(pl.Utf8))
                .alias("NUM_INTRP_PERCEBIDA"),
                pl.when(pl.col("verificado"))
                .then(pl.col("__ini_percebido"))
                .otherwise(pl.col(col_ini).cast(pl.Datetime, strict=False))
                .alias("DTHR_INICIO_INTRP_PERCEBIDO"),
                pl.when(pl.col("verificado"))
                .then(pl.col("__fim_percebido"))
                .otherwise(pl.col(col_fim).cast(pl.Datetime, strict=False))
                .alias("DATA_HORA_FIM_INTR_PERCEBIDO"),
                pl.when(pl.col("verificado"))
                .then(((pl.col("__fim_percebido") - pl.col("__ini_percebido")).dt.total_seconds() / 60.0))
                .otherwise(pl.col("__dur_min"))
                .alias("DURACAO_PERCEBIDA_MINUTOS"),
            ]
        )
        .with_columns(
            [
                pl.col("NUM_INTRP_PERCEBIDA").fill_null(pl.col(col_intrp).cast(pl.Utf8)),
                pl.col("DTHR_INICIO_INTRP_PERCEBIDO").fill_null(pl.col(col_ini).cast(pl.Datetime, strict=False)),
                pl.col("DATA_HORA_FIM_INTR_PERCEBIDO").fill_null(pl.col(col_fim).cast(pl.Datetime, strict=False)),
                pl.col("DURACAO_PERCEBIDA_MINUTOS").fill_null(pl.col("__dur_min")).fill_null(0.0),
            ]
        )
        .sort("__rid")
        .drop(
            [
                "__rid",
                "__cliente",
                "__intrp",
                "__ini",
                "__fim",
                "__prev_max_fim",
                "__next_min_ini",
                "__valido",
                "__sobrepoe_anterior",
                "__prev_fim",
                "__dur_min",
                "__gap_prev_min",
                "__prev_dur_min",
                "__merge_percebido",
                "__grupo_percebido",
                "__ini_percebido",
                "__fim_percebido",
                "__intrp_percebida",
            ]
        )
    )

    tmp_path = path.with_suffix(".tmp.parquet")
    processed.write_parquet(str(tmp_path), compression="snappy")
    tmp_path.replace(path)

    verificado_true = int(processed.filter(pl.col("verificado") == True).height)
    contido_true = int(processed.filter(pl.col("Contido_outra_intrp") == True).height)
    sobreposto_true = int(processed.filter(pl.col("Sobreposicao_tempo") == True).height)
    conta_unico_true = int(processed.filter(pl.col("Conta_desligamento_unico") == True).height)

    return {
        "arquivo": str(path),
        "status": "ok",
        "total": int(processed.height),
        "verificado_true": verificado_true,
        "contido_true": contido_true,
        "sobreposto_true": sobreposto_true,
        "conta_unico_true": conta_unico_true,
        "col_cliente": col_cliente,
        "col_intrp": col_intrp,
        "col_ini": col_ini,
        "col_fim": col_fim,
    }


def _collect_contained_sample(paths: List[Path], max_rows: int) -> pl.DataFrame:
    frames: List[pl.DataFrame] = []
    for p in paths:
        try:
            df = pl.read_parquet(str(p), memory_map=False)
            resolved = _resolve_columns(df.columns)
            if resolved is None or "Contido_outra_intrp" not in df.columns:
                continue
            col_cliente, col_intrp, col_ini, col_fim = resolved
            sub = (
                df.filter(pl.col("Contido_outra_intrp") == True)
                .select(
                    [
                        pl.lit(str(p)).alias("arquivo"),
                        pl.col(col_cliente).alias("NUM_UC_UCI"),
                        pl.col(col_intrp).alias("NUM_INTRP_INIC_MANOBRA_UCI"),
                        pl.col(col_ini).alias("DTHR_INICIO_INTRP_UC"),
                        pl.col(col_fim).alias("DATA_HORA_FIM_INTR"),
                        pl.col("verificado"),
                        pl.col("Contido_outra_intrp"),
                        pl.col("Sobreposicao_tempo"),
                        pl.col("Conta_desligamento_unico"),
                        pl.col("Grupo_desligamento_uc"),
                    ]
                )
                .head(max_rows)
            )
            if sub.height:
                frames.append(sub)
            if sum(f.height for f in frames) >= max_rows:
                break
        except Exception:
            continue

    if not frames:
        return pl.DataFrame(
            {
                "arquivo": [],
                "NUM_UC_UCI": [],
                "NUM_INTRP_INIC_MANOBRA_UCI": [],
                "DTHR_INICIO_INTRP_UC": [],
                "DATA_HORA_FIM_INTR": [],
                "verificado": [],
                "Contido_outra_intrp": [],
                "Sobreposicao_tempo": [],
                "Conta_desligamento_unico": [],
                "Grupo_desligamento_uc": [],
            }
        )
    out = pl.concat(frames, how="vertical_relaxed")
    return out.head(max_rows)


def render_interruptions_overlap_panel() -> None:
    st.subheader("Sobreposicao de interrupcoes por cliente")
    st.caption(
        "Valida sobreposicao de intervalo por NUM_UC_UCI e marca colunas: "
        "`verificado`, `Contido_outra_intrp`, campos `PERCEBIDO` e contagem unica no dic_fic_uc_processed."
    )

    files = _discover_processed_files()
    if not files:
        st.warning("Nenhum parquet processed encontrado.")
        return

    st.write(f"Arquivos detectados: {len(files)}")
    max_sample = st.number_input(
        "Maximo de linhas para amostra de interrupcoes contidas",
        min_value=10,
        max_value=10000,
        value=500,
        step=10,
        key="_dq_intrp_sample_limit",
    )

    if st.button("Verificar sobreposicao e gravar colunas", key="_dq_intrp_run"):
        rows = []
        with st.spinner("Processando arquivos e gravando colunas de verificacao..."):
            for p in files:
                rows.append(_mark_file(p))

        result_df = pl.DataFrame(rows)
        st.session_state[RESULT_KEY] = result_df

        sample_df = _collect_contained_sample(files, int(max_sample))
        st.session_state[SAMPLE_KEY] = sample_df

    result = st.session_state.get(RESULT_KEY)
    sample = st.session_state.get(SAMPLE_KEY)

    if isinstance(result, pl.DataFrame) and not result.is_empty():
        st.write("Resumo da verificacao")
        st.dataframe(result, use_container_width=True)

        ok = result.filter(pl.col("status") == "ok")
        if ok.height:
            total = int(ok["total"].sum())
            verif = int(ok["verificado_true"].sum())
            cont = int(ok["contido_true"].sum())
            sobre = int(ok["sobreposto_true"].sum())
            unico = int(ok["conta_unico_true"].sum())
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Total linhas", f"{total:,}".replace(",", "."))
            c2.metric("verificado=true", f"{verif:,}".replace(",", "."))
            c3.metric("Contido_outra_intrp=true", f"{cont:,}".replace(",", "."))
            c4.metric("Sobrepostos", f"{sobre:,}".replace(",", "."))
            c5.metric("Desligamento unico", f"{unico:,}".replace(",", "."))

    if isinstance(sample, pl.DataFrame):
        st.write("Interrupcoes contidas em outras (amostra)")
        if sample.is_empty():
            st.success("Nao foram encontradas interrupcoes contidas nas particoes analisadas.")
        else:
            st.dataframe(sample, use_container_width=True)
