from __future__ import annotations

from datetime import date
from typing import Dict

import streamlit as st


RESULT_KEY = "_dq_uc_faturada_last_result"


def render_uc_faturada_panel() -> None:
    st.subheader("Extracao independente - Historico UC Faturada")
    st.caption(
        "Executa somente o SQL IQS_Historico_UC_faturado e grava em data/raw/UC_faturada.parquet. "
        "Conexao Oracle via .env (ORACLE_UID/ORACLE_PWD/ORACLE_DB)."
    )

    default_data = date(2025, 1, 1)
    data_ini = st.date_input(
        "Data inicial historico UC faturada",
        value=default_data,
        key="_dq_uc_faturada_data_ini",
    )
    data_ini_str = data_ini.strftime("%Y-%m-%d")

    if st.button("Extrair Historico UC Faturada", key="_dq_uc_faturada_run"):
        try:
            try:
                from app.services.extracao_uc_faturada import extrair_uc_faturada_raw
            except Exception:
                from services.extracao_uc_faturada import extrair_uc_faturada_raw

            with st.spinner("Extraindo historico UC faturada..."):
                info = extrair_uc_faturada_raw(data_ini=data_ini_str)
            st.session_state[RESULT_KEY] = {"ok": True, "info": info}
        except Exception as e:
            import traceback

            st.session_state[RESULT_KEY] = {"ok": False, "error": traceback.format_exc(), "message": str(e)}

    result = st.session_state.get(RESULT_KEY)
    if not isinstance(result, dict):
        return

    if result.get("ok"):
        info = result.get("info", {}) or {}
        rows = int(info.get("rows", 0))
        chunks = int(info.get("chunks", 0))
        out = str(info.get("output_path", "") or "")
        st.success(f"Concluido: {rows:,} linhas em {chunks} chunks.".replace(",", "."))
        if out:
            st.caption(f"Arquivo: {out}")
    else:
        st.error("Falha ao extrair historico UC faturada.")
        msg = str(result.get("message", "") or "")
        if msg:
            st.caption(msg)
        err = str(result.get("error", "") or "")
        if err:
            with st.expander("Detalhes do erro"):
                st.code(err, language="text")
