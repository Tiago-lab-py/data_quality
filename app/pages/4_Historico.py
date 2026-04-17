import streamlit as st
import os
from pathlib import Path

st.title("Histórico")

st.subheader("Execuções Anteriores")

# List output files
output_dir = Path("data/output")
if output_dir.exists():
    files = list(output_dir.glob("*"))
    if files:
        for file in files:
            st.write(f"- {file.name}")
            if st.button(f"Ver {file.name}"):
                # Display file content or download
                pass
    else:
        st.write("Nenhuma execução anterior encontrada.")
else:
    st.write("Diretório de saída não encontrado.")