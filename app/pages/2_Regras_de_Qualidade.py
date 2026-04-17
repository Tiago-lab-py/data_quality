import streamlit as st
from lib.quality_rules import apply_quality_rules
from lib.io_utils import load_data, save_processed_data

st.title("Regras de Qualidade")

if 'data_file' not in st.session_state:
    st.warning("Por favor, importe os dados primeiro.")
else:
    st.subheader("Aplicar Regras de Qualidade")
    
    # Load data
    df = load_data(st.session_state['data_file'])
    
    # Apply rules
    if st.button("Executar Verificação de Qualidade"):
        results = apply_quality_rules(df)
        
        # Save processed data
        processed_path = save_processed_data(df, "processed_data")
        st.session_state['processed_file'] = processed_path
        st.session_state['quality_results'] = results
        
        st.success("Verificação de qualidade concluída!")
        st.json(results)