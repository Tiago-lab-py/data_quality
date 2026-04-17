import streamlit as st
from lib.report_utils import generate_report

st.title("Resultados e Dashboards")

if 'quality_results' not in st.session_state:
    st.warning("Por favor, execute as regras de qualidade primeiro.")
else:
    results = st.session_state['quality_results']
    
    st.subheader("Métricas de Qualidade")
    
    # Display results
    for rule, result in results.items():
        st.metric(rule, result['passed'], result['failed'])
    
    # Generate report
    if st.button("Gerar Relatório"):
        report_path = generate_report(results)
        st.success(f"Relatório gerado: {report_path}")
        
        # Download button
        with open(report_path, "rb") as f:
            st.download_button("Baixar Relatório", f, file_name="quality_report.html")