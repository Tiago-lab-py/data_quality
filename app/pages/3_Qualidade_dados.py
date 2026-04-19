import streamlit as st

try:
    from app.services.quality_outliers import render_outlier_consistency_panel, render_negative_duration_panel
except Exception:
    try:
        from services.quality_outliers import render_outlier_consistency_panel, render_negative_duration_panel
    except Exception:
        render_outlier_consistency_panel = None
        render_negative_duration_panel = None

try:
    from app.services.quality_regional import render_regional_quality_panel
except Exception:
    try:
        from services.quality_regional import render_regional_quality_panel
    except Exception:
        render_regional_quality_panel = None

try:
    from app.services.quality_interruptions import render_interruptions_overlap_panel
except Exception:
    try:
        from services.quality_interruptions import render_interruptions_overlap_panel
    except Exception:
        render_interruptions_overlap_panel = None

try:
    from app.services.quality_precision_nrt import render_nrt_precision_panel
except Exception:
    try:
        from services.quality_precision_nrt import render_nrt_precision_panel
    except Exception:
        render_nrt_precision_panel = None
import sys
from pathlib import Path

_THIS_FILE = Path(__file__).resolve()
_APP_DIR = _THIS_FILE.parents[1]
_REPO_DIR = _THIS_FILE.parents[2]
for _p in (_APP_DIR, _REPO_DIR):
    _sp = str(_p)
    if _sp not in sys.path:
        sys.path.insert(0, _sp)

try:
    from lib.report_utils import generate_report
except ModuleNotFoundError:
    try:
        from app.lib.report_utils import generate_report
    except ModuleNotFoundError:
        try:
            from report_utils import generate_report
        except ModuleNotFoundError:
            def generate_report(*args, **kwargs):
                raise ModuleNotFoundError(
                    "Nao foi possivel localizar report_utils. "
                    "Verifique os caminhos: lib.report_utils ou app.lib.report_utils."
                )

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
if render_outlier_consistency_panel is not None:
    render_outlier_consistency_panel()
if render_negative_duration_panel is not None:
    render_negative_duration_panel()
if render_regional_quality_panel is not None:
    render_regional_quality_panel()
if render_interruptions_overlap_panel is not None:
    render_interruptions_overlap_panel()
if render_nrt_precision_panel is not None:
    render_nrt_precision_panel()
