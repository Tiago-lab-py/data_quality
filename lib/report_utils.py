import pandas as pd

def generate_report(results):
    """Generate HTML report from results."""
    html = "<html><body><h1>Data Quality Report</h1>"
    
    for rule, result in results.items():
        html += f"<h2>{rule}</h2>"
        html += f"<p>Passed: {result['passed']}</p>"
        html += f"<p>Failed: {result['failed']}</p>"
    
    html += "</body></html>"
    
    # Save to output
    from lib.io_utils import save_output
    # Since it's HTML, we'll save as text file for now
    output_path = "data/output/quality_report.html"
    with open(output_path, "w") as f:
        f.write(html)
    
    return output_path

def export_results(results, format='csv'):
    """Export results to file."""
    df_results = pd.DataFrame(results).T
    from lib.io_utils import save_output
    return save_output(df_results, "quality_results", format)