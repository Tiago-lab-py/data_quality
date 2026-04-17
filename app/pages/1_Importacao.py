import streamlit as st
import pandas as pd
import polars as pl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import threading
import sys
import time
import warnings

# Suprimir warning de ScriptRunContext (é seguro ignorar)
warnings.filterwarnings("ignore", message=".*ScriptRunContext.*")

# Adicionar lib ao path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'lib'))

from oracle_extractor import OracleExtractor

executor = ThreadPoolExecutor(max_workers=1)

# Estado compartilhado para extração em background
background_extraction_state = {
    'running': False,
    'logs': [],
    'result': None,
    'error': None,
}
background_state_lock = threading.Lock()

# Inicializar session state
if 'extraction_running' not in st.session_state:
    st.session_state.extraction_running = False
if 'extraction_logs' not in st.session_state:
    st.session_state.extraction_logs = []
if 'extraction_result' not in st.session_state:
    st.session_state.extraction_result = None
if 'extraction_error' not in st.session_state:
    st.session_state.extraction_error = None
if 'extraction_future' not in st.session_state:
    st.session_state.extraction_future = None
if 'auto_refresh' not in st.session_state:
    st.session_state.auto_refresh = False


def reset_background_extraction():
    st.session_state.extraction_running = False
    st.session_state.extraction_logs = []
    st.session_state.extraction_result = None
    st.session_state.extraction_error = None
    st.session_state.extraction_future = None
    st.session_state.auto_refresh = False

    with background_state_lock:
        background_extraction_state['running'] = False
        background_extraction_state['logs'] = []
        background_extraction_state['result'] = None
        background_extraction_state['error'] = None


def append_background_log(message: str):
    with background_state_lock:
        background_extraction_state['logs'].append(message)


def sync_background_state_to_session():
    with background_state_lock:
        st.session_state.extraction_logs = list(background_extraction_state['logs'])
        st.session_state.extraction_result = background_extraction_state['result']
        st.session_state.extraction_error = background_extraction_state['error']
        st.session_state.extraction_running = background_extraction_state['running']


def get_raw_parquet_path(project_root: Path):
    raw_dir = project_root / "data" / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Prioridade: arquivo padrão, depois teste.
    preferred_files = [
        raw_dir / "dic_fic_uc.parquet",
        raw_dir / "dic_fic_uc_teste.parquet",
    ]
    for parquet_path in preferred_files:
        if parquet_path.exists():
            return parquet_path

    # Fallback: qualquer parquet mais recente.
    parquet_files = sorted(
        raw_dir.glob("*.parquet"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return parquet_files[0] if parquet_files else None


def run_extraction_background(data_mes: str, filename: str, limit_rows):
    with background_state_lock:
        background_extraction_state['running'] = True
        background_extraction_state['result'] = None
        background_extraction_state['error'] = None
        background_extraction_state['logs'] = []

    append_background_log("=" * 70)
    append_background_log("📊 EXTRAÇÃO DE DADOS HCAI DO ORACLE")
    append_background_log("=" * 70)
    append_background_log(f"📅 Mês: {data_mes}")
    append_background_log(f"📁 Arquivo de saída: data/raw/{filename}")
    append_background_log(f"📊 Limite de registros: {limit_rows if limit_rows else 'Sem limite'}")
    append_background_log("=" * 70)

    extractor = None
    try:
        append_background_log("\n🔌 Conectando ao Oracle...")
        extractor = OracleExtractor.from_env()
        append_background_log("✅ Conexão estabelecida com sucesso!")

        append_background_log(f"\n📥 Iniciando extração para {data_mes}...")
        total_chunks = 0
        total_registros = 0

        for log_msg in extractor.extract_hcai_to_parquet(
            data_mes=data_mes,
            filename=filename,
            limit_rows=limit_rows,
        ):
            if log_msg is None:
                continue
            append_background_log(f"   {log_msg}")
            if "chunk" in log_msg.lower():
                total_chunks += 1
            if "total:" in log_msg.lower():
                try:
                    import re
                    match = re.search(r'Total: (\d+)', log_msg, re.IGNORECASE)
                    if match:
                        total_registros = int(match.group(1))
                except Exception:
                    pass
            if 'dados salvos em:' in log_msg.lower() or 'arquivo criado:' in log_msg.lower() or 'arquivo salvo:' in log_msg.lower():
                with background_state_lock:
                    background_extraction_state['result'] = log_msg

        with background_state_lock:
            logs_copy = list(background_extraction_state['logs'])
        if not background_extraction_state['result']:
            if any("erro" in log.lower() or "falha" in log.lower() for log in logs_copy):
                with background_state_lock:
                    background_extraction_state['error'] = 'A extração encontrou erros durante o processamento.'
            elif any("nenhum registro retornado" in log.lower() for log in logs_copy):
                with background_state_lock:
                    background_extraction_state['error'] = 'Nenhum registro foi retornado pela query Oracle.'
            else:
                with background_state_lock:
                    background_extraction_state['error'] = 'A extração terminou sem gerar arquivo Parquet.'

        append_background_log("\n" + "=" * 70)
        append_background_log("✅ EXTRAÇÃO CONCLUÍDA COM SUCESSO!")
        append_background_log(f"📊 Total de chunks: {total_chunks}")
        append_background_log(f"📊 Total de registros: {total_registros}")
        append_background_log(f"📁 Arquivo: data/raw/{filename}")
        append_background_log("=" * 70)

        expected_path = Path(__file__).resolve().parent.parent.parent / 'data' / 'raw' / filename
        if expected_path.exists():
            file_size = expected_path.stat().st_size / (1024 * 1024)
            append_background_log(f"✅ Verificação: Arquivo existe ({file_size:.2f} MB)")
        else:
            append_background_log("⚠️  Verificação: Arquivo NÃO foi encontrado no disco")
            with background_state_lock:
                if not background_extraction_state['error']:
                    background_extraction_state['error'] = f'Arquivo não foi criado: {expected_path}'

    except ValueError as e:
        with background_state_lock:
            background_extraction_state['error'] = f"❌ ERRO DE CONFIGURAÇÃO: {e}"
        append_background_log(f"\n❌ ERRO DE CONFIGURAÇÃO: {e}")
        append_background_log("\nVerifique se as variáveis de ambiente estão definidas no arquivo `.env`:")
        append_background_log("  - ORACLE_UID")
        append_background_log("  - ORACLE_PWD")
        append_background_log("  - ORACLE_DB (ou ORACLE_HOST + ORACLE_PORT)")

    except Exception as e:
        with background_state_lock:
            background_extraction_state['error'] = f"❌ ERRO NA EXTRAÇÃO: {e}"
        append_background_log(f"\n❌ ERRO NA EXTRAÇÃO: {e}")
        import traceback
        append_background_log(traceback.format_exc())

    finally:
        if extractor is not None:
            try:
                extractor.close()
            except Exception:
                pass
        with background_state_lock:
            background_extraction_state['running'] = False


st.title("Importação de Dados")

# Abas para importação
tab1, tab2, tab3 = st.tabs(["Upload de Arquivo", "Extração SQL (Oracle)", "Processamento de Dados"])

with tab1:
    st.markdown("Faça upload dos seus arquivos de dados (CSV, Excel, Parquet) para avaliação de qualidade.")
    
    uploaded_file = st.file_uploader("Escolha um arquivo", type=['csv', 'xlsx', 'xls', 'parquet'])
    
    if uploaded_file is not None:
        # Save to raw data folder
        raw_path = Path("data/raw")
        raw_path.mkdir(parents=True, exist_ok=True)
        
        file_path = raw_path / uploaded_file.name
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        st.success(f"Arquivo {uploaded_file.name} salvo na pasta de dados brutos.")
        
        # Basic preview
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        elif uploaded_file.name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(uploaded_file)
        elif uploaded_file.name.endswith('.parquet'):
            df = pd.read_parquet(uploaded_file)
        
        st.subheader("Pré-visualização dos Dados")
        st.dataframe(df.head())
        
        st.session_state['data_file'] = str(file_path)
        st.session_state['data_preview'] = df

with tab2:
    st.markdown("""### 📊 Extração de Dados HCAI do Oracle

    Extraia dados diretamente do Oracle usando SQL HCAI.

    ⚠️ **Esta extração processa grandes volumes (até 20 milhões de registros).**
    Os dados serão salvos em chunks de 200.000 registros em formato Parquet.

    As credenciais são carregadas automaticamente do arquivo `.env`.
    """)

    # Configurações de extração
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("📅 Período de Extração")
        oracle_mes = st.text_input("Mês (YYYYMM)", value="202603", key="oracle_mes_input")

    with col2:
        st.subheader("📊 Limite de Registros")
        limite_opcao = st.radio(
            "Tipo de extração:",
            ["Completa (sem limite)", "Teste (100 registros)", "Personalizado"],
            key="limite_opcao"
        )

        if limite_opcao == "Teste (100 registros)":
            limit_rows = 100
            st.info("🧪 Modo teste: 100 registros")
        elif limite_opcao == "Personalizado":
            limit_rows = st.number_input(
                "Número de registros:",
                min_value=1000,
                max_value=20000000,
                value=100000,
                step=1000,
                key="limit_rows_input"
            )
        else:
            limit_rows = None
            st.info("🔄 Extração completa")

    # Debug: mostrar configuração Oracle (opcional)
    if st.checkbox("🔧 Mostrar Configuração Oracle", value=False, key="debug_oracle"):
        st.divider()
        st.subheader("🔍 Configuração Oracle (Debug)")

        import os
        from dotenv import load_dotenv
        from pathlib import Path

        # Carregar .env
        env_path = Path(__file__).parent.parent.parent / '.env'
        if env_path.exists():
            load_dotenv(env_path)
            st.success(f"✅ Arquivo .env encontrado: {env_path}")
        else:
            st.error(f"❌ Arquivo .env não encontrado: {env_path}")

        # Mostrar variáveis
        oracle_vars = ['ORACLE_UID', 'ORACLE_PWD', 'ORACLE_DB', 'ORACLE_HOST', 'ORACLE_PORT', 'ORACLE_LIB_DIR']
        for var in oracle_vars:
            value = os.getenv(var, 'NÃO DEFINIDA')
            if var == 'ORACLE_PWD':
                value = '***' if value != 'NÃO DEFINIDA' else value
            st.text(f"{var}: {value}")

        st.divider()
        st.divider()
        st.subheader("🔍 Teste de Conexão")

        try:
            with st.spinner("Testando conexão com Oracle..."):
                extractor = OracleExtractor.from_env()
                st.success("✅ Conexão com Oracle estabelecida com sucesso!")

                # Extrair 10 registros como teste rápido
                with st.spinner("Extraindo 10 registros de teste..."):
                    log_area = st.empty()

                    all_logs = []
                    test_path = None
                    for log_msg in extractor.extract_hcai_to_parquet(
                        data_mes=oracle_mes,
                        filename='dic_fic_uc_teste_conexao.parquet',
                        limit_rows=10
                    ):
                        all_logs.append(log_msg)
                        if "dados salvos em:" in log_msg.lower() or "arquivo criado:" in log_msg.lower():
                            test_path = log_msg
                        with log_area.container():
                            for log in all_logs[-5:]:  # Mostrar últimos 5 logs
                                if "erro" in log.lower():
                                    st.error(log)
                                elif "conclu" in log.lower() or "sucesso" in log.lower():
                                    st.success(log)
                                else:
                                    st.text(log)

                    extractor.close()

                    if test_path:
                        st.success("✅ Teste de conexão concluído com sucesso!")
                        st.info(f"Arquivo de teste: {test_path}")
                    else:
                        st.error("❌ O teste terminou sem salvar o arquivo.")

        except ValueError as e:
            st.error(f"❌ Erro de configuração: {str(e)}")
            st.info("Verifique se as variáveis ORACLE_UID, ORACLE_PWD e ORACLE_DB estão definidas no arquivo `.env`")
        except Exception as e:
            st.error(f"❌ Erro ao testar conexão: {str(e)}")

        st.divider()
    
    # Extração Principal
    if st.button("🚀 Iniciar Extração Oracle", type="primary", use_container_width=True):
        if st.session_state.extraction_running:
            st.warning("Já existe uma extração em andamento. Aguarde a conclusão ou atualize o status.")
        else:
            reset_background_extraction()
            # limit_rows já foi definido acima baseado na seleção do usuário
            filename = 'dic_fic_uc_teste.parquet' if limit_rows == 100 else 'dic_fic_uc.parquet'
            st.session_state.extraction_future = executor.submit(
                run_extraction_background,
                oracle_mes,
                filename,
                limit_rows,
            )
            st.session_state.extraction_running = True
            st.session_state.auto_refresh = True
            st.rerun()

    # Sincronizar estado do background thread antes de exibir
    sync_background_state_to_session()
    st.divider()
    st.subheader("📊 Acompanhamento da Extração")

    if st.session_state.extraction_running:
        st.info("⏳ Extração em andamento... Clique em Atualizar para ver o progresso.")
    else:
        if st.session_state.extraction_error:
            st.error("❌ Extração Finalizada com Erro")
        else:
            st.success("✅ Extração Finalizada com Sucesso")

    st.markdown("**Logs da Extração:**")
    if st.session_state.extraction_logs:
        logs_to_show = st.session_state.extraction_logs[-50:]
        for log in logs_to_show:
            if "erro" in log.lower() or "falha" in log.lower():
                st.error(log)
            elif "conclu" in log.lower() or "sucesso" in log.lower() or "dados salvos" in log.lower() or "arquivo criado" in log.lower():
                st.success(log)
            elif "aviso" in log.lower() or "⚠️" in log:
                st.warning(log)
            else:
                st.text(log)
    else:
        st.write("Nenhum log disponível ainda. Inicie a extração e pressione Atualizar.")

    if st.button('🔄 Atualizar status', use_container_width=True):
        st.experimental_rerun()

    st.divider()
    if st.session_state.extraction_result:
        st.success("✅ Extração concluída com sucesso!")
        st.session_state['data_file'] = st.session_state.extraction_result
        st.info(f"📁 {st.session_state.extraction_result}")
    elif st.session_state.extraction_error:
        st.error(f"❌ {st.session_state.extraction_error}")

    if st.checkbox("🐞 Mostrar estado interno da extração", value=False, key="show_extraction_debug"):
        with background_state_lock:
            st.write("running:", background_extraction_state['running'])
            st.write("logs_count:", len(background_extraction_state['logs']))
            st.write("result:", background_extraction_state['result'])
            st.write("error:", background_extraction_state['error'])

    if not st.session_state.extraction_running and st.session_state.extraction_logs:
        with st.expander("📋 Ver Logs Completos", expanded=False):
            for log in st.session_state.extraction_logs:
                if "erro" in log.lower() or "falha" in log.lower():
                    st.error(log)
                elif "conclu" in log.lower() or "sucesso" in log.lower() or "dados salvos" in log.lower() or "arquivo criado" in log.lower():
                    st.success(log)
                elif "aviso" in log.lower() or "⚠️" in log:
                    st.warning(log)
                else:
                    st.text(log)


def processar_dados_hcai(raw_path, processed_dir, converter_datas, converter_decimal,
                        validar_sobreposicao, consolidar_duplicados):
    """Processa os dados HCAI aplicando transformações e validações"""

    logs = []
    logs.append("=" * 70)
    logs.append("🔄 PROCESSAMENTO DE DADOS HCAI")
    logs.append("=" * 70)
    logs.append(f"📁 Arquivo de entrada: {raw_path}")
    logs.append(f"📁 Diretório de saída: {processed_dir}")
    logs.append("")

    log_container = st.empty()

    def update_logs():
        with log_container.container():
            for log in logs[-15:]:
                if "erro" in log.lower() or "falha" in log.lower():
                    st.error(log)
                elif "sucesso" in log.lower() or "conclu" in log.lower():
                    st.success(log)
                elif "aviso" in log.lower() or "atenção" in log.lower():
                    st.warning(log)
                else:
                    st.text(log)

    try:
        logs.append("📖 Carregando dados do arquivo Parquet...")
        update_logs()

        df = pl.read_parquet(raw_path)
        registros_iniciais = len(df)
        logs.append(f"✅ Dados carregados: {registros_iniciais:,} registros")
        update_logs()

        if converter_datas:
            logs.append("")
            logs.append("📅 Convertendo colunas para datetime...")
            update_logs()

            colunas_datetime = ['DTHR_INICIO_INTRP_UC', 'DATA_HORA_FIM_INT']
            for col in colunas_datetime:
                if col in df.columns:
                    try:
                        df = df.with_columns(pl.col(col).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").alias(col))
                        logs.append(f"✅ {col}: conversão direta realizada")
                    except Exception:
                        try:
                            df = df.with_columns(pl.col(col).str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S").alias(col))
                            logs.append(f"✅ {col}: conversão DD/MM/YYYY realizada")
                        except Exception:
                            try:
                                df = df.with_columns(pl.col(col).str.strptime(pl.Datetime, "%Y%m%d %H:%M:%S").alias(col))
                                logs.append(f"✅ {col}: conversão YYYYMMDD realizada")
                            except Exception as e:
                                logs.append(f"⚠️  {col}: falha na conversão - {str(e)}")
                else:
                    logs.append(f"⚠️  Coluna {col} não encontrada")
            update_logs()

        if converter_decimal:
            logs.append("")
            logs.append("🔢 Convertendo colunas para decimal...")
            update_logs()

            if 'DURACAO_PERCEBIDA_MINUTOS' in df.columns:
                try:
                    df = df.with_columns(pl.col('DURACAO_PERCEBIDA_MINUTOS').cast(pl.Float64).alias('DURACAO_PERCEBIDA_MINUTOS'))
                    logs.append("✅ DURACAO_PERCEBIDA_MINUTOS: convertido para decimal")
                except Exception as e:
                    logs.append(f"⚠️  DURACAO_PERCEBIDA_MINUTOS: falha na conversão - {str(e)}")
            else:
                logs.append("⚠️  Coluna DURACAO_PERCEBIDA_MINUTOS não encontrada")
            update_logs()

        if validar_sobreposicao:
            logs.append("")
            logs.append("⏰ Validando sobreposição temporal por NUM_UC_UCI...")
            update_logs()

            if 'NUM_UC_UCI' in df.columns and 'DTHR_INICIO_INTRP_UC' in df.columns and 'DATA_HORA_FIM_INT' in df.columns:
                try:
                    df_sorted = df.sort(['NUM_UC_UCI', 'DTHR_INICIO_INTRP_UC'])
                    grupos_validos = []

                    for _, group_df in df_sorted.group_by('NUM_UC_UCI'):
                        group_list = group_df.rows(named=True)

                        if len(group_list) == 1:
                            grupos_validos.extend(group_list)
                            continue

                        registros_filtrados = []
                        group_list.sort(key=lambda x: x['DTHR_INICIO_INTRP_UC'])

                        for reg_atual in group_list:
                            manter = True
                            for reg_anterior in list(registros_filtrados):
                                if (reg_atual['DTHR_INICIO_INTRP_UC'] < reg_anterior['DATA_HORA_FIM_INT'] and
                                    reg_atual['DATA_HORA_FIM_INT'] > reg_anterior['DTHR_INICIO_INTRP_UC']):
                                    dur_atual = (reg_atual['DATA_HORA_FIM_INT'] - reg_atual['DTHR_INICIO_INTRP_UC']).total_seconds() / 60
                                    dur_anterior = (reg_anterior['DATA_HORA_FIM_INT'] - reg_anterior['DTHR_INICIO_INTRP_UC']).total_seconds() / 60
                                    if dur_atual <= dur_anterior:
                                        manter = False
                                        break
                                    registros_filtrados.remove(reg_anterior)
                            if manter:
                                registros_filtrados.append(reg_atual)

                        grupos_validos.extend(registros_filtrados)

                    df = pl.DataFrame(grupos_validos)
                    registros_apos_sobreposicao = len(df)
                    removidos_sobreposicao = registros_iniciais - registros_apos_sobreposicao
                    logs.append(f"✅ Sobreposições validadas: {removidos_sobreposicao} registros removidos")
                except Exception as e:
                    logs.append(f"⚠️  Erro na validação de sobreposição: {str(e)}")
            else:
                logs.append("⚠️  Colunas necessárias não encontradas para validação de sobreposição")
            update_logs()

        if consolidar_duplicados:
            logs.append("")
            logs.append("🔗 Consolidando registros duplicados...")
            update_logs()

            if 'NUM_UC_UCI' in df.columns and 'NUM_INTRP_INIC_MAN' in df.columns:
                try:
                    registros_antes_consolidacao = len(df)
                    df = df.unique(subset=['NUM_UC_UCI', 'NUM_INTRP_INIC_MAN'], keep='first')
                    registros_apos_consolidacao = len(df)
                    removidos_duplicados = registros_antes_consolidacao - registros_apos_consolidacao
                    logs.append(f"✅ Duplicados consolidados: {removidos_duplicados} registros removidos")
                except Exception as e:
                    logs.append(f"⚠️  Erro na consolidação de duplicados: {str(e)}")
            else:
                logs.append("⚠️  Colunas necessárias não encontradas para consolidação")
            update_logs()

        logs.append("")
        logs.append("💾 Salvando arquivo processado...")
        update_logs()

        output_path = processed_dir / "dic_fic_uc_processed.parquet"
        df.write_parquet(output_path)

        registros_finais = len(df)
        logs.append(f"✅ Arquivo salvo: {output_path}")
        logs.append(f"📊 Registros finais: {registros_finais:,}")
        logs.append("")
        logs.append("=" * 70)
        logs.append("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
        logs.append("=" * 70)
        update_logs()

        st.success("✅ Processamento concluído com sucesso!")
        st.info(f"📁 Arquivo processado: `{output_path}`")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Registros Iniciais", f"{registros_iniciais:,}")
        with col2:
            st.metric("📊 Registros Finais", f"{registros_finais:,}")
        with col3:
            reducao = ((registros_iniciais - registros_finais) / registros_iniciais * 100) if registros_iniciais > 0 else 0
            st.metric("📉 Redução", f"{reducao:.1f}%")

    except Exception as e:
        logs.append(f"❌ ERRO NO PROCESSAMENTO: {str(e)}")
        logs.append("")
        import traceback
        logs.append(traceback.format_exc())
        update_logs()
        st.error(f"❌ Erro no processamento: {str(e)}")


with tab3:
    st.markdown("""### 🔄 Processamento de Dados HCAI

    Processe os dados extraídos do Oracle aplicando transformações e validações.

    **Funcionalidades:**
    - ✅ Verificação de existência do arquivo `raw/dic_fic_uc.parquet`
    - 🔄 Conversão de tipos de dados (datetime, decimal)
    - 📊 Validação de sobreposição temporal por `NUM_UC_UCI`
    - 🔗 Consolidação de registros duplicados
    - 📁 Salvamento em `processed/`
    """)

    # Usar caminhos absolutos baseados na raiz do projeto
    project_root = Path(__file__).resolve().parent.parent.parent
    raw_file_path = get_raw_parquet_path(project_root)
    processed_dir = project_root / "data" / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Debug: mostrar caminhos
    st.text(f"Arquivo selecionado: {raw_file_path if raw_file_path else 'Nenhum arquivo .parquet encontrado em data/raw'}")
    st.text(f"Existe: {raw_file_path.exists() if raw_file_path else False}")

    if raw_file_path and raw_file_path.exists():
        st.success(f"✅ Arquivo encontrado: `{raw_file_path}`")

        # Mostrar informações básicas do arquivo
        try:
            df_raw = pl.read_parquet(raw_file_path)
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📊 Registros", f"{len(df_raw):,}")
            with col2:
                st.metric("📋 Colunas", len(df_raw.columns))
            with col3:
                file_size = raw_file_path.stat().st_size / (1024 * 1024)  # MB
                st.metric("💾 Tamanho", f"{file_size:.1f} MB")

            # Opções de processamento
            st.divider()
            st.subheader("⚙️ Configurações de Processamento")

            col1, col2 = st.columns(2)

            with col1:
                converter_datas = st.checkbox(
                    "📅 Converter para Datetime",
                    value=True,
                    help="Converte DTHR_INICIO_INTRP_UC e DATA_HORA_FIM_INT para formato datetime"
                )

                converter_decimal = st.checkbox(
                    "🔢 Converter para Decimal",
                    value=True,
                    help="Converte DURACAO_PERCEBIDA_MINUTOS para formato decimal"
                )

            with col2:
                validar_sobreposicao = st.checkbox(
                    "⏰ Validar Sobreposição Temporal",
                    value=True,
                    help="Remove sobreposições temporais por NUM_UC_UCI, mantendo o registro de maior duração"
                )

                consolidar_duplicados = st.checkbox(
                    "🔗 Consolidar Duplicados",
                    value=True,
                    help="Consolida registros duplicados entre NUM_UC_UCI e NUM_INTRP_INIC_MAN"
                )

            # Botão de processamento
            if st.button("🚀 Iniciar Processamento", type="primary", use_container_width=True):
                processar_dados_hcai(
                    raw_file_path,
                    processed_dir,
                    converter_datas,
                    converter_decimal,
                    validar_sobreposicao,
                    consolidar_duplicados
                )

        except Exception as e:
            st.error(f"❌ Erro ao ler arquivo Parquet: {str(e)}")

    else:
        st.error("❌ Nenhum arquivo Parquet encontrado em `data/raw`")
        st.info("Execute primeiro a extração de dados na aba 'Extração SQL (Oracle)' ou faça upload de um arquivo Parquet.")


def processar_dados_hcai(raw_path, processed_dir, converter_datas, converter_decimal,
                        validar_sobreposicao, consolidar_duplicados):
    """Processa os dados HCAI aplicando transformações e validações"""

    logs = []
    logs.append("=" * 70)
    logs.append("🔄 PROCESSAMENTO DE DADOS HCAI")
    logs.append("=" * 70)
    logs.append(f"📁 Arquivo de entrada: {raw_path}")
    logs.append(f"📁 Diretório de saída: {processed_dir}")
    logs.append("")

    # Container para logs em tempo real
    log_container = st.empty()

    def update_logs():
        with log_container.container():
            for log in logs[-15:]:  # Mostrar últimos 15 logs
                if "erro" in log.lower() or "falha" in log.lower():
                    st.error(log)
                elif "sucesso" in log.lower() or "conclu" in log.lower():
                    st.success(log)
                elif "aviso" in log.lower() or "atenção" in log.lower():
                    st.warning(log)
                else:
                    st.text(log)

    try:
        # Carregar dados
        logs.append("📖 Carregando dados do arquivo Parquet...")
        update_logs()

        df = pl.read_parquet(raw_path)
        registros_iniciais = len(df)
        logs.append(f"✅ Dados carregados: {registros_iniciais:,} registros")
        update_logs()

        # Conversão de tipos
        if converter_datas:
            logs.append("")
            logs.append("📅 Convertendo colunas para datetime...")
            update_logs()

            colunas_datetime = ['DTHR_INICIO_INTRP_UC', 'DATA_HORA_FIM_INT']

            for col in colunas_datetime:
                if col in df.columns:
                    try:
                        # Tentar conversão direta primeiro
                        df = df.with_columns(pl.col(col).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").alias(col))
                        logs.append(f"✅ {col}: conversão direta realizada")
                    except:
                        try:
                            # Tentar outros formatos comuns
                            df = df.with_columns(pl.col(col).str.strptime(pl.Datetime, "%d/%m/%Y %H:%M:%S").alias(col))
                            logs.append(f"✅ {col}: conversão DD/MM/YYYY realizada")
                        except:
                            try:
                                df = df.with_columns(pl.col(col).str.strptime(pl.Datetime, "%Y%m%d %H:%M:%S").alias(col))
                                logs.append(f"✅ {col}: conversão YYYYMMDD realizada")
                            except Exception as e:
                                logs.append(f"⚠️  {col}: falha na conversão - {str(e)}")
                else:
                    logs.append(f"⚠️  Coluna {col} não encontrada")

            update_logs()

        if converter_decimal:
            logs.append("")
            logs.append("🔢 Convertendo colunas para decimal...")
            update_logs()

            if 'DURACAO_PERCEBIDA_MINUTOS' in df.columns:
                try:
                    df = df.with_columns(pl.col('DURACAO_PERCEBIDA_MINUTOS').cast(pl.Float64).alias('DURACAO_PERCEBIDA_MINUTOS'))
                    logs.append("✅ DURACAO_PERCEBIDA_MINUTOS: convertido para decimal")
                except Exception as e:
                    logs.append(f"⚠️  DURACAO_PERCEBIDA_MINUTOS: falha na conversão - {str(e)}")
            else:
                logs.append("⚠️  Coluna DURACAO_PERCEBIDA_MINUTOS não encontrada")

            update_logs()

        # Validação de sobreposição temporal
        if validar_sobreposicao:
            logs.append("")
            logs.append("⏰ Validando sobreposição temporal por NUM_UC_UCI...")
            update_logs()

            if 'NUM_UC_UCI' in df.columns and 'DTHR_INICIO_INTRP_UC' in df.columns and 'DATA_HORA_FIM_INT' in df.columns:
                try:
                    # Ordenar por NUM_UC_UCI e início
                    df_sorted = df.sort(['NUM_UC_UCI', 'DTHR_INICIO_INTRP_UC'])

                    # Agrupar por NUM_UC_UCI e verificar sobreposições
                    grupos_validos = []

                    for group_name, group_df in df_sorted.group_by('NUM_UC_UCI'):
                        group_list = group_df.rows(named=True)

                        if len(group_list) == 1:
                            grupos_validos.extend(group_list)
                            continue

                        # Verificar sobreposições dentro do grupo
                        registros_filtrados = []
                        group_list.sort(key=lambda x: x['DTHR_INICIO_INTRP_UC'])

                        for i, reg_atual in enumerate(group_list):
                            manter = True

                            # Comparar com registros anteriores no mesmo grupo
                            for reg_anterior in registros_filtrados:
                                if (reg_atual['DTHR_INICIO_INTRP_UC'] < reg_anterior['DATA_HORA_FIM_INT'] and
                                    reg_atual['DATA_HORA_FIM_INT'] > reg_anterior['DTHR_INICIO_INTRP_UC']):
                                    # Há sobreposição - manter o de maior duração
                                    dur_atual = (reg_atual['DATA_HORA_FIM_INT'] - reg_atual['DTHR_INICIO_INTRP_UC']).total_seconds() / 60
                                    dur_anterior = (reg_anterior['DATA_HORA_FIM_INT'] - reg_anterior['DTHR_INICIO_INTRP_UC']).total_seconds() / 60

                                    if dur_atual <= dur_anterior:
                                        manter = False
                                        break

                                    # Se o atual é maior, remover o anterior e manter este
                                    registros_filtrados.remove(reg_anterior)

                            if manter:
                                registros_filtrados.append(reg_atual)

                        grupos_validos.extend(registros_filtrados)

                    # Criar novo DataFrame
                    df = pl.DataFrame(grupos_validos)
                    registros_apos_sobreposicao = len(df)
                    removidos_sobreposicao = registros_iniciais - registros_apos_sobreposicao
                    logs.append(f"✅ Sobreposições validadas: {removidos_sobreposicao} registros removidos")

                except Exception as e:
                    logs.append(f"⚠️  Erro na validação de sobreposição: {str(e)}")
            else:
                logs.append("⚠️  Colunas necessárias não encontradas para validação de sobreposição")

            update_logs()

        # Consolidação de duplicados
        if consolidar_duplicados:
            logs.append("")
            logs.append("🔗 Consolidando registros duplicados...")
            update_logs()

            if 'NUM_UC_UCI' in df.columns and 'NUM_INTRP_INIC_MAN' in df.columns:
                try:
                    registros_antes_consolidacao = len(df)

                    # Remover duplicatas mantendo a primeira ocorrência
                    df = df.unique(subset=['NUM_UC_UCI', 'NUM_INTRP_INIC_MAN'], keep='first')

                    registros_apos_consolidacao = len(df)
                    removidos_duplicados = registros_antes_consolidacao - registros_apos_consolidacao
                    logs.append(f"✅ Duplicados consolidados: {removidos_duplicados} registros removidos")

                except Exception as e:
                    logs.append(f"⚠️  Erro na consolidação de duplicados: {str(e)}")
            else:
                logs.append("⚠️  Colunas necessárias não encontradas para consolidação")

            update_logs()

        # Salvar arquivo processado
        logs.append("")
        logs.append("💾 Salvando arquivo processado...")
        update_logs()

        output_path = processed_dir / "dic_fic_uc_processed.parquet"
        df.write_parquet(output_path)

        registros_finais = len(df)
        logs.append(f"✅ Arquivo salvo: {output_path}")
        logs.append(f"📊 Registros finais: {registros_finais:,}")
        logs.append("")
        logs.append("=" * 70)
        logs.append("✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
        logs.append("=" * 70)

        update_logs()

        # Mostrar resumo final
        st.success("✅ Processamento concluído com sucesso!")
        st.info(f"📁 Arquivo processado: `{output_path}`")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📊 Registros Iniciais", f"{registros_iniciais:,}")
        with col2:
            st.metric("📊 Registros Finais", f"{registros_finais:,}")
        with col3:
            reducao = ((registros_iniciais - registros_finais) / registros_iniciais * 100) if registros_iniciais > 0 else 0
            st.metric("📉 Redução", f"{reducao:.1f}%")

    except Exception as e:
        logs.append(f"❌ ERRO NO PROCESSAMENTO: {str(e)}")
        logs.append("")
        import traceback
        logs.append(traceback.format_exc())
        update_logs()
        st.error(f"❌ Erro no processamento: {str(e)}")
