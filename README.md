# Data Quality Application

Aplicação web Streamlit para avaliação e gerenciamento de qualidade de dados HCAI do Oracle.

## 📁 Estrutura do Projeto

```
data_quality/
├── app/                      # Frontend Streamlit
│   ├── Home.py              # Dashboard principal
│   └── pages/               # Páginas específicas
│       ├── 1_Importacao.py  # Upload, extração Oracle e processamento
│       ├── 2_Regras_de_Qualidade.py
│       ├── 3_Resultados.py
│       └── 4_Historico.py
├── data/                    # Camadas de armazenamento de dados
│   ├── raw/                 # Dados brutos não alterados
│   ├── processed/           # Dados processados e padronizados
│   └── output/              # Relatórios e saídas finais
├── lib/                     # Biblioteca interna com scripts reutilizáveis
│   ├── oracle_extractor.py  # Classe para extração de dados Oracle
│   ├── quality_rules.py
│   ├── transformers.py
│   └── validators.py
├── docs/                    # Documentação técnica
│   ├── 00_extracao.md       # Guia de extração Oracle
│   └── 01_importacao.md     # Guia de importação e processamento
├── config/                  # Arquivos de configuração
├── SQL/                     # Scripts SQL HCAI
│   └── SQL_HCAI.sql
└── requirements.txt         # Dependências Python
```

## ⚙️ Configuração Inicial

### Pré-requisitos

- Python 3.9+
- Oracle Client (para conexão com Oracle)
- Variáveis de ambiente do Oracle configuradas

### Instalação

1. Clone ou abra o projeto em VS Code
2. Crie e ative um ambiente virtual:
   ```cmd
   python -m venv venv
   venv\Scripts\activate
   ```
3. Instale as dependências:
   ```cmd
   pip install -r requirements.txt
   ```
4. Configure o arquivo `.env` com credenciais Oracle:
   ```env
   ORACLE_UID=seu_usuario
   ORACLE_PWD=sua_senha
   ORACLE_DB=sua_conexao_db
   ```

## 🚀 Como Usar

### Executar a Aplicação

```cmd
streamlit run app/Home.py
```

A aplicação abrirá em `http://localhost:8501`

### Principais Funcionalidades

#### 1. **Upload de Arquivo** (Tab 1)
- Envie arquivos CSV, Excel ou Parquet
- Visualize os dados em tempo real
- Os dados são salvos em `data/raw/`

#### 2. **Extração SQL (Oracle)** (Tab 2)
- Conecte-se ao Oracle HCAI automaticamente
- Execute extrações com limite configurável:
  - **Teste**: 100 registros (validação rápida)
  - **Personalizado**: Especifique o número de registros
  - **Completa**: Extração sem limite
- Acompanhe o progresso em tempo real com logs detalhados
- Os dados são salvos em chunks de 200k registros em formato Parquet

#### 3. **Processamento de Dados** (Tab 3)
- **Conversão de Tipos**: Datetime e Decimal
- **Validação de Sobreposição Temporal**: Remove conflitos por NUM_UC_UCI
- **Consolidação de Duplicados**: Unifica registros repetidos
- Salva dados processados em `data/processed/`
- Gera logs detalhados do processamento

## 📚 Documentação Técnica

Veja os guias detalhados em:
- [Extração de Dados Oracle](docs/00_extracao.md)
- [Importação e Processamento](docs/01_importacao.md)

## 🔧 Dependências Principais

- `streamlit`: Framework web
- `polars`: Processamento de dados eficiente
- `oracledb`: Conexão com Oracle
- `python-dotenv`: Gerenciamento de variáveis de ambiente
- `pandas`: Manipulação de dados

## 📝 Logs e Histórico

- Todos os processamentos geram logs detalhados
- Histórico de execuções pode ser consultado na aba "Histórico"
- Arquivos são salvos com timestamps para rastreabilidade