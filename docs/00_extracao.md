# 📊 Guia de Extração de Dados Oracle HCAI

## Visão Geral

O módulo de extração permite buscar dados diretamente do Oracle HCAI e salvá-los localmente em formato Parquet para posterior processamento.

## Configuração

### 1. Variáveis de Ambiente (.env)

Crie um arquivo `.env` na raiz do projeto com as credenciais Oracle:

```env
ORACLE_UID=seu_usuario_oracle
ORACLE_PWD=sua_senha_oracle
ORACLE_DB=sua_conexao_db  # Pode ser TNS, host:porta ou full connection string
```

**Exemplo de conexão:**
```env
ORACLE_DB=usuario/senha@servidor:1521/SERVICO
```

### 2. Classe OracleExtractor

Localização: `lib/oracle_extractor.py`

#### Métodos principais:

```python
from lib.oracle_extractor import OracleExtractor

# Inicializar a partir de variáveis de ambiente
extractor = OracleExtractor.from_env()

# Extrair dados HCAI
for log in extractor.extract_hcai_to_parquet(
    data_mes='202603',           # YYYYMM
    filename='dic_fic_uc.parquet',
    limit_rows=None              # None para sem limite
):
    print(log)

# Fechar conexão
extractor.close()
```

## Uso via Interface Streamlit

### Acesso

1. Execute: `streamlit run app/Home.py`
2. Navegue para a aba **"Extração SQL (Oracle)"**

### Fluxo de Uso

#### Passo 1: Configurar Período
- Defina o mês de extração em formato **YYYYMM** (ex: 202603)

#### Passo 2: Escolher Tipo de Extração
- **Completa (sem limite)**: Extrai todos os registros disponíveis
- **Teste (100 registros)**: Extrai apenas 100 registros para validação rápida
- **Personalizado**: Define um limite específico de registros

#### Passo 3: Testar Conexão (Opcional)
- Marque "🔌 Testar Conexão" para validar a conectividade com Oracle
- O teste extrairá 10 registros rapidamente
- Se houver sucesso, um arquivo `dic_fic_uc_teste_conexao.parquet` será criado

#### Passo 4: Iniciar Extração
- Clique em "🚀 Iniciar Extração Oracle"
- O processamento ocorre em **background** sem bloquear a interface
- Use o botão "🔄 Atualizar status" para verificar o progresso

### Saída

Os arquivos extraídos são salvos em:
```
data/raw/dic_fic_uc.parquet          # Extração completa
data/raw/dic_fic_uc_teste.parquet    # Extração teste (100 registros)
```

## Características Técnicas

### Processamento em Chunks

- Os dados são processados em **chunks de 200.000 registros**
- Cada chunk é salvo como um arquivo Parquet separado
- Ideal para lidar com volumes muito grandes (até 20 milhões de registros)

### Tipos de Dados Mantidos

A extração mantém os tipos de dados originais do Oracle:
- **DTHR_INICIO_INTRP_UC**: String (será convertido no processamento)
- **DATA_HORA_FIM_INT**: String (será convertido no processamento)
- **DURACAO_PERCEBIDA_MINUTOS**: Numérico
- **NUM_UC_UCI**: Numérico
- **NUM_INTRP_INIC_MAN**: Numérico

### Logs Detalhados

Durante a extração, os seguintes eventos são registrados:
- ✅ Conectando ao Oracle
- ✅ Iniciando extração para mês especificado
- 📊 Chunk N: registros processados
- 📁 Dados salvos em: [caminho do arquivo]
- ✅ Extração concluída com sucesso
- 📊 Total de chunks e registros

## Tratamento de Erros

### Erro: "Verificar se as variáveis ORACLE_UID, ORACLE_PWD e ORACLE_DB estão definidas"

**Solução:**
1. Verifique se o arquivo `.env` existe na raiz do projeto
2. Valide as credenciais Oracle
3. Teste a conexão manualmente com SQL*Plus ou SQL Developer

### Erro: "O teste terminou sem salvar o arquivo"

**Possíveis causas:**
- A query SQL HCAI retornou zero registros
- Erro na escrita do arquivo Parquet
- Permissões insuficientes para criar arquivos

**Solução:**
- Verifique a query SQL em `lib/oracle_extractor.py`
- Confirme que há dados para o mês especificado
- Valide permissões na pasta `data/raw/`

## Query SQL HCAI

A query usada para extração está definida em `lib/oracle_extractor.py`:

```sql
SELECT 
    NUM_UC_UCI,
    NUM_INTRP_INIC_MAN,
    DTHR_INICIO_INTRP_UC,
    DATA_HORA_FIM_INT,
    DURACAO_PERCEBIDA_MINUTOS
FROM [tabela HCAI]
WHERE [filtros por data_mes]
```

## Performance

### Dicas de Otimização

1. **Extrações menores**: Use modo Teste para validar antes de extrair completo
2. **Horários de pico**: Extraia fora dos horários de maior uso do Oracle
3. **Limite de registros**: Para testes iniciais, use limite de 10k-100k registros

### Tempo Estimado

- **Teste (100 registros)**: 5-10 segundos
- **Personalizado (100k)**: 30-60 segundos
- **Completa (1-20M)**: 2-30 minutos (depende da rede e servidor)

## Próximas Etapas

Após extrair os dados, prossiga para o [Guia de Importação e Processamento](01_importacao.md) para:
- Converter tipos de dados
- Validar sobreposições temporais
- Consolidar duplicados
- Gerar dados processados prontos para análise
