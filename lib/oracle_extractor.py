import polars as pl
import oracledb
from pathlib import Path
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente
load_dotenv(Path(__file__).parent.parent / '.env')

# Inicializar Oracle Client se houver diretório definido
oracle_lib_dir = os.getenv('ORACLE_LIB_DIR')
oracle_config_dir = os.getenv('ORACLE_CONFIG_DIR')
if oracle_lib_dir:
    try:
        if oracle_config_dir:
            oracledb.init_oracle_client(lib_dir=oracle_lib_dir, config_dir=oracle_config_dir)
        else:
            oracledb.init_oracle_client(lib_dir=oracle_lib_dir)
    except Exception as e:
        print(f"Aviso: não foi possível inicializar Oracle Client: {e}")

class OracleExtractor:
    """Extrator de dados do banco Oracle para análise de qualidade."""
    
    CHUNK_SIZE = 200000  # 200k registros por chunk
    
    @staticmethod
    def from_env():
        """
        Cria uma instância do OracleExtractor usando credenciais do .env
        
        Returns:
            OracleExtractor configurado com as credenciais do .env
        """
        user = os.getenv('ORACLE_UID')
        password = os.getenv('ORACLE_PWD')
        host = os.getenv('ORACLE_HOST')
        port = os.getenv('ORACLE_PORT')
        service_name = os.getenv('ORACLE_DB')
        
        missing = []
        if not user:
            missing.append('ORACLE_UID')
        if not password:
            missing.append('ORACLE_PWD')
        if not service_name:
            missing.append('ORACLE_DB')
        
        if missing:
            raise ValueError(f"Variáveis faltando no .env: {', '.join(missing)}")
        
        if host and port:
            return OracleExtractor(user, password, host, int(port), service_name)
        return OracleExtractor(user, password, None, None, service_name)
    
    def __init__(self, user, password, host, port, service_name):
        """
        Inicializa a conexão com Oracle.
        
        Args:
            user: Usuário Oracle
            password: Senha do usuário
            host: Host do servidor ou None para TNS alias
            port: Porta (geralmente 1521) ou None para TNS alias
            service_name: Nome do serviço Oracle ou TNS alias
        """
        self.connection = None
        self.dsn = None

        if host and port:
            self.dsn = f"{host}:{port}/{service_name}"
            try:
                self.connection = oracledb.connect(user=user, password=password, dsn=self.dsn)
                print(f"Conexão estabelecida com sucesso usando DSN={self.dsn}")
                return
            except Exception as e:
                print(f"Falha ao conectar usando host/porta ({self.dsn}): {e}")

        # Tentar como alias TNS / service name via config_dir
        if service_name:
            self.dsn = service_name
            try:
                self.connection = oracledb.connect(user=user, password=password, dsn=self.dsn)
                print(f"Conexão estabelecida com sucesso usando alias TNS/DSN={self.dsn}")
                return
            except Exception as e:
                print(f"Falha ao conectar usando alias TNS ({self.dsn}): {e}")

        self.connection = None
    
    def extract_hcai_chunks(self, data_mes: str, limit_rows: Optional[int] = None):
        """
        Extrai dados HCAI do Oracle em chunks usando Polars.
        
        Args:
            data_mes: String no formato 'YYYYMM' (ex: '202603')
            limit_rows: Limite de linhas para teste (None = sem limite)
        
        Yields:
            Tupla (chunk_df, log_message)
        """
        if not self.connection:
            print("Erro: Conexão não estabelecida")
            return
        
        # Ler o SQL do arquivo
        sql_path = Path(__file__).parent.parent / 'SQLs' / 'SQL_HCAI.sql'
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        try:
            cursor = self.connection.cursor()
            cursor.arraysize = self.CHUNK_SIZE
            clean_sql = sql.strip().rstrip(';')
            cursor.execute(clean_sql, data_mes=data_mes)
            
            columns = [desc[0] for desc in cursor.description]
            chunk_number = 0
            total_rows = 0
            
            while True:
                rows = cursor.fetchmany(self.CHUNK_SIZE)
                if not rows:
                    break
                
                chunk_number += 1
                total_rows += len(rows)
                
                # Converter para Polars DataFrame com tipagem consistente
                try:
                    # Transpor os dados de linhas para colunas (dict de listas)
                    data_dict = {columns[i]: [row[i] for row in rows] for i in range(len(columns))}
                    # Criar com strict=False para permitir tipos mistos durante o processo
                    df_chunk = pl.DataFrame(data_dict).with_columns([
                        pl.col(col).cast(pl.String) for col in pl.DataFrame(data_dict).columns
                    ])
                except Exception as e:
                    print(f"Aviso: erro na conversão do chunk {chunk_number}: {e}")
                    print(f"Aplicando conversão robusta com string casting...")
                    try:
                        # Conversão total para string, depois Polars detecta tipos
                        data_dict = {}
                        for i, col in enumerate(columns):
                            data_dict[col] = [str(row[i]) if row[i] is not None else None for row in rows]
                        df_chunk = pl.DataFrame(data_dict)
                    except Exception as e2:
                        print(f"Erro crítico no chunk {chunk_number}: {e2}")
                        raise
                
                log_msg = f"Chunk {chunk_number}: {len(rows)} registros (Total: {total_rows})"
                print(log_msg)
                
                yield df_chunk, log_msg
                
                # Se houver limite, parar após atingi-lo
                if limit_rows and total_rows >= limit_rows:
                    log_msg_final = f"Limite de {limit_rows} registros atingido. Parando extração."
                    print(log_msg_final)
                    yield None, log_msg_final
                    break
            
            cursor.close()
            
        except Exception as e:
            print(f"Erro ao executar query: {e}")
    
    def extract_hcai_to_parquet(self, data_mes: str, filename: str = 'dic_fic_uc.parquet', limit_rows: Optional[int] = None) -> Optional[str]:
        """
        Extrai dados HCAI e salva diretamente em Parquet usando chunks.
        
        Args:
            data_mes: String no formato 'YYYYMM' (ex: '202603')
            filename: Nome do arquivo Parquet
            limit_rows: Limite de linhas para teste (None = sem limite)
        
        Yields:
            Mensagens de log dos chunks
        
        Returns:
            Caminho do arquivo salvo ou None se erro
        """
        if not self.connection:
            yield "Erro: Conexão não estabelecida"
            return None
        
        try:
            # Usar caminho absoluto
            raw_dir = Path(__file__).parent.parent / 'data' / 'raw'
            raw_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = raw_dir / filename
            
            # Processar e salvar em chunks
            first_chunk = True
            for result in self.extract_hcai_chunks(data_mes, limit_rows):
                if result is None:
                    continue
                if result[0] is None:  # Sinal de parada
                    yield result[1]
                    break
                
                chunk_df, log_msg = result
                yield log_msg
                
                if first_chunk:
                    chunk_df.write_parquet(file_path)
                    first_chunk = False
                    yield f"Arquivo criado: {file_path}"
                else:
                    # Append chunks ao arquivo Parquet com unificação de tipos
                    try:
                        existing_df = pl.read_parquet(file_path)
                        # Converter ambos para String para garantir compatibilidade
                        existing_df = existing_df.with_columns([
                            pl.col(c).cast(pl.String) for c in existing_df.columns
                        ])
                        chunk_df = chunk_df.with_columns([
                            pl.col(c).cast(pl.String) for c in chunk_df.columns
                        ])
                        combined_df = pl.concat([existing_df, chunk_df], how="vertical")
                        combined_df.write_parquet(file_path)
                    except Exception as e:
                        print(f"Aviso: erro ao fazer append do chunk: {e}")
                        # Tentar salvamento mais agressivo
                        existing_df = pl.read_parquet(file_path)
                        combined_df = pl.concat([existing_df, chunk_df], how="vertical", force_rechunk=True)
                        combined_df.write_parquet(file_path)
            
            # Se não houve nenhum chunk processado, não foi criado arquivo
            if first_chunk:
                yield "Nenhum registro retornado. Arquivo não foi criado."
                return None

            # Verificar e relatar tamanho final
            if file_path.exists():
                file_size_mb = file_path.stat().st_size / (1024 * 1024)
                final_msg = f"Dados salvos em: {file_path} ({file_size_mb:.2f} MB)"
                yield final_msg
                return str(file_path)
            else:
                yield f"Erro: Arquivo não foi criado em {file_path}"
                return None
            
        except Exception as e:
            yield f"Erro ao salvar parquet: {e}"
            return None
    
    def close(self):
        """Fecha a conexão com Oracle."""
        if self.connection:
            self.connection.close()
            print("Conexão fechada")


# Exemplo de uso
if __name__ == "__main__":
    # Configurar credenciais
    extractor = OracleExtractor(
        user='seu_usuario',
        password='sua_senha',
        host='seu_host',
        port=1521,
        service_name='seu_service'
    )
    
    # Extrair dados
    df_hcai = extractor.extract_hcai('202603')
    
    if df_hcai is not None:
        # Salvar dados brutos
        extractor.save_raw_data(df_hcai, 'HCAI_202603')
    
    # Fechar conexão
    extractor.close()
