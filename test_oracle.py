#!/usr/bin/env python3
"""
Script de teste para verificar conexão Oracle e query HCAI
"""
import sys
from pathlib import Path

# Adicionar lib ao path
sys.path.insert(0, str(Path(__file__).parent / 'lib'))

from oracle_extractor import OracleExtractor

def test_oracle_connection():
    """Testa conexão Oracle e executa query de teste"""
    print("=" * 60)
    print("TESTE DE CONEXÃO ORACLE HCAI")
    print("=" * 60)

    try:
        print("🔌 Tentando conectar ao Oracle...")
        extractor = OracleExtractor.from_env()
        print("✅ Conexão estabelecida com sucesso!")

        print("\n📊 Testando query HCAI com limite de 5 registros...")
        count = 0
        for result in extractor.extract_hcai_chunks('202603', limit_rows=5):
            if result[0] is None:  # Mensagem de parada
                print(f"📝 {result[1]}")
                break

            chunk_df, log_msg = result
            print(f"📝 {log_msg}")
            count += len(chunk_df)

            # Mostrar primeiras linhas do primeiro chunk
            if count <= 5:
                print(f"\n📋 Primeiras {min(5, len(chunk_df))} linhas do chunk:")
                print(chunk_df.head(5))
                print(f"\n📋 Colunas: {list(chunk_df.columns)}")

        print(f"\n✅ Teste concluído! Total de registros recebidos: {count}")

        if count > 0:
            print("🎉 Query HCAI está funcionando corretamente!")
        else:
            print("⚠️  Query não retornou registros. Verifique:")
            print("   - Se o mês '202603' tem dados")
            print("   - Se a query SQL está correta")
            print("   - Se as permissões de acesso estão OK")

        extractor.close()

    except ValueError as e:
        print(f"❌ ERRO DE CONFIGURAÇÃO: {e}")
        print("\nVerifique se as variáveis estão definidas no arquivo .env:")
        print("  - ORACLE_UID")
        print("  - ORACLE_PWD")
        print("  - ORACLE_DB (ou ORACLE_HOST + ORACLE_PORT)")

    except Exception as e:
        print(f"❌ ERRO NA CONEXÃO/QUERY: {e}")
        import traceback
        print("\nDetalhes do erro:")
        traceback.print_exc()

if __name__ == "__main__":
    test_oracle_connection()