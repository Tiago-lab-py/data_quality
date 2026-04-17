#!/usr/bin/env python
"""
Script CLI para extrair dados HCAI do Oracle.
Uso: python extract.py [--mes YYYYMM] [--limit N] [--output filename.parquet]

Exemplo:
    python extract.py --mes 202603
    python extract.py --mes 202603 --limit 1000 --output test_extract.parquet
"""

import argparse
import sys
from pathlib import Path

# Adicionar lib ao path
sys.path.insert(0, str(Path(__file__).parent / 'lib'))

from oracle_extractor import OracleExtractor


def main():
    parser = argparse.ArgumentParser(
        description="Extrai dados HCAI do Oracle para Parquet",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python extract.py --mes 202603
  python extract.py --mes 202603 --limit 100000 --output meu_arquivo.parquet
  python extract.py --mes 202603 --limit 100 --test
        """
    )
    
    parser.add_argument(
        '--mes',
        type=str,
        default='202603',
        help='Mês em formato YYYYMM (padrão: 202603)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limitar número de registros (opcional, padrão: sem limite)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default='dic_fic_uc.parquet',
        help='Nome do arquivo de saída (padrão: dic_fic_uc.parquet)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Modo teste (extrai 100 registros, salva em dic_fic_uc_teste.parquet)'
    )
    
    args = parser.parse_args()
    
    # Se modo teste, sobrescrever limit e output
    if args.test:
        args.limit = 100
        args.output = 'dic_fic_uc_teste.parquet'
    
    print("=" * 70)
    print("📊 EXTRAÇÃO DE DADOS HCAI DO ORACLE")
    print("=" * 70)
    print(f"📅 Mês: {args.mes}")
    print(f"📁 Arquivo de saída: data/raw/{args.output}")
    if args.limit:
        print(f"📊 Limite de registros: {args.limit}")
    else:
        print(f"📊 Limite de registros: Sem limite")
    print("=" * 70)
    
    try:
        print("\n🔌 Conectando ao Oracle...")
        extractor = OracleExtractor.from_env()
        print("✅ Conexão estabelecida com sucesso!")
        
        print(f"\n📥 Iniciando extração para {args.mes}...")
        total_chunks = 0
        total_registros = 0
        
        for log_msg in extractor.extract_hcai_to_parquet(
            data_mes=args.mes,
            filename=args.output,
            limit_rows=args.limit
        ):
            print(f"   {log_msg}")
            if "chunk" in log_msg.lower():
                total_chunks += 1
            if "total:" in log_msg.lower():
                # Extrair número de registros do log
                try:
                    import re
                    match = re.search(r'Total: (\d+)', log_msg, re.IGNORECASE)
                    if match:
                        total_registros = int(match.group(1))
                except:
                    pass
        
        extractor.close()
        
        print("\n" + "=" * 70)
        print("✅ EXTRAÇÃO CONCLUÍDA COM SUCESSO!")
        print(f"📊 Total de chunks: {total_chunks}")
        print(f"📊 Total de registros: {total_registros}")
        print(f"📁 Arquivo: data/raw/{args.output}")
        print("=" * 70)
        
        return 0
    
    except ValueError as e:
        print(f"\n❌ ERRO DE CONFIGURAÇÃO: {e}")
        print("\nVerifique se as variáveis de ambiente estão definidas no arquivo `.env`:")
        print("  - ORACLE_UID")
        print("  - ORACLE_PWD")
        print("  - ORACLE_DB (ou ORACLE_HOST + ORACLE_PORT)")
        return 1
    
    except Exception as e:
        print(f"\n❌ ERRO NA EXTRAÇÃO: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
