from __future__ import annotations

import os
import time
import inspect
import importlib
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


RAW_OUTPUT = Path("D:/data_quality/data/raw/UC_faturada.parquet")
DEFAULT_SQL_PATH = Path("D:/data_quality/SQLs/IQS_Historico_UC_faturado.sql")
DEFAULT_ENV_PATH = Path("D:/data_quality/.env")
_CLIENT_INIT_DONE = False


def _read_sql() -> str:
    def _normalize(s: str) -> str:
        return s.strip().rstrip(";").rstrip("/").strip()

    if DEFAULT_SQL_PATH.exists():
        sql = DEFAULT_SQL_PATH.read_text(encoding="utf-8")
        return _normalize(sql)

    sql = """
SELECT
    CASE
        WHEN GROUPING(rd.SIGLA_RDIS) = 1 THEN 'COPEL'
        ELSE rd.SIGLA_RDIS
    END AS REGIONAL_TOTAL,
    CASE rd.SIGLA_RDIS
        WHEN 'CSL' THEN 'P'
        WHEN 'NRT' THEN 'L'
        WHEN 'NRO' THEN 'M'
        WHEN 'LES' THEN 'C'
        WHEN 'OES' THEN 'V'
        ELSE 'COPEL'
    END AS SIGLA_REGIONAL,
    TO_CHAR(ANO_MES_HQCL, 'yyyy-mm') AS ANOMES,
    SUM(QTDE_CONS_FAT_URB_HQCL + QTDE_CONS_FAT_RUR_HQCL) AS UC_FATURADA
FROM
    IQS.HIST_QTDE_CONS_LOCAL hqcl
INNER JOIN CIS.LOCALIDADE l ON
    l.COD_LOCAL = hqcl.COD_LOCAL_HQCL
LEFT JOIN CIS.SECCIONAL_DISTRIBUICAO sc ON
    l.COD_SECCION_DISTR_LOCAL = sc.COD_SDIS
LEFT JOIN CIS.REGIONAL_DISTRIBUICAO rd ON
    sc.COD_REG_DISTR_SDIS = rd.COD_RDIS
WHERE
    ANO_MES_HQCL >= TO_DATE(:p_data_ini, 'yyyy-mm-dd')
    AND rd.SIGLA_RDIS <> 'SCD'
GROUP BY
    ROLLUP (rd.SIGLA_RDIS),
    TO_CHAR(ANO_MES_HQCL, 'yyyy-mm')
ORDER BY
    ANOMES DESC,
    REGIONAL_TOTAL ASC
"""
    return _normalize(sql)


def _load_env_file(path: Path = DEFAULT_ENV_PATH) -> None:
    if not path.exists():
        return
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except Exception:
        pass


def _get_env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


def _get_oracle_config(*, use_streamlit: bool = True) -> Dict[str, Optional[str]]:
    _load_env_file()
    user = _get_env("ORACLE_UID")
    password = _get_env("ORACLE_PWD")
    dsn = _get_env("ORACLE_DB", "amon")
    lib_dir = _get_env("ORACLE_LIB_DIR")
    config_dir = _get_env("ORACLE_CONFIG_DIR")
    return {"user": user, "password": password, "dsn": dsn}


def _init_oracle_client() -> None:
    global _CLIENT_INIT_DONE
    if _CLIENT_INIT_DONE:
        return
    _load_env_file()
    lib_dir = _get_env("ORACLE_LIB_DIR")
    config_dir = _get_env("ORACLE_CONFIG_DIR")
    if not lib_dir and not config_dir:
        _CLIENT_INIT_DONE = True
        return
    try:
        import oracledb

        kwargs = {}
        if lib_dir:
            kwargs["lib_dir"] = lib_dir
        if config_dir:
            kwargs["config_dir"] = config_dir
        if kwargs:
            oracledb.init_oracle_client(**kwargs)
    except Exception:
        pass
    _CLIENT_INIT_DONE = True


def _pick_cfg(names: List[str], cfg: Dict[str, Optional[str]]) -> Optional[str]:
    for n in names:
        v = cfg.get(n)
        if v:
            return str(v)
    return None


def _try_project_connection_helper(cfg: Dict[str, Optional[str]]):
    module_names = [
        "app.lib.oracle_extractor",
        "lib.oracle_extractor",
        "app.lib.io_utils",
        "lib.io_utils",
    ]
    candidate_fns = (
        "connect_oracle",
        "get_oracle_connection",
        "get_connection",
        "criar_conexao_oracle",
        "oracle_connect",
        "connect",
    )

    user_val = _pick_cfg(["user", "oracle_user", "db_user", "username"], cfg)
    password_val = _pick_cfg(["password", "oracle_password", "db_password", "passwd"], cfg)
    dsn_val = _pick_cfg(["dsn", "oracle_dsn", "db_dsn", "tns", "tns_alias", "alias"], cfg) or "amon"

    for mod_name in module_names:
        try:
            mod = importlib.import_module(mod_name)
        except Exception:
            continue

        for fn_name in candidate_fns:
            fn = getattr(mod, fn_name, None)
            if not callable(fn):
                continue
            try:
                sig = inspect.signature(fn)
                params = list(sig.parameters.values())

                # Tentativa 1: sem argumentos (helper totalmente encapsulado)
                try:
                    conn = fn()
                    if conn is not None and hasattr(conn, "cursor"):
                        return conn
                except Exception:
                    pass

                # Tentativa 2: preencher kwargs por nomes comuns
                kwargs = {}
                for p in params:
                    pname = p.name.lower()
                    if pname in {"user", "usuario", "username", "login"} and user_val:
                        kwargs[p.name] = user_val
                    elif pname in {"password", "senha", "passwd"} and password_val:
                        kwargs[p.name] = password_val
                    elif pname in {"dsn", "tns", "alias", "service_name", "conn_str", "connection_string"} and dsn_val:
                        kwargs[p.name] = dsn_val

                if kwargs:
                    try:
                        conn = fn(**kwargs)
                        if conn is not None and hasattr(conn, "cursor"):
                            return conn
                    except Exception:
                        pass

                # Tentativa 3: assinatura simples posicional (user, password, dsn)
                positional = []
                for p in params:
                    pname = p.name.lower()
                    if pname in {"user", "usuario", "username", "login"}:
                        positional.append(user_val)
                    elif pname in {"password", "senha", "passwd"}:
                        positional.append(password_val)
                    elif pname in {"dsn", "tns", "alias", "service_name"}:
                        positional.append(dsn_val)
                    elif p.default is inspect._empty:
                        positional = []
                        break
                if positional:
                    try:
                        conn = fn(*positional)
                        if conn is not None and hasattr(conn, "cursor"):
                            return conn
                    except Exception:
                        pass
            except Exception:
                continue

    return None


def _connect_oracle(oracle_cfg: Optional[Dict[str, Optional[str]]] = None):
    import oracledb

    _init_oracle_client()
    cfg = dict(oracle_cfg or _get_oracle_config(use_streamlit=False))
    helper_conn = _try_project_connection_helper(cfg)
    if helper_conn is not None:
        return helper_conn

    user = _pick_cfg(["user", "oracle_user", "db_user", "username"], cfg)
    password = _pick_cfg(["password", "oracle_password", "db_password", "passwd"], cfg)
    dsn = _pick_cfg(["dsn", "oracle_dsn", "db_dsn", "tns", "tns_alias", "alias"], cfg) or "amon"

    if user and password:
        return oracledb.connect(user=user, password=password, dsn=dsn)

    # Fallback para cenarios com autenticacao externa.
    return oracledb.connect(dsn=dsn)


def _query_chunks(conn, sql: str, bind_params: Dict[str, object], chunk_size: int) -> Iterator[pd.DataFrame]:
    cur = conn.cursor()
    cur.arraysize = chunk_size
    cur.execute(sql, bind_params)
    cols = [d[0] for d in (cur.description or [])]

    while True:
        rows = cur.fetchmany(chunk_size)
        if not rows:
            break
        df = pd.DataFrame(rows, columns=cols)
        yield df

    cur.close()


def _safe_replace(src: Path, dst: Path, attempts: int = 6) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    for i in range(attempts):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if i == attempts - 1:
                raise
            time.sleep(0.8 * (i + 1))


def extrair_uc_faturada_raw(
    *,
    data_ini: str = "2025-01-01",
    output_path: Path = RAW_OUTPUT,
    chunk_size: int = 20000,
    oracle_cfg: Optional[Dict[str, Optional[str]]] = None,
) -> Dict[str, object]:
    sql = _read_sql()
    tmp = output_path.with_suffix(".tmp.parquet")
    if tmp.exists():
        tmp.unlink()

    conn = _connect_oracle(oracle_cfg=oracle_cfg)
    writer = None
    total_rows = 0
    chunks = 0
    try:
        for df in _query_chunks(conn, sql, {"p_data_ini": data_ini}, chunk_size):
            if df.empty:
                continue
            table = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(str(tmp), table.schema, compression="snappy")
            writer.write_table(table)
            total_rows += len(df.index)
            chunks += 1

        if writer is None:
            empty_df = pd.DataFrame(columns=["REGIONAL_TOTAL", "SIGLA_REGIONAL", "ANOMES", "UC_FATURADA"])
            table = pa.Table.from_pandas(empty_df, preserve_index=False)
            writer = pq.ParquetWriter(str(tmp), table.schema, compression="snappy")

        writer.close()
        writer = None
        _safe_replace(tmp, output_path)
        return {
            "ok": True,
            "rows": total_rows,
            "chunks": chunks,
            "output_path": str(output_path),
            "data_ini": data_ini,
        }
    finally:
        if writer is not None:
            writer.close()
        conn.close()
        if tmp.exists() and not output_path.exists():
            try:
                tmp.unlink()
            except Exception:
                pass
