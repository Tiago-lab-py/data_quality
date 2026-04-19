from __future__ import annotations

import pandas as pd


def _add_longa_duracao_if_possible(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # Normaliza colunas percebidas para evitar nulos que quebram a apuracao.
    fim_perc = "DATA_HORA_FIM_INTR_PERCEBIDO"
    fim_orig_candidates = ["DATA_HORA_FIM_INTRP", "DATA_HORA_FIM_INTR", "DT_FIM"]
    ini_perc = "DTHR_INICIO_INTRP_PERCEBIDO"
    ini_orig_candidates = ["DTHR_INICIO_INTRP_UC", "DTHR_INICIO_INTRP", "DT_INI"]

    if fim_perc in out.columns:
        for c in fim_orig_candidates:
            if c in out.columns:
                out[fim_perc] = out[fim_perc].fillna(out[c])
                break
    if ini_perc in out.columns:
        for c in ini_orig_candidates:
            if c in out.columns:
                out[ini_perc] = out[ini_perc].fillna(out[c])
                break

    if ini_perc in out.columns and fim_perc in out.columns:
        ini_dt = pd.to_datetime(out[ini_perc], errors="coerce")
        fim_dt = pd.to_datetime(out[fim_perc], errors="coerce")
        if "DURACAO_PERCEBIDA_MINUTOS" in out.columns:
            dur_calc = (fim_dt - ini_dt).dt.total_seconds().div(60.0)
            out["DURACAO_PERCEBIDA_MINUTOS"] = out["DURACAO_PERCEBIDA_MINUTOS"].where(
                out["DURACAO_PERCEBIDA_MINUTOS"].notna(),
                dur_calc,
            )

    if "longa_duracao" in out.columns:
        return out
    dur_candidates = [
        "DURACAO_PERCEBIDA_MINUTOS",
        "DURACAO_PERC_MIN",
        "DURACAO_MINUTOS",
    ]
    for col in dur_candidates:
        if col in out.columns:
            out["longa_duracao"] = pd.to_numeric(out[col], errors="coerce").fillna(0).ge(3)
            return out
    return out


def ensure_longa_duracao_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enriquecimento explícito do DataFrame em memória.
    Nao faz escrita automática em disco durante import do módulo para evitar
    corrupção por concorrência no Streamlit.
    """
    return _add_longa_duracao_if_possible(df)


def _install_safe_processed_writer_patch() -> None:
    """
    Patch leve e idempotente para garantir:
    1) coluna longa_duracao no processed
    2) escrita atomica para evitar parquet truncado/corrompido
    """
    if getattr(pd.DataFrame.to_parquet, "_dq_safe_processed_patch", False):
        return

    original_to_parquet = pd.DataFrame.to_parquet

    def _safe_to_parquet(self, *args, **kwargs):
        path = args[0] if args else kwargs.get("path")
        if path is None:
            return original_to_parquet(self, *args, **kwargs)

        try:
            path_str = str(path).replace("\\", "/").lower()
        except Exception:
            path_str = ""

        if not path_str.endswith("dic_fic_uc_processed.parquet"):
            return original_to_parquet(self, *args, **kwargs)

        df = _add_longa_duracao_if_possible(self)

        from pathlib import Path
        import uuid

        final_path = Path(path)
        tmp_path = final_path.with_name(f"{final_path.name}.tmp.{uuid.uuid4().hex}.parquet")

        new_args = list(args)
        if new_args:
            new_args[0] = tmp_path
        else:
            kwargs["path"] = tmp_path

        original_to_parquet(df, *tuple(new_args), **kwargs)
        tmp_path.replace(final_path)
        return None

    _safe_to_parquet._dq_safe_processed_patch = True
    pd.DataFrame.to_parquet = _safe_to_parquet


_install_safe_processed_writer_patch()

import inspect
import re
import threading
import time
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Dict, Optional

import streamlit as st


RAW_BASE_DIR = Path("D:/data_quality/data/raw")
LEGACY_LOCK_PATH = RAW_BASE_DIR / "dic_fic_uc.lock"
STATE_KEY = "_dq_import_job"
_PAGE_GLOBALS: Dict[str, object] = {}
_ACTIVE_MONTH_CTX: Optional[Dict[str, object]] = None


def _get_state() -> Dict[str, object]:
    state = st.session_state.get(STATE_KEY)
    if not isinstance(state, dict):
        state = {
            "status": "idle",
            "message": "",
            "thread": None,
            "entrypoint": "",
            "error": "",
            "month_ref": "",
            "lock_path": "",
        }
        st.session_state[STATE_KEY] = state
    return state


def _norm(s: str) -> str:
    table = str.maketrans(
        "áàâãéèêíìîóòôõúùûçÁÀÂÃÉÈÊÍÌÎÓÒÔÕÚÙÛÇ",
        "aaaaeeeiiioooouuucAAAAEEEIIIOOOOUUUC",
    )
    return s.translate(table).lower().strip()


def _to_date(value: object) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    to_py = getattr(value, "to_pydatetime", None)
    if callable(to_py):
        try:
            d = to_py()
            if isinstance(d, datetime):
                return d.date()
        except Exception:
            return None
    return None


def _parse_year_month_text(text: str) -> Optional[tuple[int, int]]:
    s = text.strip()
    if not s:
        return None

    m = re.match(r"^(\d{4})[-_/](\d{1,2})$", s)
    if m:
        y = int(m.group(1))
        mo = int(m.group(2))
        if 1 <= mo <= 12:
            return y, mo

    m = re.match(r"^(\d{1,2})[-_/](\d{4})$", s)
    if m:
        mo = int(m.group(1))
        y = int(m.group(2))
        if 1 <= mo <= 12:
            return y, mo

    m = re.match(r"^(\d{6})$", s)
    if m:
        y = int(s[:4])
        mo = int(s[4:])
        if 1 <= mo <= 12:
            return y, mo

    return None


def _month_context_from_session_state() -> tuple[Optional[Dict[str, object]], Optional[str]]:
    candidates: set[tuple[int, int]] = set()
    explicit_year: Optional[int] = None
    explicit_month: Optional[int] = None

    for key, value in st.session_state.items():
        nkey = _norm(str(key))

        if isinstance(value, (tuple, list)) and len(value) == 2:
            d1 = _to_date(value[0])
            d2 = _to_date(value[1])
            if d1 and d2:
                if (d1.year, d1.month) != (d2.year, d2.month):
                    return None, "Selecione apenas 1 mes por importacao."
                candidates.add((d1.year, d1.month))
                continue

        d = _to_date(value)
        if d:
            if "data" in nkey or "date" in nkey or "periodo" in nkey or "mes" in nkey or "month" in nkey:
                candidates.add((d.year, d.month))
            continue

        if isinstance(value, (tuple, list)) and value and ("mes" in nkey or "month" in nkey):
            parsed_months: set[int] = set()
            parsed_years: set[int] = set()
            for item in value:
                if isinstance(item, int) and 1 <= item <= 12:
                    parsed_months.add(item)
                    continue
                if isinstance(item, str):
                    ym = _parse_year_month_text(item)
                    if ym:
                        parsed_years.add(ym[0])
                        parsed_months.add(ym[1])
            if len(parsed_months) > 1:
                return None, "Selecione apenas 1 mes por importacao."
            if len(parsed_months) == 1:
                explicit_month = list(parsed_months)[0]
                if len(parsed_years) == 1:
                    explicit_year = list(parsed_years)[0]
            continue

        if isinstance(value, int):
            if ("mes" in nkey or "month" in nkey) and 1 <= value <= 12:
                explicit_month = value
            elif ("ano" in nkey or "year" in nkey) and 2000 <= value <= 2100:
                explicit_year = value
            continue

        if isinstance(value, str):
            ym = _parse_year_month_text(value)
            if ym:
                candidates.add(ym)
                continue
            if ("mes" in nkey or "month" in nkey) and value.isdigit():
                v = int(value)
                if 1 <= v <= 12:
                    explicit_month = v
                continue
            if ("ano" in nkey or "year" in nkey) and value.isdigit():
                v = int(value)
                if 2000 <= v <= 2100:
                    explicit_year = v
                continue

    if explicit_year is not None and explicit_month is not None:
        candidates.add((explicit_year, explicit_month))

    if not candidates:
        return None, "Nao foi possivel identificar o mes selecionado. Selecione Ano e Mes na tela."

    if len(candidates) > 1:
        return None, "Detectei mais de 1 mes na selecao. Ajuste para apenas 1 mes."

    year, month = list(candidates)[0]
    month_ref = f"{year:04d}-{month:02d}"
    lock_path = RAW_BASE_DIR / f"dic_fic_uc_{year:04d}{month:02d}.lock"
    partition_dir = RAW_BASE_DIR / "dic_fic_uc" / f"ano={year:04d}" / f"mes={month:02d}"
    partition_file = partition_dir / "dic_fic_uc.parquet"

    return {
        "year": year,
        "month": month,
        "month_ref": month_ref,
        "lock_path": str(lock_path),
        "partition_dir": str(partition_dir),
        "partition_file": str(partition_file),
    }, None


def _has_required_args(fn: Callable[..., object]) -> bool:
    try:
        sig = inspect.signature(fn)
    except Exception:
        return True
    for p in sig.parameters.values():
        if p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            continue
        if p.default is inspect._empty:
            return True
    return False


def _looks_like_zeroarg_callable(fn: object) -> bool:
    if not callable(fn):
        return False
    module_name = str(getattr(fn, "__module__", ""))
    if "import_runtime" in module_name:
        return False
    if _has_required_args(fn):  # type: ignore[arg-type]
        return False
    return True


def _looks_like_entrypoint(name: str, fn: object) -> bool:
    if not _looks_like_zeroarg_callable(fn):
        return False
    lname = name.lower()
    if lname.startswith("_"):
        return False
    if not any(k in lname for k in ("extr", "import")):
        return False
    if _has_required_args(fn):
        return False
    return True


def _find_entrypoint(page_globals: Dict[str, object]) -> tuple[str, Optional[Callable[..., object]]]:
    preferred = (
        "executar_importacao",
        "iniciar_importacao",
        "run_importacao",
        "rodar_importacao",
        "extrair_dados",
        "extrair",
    )
    for name in preferred:
        fn = page_globals.get(name)
        if _looks_like_entrypoint(name, fn):
            return name, fn  # type: ignore[return-value]

    for name, fn in page_globals.items():
        if _looks_like_entrypoint(name, fn):
            return name, fn  # type: ignore[return-value]

    return "", None


def _set_state(
    status: str,
    message: str = "",
    *,
    entrypoint: str = "",
    error: str = "",
    month_ref: str = "",
    lock_path: str = "",
) -> None:
    state = _get_state()
    state["status"] = status
    state["message"] = message
    if entrypoint:
        state["entrypoint"] = entrypoint
    if error:
        state["error"] = error
    if month_ref:
        state["month_ref"] = month_ref
    if lock_path:
        state["lock_path"] = lock_path


def _acquire_lock(lock_path: Path) -> bool:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        if lock_path.exists():
            age = time.time() - lock_path.stat().st_mtime
            if age > 12 * 60 * 60:
                lock_path.unlink()
    except Exception:
        pass

    try:
        with open(lock_path, "x", encoding="utf-8"):
            pass
        return True
    except FileExistsError:
        return False


def _release_lock(lock_path: Path) -> None:
    try:
        if lock_path.exists():
            lock_path.unlink()
    except Exception:
        pass


def _with_lock_retry(fn: Callable[[], object], attempts: int = 6, base_wait: float = 0.8) -> object:
    last_err: Optional[Exception] = None
    for i in range(attempts):
        try:
            return fn()
        except OSError as e:
            last_err = e
            code = getattr(e, "winerror", None) or getattr(e, "errno", None)
            if code not in (1224, 13) and "1224" not in str(e):
                raise
            if i == attempts - 1:
                break
            time.sleep(base_wait * (i + 1))
    if last_err is not None:
        raise last_err
    raise RuntimeError("Falha inesperada no retry de lock.")


def _rewrite_target_path(target: object, month_ctx: Optional[Dict[str, object]] = None) -> object:
    if not isinstance(target, (str, Path)):
        return target

    ctx = month_ctx or _ACTIVE_MONTH_CTX
    if not isinstance(ctx, dict):
        return target

    try:
        p = Path(str(target))
    except Exception:
        return target

    if p.name.lower() != "dic_fic_uc.parquet":
        return target

    partition_file = Path(str(ctx["partition_file"]))
    partition_file.parent.mkdir(parents=True, exist_ok=True)
    return str(partition_file)


def _install_write_retries(month_ctx: Dict[str, object]) -> None:
    global _ACTIVE_MONTH_CTX
    _ACTIVE_MONTH_CTX = month_ctx

    try:
        import polars as pl

        original = getattr(pl.DataFrame, "write_parquet", None)
        if callable(original) and not getattr(original, "__dq_retry__", False):

            def _write_parquet_retry(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                call_args = list(args)
                if call_args:
                    call_args[0] = _rewrite_target_path(call_args[0])
                elif "file" in kwargs:
                    kwargs["file"] = _rewrite_target_path(kwargs["file"])
                return _with_lock_retry(lambda: original(self, *call_args, **kwargs))

            setattr(_write_parquet_retry, "__dq_retry__", True)
            pl.DataFrame.write_parquet = _write_parquet_retry  # type: ignore[assignment]
    except Exception:
        pass

    try:
        import pandas as pd

        original_pd = getattr(pd.DataFrame, "to_parquet", None)
        if callable(original_pd) and not getattr(original_pd, "__dq_retry__", False):

            def _to_parquet_retry(self, *args, **kwargs):  # type: ignore[no-untyped-def]
                call_args = list(args)
                if call_args:
                    call_args[0] = _rewrite_target_path(call_args[0])
                elif "path" in kwargs:
                    kwargs["path"] = _rewrite_target_path(kwargs["path"])
                return _with_lock_retry(lambda: original_pd(self, *call_args, **kwargs))

            setattr(_to_parquet_retry, "__dq_retry__", True)
            pd.DataFrame.to_parquet = _to_parquet_retry  # type: ignore[assignment]
    except Exception:
        pass

    try:
        import pyarrow.parquet as pq

        original_write_table = getattr(pq, "write_table", None)
        if callable(original_write_table) and not getattr(original_write_table, "__dq_retry__", False):

            def _write_table_retry(*args, **kwargs):  # type: ignore[no-untyped-def]
                call_args = list(args)
                if len(call_args) >= 2:
                    call_args[1] = _rewrite_target_path(call_args[1])
                elif "where" in kwargs:
                    kwargs["where"] = _rewrite_target_path(kwargs["where"])
                return _with_lock_retry(lambda: original_write_table(*call_args, **kwargs))

            setattr(_write_table_retry, "__dq_retry__", True)
            pq.write_table = _write_table_retry  # type: ignore[assignment]

        original_writer = getattr(pq, "ParquetWriter", None)
        if callable(original_writer) and not getattr(original_writer, "__dq_retry__", False):

            class _DQParquetWriter(original_writer):  # type: ignore[misc,valid-type]
                __dq_retry__ = True

                def __init__(self, where, *args, **kwargs):  # type: ignore[no-untyped-def]
                    rewritten = _rewrite_target_path(where)
                    super().__init__(rewritten, *args, **kwargs)

            pq.ParquetWriter = _DQParquetWriter  # type: ignore[assignment]
    except Exception:
        pass


def _worker(
    entrypoint_name: str,
    fn: Callable[..., object],
    month_ctx: Dict[str, object],
    oracle_cfg: Optional[Dict[str, Optional[str]]] = None,
) -> None:
    lock_path = Path(str(month_ctx["lock_path"]))
    month_ref = str(month_ctx["month_ref"])

    if not _acquire_lock(lock_path):
        _set_state(
            "error",
            f"Importacao do mes {month_ref} ja esta em andamento.",
            entrypoint=entrypoint_name,
            error="lock_exists",
            month_ref=month_ref,
            lock_path=str(lock_path),
        )
        return

    try:
        _install_write_retries(month_ctx)
        started_at = time.time()
        _set_state(
            "running",
            f"Importacao em background em execucao ({month_ref}).",
            entrypoint=entrypoint_name,
            month_ref=month_ref,
            lock_path=str(lock_path),
        )
        fn()
        uc_msg = ""
        try:
            from app.services.extracao_uc_faturada import extrair_uc_faturada_raw
        except Exception:
            try:
                from services.extracao_uc_faturada import extrair_uc_faturada_raw
            except Exception:
                extrair_uc_faturada_raw = None  # type: ignore[assignment]

        if extrair_uc_faturada_raw is not None:
            uc_info = extrair_uc_faturada_raw(oracle_cfg=oracle_cfg)
            uc_rows = int(uc_info.get("rows", 0))
            uc_msg = f" | UC_faturada={uc_rows:,}".replace(",", ".")

        elapsed = time.time() - started_at
        if elapsed < 10:
            _set_state(
                "submitted",
                f"Extracao disparada para {month_ref}. Acompanhe no terminal.{uc_msg}",
                entrypoint=entrypoint_name,
                month_ref=month_ref,
                lock_path=str(lock_path),
            )
        else:
            _set_state(
                "done",
                f"Importacao concluida com sucesso ({month_ref}).{uc_msg}",
                entrypoint=entrypoint_name,
                month_ref=month_ref,
                lock_path=str(lock_path),
            )
    except Exception:
        _set_state(
            "error",
            f"Falha durante a importacao em background ({month_ref}).",
            entrypoint=entrypoint_name,
            error=traceback.format_exc(),
            month_ref=month_ref,
            lock_path=str(lock_path),
        )
    finally:
        _release_lock(lock_path)


def _is_running() -> bool:
    state = _get_state()
    if state.get("status") != "running":
        return False
    t = state.get("thread")
    if isinstance(t, threading.Thread) and t.is_alive():
        return True
    state["status"] = "done"
    state["message"] = "Importacao finalizada."
    return False


def _should_intercept(label: str) -> bool:
    return "import" in _norm(label)


def _is_show_oracle_button(label: str) -> bool:
    n = _norm(label)
    return "mostrar" in n and "conex" in n and "oracle" in n


def _collect_oracle_cfg_from_main_thread() -> Dict[str, Optional[str]]:
    return {}


def _start_background_import(page_globals: Dict[str, object]) -> bool:
    if _is_running():
        st.warning("Importacao ja esta em execucao em background.")
        return False

    month_ctx, month_err = _month_context_from_session_state()
    if month_ctx is None:
        st.error(month_err or "Nao foi possivel validar o mes da importacao.")
        return False

    entrypoint_name, fn = _find_entrypoint(page_globals)
    if fn is None:
        st.error("Nao encontrei a funcao de importacao para iniciar em background.")
        return False

    oracle_cfg = _collect_oracle_cfg_from_main_thread()
    t = threading.Thread(
        target=_worker,
        args=(entrypoint_name, fn, month_ctx, oracle_cfg),
        daemon=True,
        name="dq-import-bg",
    )
    state = _get_state()
    state["thread"] = t
    t.start()
    st.info(f"Importacao iniciada em background para {month_ctx['month_ref']}.")
    return True


def install_background_import_button(page_globals: Dict[str, object]) -> None:
    global _PAGE_GLOBALS
    _PAGE_GLOBALS = page_globals

    if getattr(st.button, "__dq_bg_patched__", False):
        return

    original_button = st.button
    original_sidebar_button = st.sidebar.button

    def _handle_click(label: str, clicked: bool) -> bool:
        if not clicked or not _should_intercept(label):
            return clicked

        started = _start_background_import(_PAGE_GLOBALS)
        if started:
            return False
        return clicked

    def patched_button(label: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        if _is_show_oracle_button(label):
            return False
        clicked = original_button(label, *args, **kwargs)
        return _handle_click(label, clicked)

    def patched_sidebar_button(label: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        if _is_show_oracle_button(label):
            return False
        clicked = original_sidebar_button(label, *args, **kwargs)
        return _handle_click(label, clicked)

    setattr(patched_button, "__dq_bg_patched__", True)
    st.button = patched_button  # type: ignore[assignment]
    st.sidebar.button = patched_sidebar_button  # type: ignore[assignment]


def render_import_status() -> None:
    state = _get_state()
    status = state.get("status", "idle")
    if status == "running" and not _is_running():
        status = state.get("status", "done")

    month_ref = str(state.get("month_ref") or "")
    month_suffix = f" ({month_ref})" if month_ref else ""

    if status == "running":
        st.info(str(state.get("message") or f"Importacao em andamento{month_suffix}."))
        if hasattr(st, "autorefresh"):
            st.autorefresh(interval=1500, key="dq-import-bg-refresh")
    elif status == "submitted":
        st.info(str(state.get("message") or f"Extracao disparada{month_suffix}."))
    elif status == "done":
        st.success(str(state.get("message") or f"Importacao concluida{month_suffix}."))
    elif status == "error":
        st.error(str(state.get("message") or f"Falha na importacao{month_suffix}."))
        err = str(state.get("error") or "")
        if err:
            with st.expander("Detalhes do erro"):
                st.code(err, language="text")
