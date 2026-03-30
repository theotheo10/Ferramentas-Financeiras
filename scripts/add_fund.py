#!/usr/bin/env python3
"""
add_fund.py — Adiciona um ou mais fundos ao histórico do site.

Uso:
    python scripts/add_fund.py '[{"cnpj":"...","nome":"...","exibicao":"...","curto":"...","tipo":"...","trib":"...","expo":"...","banco":"..."}]'

Campos obrigatórios:
    cnpj      — CNPJ só com dígitos (ex: 46351969000108)
    nome      — nome completo CVM
    exibicao  — nome no ranking e painéis (ex: "Opportunity Global")
    curto     — nome em gráficos e seletores (ex: "Opportunity")
    tipo      — "Long Only" | "Long Biased" | "Multimercado" | "Long & Short"
              | "Renda Fixa" | "Renda Fixa - Crédito Privado"
              | "Renda Fixa - Pós-fixado" | "Renda Fixa - Debêntures Incentivadas"
    trib      — "RV" | "TR" | "Isento"
    expo      — "Brasil" | "Internacional" | "Majoritariamente Brasil"
    banco     — custodiante (ex: "Itaú", "BTG", "XP")

Campos opcionais (inferidos do tipo+expo se omitidos):
    expo_liquida_normal  — exposição líquida típica 0.0–1.0 (ex: 0.70)
    expo_liquida_crise   — exposição líquida em crise severa (ex: 0.50)
    benchmark            — "ibov" | "sp500" | "mixed" | "cdi"
    gross_up             — true para fundos isentos (debêntures incentivadas)
    obs                  — observação livre exibida no painel do fundo
"""

import sys, json, zipfile, io, math, datetime, urllib.request
from pathlib import Path

FIRST_MONTHLY   = 2021
CVM_OLDEST_YEAR = 2005
DOCS_DIR        = Path(__file__).parent.parent / "docs"
SCRIPTS_DIR     = Path(__file__).parent
HIST_PATH       = DOCS_DIR / "history.json"
INDEX_PATH      = DOCS_DIR / "fundos.html"
FETCH_PATH      = SCRIPTS_DIR / "fetch_data.py"

REQUIRED_KEYS = {"cnpj", "nome", "exibicao", "curto", "tipo", "trib", "expo", "banco"}

# ── Exposure defaults by (tipo, expo) ─────────────────────────────────────

def default_exposure(tipo: str, expo: str) -> dict:
    """
    Returns default exposure profile based on fund type and geography.
    Can be overridden per-fund via expo_liquida_normal/crise/benchmark fields.
    """
    is_intl = "Internacional" in expo

    if "Long & Short" in tipo:
        return {
            "net_normal":  0.0,
            "net_crisis":  0.0,
            "benchmark":   "sp500" if is_intl else "ibov",
        }
    if "Long Biased" in tipo:
        return {
            "net_normal":  0.65,
            "net_crisis":  0.40,
            "benchmark":   "sp500" if is_intl else "ibov",
        }
    if "Multimercado" in tipo:
        return {
            "net_normal":  0.30,
            "net_crisis":  0.10,
            "benchmark":   "mixed",
        }
    if "Renda Fixa" in tipo:
        return {
            "net_normal":  0.0,
            "net_crisis":  0.0,
            "benchmark":   "cdi",
        }
    # Long Only (default)
    return {
        "net_normal":  1.0,
        "net_crisis":  0.95,
        "benchmark":   "sp500" if is_intl else "ibov",
    }


# ── Fetch CVM ──────────────────────────────────────────────────────────────

def fetch_zip(url: str, timeout: int = 120) -> str | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            return zf.read(zf.namelist()[0]).decode("windows-1252", errors="replace")
    except Exception as e:
        print(f"  WARN: {e}")
        return None


def parse_csv(content: str) -> dict:
    lines  = content.split("\n")
    header = [h.strip().lstrip("\ufeff") for h in lines[0].split(";")]
    return {
        "lines":     lines,
        "col_cnpj":  next((i for i, h in enumerate(header) if h.startswith("CNPJ")), -1),
        "col_date":  header.index("DT_COMPTC") if "DT_COMPTC"  in header else -1,
        "col_quota": header.index("VL_QUOTA")  if "VL_QUOTA"   in header else -1,
    }


def extract_fund(data: dict, cnpj_digits: str, cnpj_fmt: str) -> dict[str, float]:
    """
    Extrai cotas para um fundo de um arquivo CVM parseado.
    Trata corretamente a RCVM 175 (pós-2023): após a reforma, o mesmo CNPJ
    pode aparecer múltiplas vezes por dia (casca com cota ~1.0 + subclasses com
    cota real). A lógica abaixo replica _extract_rows() do fetch_data.py:
      1. Coleta todas as cotas por data.
      2. Para datas com múltiplas cotas, descarta cotas < 1.5 (cascas).
      3. Entre as remanescentes, escolhe a menor (mais próxima da série histórica).
      4. Verifica continuidade com a última cota de linha única como referência.
    """
    if not data or data["col_date"] < 0 or data["col_quota"] < 0:
        return {}

    # Passo 1: coletar todas as cotas por data
    all_rows: dict[str, list[float]] = {}
    for line in data["lines"][1:]:
        if cnpj_digits not in line and cnpj_fmt not in line:
            continue
        cols = line.split(";")
        try:
            if data["col_cnpj"] >= 0:
                raw = cols[data["col_cnpj"]].strip().replace(".", "").replace("/", "").replace("-", "")
                if raw != cnpj_digits:
                    continue
            d = cols[data["col_date"]].strip()
            q = float(cols[data["col_quota"]].replace(",", "."))
            if d and q > 0:
                all_rows.setdefault(d, []).append(q)
        except (ValueError, IndexError):
            continue

    if not all_rows:
        return {}

    # Referência histórica: última data com cota única (pré-RCVM 175)
    single_qs = {d: qs[0] for d, qs in all_rows.items() if len(qs) == 1}
    last_ref = single_qs[max(single_qs)] if single_qs else None

    result: dict[str, float] = {}
    warned = False
    for d in sorted(all_rows.keys()):
        qs = all_rows[d]
        if len(qs) == 1:
            result[d] = qs[0]
        else:
            # Descartar cascas (cota < 1.5) e escolher a menor das reais
            real_qs = sorted(q for q in qs if q >= 1.5)
            if real_qs:
                chosen = min(real_qs)
                # Verificar continuidade para alertar sobre possível subclasse errada
                if last_ref is not None and not warned:
                    ratio = chosen / last_ref
                    if ratio < 0.5 or ratio > 2.0:
                        print(f"      [AVISO RCVM175] {d}: escolhida={chosen:.6f} "
                              f"vs ref={last_ref:.6f} (ratio={ratio:.2f}) — verificar")
                        warned = True
                result[d] = chosen
            else:
                result[d] = max(qs)
    return result


_zip_cache: dict[str, dict | None] = {}

def fetch_cached(url: str, timeout: int = 120) -> dict | None:
    if url not in _zip_cache:
        content = fetch_zip(url, timeout)
        _zip_cache[url] = parse_csv(content) if content else None
    return _zip_cache[url]


def fetch_full_history(cnpj_digits: str, cnpj_fmt: str) -> dict[str, float]:
    today  = datetime.date.today()
    quotas: dict[str, float] = {}

    for year in range(CVM_OLDEST_YEAR, FIRST_MONTHLY):
        url  = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/HIST/inf_diario_fi_{year}.zip"
        data = fetch_cached(url, timeout=120)
        if data:
            rows = extract_fund(data, cnpj_digits, cnpj_fmt)
            if rows:
                quotas.update(rows)
                print(f"  {year}: +{len(rows)} cotas")

    y, m = FIRST_MONTHLY, 1
    while (y, m) <= (today.year, today.month):
        url  = f"https://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_{y}{m:02d}.zip"
        data = fetch_cached(url)
        if data:
            rows = extract_fund(data, cnpj_digits, cnpj_fmt)
            if rows:
                quotas.update(rows)
                print(f"  {y}-{m:02d}: +{len(rows)} cotas")
        m += 1
        if m > 12:
            m = 1; y += 1

    return quotas


# ── Interpolação e retornos ────────────────────────────────────────────────

def interpolate(quotas: dict[str, float], all_dates: list[str]) -> dict[str, float]:
    if not quotas:
        return quotas
    sorted_known = sorted(k for k, v in quotas.items() if v and v > 0)
    if not sorted_known:
        return quotas
    fund_start = sorted_known[0]
    fund_end   = sorted_known[-1]

    for d in all_dates:
        if d < fund_start or d > fund_end:
            continue
        if d in quotas and quotas[d] > 0:
            continue
        prev_d = next((x for x in reversed(sorted_known) if x < d), None)
        next_d = next((x for x in sorted_known           if x > d), None)
        if prev_d and next_d and quotas.get(prev_d, 0) > 0 and quotas.get(next_d, 0) > 0:
            t0    = datetime.date.fromisoformat(prev_d)
            t1    = datetime.date.fromisoformat(next_d)
            td    = datetime.date.fromisoformat(d)
            alpha = (td - t0).days / max((t1 - t0).days, 1)
            quotas[d] = round(quotas[prev_d] * ((quotas[next_d] / quotas[prev_d]) ** alpha), 8)
        elif prev_d and quotas.get(prev_d, 0) > 0:
            quotas[d] = quotas[prev_d]

    return quotas


def safe_returns(qs: dict[str, float], dates: list[str]) -> list[float]:
    rets = []
    for i in range(1, len(dates)):
        q0 = qs.get(dates[i-1], 0.0)
        q1 = qs.get(dates[i],   0.0)
        rets.append((q1 / q0) - 1 if q0 and q0 > 0 and q1 and q1 > 0 else None)
    return rets


def pearson_safe(ca: str, cb: str,
                 all_quotas: dict[str, dict[str, float]]) -> float:
    qs_a    = all_quotas.get(ca, {})
    qs_b    = all_quotas.get(cb, {})
    valid_a = {d for d, v in qs_a.items() if v and v > 0}
    valid_b = {d for d, v in qs_b.items() if v and v > 0}
    common  = sorted(valid_a & valid_b)
    if len(common) < 30:
        return 0.0
    ra = [(qs_a[common[i]] / qs_a[common[i-1]]) - 1
          for i in range(1, len(common))
          if qs_a.get(common[i-1]) and qs_a.get(common[i])]
    rb = [(qs_b[common[i]] / qs_b[common[i-1]]) - 1
          for i in range(1, len(common))
          if qs_b.get(common[i-1]) and qs_b.get(common[i])]
    n = min(len(ra), len(rb))
    if n < 30:
        return 0.0
    ra, rb = ra[:n], rb[:n]
    ma, mb = sum(ra)/n, sum(rb)/n
    num = sum((ra[i]-ma)*(rb[i]-mb) for i in range(n))
    sa  = math.sqrt(sum((x-ma)**2 for x in ra))
    sb  = math.sqrt(sum((x-mb)**2 for x in rb))
    return round(num/(sa*sb), 4) if sa*sb > 0 else 0.0


# ── Atualizar history.json ────────────────────────────────────────────────

def update_history(new_funds: list[dict]) -> None:
    print(f"\n── Atualizando history.json")

    hist = {}
    if HIST_PATH.exists():
        hist = json.loads(HIST_PATH.read_text())

    all_fund_quotas: dict[str, dict[str, float]] = {}
    for cnpj, fd in hist.get("funds", {}).items():
        qs = dict(zip(fd["dates"], fd["quotas"]))
        all_fund_quotas[cnpj] = {d: v for d, v in qs.items() if v and v > 0}

    for nf in new_funds:
        all_fund_quotas[nf["cnpj_fmt"]] = nf["quotas"]

    all_dates = sorted({d for qs in all_fund_quotas.values()
                        for d, v in qs.items() if v and v > 0})
    print(f"  Total datas (union): {len(all_dates)} ({all_dates[0]} → {all_dates[-1]})")

    for cnpj, qs in all_fund_quotas.items():
        all_fund_quotas[cnpj] = interpolate(qs, all_dates)

    nome_map = {fd["cnpj_fmt"]: fd["nome_exibicao"] for fd in new_funds}
    for cnpj, fd in hist.get("funds", {}).items():
        if cnpj not in nome_map:
            nome_map[cnpj] = fd.get("nome", cnpj)

    funds_out = {}
    for cnpj, qs in all_fund_quotas.items():
        fund_dates  = sorted(d for d, v in qs.items() if v and v > 0)
        fund_quotas = [qs[d] for d in fund_dates]
        returns     = safe_returns(qs, fund_dates)
        cum = pk = 1.0; mdd = 0.0
        for r in returns:
            if r is None: continue  # pré-inception ou gap
            cum *= (1 + r)
            if cum > pk: pk = cum
            dd = (cum - pk) / pk
            if dd < mdd: mdd = dd
        funds_out[cnpj] = {
            "nome":        nome_map.get(cnpj, cnpj),
            "dates":       fund_dates,
            "quotas":      fund_quotas,
            "returns":     returns,
            "maxDrawdown": round(mdd*100, 2),
            "nDays":       len(fund_dates),
            "start":       fund_dates[0],
            "end":         fund_dates[-1],
        }

    cnpjs = list(funds_out.keys())
    corr  = {ca: {cb: (1.0 if ca == cb else pearson_safe(ca, cb, all_fund_quotas))
                  for cb in cnpjs} for ca in cnpjs}

    n_years = (datetime.date.fromisoformat(all_dates[-1]) -
               datetime.date.fromisoformat(all_dates[0])).days / 365.25 if all_dates else 0

    output = {
        "generatedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "from":        all_dates[0],
        "to":          all_dates[-1],
        "nDays":       len(all_dates),
        "nYears":      round(n_years, 2),
        "commonDates": all_dates,
        "correlation": corr,
        "funds":       funds_out,
    }

    HIST_PATH.write_text(json.dumps(output, ensure_ascii=False, separators=(",", ":")))
    size_kb = HIST_PATH.stat().st_size // 1024
    print(f"  ✓ history.json: {len(all_dates)} datas, {n_years:.1f} anos, {size_kb} KB")


# ── Atualizar fetch_data.py ───────────────────────────────────────────────

def update_fetch_data(funds: list[dict]) -> None:
    print(f"\n── Atualizando fetch_data.py")
    src = FETCH_PATH.read_text()
    added = 0
    for f in funds:
        if f["cnpj_digits"] in src:
            print(f"  {f['exibicao']}: já está em FUNDS")
            continue
        src = src.replace(
            ']\n\nFIRST_MONTHLY_YEAR',
            f'    {{"name": "{f["nome"]}", "exibicao": "{f["exibicao"]}", "curto": "{f["curto"]}", "cnpj": "{f["cnpj_digits"]}", "cnpjFmt": "{f["cnpj_fmt"]}"}},\n]\n\nFIRST_MONTHLY_YEAR',
            1
        )
        added += 1
        print(f"  ✓ {f['exibicao']} adicionado")
    if added:
        FETCH_PATH.write_text(src)


# ── Atualizar index.html ──────────────────────────────────────────────────

def update_index(funds: list[dict]) -> None:
    print(f"\n── Atualizando index.html")
    src = INDEX_PATH.read_text()

    for f in funds:
        if f["cnpj_fmt"] in src:
            print(f"  {f['exibicao']}: já está em FUND_META")
            continue

        # FUND_META entry
        gross_up_str = "true" if f.get("gross_up") else "false"
        obs_str = f.get("obs", "")
        new_meta = (
            f'  "{f["cnpj_fmt"]}": {{ nome:"{f["exibicao"]}", short:"{f["curto"]}", '
            f'inception:"{f["inception_date"]}", initialQuota:{f["initial_quota"]}, '
            f'maxQuota:{f["max_quota"]}, tipo:"{f["tipo"]}", trib:"{f["trib"]}", '
            f'expo:"{f["expo"]}", banco:"{f["banco"]}", grossUp:{gross_up_str}, obs:"{obs_str}" }},\n'
        )
        src = src.replace('};\nconst TRIB_LABEL', f'{new_meta}}};\nconst TRIB_LABEL', 1)

        # FUND_EXPOSURE entry for stress test
        expo = f["exposure"]
        new_exposure = (
            f'  "{f["cnpj_fmt"]}": {{ '
            f'net_normal:{expo["net_normal"]}, '
            f'net_crisis:{expo["net_crisis"]}, '
            f'primary:"{expo["benchmark"]}" '
            f'}}, // {f["exibicao"]}\n'
        )
        src = src.replace(
            '};\n\n// Historical stress scenarios',
            f'{new_exposure}}};\n\n// Historical stress scenarios',
            1
        )

        print(f"  ✓ {f['exibicao']} — FUND_META + FUNDS + FUND_EXPOSURE")
        print(f"     expo: normal={expo['net_normal']} crise={expo['net_crisis']} benchmark={expo['benchmark']}")

    INDEX_PATH.write_text(src)


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print("Uso: python add_fund.py '<json>'")
        sys.exit(1)

    try:
        raw = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(f"ERRO: JSON inválido — {e}")
        sys.exit(1)

    if isinstance(raw, dict):
        raw = [raw]

    for i, f in enumerate(raw):
        missing = REQUIRED_KEYS - set(f.keys())
        if missing:
            print(f"ERRO: fundo #{i+1} faltam campos: {missing}")
            sys.exit(1)

    print(f"=== Adicionando {len(raw)} fundo(s) ===\n")

    processed = []
    history_entries = []

    for f in raw:
        d = f["cnpj"].strip().replace(".", "").replace("/", "").replace("-", "").zfill(14)
        cnpj_fmt = f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"

        # Resolve exposure profile
        expo_defaults = default_exposure(f["tipo"], f["expo"])
        exposure = {
            "net_normal": f.get("expo_liquida_normal", expo_defaults["net_normal"]),
            "net_crisis":  f.get("expo_liquida_crise",  expo_defaults["net_crisis"]),
            "benchmark":   f.get("benchmark",            expo_defaults["benchmark"]),
        }

        print(f"── {f['exibicao']} ({cnpj_fmt})")
        print(f"   {f['tipo']} | {f['trib']} | {f['expo']} | {f['banco']}")
        print(f"   Exposição: normal={exposure['net_normal']} crise={exposure['net_crisis']} benchmark={exposure['benchmark']}")

        print(f"   Buscando histórico na CVM…")
        quotas = fetch_full_history(d, cnpj_fmt)

        if not quotas:
            print(f"   ERRO: nenhuma cota encontrada — pulando")
            continue

        sorted_dates   = sorted(quotas.keys())
        inception_date = sorted_dates[0]
        initial_quota  = quotas[inception_date]
        max_quota_val  = max(quotas.values())
        max_quota_date = max(quotas, key=quotas.get)
        print(f"   Inception: {inception_date} · {len(quotas)} cotas")
        print(f"   Máxima: {max_quota_val:.6f} em {max_quota_date}")

        entry = {
            "cnpj_digits":    d,
            "cnpj_fmt":       cnpj_fmt,
            "nome":           f["nome"],
            "exibicao":       f["exibicao"],
            "curto":          f["curto"],
            "tipo":           f["tipo"],
            "trib":           f["trib"],
            "expo":           f["expo"],
            "banco":          f["banco"],
            "gross_up":       f.get("gross_up", False),
            "obs":            f.get("obs", ""),
            "inception_date": inception_date,
            "initial_quota":  initial_quota,
            "max_quota":      max_quota_val,
            "max_quota_date": max_quota_date,
            "exposure":       exposure,
        }
        processed.append(entry)
        history_entries.append({
            "cnpj_fmt":      cnpj_fmt,
            "nome_exibicao": f["exibicao"],
            "quotas":        quotas,
        })
        print()

    if not processed:
        print("Nenhum fundo processado.")
        sys.exit(1)

    update_history(history_entries)
    update_fetch_data(processed)
    update_index(processed)

    print(f"\n✓ {len(processed)} fundo(s) adicionado(s)!")
    for p in processed:
        print(f"  · {p['exibicao']} ({p['cnpj_fmt']}) — desde {p['inception_date']}")
        print(f"    expo: normal={p['exposure']['net_normal']} crise={p['exposure']['net_crisis']} benchmark={p['exposure']['benchmark']}")

    # Rodar fetch_data.main() para atualizar data.json imediatamente.
    # Sem isso, o novo fundo ficaria com N/D em todas as métricas até o próximo
    # update diário, porque data.json não é gerado por add_fund.py.
    # Importar aqui (não no topo) para evitar circular import se fetch_data
    # eventualmente importar add_fund no futuro.
    print("\n── Rodando fetch_data para atualizar data.json com o(s) novo(s) fundo(s)…")
    try:
        import sys as _sys
        _sys.path.insert(0, str(SCRIPTS_DIR))
        import fetch_data as _fd
        _fd.main()
        print("✓ data.json atualizado com sucesso.")
    except Exception as _e:
        print(f"⚠ fetch_data.main() falhou: {_e}")
        print("  Execute manualmente: python scripts/fetch_data.py")


if __name__ == "__main__":
    main()
