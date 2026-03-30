#!/usr/bin/env python3
"""
clean_fund_history.py — Remove um ou mais fundos do history.json e reseta o
maxQuota no data.json para forçar backfill completo com dados corretos.

Uso:
    python scripts/clean_fund_history.py 29.726.133/0001-21
    python scripts/clean_fund_history.py 29.726.133/0001-21 35.828.684/0001-07
"""
import sys, json
from pathlib import Path

DOCS_DIR  = Path(__file__).parent.parent / "docs"
HIST_PATH = DOCS_DIR / "history.json"
DATA_PATH = DOCS_DIR / "data.json"

def main():
    if len(sys.argv) < 2:
        print("Uso: python scripts/clean_fund_history.py <cnpj> [cnpj2 ...]")
        sys.exit(1)

    cnpjs_to_remove = set(sys.argv[1:])
    print(f"Limpando {len(cnpjs_to_remove)} fundo(s): {cnpjs_to_remove}")

    # ── history.json ──────────────────────────────────────────────────────────
    if HIST_PATH.exists():
        hist = json.loads(HIST_PATH.read_text())
        removed = []
        for cnpj in cnpjs_to_remove:
            if cnpj in hist.get("funds", {}):
                del hist["funds"][cnpj]
                removed.append(cnpj)
                print(f"  ✓ history.json: removido {cnpj}")
            else:
                print(f"  ✗ history.json: não encontrado {cnpj}")
            # Remove de correlation
            hist.get("correlation", {}).pop(cnpj, None)
            for other in hist.get("correlation", {}).values():
                other.pop(cnpj, None)
        if removed:
            HIST_PATH.write_text(json.dumps(hist, ensure_ascii=False, separators=(",", ":")))
    else:
        print("  history.json não encontrado")

    # ── data.json — zerar maxQuota para forçar recálculo ─────────────────────
    if DATA_PATH.exists():
        data = json.loads(DATA_PATH.read_text())
        for fund in data.get("funds", []):
            if fund.get("cnpjFmt") in cnpjs_to_remove:
                fund["maxQuota"]     = 0.0
                fund["maxQuotaDate"] = ""
                print(f"  ✓ data.json: maxQuota zerado para {fund['cnpjFmt']}")
        # Remove de fund_betas também
        for cnpj in cnpjs_to_remove:
            data.get("fund_betas", {}).pop(cnpj, None)
        DATA_PATH.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")))
    else:
        print("  data.json não encontrado")

    print(f"\n✓ Pronto. Execute o workflow de update para regenerar o histórico completo.")

if __name__ == "__main__":
    main()
