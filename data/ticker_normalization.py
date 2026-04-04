"""
Ticker normalization for Brazilian equities.
Handles: renames, class conversions, mergers, corporate actions.
All tickers in Yahoo Finance .SA format.

Last updated: 2026-03 — covers all B3 changes through early 2026.
"""

# Map old/dead tickers → current valid ticker
# Format: "OLD.SA": "NEW.SA"  or  "OLD.SA": None (delisted, no successor)
TICKER_MAP = {
    # ── Historical renames (pre-2024) ────────────────────────────────────────

    # B3 itself (BVMF3 merged into B3SA3 in 2017)
    "BVMF3.SA": "B3SA3.SA",

    # Suzano merged Fibria (FIBR3) → SUZB3 in 2019
    "FIBR3.SA": "SUZB3.SA",
    "SUZB5.SA": "SUZB3.SA",

    # Vale (preferred shares discontinued)
    "VALE5.SA": "VALE3.SA",

    # TIM (ticker change)
    "TIMP3.SA": "TIMS3.SA",

    # Telefonica/Vivo
    "VIVT4.SA": "VIVT3.SA",
    "TLPP4.SA": "VIVT3.SA",

    # Klabim (PN → units)
    "KLBN4.SA": "KLBN11.SA",

    # Lojas Americanas / LAME → AMER3 → collapsed 2023
    "LAME4.SA": None,
    "BTOW3.SA": None,
    "AMER3.SA": None,

    # Cogna (ex-Kroton)
    "KROT3.SA": "COGN3.SA",

    # Via Varejo → Grupo Casas Bahia → BHIA3
    "VVAR3.SA": "BHIA3.SA",

    # HGTX → CTC (CTCA3)
    "HGTX3.SA": "CTCA3.SA",

    # Estácio → Yduqs
    "ESTC3.SA": "YDUQ3.SA",

    # ELPL4 → ENBR3 (EDP Energias do Brasil)
    "ELPL4.SA": "ENBR3.SA",

    # Pão de Açúcar PN → ON
    "PCAR4.SA": "PCAR3.SA",

    # ── Delistings (no successor) ─────────────────────────────────────────────

    "OIBR3.SA":  None,   # Oi — delisted/restructuring
    "OIBR4.SA":  None,
    "OGXP3.SA":  None,   # OGX
    "PDGR3.SA":  None,   # PDG Realty
    "BISA3.SA":  None,   # Brookfield Incorporações
    "RSID3.SA":  None,   # Rossi
    "CZRS4.SA":  None,   # Cyrela/Helbor
    "DTEX3.SA":  None,   # Duratex (saiu do IBOV, cancelado)
    "GFSA3.SA":  None,   # Gafisa (saiu do IBOV)
    "PORT3.SA":  None,   # Porto Seguro ON (saiu do IBOV)
    "PMAM3.SA":  None,   # Paranapanema
    "LLXL3.SA":  None,   # LLX Logística
    "SMLE3.SA":  None,   # Smiles (incorporada pela Gol)
    "BRPR3.SA":  None,   # BR Properties

    # ── 2024 renames ─────────────────────────────────────────────────────────

    # ISA CTEEP (nov/2024): TRPL3/TRPL4 → ISAE3/ISAE4
    "TRPL3.SA": "ISAE3.SA",
    "TRPL4.SA": "ISAE4.SA",

    # Azzas (2024): fusão Arezzo + Soma → AZZA3
    "SOMA3.SA": "AZZA3.SA",
    "ARZZ3.SA": "AZZA3.SA",

    # ── 2025 renames ─────────────────────────────────────────────────────────

    # Eletrobras → Axia Energia (nov/2025)
    # NOTA: Yahoo Finance serve AXIA3/6 apenas desde ~2025-11.
    # MA200 ficará NaN até ~2026-08.
    "ELET3.SA": "AXIA3.SA",
    "ELET5.SA": "AXIA5.SA",
    "ELET6.SA": "AXIA6.SA",

    # Embraer (nov/2025): EMBR3 → EMBJ3
    # NOTA: Yahoo Finance serve EMBJ3 apenas desde ~2025-10-27.
    # MA200 ficará NaN até ~2026-06 (200 pregões após o rename).
    "EMBR3.SA": "EMBJ3.SA",

    # CCR → Motiva (mai/2025): CCRO3 → MOTV3
    # NOTA: Yahoo Finance serve MOTV3 apenas desde ~2025-04-23 (245 dias hoje).
    # MA200 deve fechar ~2025-12 / início de 2026.
    "CCRO3.SA": "MOTV3.SA",

    # Natura (jul/2025): NTCO3 → NATU3 (voltou ao ticker original)
    # NOTA: Yahoo Finance serve NATU3 apenas desde ~2025-07.
    # MA200 deve fechar ~2026-04.
    "NTCO3.SA": "NATU3.SA",

    # JBS (2025): JBSS3 extinto → BDR JBSS32
    # Yahoo Finance não serve JBSS32 de forma confiável; tratamos como None
    "JBSS3.SA": None,

    # BRF + Marfrig → MBRF3 (set/2025)
    "BRFS3.SA": "MBRF3.SA",
    "MRFG3.SA": "MBRF3.SA",

    # Gol recuperação judicial (jun/2025): GOLL4 → GOLL54 (lote de 1000)
    # Ação vale frações de centavo; excluímos do cálculo de breadth
    "GOLL4.SA": None,

    # Azul recuperação judicial (dez/2025): AZUL4 → AZUL54
    "AZUL4.SA": None,

    # Cielo (saída do IBOV e fechamento de capital em andamento)
    "CIEL3.SA": "CIEL3.SA",   # ainda ativa, mantém ticker

    # ── Tickers sem mudança confirmada (mantidos por clareza) ─────────────────
    "CESP6.SA":  "CESP6.SA",
    "CPLE6.SA":  "CPLE6.SA",
    "ECOR3.SA":  "ECOR3.SA",
    "QUAL3.SA":  "QUAL3.SA",
    "CSAN3.SA":  "CSAN3.SA",
    "SMFT3.SA":  "SMFT3.SA",
    "MULT3.SA":  "MULT3.SA",
    "MRVE3.SA":  "MRVE3.SA",
    "SANB11.SA": "SANB11.SA",
    "HAPV3.SA":  "HAPV3.SA",
    "SEER3.SA":  "SEER3.SA",
    "EVEN3.SA":  "EVEN3.SA",
    "GOAU4.SA":  "GOAU4.SA",
    "DASA3.SA":  "DASA3.SA",
}


def normalize_ticker(ticker: str) -> str | None:
    """
    Normalize a ticker to its current valid form.
    Returns None if the ticker is delisted with no successor.
    Returns the ticker unchanged if not in the map.
    """
    return TICKER_MAP.get(ticker, ticker)


def normalize_tickers(tickers: list[str]) -> list[str]:
    """
    Normalize a list of tickers.
    Removes None entries (delisted) and deduplicates.
    """
    normalized = set()
    for t in tickers:
        result = normalize_ticker(t)
        if result is not None:
            normalized.add(result)
    return sorted(list(normalized))


def get_all_historical_tickers(composition_history: list[dict]) -> list[str]:
    """
    Build union of all tickers ever in IBOV (historical + normalized).
    This is the full universe needed to avoid survivorship bias.
    """
    all_tickers = set()
    for period in composition_history:
        for ticker in period["tickers"]:
            # Keep original for historical price fetching
            all_tickers.add(ticker)
            # Also add normalized form
            normalized = normalize_ticker(ticker)
            if normalized:
                all_tickers.add(normalized)
    return sorted(list(all_tickers))
