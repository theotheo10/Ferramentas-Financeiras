"""
Ticker normalization for Brazilian equities.
Handles: renames, class conversions, mergers, corporate actions.
All tickers in Yahoo Finance .SA format.

Last updated: 2026-04 — covers all B3 changes through early 2026.

Rules:
  "OLD.SA": "NEW.SA"  — yfinance serves full history under NEW
  "OLD.SA": None      — delisted with no B3 successor (excluded from breadth)

Tickers not in this map are returned unchanged by normalize_ticker().
"""

# Map old/dead tickers → current valid ticker
TICKER_MAP = {
    # ── Historical renames — yfinance serves retroactively under new name ─────

    # B3 (BVMF3 merged into B3SA3 in 2017)
    "BVMF3.SA": "B3SA3.SA",

    # Cetip merged into B3 in 2017
    "CTIP3.SA": "B3SA3.SA",

    # Suzano absorbed Fibria (FIBR3) in 2019; preferred (SUZB5) converted to ON
    "FIBR3.SA": "SUZB3.SA",
    "SUZB5.SA": "SUZB3.SA",

    # Vale preferred shares discontinued
    "VALE5.SA": "VALE3.SA",

    # TIM Participações renamed to TIM S.A.
    "TIMP3.SA": "TIMS3.SA",

    # Telefônica/Vivo share class consolidation
    "VIVT4.SA": "VIVT3.SA",
    "TLPP4.SA": "VIVT3.SA",

    # Klabin PN → units
    "KLBN4.SA": "KLBN11.SA",

    # Cogna (ex-Kroton)
    "KROT3.SA": "COGN3.SA",

    # Via Varejo → Casas Bahia → BHIA3
    "VVAR3.SA":  "BHIA3.SA",
    "VVAR11.SA": "BHIA3.SA",
    "VIIA3.SA":  "BHIA3.SA",

    # Hering renamed to CTC
    "HGTX3.SA": "CTCA3.SA",

    # Estácio → Yduqs
    "ESTC3.SA": "YDUQ3.SA",

    # Iguatemi → IGTI11
    "IGTA3.SA": "IGTI11.SA",

    # Duratex renamed to Dexco
    "DTEX3.SA": "DXCO3.SA",

    # GNDI merged into Hapvida
    "GNDI3.SA": "HAPV3.SA",

    # EDP Energias do Brasil (old ticker ELPL4)
    "ELPL4.SA": "ENBR3.SA",

    # Pão de Açúcar PN → ON
    "PCAR4.SA": "PCAR3.SA",

    # LC AM absorbed by Localiza (RENT3)
    "LCAM3.SA": "RENT3.SA",

    # Eneva (old ticker ENAT3)
    "ENAT3.SA": "ENEV3.SA",

    # Copel PNB converted to ON in 2023
    "CPLE6.SA": "CPLE3.SA",

    # ── 2024 renames ─────────────────────────────────────────────────────────

    # ISA CTEEP (nov/2024): TRPL3/TRPL4 → ISAE3/ISAE4
    "TRPL3.SA": "ISAE3.SA",
    "TRPL4.SA": "ISAE4.SA",

    # Azzas (2024): fusão Arezzo + Soma → AZZA3
    "SOMA3.SA": "AZZA3.SA",
    "ARZZ3.SA": "AZZA3.SA",

    # ── 2025 renames ─────────────────────────────────────────────────────────

    # Eletrobras → Axia Energia (nov/2025)
    # Yahoo Finance serve AXIA3/6 apenas desde ~2025-11 (rename date).
    # MA200 ficará NaN até ~2026-08.
    "ELET3.SA": "AXIA3.SA",
    "ELET5.SA": "AXIA5.SA",
    "ELET6.SA": "AXIA6.SA",

    # Embraer (nov/2025): EMBR3 → EMBJ3
    # Yahoo Finance serve EMBJ3 apenas desde ~2025-10-27.
    # MA200 ficará NaN até ~2026-06.
    "EMBR3.SA": "EMBJ3.SA",

    # CCR → Motiva (abr/2025): CCRO3 → MOTV3
    # Yahoo Finance serve MOTV3 apenas desde ~2025-04-23.
    "CCRO3.SA": "MOTV3.SA",

    # Natura (jul/2025): NTCO3 → NATU3 (voltou ao ticker original)
    "NTCO3.SA": "NATU3.SA",

    # BRF + Marfrig → MBRF3 (set/2025)
    "BRFS3.SA": "MBRF3.SA",
    "MRFG3.SA": "MBRF3.SA",

    # ── Delistings — sem sucessor na B3 ──────────────────────────────────────

    # Lojas Americanas: LAME→AMER3 (2022 reestruturação) → colapso 2023
    "LAME3.SA": None,
    "LAME4.SA": None,
    "BTOW3.SA": None,
    "AMER3.SA": None,

    # JBS (2025): JBSS3 migrou para NYSE via BDR JBSS32 — não serve no yfinance .SA
    "JBSS3.SA": None,

    # Gol: recuperação judicial (jun/2025), ação vale frações de centavo
    "GOLL4.SA": None,

    # Azul: recuperação judicial (dez/2025)
    "AZUL4.SA": None,

    # Smiles: incorporada pela Gol (ela mesma em recuperação judicial)
    "SMLE3.SA": None,

    # Oi: em recuperação judicial, delisted
    "OIBR3.SA": None,
    "OIBR4.SA": None,

    # OGX: falência
    "OGXP3.SA": None,

    # PDG Realty: falência
    "PDGR3.SA": None,

    # Brookfield Incorporações
    "BISA3.SA": None,

    # Rossi Residencial: falência
    "RSID3.SA": None,

    # LLX Logística: falência
    "LLXL3.SA": None,

    # Souza Cruz: acquired by BAT, delisted 2016
    "CRUZ3.SA": None,

    # Cyrela/Helbor spin-off vehicle
    "CZRS4.SA": None,

    # Paranapanema: delisted
    "PMAM3.SA": None,

    # CESP: acquired by Equatorial, delisted 2021
    "CESP6.SA": None,

    # Getnet: acquired by Santander, moved to NYSE, delisted B3 2022
    "GETT11.SA": None,

    # Carrefour Brasil: parent buyout, delisted 2024
    "CRFB3.SA": None,

    # SulAmérica: acquired by Rede D'Or (RDOR3), delisted 2022
    "SULA11.SA": None,

    # Banco Inter ON/PN: converted to NYSE BDR (INTR34), delisted B3 2022
    "BIDI3.SA": None,
    "BIDI4.SA": None,
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
