"""
Bloomberg Data Refresh Script
Fetches historical data from Bloomberg Terminal via blpapi and writes JSON for the dashboard.

Uses parallel workers with multi-security batch requests for fast fetching.

Usage:
    python refresh_data.py

Requirements:
    - Bloomberg Terminal running on this machine
    - blpapi Python package installed (pip install blpapi)
"""

import blpapi
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
JSON_PATH = os.path.join(DATA_DIR, "bloomberg.json")
JS_PATH = os.path.join(DATA_DIR, "bloomberg_data.js")

START_DATE = "20200101"
NUM_WORKERS = 4  # parallel Bloomberg sessions

SERIES = [
    # Economic Indicators (20 series)
    {"key": "nfp",              "ticker": "NFP TCH Index",      "field": "PX_LAST", "label": "Nonfarm Payrolls"},
    {"key": "core_cpi",         "ticker": "CPI XYOY Index",     "field": "PX_LAST", "label": "Core CPI Y/Y"},
    {"key": "core_pce",         "ticker": "PCE CYOY Index",     "field": "PX_LAST", "label": "Core PCE Y/Y"},
    {"key": "gdp",              "ticker": "GDP CYOY Index",     "field": "PX_LAST", "label": "GDP Growth Y/Y"},
    {"key": "ism_mfg",          "ticker": "NAPMPMI Index",      "field": "PX_LAST", "label": "ISM Manufacturing PMI"},
    {"key": "ism_svc",          "ticker": "NAPMNMI Index",      "field": "PX_LAST", "label": "ISM Services PMI"},
    {"key": "retail_sales",     "ticker": "RSTAYOY Index",      "field": "PX_LAST", "label": "Retail Sales Y/Y"},
    {"key": "umich",            "ticker": "CONSSENT Index",     "field": "PX_LAST", "label": "UMich Consumer Sentiment"},
    {"key": "core_ppi",         "ticker": "PPI XYOY Index",     "field": "PX_LAST", "label": "Core PPI Y/Y"},
    {"key": "adp",              "ticker": "ADP CHNG Index",     "field": "PX_LAST", "label": "ADP Employment Change"},
    {"key": "claims",           "ticker": "INJCJC4 Index",      "field": "PX_LAST", "label": "Initial Jobless Claims 4W MA"},
    {"key": "durable_goods",    "ticker": "DGNOCHNG Index",     "field": "PX_LAST", "label": "Durable Goods Orders"},
    {"key": "housing_starts",   "ticker": "NHSPSTOT Index",     "field": "PX_LAST", "label": "Housing Starts"},
    {"key": "existing_homes",   "ticker": "ETSLTOTL Index",     "field": "PX_LAST", "label": "Existing Home Sales"},
    {"key": "industrial_prod",  "ticker": "IP CHNG Index",      "field": "PX_LAST", "label": "Industrial Production"},
    {"key": "personal_income",  "ticker": "PITLCHNG Index",     "field": "PX_LAST", "label": "Personal Income & Outlays"},
    {"key": "eci",              "ticker": "ECI SA% Index",      "field": "PX_LAST", "label": "Employment Cost Index Y/Y"},
    {"key": "consumer_conf",    "ticker": "CONCCONF Index",     "field": "PX_LAST", "label": "Conference Board Consumer Confidence"},
    {"key": "trade_balance",    "ticker": "USTBTOT Index",      "field": "PX_LAST", "label": "Trade Balance"},
    {"key": "nfib",             "ticker": "SBOITOTL Index",     "field": "PX_LAST", "label": "NFIB Small Business Index"},
    {"key": "vix",              "ticker": "VIX Index",          "field": "PX_LAST", "label": "VIX"},
    {"key": "gdpnow",           "ticker": "GDGCAFJP Index",    "field": "PX_LAST", "label": "Atlanta Fed GDPNow"},
    {"key": "unrate",            "ticker": "USURTOT Index",     "field": "PX_LAST", "label": "Unemployment Rate"},
    {"key": "cfnai",             "ticker": "CFNAI Index",       "field": "PX_LAST", "label": "Chicago Fed National Activity Index"},
    {"key": "nfci",              "ticker": "NFCIINDX Index",    "field": "PX_LAST", "label": "Chicago Fed Financial Conditions"},
    {"key": "gsfci",             "ticker": "GSUSFCI Index",     "field": "PX_LAST", "label": "GS Financial Conditions Index"},
    {"key": "bfcius",            "ticker": "BFCIUS Index",      "field": "PX_LAST", "label": "Bloomberg Financial Conditions"},
    # Equities
    {"key": "spx",          "ticker": "SPX Index",       "field": "PX_LAST", "label": "S&P 500"},
    {"key": "ndx",          "ticker": "NDX Index",       "field": "PX_LAST", "label": "Nasdaq 100"},
    {"key": "djia",         "ticker": "INDU Index",      "field": "PX_LAST", "label": "Dow Jones Industrial Average"},
    {"key": "tsx",          "ticker": "SPTSX Index",     "field": "PX_LAST", "label": "S&P/TSX Composite"},
    {"key": "eurostoxx",    "ticker": "SX5E Index",      "field": "PX_LAST", "label": "Euro Stoxx 50"},
    {"key": "dax",          "ticker": "DAX Index",       "field": "PX_LAST", "label": "DAX 40"},
    {"key": "ftse",         "ticker": "UKX Index",       "field": "PX_LAST", "label": "FTSE 100"},
    {"key": "cac",          "ticker": "CAC Index",       "field": "PX_LAST", "label": "CAC 40"},
    {"key": "ibex",         "ticker": "IBEX Index",      "field": "PX_LAST", "label": "IBEX 35"},
    {"key": "ftsemib",      "ticker": "FTSEMIB Index",   "field": "PX_LAST", "label": "FTSE MIB"},
    {"key": "nikkei",       "ticker": "NKY Index",       "field": "PX_LAST", "label": "Nikkei 225"},
    {"key": "kospi",        "ticker": "KOSPI Index",     "field": "PX_LAST", "label": "KOSPI"},
    {"key": "taiex",        "ticker": "TWSE Index",      "field": "PX_LAST", "label": "TAIEX"},
    {"key": "csi300",       "ticker": "SHSZ300 Index",   "field": "PX_LAST", "label": "CSI 300"},
    {"key": "hsi",          "ticker": "HSI Index",       "field": "PX_LAST", "label": "Hang Seng"},
    {"key": "asx200",       "ticker": "AS51 Index",      "field": "PX_LAST", "label": "ASX 200"},
    {"key": "ibov",         "ticker": "IBOV Index",      "field": "PX_LAST", "label": "IBOVESPA"},
    {"key": "mexbol",       "ticker": "MEXBOL Index",    "field": "PX_LAST", "label": "Mexico IPC"},
    {"key": "acwi",         "ticker": "MXWD Index",      "field": "PX_LAST", "label": "MSCI ACWI"},
    {"key": "em",           "ticker": "MXEF Index",      "field": "PX_LAST", "label": "MSCI Emerging Markets"},
    # Equity Breadth
    {"key": "nyad",         "ticker": "NYAD Index",      "field": "PX_LAST", "label": "NYSE Advance/Decline Line"},
    {"key": "spx_above200", "ticker": "TRADPAUS Index",   "field": "PX_LAST", "label": "S&P 500 % Above 200 DMA"},
    {"key": "highs_lows",   "ticker": "TRADHILO Index",  "field": "PX_LAST", "label": "NYSE New 52W Highs - Lows"},
    # Fixed Income (Fed Funds forward curve fetched separately via BDP)
    {"key": "ust_10y",      "ticker": "USGG10YR Index",  "field": "PX_LAST", "label": "US 10Y Treasury Yield"},
    {"key": "ukg_10y",      "ticker": "GUKG10 Index",    "field": "PX_LAST", "label": "UK 10Y Gilt Yield"},
    {"key": "ita_10y",      "ticker": "GBTPGR10 Index",  "field": "PX_LAST", "label": "Italy 10Y BTP Yield"},
    {"key": "ger_10y",      "ticker": "GDBR10 Index",    "field": "PX_LAST", "label": "Germany 10Y Bund Yield"},
    {"key": "jpn_10y",      "ticker": "GJGB10 Index",    "field": "PX_LAST", "label": "Japan 10Y JGB Yield"},
    {"key": "spread_2s10s", "ticker": "USYC2Y10 Index",  "field": "PX_LAST", "label": "2s10s Treasury Yield Spread"},
    {"key": "real_5y",      "ticker": "USGGT05Y Index",  "field": "PX_LAST", "label": "US 5Y Real Yield (TIPS)"},
    {"key": "real_10y",     "ticker": "USGGT10Y Index",  "field": "PX_LAST", "label": "US 10Y Real Yield (TIPS)"},
    {"key": "breakeven_5y", "ticker": "USGGBE05 Index",  "field": "PX_LAST", "label": "5Y Inflation Breakeven"},
    {"key": "move",         "ticker": "MOVE Index",      "field": "PX_LAST", "label": "MOVE Index"},
    {"key": "ig_oas",       "ticker": "LF98OAS Index",   "field": "PX_LAST", "label": "IG Credit Spreads (OAS)"},
    {"key": "hy_oas",       "ticker": "LP01OAS Index",   "field": "PX_LAST", "label": "HY Credit Spreads (OAS)"},
    {"key": "muni",         "ticker": "NMCMFUS Index",   "field": "PX_LAST", "label": "Muni Bond Fund Flows"},
    {"key": "agg",          "ticker": "LUACTRUU Index",  "field": "PX_LAST", "label": "Bloomberg US Aggregate Bond"},
    {"key": "hy_ytw",       "ticker": "LF98YW Index",    "field": "PX_LAST", "label": "US HY Yield to Worst"},
    {"key": "cdx_ig",       "ticker": "CDX IG CDSI GEN 5Y Corp",  "field": "PX_LAST", "label": "CDX IG 5Y CDS"},
    {"key": "cdx_hy",       "ticker": "CDX HY CDSI GEN 5Y Corp",  "field": "PX_LAST", "label": "CDX HY 5Y CDS"},
    {"key": "itrx_eur",     "ticker": "ITRX EUR CDSI GEN 5Y Corp","field": "PX_LAST", "label": "iTraxx Europe 5Y CDS"},
    # Commodities — Energy
    {"key": "wti",          "ticker": "CL1 Comdty",      "field": "PX_LAST", "label": "WTI Crude Oil"},
    {"key": "brent",        "ticker": "CO1 Comdty",      "field": "PX_LAST", "label": "Brent Crude Oil"},
    {"key": "natgas",       "ticker": "NG1 Comdty",      "field": "PX_LAST", "label": "Henry Hub Natural Gas"},
    {"key": "rbob",         "ticker": "XB1 Comdty",      "field": "PX_LAST", "label": "RBOB Gasoline"},
    {"key": "heating_oil",  "ticker": "HO1 Comdty",      "field": "PX_LAST", "label": "Heating Oil"},
    {"key": "lng_eu",       "ticker": "TTFG1MON Index",  "field": "PX_LAST", "label": "LNG Europe (TTF)"},
    {"key": "lng_asia",     "ticker": "JGLA Comdty",     "field": "PX_LAST", "label": "LNG Asia (JKM)"},
    # Commodities — Metals
    {"key": "copper",       "ticker": "HG1 Comdty",      "field": "PX_LAST", "label": "Copper"},
    {"key": "aluminum",     "ticker": "LA1 Comdty",      "field": "PX_LAST", "label": "Aluminum"},
    {"key": "steel",        "ticker": "HRCA Comdty",      "field": "PX_LAST", "label": "Steel (HRC)"},
    {"key": "nickel",       "ticker": "LN1 Comdty",      "field": "PX_LAST", "label": "Nickel"},
    {"key": "iron_ore",     "ticker": "SCO1 Comdty",     "field": "PX_LAST", "label": "Iron Ore"},
    {"key": "gold",         "ticker": "GC1 Comdty",      "field": "PX_LAST", "label": "Gold"},
    {"key": "silver",       "ticker": "SI1 Comdty",      "field": "PX_LAST", "label": "Silver"},
    # Commodities — Agriculture
    {"key": "corn",         "ticker": "C 1 Comdty",      "field": "PX_LAST", "label": "Corn"},
    {"key": "wheat",        "ticker": "W 1 Comdty",      "field": "PX_LAST", "label": "Wheat"},
    {"key": "soybeans",     "ticker": "S 1 Comdty",      "field": "PX_LAST", "label": "Soybeans"},
    {"key": "coffee",       "ticker": "KC1 Comdty",      "field": "PX_LAST", "label": "Coffee"},
    {"key": "cotton",       "ticker": "CT1 Comdty",      "field": "PX_LAST", "label": "Cotton"},
    {"key": "sugar",        "ticker": "SB1 Comdty",      "field": "PX_LAST", "label": "Sugar"},
    {"key": "cocoa",        "ticker": "CC1 Comdty",      "field": "PX_LAST", "label": "Cocoa"},
    # Currencies
    {"key": "dxy",          "ticker": "DXY Curncy",      "field": "PX_LAST", "label": "US Dollar Index (DXY)"},
    {"key": "eurusd",       "ticker": "EURUSD Curncy",   "field": "PX_LAST", "label": "EUR / USD"},
    {"key": "gbpusd",       "ticker": "GBPUSD Curncy",   "field": "PX_LAST", "label": "GBP / USD"},
    {"key": "usdjpy",       "ticker": "USDJPY Curncy",   "field": "PX_LAST", "label": "USD / JPY"},
    {"key": "usdchf",       "ticker": "USDCHF Curncy",   "field": "PX_LAST", "label": "USD / CHF"},
    {"key": "audusd",       "ticker": "AUDUSD Curncy",   "field": "PX_LAST", "label": "AUD / USD"},
    {"key": "usdcad",       "ticker": "USDCAD Curncy",   "field": "PX_LAST", "label": "USD / CAD"},
    {"key": "nzdusd",       "ticker": "NZDUSD Curncy",   "field": "PX_LAST", "label": "NZD / USD"},
    {"key": "btcusd",       "ticker": "XBTUSD Curncy",   "field": "PX_LAST", "label": "Bitcoin / USD"},
]

# Build a lookup: ticker -> series dict
TICKER_TO_SERIES = {s["ticker"]: s for s in SERIES}


def create_session():
    """Open a new blpapi session to the local Bloomberg Terminal."""
    options = blpapi.SessionOptions()
    options.setServerHost("localhost")
    options.setServerPort(8194)

    session = blpapi.Session(options)
    if not session.start():
        raise ConnectionError(
            "Failed to start Bloomberg session. Is Bloomberg Terminal running?"
        )
    if not session.openService("//blp/refdata"):
        session.stop()
        raise ConnectionError("Failed to open //blp/refdata service.")

    return session


def fetch_batch(batch):
    """
    Fetch a batch of series using a single Bloomberg session and a single
    multi-security HistoricalDataRequest. Returns dict of {key: {label, ticker, dates, values}}
    and a list of error strings.

    Bloomberg returns one securityData element per PARTIAL_RESPONSE/RESPONSE message
    for multi-security requests, so we collect them as they arrive.
    """
    session = create_session()
    results = {}
    errors = []

    try:
        service = session.getService("//blp/refdata")
        request = service.createRequest("HistoricalDataRequest")

        # Add all tickers in this batch to one request
        for s in batch:
            request.append("securities", s["ticker"])

        request.append("fields", "PX_LAST")
        request.set("startDate", START_DATE)
        request.set("endDate", date.today().strftime("%Y%m%d"))
        request.set("periodicitySelection", "DAILY")

        session.sendRequest(request)

        # Collect responses — Bloomberg sends one securityData per message
        while True:
            event = session.nextEvent(15000)

            for msg in event:
                if not msg.hasElement("securityData"):
                    continue

                sec_data = msg.getElement("securityData")
                ticker = sec_data.getElementAsString("security")
                series_def = TICKER_TO_SERIES.get(ticker)

                if not series_def:
                    continue

                # Check for security-level errors
                if sec_data.hasElement("securityError"):
                    err_elem = sec_data.getElement("securityError")
                    err_msg = err_elem.getElementAsString("message")
                    errors.append(f"{ticker}: {err_msg}")
                    print(f"    ERROR {ticker}: {err_msg}")
                    continue

                dates = []
                values = []

                if sec_data.hasElement("fieldData"):
                    field_data = sec_data.getElement("fieldData")
                    for i in range(field_data.numValues()):
                        point = field_data.getValueAsElement(i)
                        d = point.getElementAsDatetime("date")
                        v = point.getElementAsFloat("PX_LAST")
                        if hasattr(d, "strftime"):
                            dates.append(d.strftime("%Y-%m-%d"))
                        else:
                            dates.append(str(d))
                        values.append(round(v, 4))

                results[series_def["key"]] = {
                    "label": series_def["label"],
                    "ticker": ticker,
                    "dates": dates,
                    "values": values,
                }
                print(f"    {ticker:<25s} -> {len(dates)} pts")

            if event.eventType() == blpapi.Event.RESPONSE:
                break

    except Exception as e:
        # If the whole batch fails, record errors for all tickers
        for s in batch:
            if s["key"] not in results:
                errors.append(f"{s['ticker']}: {e}")
    finally:
        session.stop()

    return results, errors


def fetch_ff_forward_curve():
    """
    Fetch Fed Funds forward curve: current prices for FF1–FF24 Comdty
    via BDP (ReferenceDataRequest), plus LAST_TRADEABLE_DT to get contract dates.
    Returns list of {date, rate} dicts sorted by date.
    """
    session = create_session()
    curve = []

    try:
        service = session.getService("//blp/refdata")

        tickers = [f"FF{i} Comdty" for i in range(1, 25)]

        req = service.createRequest("ReferenceDataRequest")
        for t in tickers:
            req.append("securities", t)
        req.append("fields", "PX_LAST")
        req.append("fields", "LAST_TRADEABLE_DT")
        session.sendRequest(req)

        while True:
            event = session.nextEvent(15000)
            for msg in event:
                if not msg.hasElement("securityData"):
                    continue
                sec_arr = msg.getElement("securityData")
                for i in range(sec_arr.numValues()):
                    sec = sec_arr.getValueAsElement(i)
                    if sec.hasElement("securityError"):
                        continue
                    fd = sec.getElement("fieldData")
                    try:
                        px = fd.getElementAsFloat("PX_LAST")
                        dt = fd.getElementAsDatetime("LAST_TRADEABLE_DT")
                        if hasattr(dt, "strftime"):
                            date_str = dt.strftime("%Y-%m-%d")
                        else:
                            date_str = str(dt)
                        curve.append({
                            "date": date_str,
                            "rate": round(100 - px, 4),
                        })
                    except Exception:
                        continue
            if event.eventType() == blpapi.Event.RESPONSE:
                break

    except Exception as e:
        print(f"    FF CURVE ERROR: {e}")
    finally:
        session.stop()

    curve.sort(key=lambda x: x["date"])
    print(f"    -> {len(curve)} contract months")
    return curve


UST_YIELD_CURVE_TICKERS = [
    {"tenor": "1M",  "ticker": "USGG1M Index"},
    {"tenor": "3M",  "ticker": "USGG3M Index"},
    {"tenor": "6M",  "ticker": "USGG6M Index"},
    {"tenor": "1Y",  "ticker": "USGG12M Index"},
    {"tenor": "2Y",  "ticker": "USGG2YR Index"},
    {"tenor": "3Y",  "ticker": "USGG3YR Index"},
    {"tenor": "5Y",  "ticker": "USGG5YR Index"},
    {"tenor": "7Y",  "ticker": "USGG7YR Index"},
    {"tenor": "10Y", "ticker": "USGG10YR Index"},
    {"tenor": "20Y", "ticker": "USGG20YR Index"},
    {"tenor": "30Y", "ticker": "USGG30YR Index"},
]


def fetch_ust_yield_curve():
    """
    Fetch current US Treasury yield curve via BDP for standard tenors.
    Returns list of {tenor, yield} dicts.
    """
    session = create_session()
    by_ticker = {}

    try:
        service = session.getService("//blp/refdata")
        req = service.createRequest("ReferenceDataRequest")
        for t in UST_YIELD_CURVE_TICKERS:
            req.append("securities", t["ticker"])
        req.append("fields", "PX_LAST")
        session.sendRequest(req)

        while True:
            event = session.nextEvent(15000)
            for msg in event:
                if not msg.hasElement("securityData"):
                    continue
                sec_arr = msg.getElement("securityData")
                for i in range(sec_arr.numValues()):
                    sec = sec_arr.getValueAsElement(i)
                    ticker = sec.getElementAsString("security")
                    if sec.hasElement("securityError"):
                        continue
                    fd = sec.getElement("fieldData")
                    try:
                        yld = fd.getElementAsFloat("PX_LAST")
                        by_ticker[ticker] = round(yld, 4)
                    except Exception:
                        continue
            if event.eventType() == blpapi.Event.RESPONSE:
                break

    except Exception as e:
        print(f"    UST YIELD CURVE ERROR: {e}")
    finally:
        session.stop()

    # Build result in tenor order
    result = []
    for t in UST_YIELD_CURVE_TICKERS:
        if t["ticker"] in by_ticker:
            result.append({"tenor": t["tenor"], "yield": by_ticker[t["ticker"]]})
    print(f"    -> {len(result)} tenors")
    return result


def fetch_spx_heatmap():
    """
    Fetch S&P 500 heatmap data from Bloomberg:
    1. BDS on SPX Index -> INDX_MEMBERS to get member tickers
    2. BDP on all members -> GICS_SECTOR_NAME, CHG_PCT_1D, CUR_MKT_CAP, SHORT_NAME

    Returns a list of dicts: [{ticker, name, sector, mkt_cap, chg_pct}, ...]
    """
    session = create_session()
    members = []
    errors = []

    try:
        service = session.getService("//blp/refdata")

        # ── Step 1: Get SPX member tickers ──────────────────────────────
        print("  Fetching SPX Index members...")
        req = service.createRequest("ReferenceDataRequest")
        req.append("securities", "SPX Index")
        req.append("fields", "INDX_MEMBERS")
        session.sendRequest(req)

        member_tickers = []
        while True:
            event = session.nextEvent(15000)
            for msg in event:
                if not msg.hasElement("securityData"):
                    continue
                sec_arr = msg.getElement("securityData")
                for i in range(sec_arr.numValues()):
                    sec = sec_arr.getValueAsElement(i)
                    if sec.hasElement("fieldData"):
                        fd = sec.getElement("fieldData")
                        if fd.hasElement("INDX_MEMBERS"):
                            bulk = fd.getElement("INDX_MEMBERS")
                            for j in range(bulk.numValues()):
                                row = bulk.getValueAsElement(j)
                                ticker = row.getElementAsString("Member Ticker and Exchange Code")
                                member_tickers.append(ticker.strip() + " Equity")
            if event.eventType() == blpapi.Event.RESPONSE:
                break

        print(f"    -> {len(member_tickers)} members")

        if not member_tickers:
            raise ValueError("No SPX members returned")

        # ── Step 2: Get ref data for all members (batched) ──────────────
        # Bloomberg ReferenceDataRequest supports many securities at once.
        # We batch in groups of ~100 to avoid overly large requests.
        BATCH_SIZE = 100
        ref_fields = ["SHORT_NAME", "GICS_SECTOR_NAME", "CUR_MKT_CAP", "CHG_PCT_1D"]

        print(f"  Fetching ref data for {len(member_tickers)} members...")

        for batch_start in range(0, len(member_tickers), BATCH_SIZE):
            batch_tickers = member_tickers[batch_start : batch_start + BATCH_SIZE]

            req = service.createRequest("ReferenceDataRequest")
            for t in batch_tickers:
                req.append("securities", t)
            for f in ref_fields:
                req.append("fields", f)
            session.sendRequest(req)

            while True:
                event = session.nextEvent(15000)
                for msg in event:
                    if not msg.hasElement("securityData"):
                        continue
                    sec_arr = msg.getElement("securityData")
                    for i in range(sec_arr.numValues()):
                        sec = sec_arr.getValueAsElement(i)
                        ticker = sec.getElementAsString("security")

                        if sec.hasElement("securityError"):
                            errors.append(ticker)
                            continue

                        fd = sec.getElement("fieldData")

                        def safe_str(elem, field):
                            try:
                                return elem.getElementAsString(field)
                            except Exception:
                                return ""

                        def safe_float(elem, field):
                            try:
                                return elem.getElementAsFloat(field)
                            except Exception:
                                return 0.0

                        name = safe_str(fd, "SHORT_NAME")
                        sector = safe_str(fd, "GICS_SECTOR_NAME")
                        mkt_cap = safe_float(fd, "CUR_MKT_CAP")
                        chg_pct = safe_float(fd, "CHG_PCT_1D")

                        if sector and mkt_cap > 0:
                            members.append({
                                "ticker": ticker.replace(" Equity", ""),
                                "name": name or ticker.replace(" Equity", ""),
                                "sector": sector,
                                "mkt_cap": round(mkt_cap, 2),
                                "chg_pct": round(chg_pct, 4),
                            })

                if event.eventType() == blpapi.Event.RESPONSE:
                    break

        print(f"    -> {len(members)} members with valid data")

    except Exception as e:
        print(f"    HEATMAP ERROR: {e}")
    finally:
        session.stop()

    # Sort by GICS Sector Name, then by market cap descending within sector
    members.sort(key=lambda m: (m["sector"], -m["mkt_cap"]))
    return members


COMMODITY_CURVES = [
    {"key": "wti_curve",   "prefix": "CL",  "suffix": "Comdty", "months": 24, "label": "WTI Crude Oil Forward Curve"},
    {"key": "brent_curve", "prefix": "CO",   "suffix": "Comdty", "months": 24, "label": "Brent Crude Oil Forward Curve"},
    {"key": "natgas_curve","prefix": "NG",   "suffix": "Comdty", "months": 24, "label": "Henry Hub Natural Gas Forward Curve"},
]


def fetch_commodity_forward_curves():
    """
    Fetch forward curves for WTI, Brent, and Henry Hub.
    Uses generic commodity tickers CL1-CL24, CO1-CO24, NG1-NG24 Comdty.
    Returns dict of {key: [{date, price}, ...]} sorted by date.
    """
    session = create_session()
    results = {}

    try:
        service = session.getService("//blp/refdata")

        for curve_def in COMMODITY_CURVES:
            tickers = [
                f"{curve_def['prefix']}{i} {curve_def['suffix']}"
                for i in range(1, curve_def["months"] + 1)
            ]

            req = service.createRequest("ReferenceDataRequest")
            for t in tickers:
                req.append("securities", t)
            req.append("fields", "PX_LAST")
            req.append("fields", "LAST_TRADEABLE_DT")
            session.sendRequest(req)

            points = []
            while True:
                event = session.nextEvent(15000)
                for msg in event:
                    if not msg.hasElement("securityData"):
                        continue
                    sec_arr = msg.getElement("securityData")
                    for i in range(sec_arr.numValues()):
                        sec = sec_arr.getValueAsElement(i)
                        if sec.hasElement("securityError"):
                            continue
                        fd = sec.getElement("fieldData")
                        try:
                            px = fd.getElementAsFloat("PX_LAST")
                            dt = fd.getElementAsDatetime("LAST_TRADEABLE_DT")
                            if hasattr(dt, "strftime"):
                                date_str = dt.strftime("%Y-%m-%d")
                            else:
                                date_str = str(dt)
                            points.append({"date": date_str, "price": round(px, 4)})
                        except Exception:
                            continue
                if event.eventType() == blpapi.Event.RESPONSE:
                    break

            points.sort(key=lambda x: x["date"])
            results[curve_def["key"]] = points
            print(f"    {curve_def['prefix']:<5s} -> {len(points)} contract months")

    except Exception as e:
        print(f"    COMMODITY CURVES ERROR: {e}")
    finally:
        session.stop()

    return results


EQUITY_INDICES = [
    {"key": "spx",       "ticker": "SPX Index",       "label": "S&P 500"},
    {"key": "ndx",       "ticker": "NDX Index",       "label": "Nasdaq 100"},
    {"key": "tsx",       "ticker": "SPTSX Index",      "label": "S&P/TSX"},
    {"key": "mexbol",    "ticker": "MEXBOL Index",     "label": "BMV IPC"},
    {"key": "ibov",      "ticker": "IBOV Index",       "label": "IBOVESPA"},
    {"key": "eurostoxx", "ticker": "SX5E Index",       "label": "Euro Stoxx 50"},
    {"key": "ftse",      "ticker": "UKX Index",        "label": "FTSE 100"},
    {"key": "cac",       "ticker": "CAC Index",        "label": "CAC 40"},
    {"key": "dax",       "ticker": "DAX Index",        "label": "DAX 40"},
    {"key": "ibex",      "ticker": "IBEX Index",       "label": "IBEX 35"},
    {"key": "ftsemib",   "ticker": "FTSEMIB Index",    "label": "FTSE MIB"},
    {"key": "nikkei",    "ticker": "NKY Index",        "label": "Nikkei 225"},
    {"key": "hsi",       "ticker": "HSI Index",        "label": "Hang Seng"},
    {"key": "csi300",    "ticker": "SHSZ300 Index",    "label": "CSI 300"},
    {"key": "asx200",    "ticker": "AS51 Index",       "label": "ASX 200"},
    {"key": "djia",      "ticker": "INDU Index",      "label": "DJIA"},
    {"key": "kospi",     "ticker": "KOSPI Index",      "label": "KOSPI"},
    {"key": "taiex",     "ticker": "TWSE Index",       "label": "TAIEX"},
    {"key": "acwi",      "ticker": "MXWD Index",       "label": "MSCI ACWI"},
    {"key": "em",        "ticker": "MXEF Index",       "label": "MSCI EM"},
]

EQUITY_TICKER_TO_DEF = {e["ticker"]: e for e in EQUITY_INDICES}


def fetch_equity_valuations():
    """
    Fetch valuation data for equity indices via BDP:
    PE_RATIO, BEST_PE_RATIO (FY1), IDX_EST_DVD_YLD, CHG_PCT_YTD
    Then a second request for FY2 P/E using BEST_PE_RATIO with BEST_FPERIOD_OVERRIDE=2FY.
    Returns list of dicts in EQUITY_INDICES order.
    """
    session = create_session()
    # Collect results keyed by ticker for merging
    by_ticker = {}

    def safe_float(elem, field):
        try:
            return round(elem.getElementAsFloat(field), 2)
        except Exception:
            return None

    try:
        service = session.getService("//blp/refdata")

        # ── Request 1: standard fields (PE, FY1 PE, div yield, YTD) ──
        req1 = service.createRequest("ReferenceDataRequest")
        for e in EQUITY_INDICES:
            req1.append("securities", e["ticker"])
        for f in ["PE_RATIO", "BEST_PE_RATIO", "IDX_EST_DVD_YLD", "CHG_PCT_YTD"]:
            req1.append("fields", f)
        session.sendRequest(req1)

        while True:
            event = session.nextEvent(15000)
            for msg in event:
                if not msg.hasElement("securityData"):
                    continue
                sec_arr = msg.getElement("securityData")
                for i in range(sec_arr.numValues()):
                    sec = sec_arr.getValueAsElement(i)
                    ticker = sec.getElementAsString("security")
                    edef = EQUITY_TICKER_TO_DEF.get(ticker)
                    if not edef or sec.hasElement("securityError"):
                        continue
                    fd = sec.getElement("fieldData")
                    by_ticker[ticker] = {
                        "key": edef["key"],
                        "label": edef["label"],
                        "pe": safe_float(fd, "PE_RATIO"),
                        "pe_fy1": safe_float(fd, "BEST_PE_RATIO"),
                        "pe_fy2": None,
                        "div_yield": safe_float(fd, "IDX_EST_DVD_YLD"),
                        "ytd": safe_float(fd, "CHG_PCT_YTD"),
                    }
                    print(f"    {ticker:<25s} -> valuations OK")
            if event.eventType() == blpapi.Event.RESPONSE:
                break

        # ── Request 2: FY2 P/E via BEST_PE_RATIO with 2FY override ──
        req2 = service.createRequest("ReferenceDataRequest")
        for e in EQUITY_INDICES:
            req2.append("securities", e["ticker"])
        req2.append("fields", "BEST_PE_RATIO")
        overrides = req2.getElement("overrides")
        ovrd = overrides.appendElement()
        ovrd.setElement("fieldId", "BEST_FPERIOD_OVERRIDE")
        ovrd.setElement("value", "2FY")
        session.sendRequest(req2)

        while True:
            event = session.nextEvent(15000)
            for msg in event:
                if not msg.hasElement("securityData"):
                    continue
                sec_arr = msg.getElement("securityData")
                for i in range(sec_arr.numValues()):
                    sec = sec_arr.getValueAsElement(i)
                    ticker = sec.getElementAsString("security")
                    if ticker not in by_ticker or sec.hasElement("securityError"):
                        continue
                    fd = sec.getElement("fieldData")
                    by_ticker[ticker]["pe_fy2"] = safe_float(fd, "BEST_PE_RATIO")
                    print(f"    {ticker:<25s} -> FY2 PE OK")
            if event.eventType() == blpapi.Event.RESPONSE:
                break

    except Exception as e:
        print(f"    EQUITY VALUATIONS ERROR: {e}")
    finally:
        session.stop()

    # Return in EQUITY_INDICES order
    results = []
    for e in EQUITY_INDICES:
        if e["ticker"] in by_ticker:
            results.append(by_ticker[e["ticker"]])
    return results


def split_batches(items, n_batches):
    """Split items into n roughly equal batches."""
    k, m = divmod(len(items), n_batches)
    batches = []
    idx = 0
    for i in range(n_batches):
        size = k + (1 if i < m else 0)
        if size > 0:
            batches.append(items[idx : idx + size])
        idx += size
    return batches


def main():
    t0 = time.time()
    n_workers = min(NUM_WORKERS, len(SERIES))
    batches = split_batches(SERIES, n_workers)

    print(f"Connecting to Bloomberg Terminal ({n_workers} parallel sessions)...")
    print(f"Fetching {len(SERIES)} series in {len(batches)} batches...\n")

    output = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "series": {},
        "heatmap": [],
        "ff_curve": [],
        "equity_valuations": [],
        "commodity_curves": {},
        "ust_yield_curve": [],
    }
    all_errors = []

    # Launch parallel workers — each opens its own session and sends one
    # multi-security request for its batch.
    with ThreadPoolExecutor(max_workers=n_workers + 5) as executor:
        heatmap_future = executor.submit(fetch_spx_heatmap)
        ff_curve_future = executor.submit(fetch_ff_forward_curve)
        eq_val_future = executor.submit(fetch_equity_valuations)
        cmdty_curve_future = executor.submit(fetch_commodity_forward_curves)
        ust_curve_future = executor.submit(fetch_ust_yield_curve)
        futures = {
            executor.submit(fetch_batch, batch): i
            for i, batch in enumerate(batches)
        }

        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                results, errors = future.result()
                output["series"].update(results)
                all_errors.extend(errors)
            except Exception as e:
                batch = batches[batch_idx]
                tickers = ", ".join(s["ticker"] for s in batch)
                all_errors.append(f"Batch {batch_idx} ({tickers}): {e}")
                print(f"  Batch {batch_idx} FAILED: {e}")

    # Apply transforms (e.g. implied rate = 100 - price)
    TRANSFORMS = {s["key"]: s.get("transform") for s in SERIES if s.get("transform")}
    for key, transform in TRANSFORMS.items():
        if key in output["series"] and transform == "implied_rate":
            output["series"][key]["values"] = [
                round(100 - v, 4) for v in output["series"][key]["values"]
            ]

    # Collect heatmap results
    try:
        output["heatmap"] = heatmap_future.result()
        print(f"\n  Heatmap: {len(output['heatmap'])} stocks")
    except Exception as e:
        all_errors.append(f"SPX Heatmap: {e}")
        print(f"  Heatmap FAILED: {e}")

    # Collect FF forward curve results
    try:
        output["ff_curve"] = ff_curve_future.result()
        print(f"  FF Curve: {len(output['ff_curve'])} contract months")
    except Exception as e:
        all_errors.append(f"FF Forward Curve: {e}")
        print(f"  FF Curve FAILED: {e}")

    # Collect equity valuations results
    try:
        output["equity_valuations"] = eq_val_future.result()
        print(f"  Equity Valuations: {len(output['equity_valuations'])} indices")
    except Exception as e:
        all_errors.append(f"Equity Valuations: {e}")
        print(f"  Equity Valuations FAILED: {e}")

    # Collect commodity forward curves results
    try:
        output["commodity_curves"] = cmdty_curve_future.result()
        for k, v in output["commodity_curves"].items():
            print(f"  {k}: {len(v)} contract months")
    except Exception as e:
        all_errors.append(f"Commodity Curves: {e}")
        print(f"  Commodity Curves FAILED: {e}")

    # Collect UST yield curve results
    try:
        output["ust_yield_curve"] = ust_curve_future.result()
        print(f"  UST Yield Curve: {len(output['ust_yield_curve'])} tenors")
    except Exception as e:
        all_errors.append(f"UST Yield Curve: {e}")
        print(f"  UST Yield Curve FAILED: {e}")

    elapsed = time.time() - t0

    # Write output files
    os.makedirs(DATA_DIR, exist_ok=True)

    with open(JSON_PATH, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nWrote {JSON_PATH}")

    with open(JS_PATH, "w") as f:
        f.write("var BLOOMBERG_DATA = ")
        json.dump(output, f, indent=2)
        f.write(";\n")
    print(f"Wrote {JS_PATH}")

    print(f"\nCompleted in {elapsed:.1f}s ({len(output['series'])}/{len(SERIES)} series)")

    if all_errors:
        print(f"\nWARNING: {len(all_errors)} errors:")
        for e in all_errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print("All series fetched successfully.")


def git_push():
    """Commit updated data files and push to GitHub to update Pages."""
    try:
        subprocess.run(["git", "add", "data/bloomberg.json", "data/bloomberg_data.js"],
                       cwd=SCRIPT_DIR, check=True)

        # Check if there are staged changes
        result = subprocess.run(["git", "diff", "--cached", "--quiet"],
                                cwd=SCRIPT_DIR)
        if result.returncode == 0:
            print("\nNo data changes to push.")
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
        subprocess.run(
            ["git", "commit", "-m", f"Update Bloomberg data {timestamp}"],
            cwd=SCRIPT_DIR, check=True
        )
        subprocess.run(["git", "push", "origin", "master"],
                       cwd=SCRIPT_DIR, check=True)
        print("\nPushed updated data to GitHub Pages.")
    except subprocess.CalledProcessError as e:
        print(f"\nGit push failed: {e}")
    except FileNotFoundError:
        print("\nGit not found on PATH — skipping push.")


if __name__ == "__main__":
    main()
    git_push()
