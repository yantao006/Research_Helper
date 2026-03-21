import { NextRequest, NextResponse } from "next/server";
import { unstable_cache } from "next/cache";
import https from "https";

type QuoteOutput = {
  ticker: string;
  price: number | null;
  changePercent: number | null;
  marketTime: number | null;
};

type MissingItem = {
  ticker: string;
  source: "tencent";
  details: string;
};

type QuotesCoreResult = {
  itemByTicker: Record<string, QuoteOutput>;
  missing: MissingItem[];
};

export const runtime = "nodejs";

const QUOTES_REVALIDATE_SECONDS = (() => {
  const parsed = Number(process.env.QUOTES_REVALIDATE_SECONDS || "45");
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 45;
})();

const QUOTES_FETCH_RETRIES = 1;
const QUOTES_CHUNK_SIZE = 40;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function toNumber(value: string | undefined): number | null {
  if (!value) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function normalizeInputTickers(tickersRaw: string): string[] {
  const seen = new Set<string>();
  const out: string[] = [];
  for (const part of tickersRaw.split(",")) {
    const ticker = part.trim().toUpperCase();
    if (!ticker || seen.has(ticker)) continue;
    seen.add(ticker);
    out.push(ticker);
    if (out.length >= 80) break;
  }
  return out;
}

function tickerToTencentSymbol(ticker: string): string | null {
  const t = ticker.trim().toUpperCase();
  if (/^\d{6}\.SH$/.test(t)) return `sh${t.slice(0, 6)}`;
  if (/^\d{6}\.SZ$/.test(t)) return `sz${t.slice(0, 6)}`;
  if (/^\d{4,5}\.HK$/.test(t)) return `hk${t.slice(0, -3).padStart(5, "0")}`;
  if (/^[A-Z0-9.-]+$/.test(t)) return `us${t}`;
  return null;
}

function parseTencentMarketTime(raw: string | undefined): number | null {
  if (!raw) return null;
  const text = raw.trim();
  if (!text) return null;

  if (/^\d{14}$/.test(text)) {
    const normalized = `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}T${text.slice(
      8,
      10
    )}:${text.slice(10, 12)}:${text.slice(12, 14)}+08:00`;
    const ts = Date.parse(normalized);
    return Number.isFinite(ts) ? ts : null;
  }

  if (/^\d{4}[-/]\d{2}[-/]\d{2}\s\d{2}:\d{2}:\d{2}$/.test(text)) {
    const normalized = text.replace(/\//g, "-");
    const ts = Date.parse(normalized);
    return Number.isFinite(ts) ? ts : null;
  }

  return null;
}

async function httpGetText(url: string, retries = QUOTES_FETCH_RETRIES): Promise<string> {
  let lastError: unknown = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const body = await new Promise<string>((resolve, reject) => {
        const req = https.get(
          url,
          {
            headers: {
              "User-Agent": "Mozilla/5.0",
              Referer: "https://stockapp.finance.qq.com/",
              Accept: "*/*",
            },
          },
          (res) => {
            if ((res.statusCode ?? 500) >= 400) {
              reject(new Error(`HTTP ${res.statusCode}`));
              return;
            }
            const chunks: Buffer[] = [];
            res.on("data", (chunk) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
            res.on("end", () => {
              resolve(Buffer.concat(chunks).toString("utf-8"));
            });
          }
        );
        req.on("error", reject);
        req.setTimeout(12000, () => req.destroy(new Error("request timeout")));
      });
      return body;
    } catch (error) {
      lastError = error;
      if (attempt < retries) {
        await sleep(140 * (attempt + 1));
      }
    }
  }
  throw lastError instanceof Error ? lastError : new Error("quote request failed");
}

async function fetchTencentRaw(symbols: string[]): Promise<Record<string, string[]>> {
  const symbolMap: Record<string, string[]> = {};
  if (symbols.length === 0) return symbolMap;

  for (let i = 0; i < symbols.length; i += QUOTES_CHUNK_SIZE) {
    const chunk = symbols.slice(i, i + QUOTES_CHUNK_SIZE);
    const url = `https://qt.gtimg.cn/q=${encodeURIComponent(chunk.join(","))}`;
    const text = await httpGetText(url);
    const regex = /v_([^=]+)="([^"]*)";/g;
    let match: RegExpExecArray | null;
    while ((match = regex.exec(text)) !== null) {
      const symbol = match[1];
      const fields = match[2].split("~");
      symbolMap[symbol] = fields;
    }
  }

  return symbolMap;
}

async function fetchQuotesCore(tickers: string[]): Promise<QuotesCoreResult> {
  const symbolByTicker: Record<string, string | null> = {};
  const uniqueSymbols: string[] = [];
  const seenSymbols = new Set<string>();

  for (const ticker of tickers) {
    const symbol = tickerToTencentSymbol(ticker);
    symbolByTicker[ticker] = symbol;
    if (symbol && !seenSymbols.has(symbol)) {
      seenSymbols.add(symbol);
      uniqueSymbols.push(symbol);
    }
  }

  const symbolFields = await fetchTencentRaw(uniqueSymbols);
  const itemByTicker: Record<string, QuoteOutput> = {};
  const missing: MissingItem[] = [];

  for (const ticker of tickers) {
    const symbol = symbolByTicker[ticker];
    if (!symbol) {
      itemByTicker[ticker] = { ticker, price: null, changePercent: null, marketTime: null };
      missing.push({ ticker, source: "tencent", details: "unsupported ticker format" });
      continue;
    }

    const fields = symbolFields[symbol];
    if (!fields || fields.length < 33) {
      itemByTicker[ticker] = { ticker, price: null, changePercent: null, marketTime: null };
      missing.push({ ticker, source: "tencent", details: `symbol not found: ${symbol}` });
      continue;
    }

    const price = toNumber(fields[3]);
    const changePercent = toNumber(fields[32]);
    const marketTime = parseTencentMarketTime(fields[30]);
    itemByTicker[ticker] = {
      ticker,
      price,
      changePercent,
      marketTime,
    };

    if (price === null) {
      missing.push({ ticker, source: "tencent", details: `invalid quote payload: ${symbol}` });
    }
  }

  return { itemByTicker, missing };
}

const fetchQuotesCached = unstable_cache(
  async (tickersKey: string) => {
    const tickers = tickersKey.split(",").filter(Boolean);
    return fetchQuotesCore(tickers);
  },
  ["quotes-tencent-v1"],
  { revalidate: QUOTES_REVALIDATE_SECONDS }
);

export async function GET(request: NextRequest) {
  const tickersRaw = (request.nextUrl.searchParams.get("tickers") || "").trim();
  const debugEnabled =
    request.nextUrl.searchParams.get("debug") === "1" || process.env.QUOTES_DEBUG === "true";
  const bypassCache = debugEnabled || request.nextUrl.searchParams.get("no_cache") === "1";

  if (!tickersRaw) {
    return NextResponse.json({ items: [] });
  }

  const inputTickers = normalizeInputTickers(tickersRaw);
  const cacheKey = [...inputTickers].sort().join(",");
  const startedAt = Date.now();

  const coreResult = bypassCache ? await fetchQuotesCore(inputTickers) : await fetchQuotesCached(cacheKey);
  const { itemByTicker, missing } = coreResult;
  const elapsed = Date.now() - startedAt;

  const items = inputTickers.map((ticker) => itemByTicker[ticker] ?? {
    ticker,
    price: null,
    changePercent: null,
    marketTime: null,
  });

  if (debugEnabled || missing.length > 0) {
    console.info(
      `[quotes] requested=${inputTickers.length} ok=${inputTickers.length - missing.length} missing=${missing.length} elapsed_ms=${elapsed} revalidate_s=${QUOTES_REVALIDATE_SECONDS}`
    );
    if (missing.length > 0) {
      console.info(`[quotes] missing_tickers=${missing.map((x) => x.ticker).join(",")}`);
    }
  }

  if (debugEnabled) {
    return NextResponse.json({
      items,
      debug: {
        requested: inputTickers.length,
        cacheKey,
        bypassCache,
        elapsedMs: elapsed,
        revalidateSeconds: QUOTES_REVALIDATE_SECONDS,
        missing,
      },
    });
  }

  return NextResponse.json(
    { items },
    {
      headers: {
        "Cache-Control": `public, max-age=0, s-maxage=${QUOTES_REVALIDATE_SECONDS}, stale-while-revalidate=${Math.max(
          QUOTES_REVALIDATE_SECONDS * 2,
          60
        )}`,
      },
    }
  );
}
