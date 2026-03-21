import { NextRequest, NextResponse } from "next/server";
import { getResearchRuns } from "@/lib/research";

type Suggestion = {
  company: string;
  ticker: string;
  market: string;
  researched: boolean;
  runId?: string;
};

function normalizeTicker(raw: string): string {
  const t = raw.toUpperCase().trim();
  if (t.endsWith(".SS")) return `${t.slice(0, -3)}.SH`;
  return t;
}

function normalizeCnTicker(code: string): string {
  const c = code.trim().toUpperCase();
  if (/^[0-9]{5}$/.test(c)) {
    return `${c}.HK`;
  }
  if (/^[0-9]{6}$/.test(c)) {
    if (c.startsWith("6") || c.startsWith("9")) return `${c}.SH`;
    return `${c}.SZ`;
  }
  return normalizeTicker(c);
}

function inferMarket(ticker: string): string {
  const t = ticker.toUpperCase();
  if (t.endsWith(".HK")) return "HK";
  if (t.endsWith(".SH") || t.endsWith(".SS")) return "SH";
  if (t.endsWith(".SZ")) return "SZ";
  if (t.includes(".")) return "OTHER";
  return "US";
}

async function searchYahoo(q: string): Promise<Array<{ company: string; ticker: string }>> {
  const url = new URL("https://query1.finance.yahoo.com/v1/finance/search");
  url.searchParams.set("q", q);
  url.searchParams.set("quotesCount", "12");
  url.searchParams.set("newsCount", "0");
  url.searchParams.set("lang", "en-US");
  url.searchParams.set("region", "US");

  const response = await fetch(url.toString(), { cache: "no-store" });
  if (!response.ok) {
    return [];
  }
  const payload = (await response.json()) as {
    quotes?: Array<{ symbol?: string; shortname?: string; longname?: string; quoteType?: string }>;
  };
  const quotes = payload.quotes ?? [];
  return quotes
    .filter((item) => item.symbol && (item.quoteType === "EQUITY" || item.quoteType === "ETF"))
    .map((item) => ({
      company: (item.longname || item.shortname || item.symbol || "").trim(),
      ticker: normalizeTicker((item.symbol || "").trim()),
    }))
    .filter((item) => item.company && item.ticker);
}

async function searchEastmoney(q: string): Promise<Array<{ company: string; ticker: string }>> {
  const url = new URL("https://searchapi.eastmoney.com/api/suggest/get");
  url.searchParams.set("input", q);
  url.searchParams.set("type", "14");
  url.searchParams.set("token", "D43BF722C8E33BDC906FB84D85E326E8");

  const response = await fetch(url.toString(), { cache: "no-store" });
  if (!response.ok) {
    return [];
  }

  const payload = (await response.json()) as {
    QuotationCodeTable?: {
      Data?: Array<{
        Code?: string;
        Name?: string;
        Classify?: string;
        SecurityTypeName?: string;
      }>;
    };
  };

  const data = payload.QuotationCodeTable?.Data ?? [];
  return data
    .filter((item) => {
      const classify = (item.Classify || "").toLowerCase();
      const secName = item.SecurityTypeName || "";
      return (
        classify === "astock" ||
        classify === "hk" ||
        classify === "usstock" ||
        secName.includes("A股") ||
        secName.includes("港股") ||
        secName.includes("美股")
      );
    })
    .map((item) => ({
      company: (item.Name || "").trim(),
      ticker: normalizeCnTicker((item.Code || "").trim()),
    }))
    .filter((item) => item.company && item.ticker);
}

export async function GET(request: NextRequest) {
  const q = (request.nextUrl.searchParams.get("q") || "").trim();
  if (!q) {
    return NextResponse.json({ items: [] as Suggestion[] });
  }

  const runs = getResearchRuns();
  const researchedByTicker = new Map(
    runs.map((run) => [run.ticker.toUpperCase(), { runId: run.runId, company: run.company }])
  );

  const localItems: Suggestion[] = runs
    .filter((run) => {
      const text = `${run.company} ${run.ticker}`.toLowerCase();
      return text.includes(q.toLowerCase());
    })
    .map((run) => ({
      company: run.company,
      ticker: run.ticker,
      market: inferMarket(run.ticker),
      researched: true,
      runId: run.runId,
    }));

  const remoteCn = await searchEastmoney(q);
  const remoteYahoo = await searchYahoo(q);
  const remote = [...remoteCn, ...remoteYahoo];
  const merged = new Map<string, Suggestion>();
  for (const item of localItems) {
    merged.set(item.ticker.toUpperCase(), item);
  }
  for (const item of remote) {
    const key = item.ticker.toUpperCase();
    if (merged.has(key)) continue;
    const researched = researchedByTicker.get(key);
    merged.set(key, {
      company: researched?.company || item.company,
      ticker: item.ticker,
      market: inferMarket(item.ticker),
      researched: Boolean(researched),
      runId: researched?.runId,
    });
  }

  return NextResponse.json({
    items: Array.from(merged.values()).slice(0, 20),
  });
}
