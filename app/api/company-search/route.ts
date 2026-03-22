import fs from "fs";
import { NextRequest, NextResponse } from "next/server";
import path from "path";
import { getResearchRuns } from "@/lib/research";

export const runtime = "nodejs";

type Suggestion = {
  company: string;
  ticker: string;
  market: string;
  researched: boolean;
  runId?: string;
};

const TASKS_CSV = path.join(process.cwd(), "tasks.csv");

function parseCsvLine(line: string): string[] {
  const cells: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === "," && !inQuotes) {
      cells.push(current);
      current = "";
      continue;
    }
    current += ch;
  }
  cells.push(current);
  return cells;
}

function includesNormalized(text: string, q: string): boolean {
  return text.toLowerCase().includes(q.toLowerCase());
}

function readLocalTaskSuggestions(q: string): Array<{ company: string; ticker: string }> {
  if (!fs.existsSync(TASKS_CSV)) {
    return [];
  }
  try {
    const raw = fs.readFileSync(TASKS_CSV, "utf-8");
    const lines = raw.split(/\r?\n/).filter((line) => line.trim().length > 0);
    if (lines.length <= 1) {
      return [];
    }
    const headers = parseCsvLine(lines[0]).map((item) => item.trim());
    const companyIdx = headers.indexOf("company");
    const tickerIdx = headers.indexOf("Ticker");
    if (companyIdx < 0 || tickerIdx < 0) {
      return [];
    }
    const items: Array<{ company: string; ticker: string }> = [];
    for (let i = 1; i < lines.length; i += 1) {
      const cells = parseCsvLine(lines[i]);
      const company = (cells[companyIdx] || "").trim();
      const ticker = normalizeCnTicker((cells[tickerIdx] || "").trim());
      const haystack = `${company} ${ticker}`;
      if (!company || !ticker || !includesNormalized(haystack, q)) {
        continue;
      }
      items.push({ company, ticker });
    }
    return items;
  } catch {
    return [];
  }
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, fallback: T): Promise<T> {
  let timer: NodeJS.Timeout | null = null;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((resolve) => {
        timer = setTimeout(() => resolve(fallback), timeoutMs);
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}

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
  try {
    const url = new URL("https://query1.finance.yahoo.com/v1/finance/search");
    url.searchParams.set("q", q);
    url.searchParams.set("quotesCount", "12");
    url.searchParams.set("newsCount", "0");
    url.searchParams.set("lang", "en-US");
    url.searchParams.set("region", "US");

    const response = await fetch(url.toString(), {
      cache: "no-store",
      signal: AbortSignal.timeout(5000),
    });
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
  } catch {
    return [];
  }
}

async function searchEastmoney(q: string): Promise<Array<{ company: string; ticker: string }>> {
  try {
    const url = new URL("https://searchapi.eastmoney.com/api/suggest/get");
    url.searchParams.set("input", q);
    url.searchParams.set("type", "14");
    url.searchParams.set("token", "D43BF722C8E33BDC906FB84D85E326E8");

    const response = await fetch(url.toString(), {
      cache: "no-store",
      signal: AbortSignal.timeout(5000),
    });
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
  } catch {
    return [];
  }
}

export async function GET(request: NextRequest) {
  const q = (request.nextUrl.searchParams.get("q") || "").trim();
  if (!q || q.length > 64) {
    return NextResponse.json({ items: [] as Suggestion[] });
  }

  const [runs, localTaskItems, remoteCn, remoteYahoo] = await Promise.all([
    withTimeout(getResearchRuns(), 1200, []),
    Promise.resolve(readLocalTaskSuggestions(q)),
    searchEastmoney(q),
    searchYahoo(q),
  ]);
  const researchedByTicker = new Map(
    runs.map((run) => [run.ticker.toUpperCase(), { runId: run.runId, company: run.company }])
  );

  const localItems: Suggestion[] = runs
    .filter((run) => {
      const text = `${run.company} ${run.ticker}`.toLowerCase();
      return includesNormalized(text, q);
    })
    .map((run) => ({
      company: run.company,
      ticker: run.ticker,
      market: inferMarket(run.ticker),
      researched: true,
      runId: run.runId,
    }));

  const taskItems: Suggestion[] = localTaskItems.map((item) => {
    const researched = researchedByTicker.get(item.ticker.toUpperCase());
    return {
      company: researched?.company || item.company,
      ticker: item.ticker,
      market: inferMarket(item.ticker),
      researched: Boolean(researched),
      runId: researched?.runId,
    };
  });

  const remote = [...remoteCn, ...remoteYahoo];
  const merged = new Map<string, Suggestion>();
  for (const item of localItems) {
    merged.set(item.ticker.toUpperCase(), item);
  }
  for (const item of taskItems) {
    const key = item.ticker.toUpperCase();
    if (merged.has(key)) continue;
    merged.set(key, item);
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
