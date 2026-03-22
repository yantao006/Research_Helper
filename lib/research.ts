import fs from "fs";
import path from "path";
import { getPostgresPool, hasPostgresDsn } from "@/lib/server/postgres";

type DocMeta = {
  company: string;
  ticker: string;
  date: string;
  provider: string;
  model: string;
  industry: string;
};

export type ResearchDoc = {
  id: string;
  question: string;
  answer: string;
  sources: Array<{ title: string; url: string }>;
  meta: DocMeta;
  fileName: string;
};

export type ResearchFactPack = {
  collectedAt: string;
  collectedWithWebSearch: boolean;
  summaryMarkdown: string;
  deltaSummaryMarkdown: string;
  coverageScore: string;
  confidence: string;
  keyFinancials: string[];
  recentCatalysts: string[];
  valuationSnapshot: string[];
  topRisks: string[];
  trackingItems: string[];
};

export type ResearchDelta = {
  summaryMarkdown: string;
  highlights: string[];
  previousReportDate: string | null;
};

export type ResearchInsightSummary = {
  oneLiner: string;
  keyPoints: string[];
  relatedDocs: Array<{ id: string; question: string }>;
};

export type ResearchRun = {
  runId: string;
  company: string;
  ticker: string;
  industry: string;
  date: string;
  provider: string;
  model: string;
  docs: ResearchDoc[];
  factPack: ResearchFactPack | null;
  delta: ResearchDelta | null;
  insightSummary: ResearchInsightSummary | null;
};

type DbDocRow = {
  company: string;
  ticker: string;
  report_date: string;
  prompt_id: string;
  question: string;
  answer_markdown: string;
  sources_json: unknown;
  provider: string;
  model: string;
  output_path: string;
  markdown: string;
};

type DbTaskRow = {
  ticker: string;
  company: string;
  extra: unknown;
};

type DbFactPackRow = {
  company: string;
  ticker: string;
  report_date: string;
  collected_at: string;
  collected_with_web_search: boolean;
  payload_json: unknown;
  summary_markdown: string;
  delta_summary_markdown: string;
};

type TaskLookup = {
  companyByTicker: Map<string, string>;
  industryByTicker: Map<string, string>;
};

const OUTPUT_ROOT = path.join(process.cwd(), "output");
const TASKS_CSV = path.join(process.cwd(), "tasks.csv");
let warnedNoDsn = false;
type ResearchRunsCacheState = {
  expiresAt: number;
  value: ResearchRun[];
  inFlight?: Promise<ResearchRun[]>;
};

declare global {
  // eslint-disable-next-line no-var
  var __researchRunsCacheState: ResearchRunsCacheState | undefined;
}

function readRunsCacheMs(): number {
  const raw = (process.env.RESEARCH_RUNS_CACHE_MS || "").trim();
  const parsed = Number(raw);
  if (Number.isFinite(parsed) && parsed >= 0) {
    return Math.floor(parsed);
  }
  return 15_000;
}

function getRunsCacheState(): ResearchRunsCacheState {
  if (!global.__researchRunsCacheState) {
    global.__researchRunsCacheState = { expiresAt: 0, value: [] };
  }
  return global.__researchRunsCacheState;
}

const CHINA_TICKER_CN_NAME: Record<string, string> = {
  "01357.HK": "美图公司",
  "02097.HK": "美的集团",
  "00823.HK": "领展房产基金",
  "09992.HK": "泡泡玛特",
  "09626.HK": "哔哩哔哩-W",
  "600900.SH": "长江电力",
  "600519.SH": "贵州茅台",
};

function isChinaMarketTicker(ticker: string): boolean {
  const t = ticker.toUpperCase();
  return t.endsWith(".HK") || t.endsWith(".SH") || t.endsWith(".SZ");
}

function hasChinese(text: string): boolean {
  return /[\u4e00-\u9fff]/.test(text);
}

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

function parseExtraObject(raw: unknown): Record<string, string> {
  if (!raw) {
    return {};
  }
  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        return Object.fromEntries(
          Object.entries(parsed).map(([k, v]) => [String(k), v == null ? "" : String(v)])
        );
      }
      return {};
    } catch {
      return {};
    }
  }
  if (typeof raw === "object" && !Array.isArray(raw)) {
    return Object.fromEntries(
      Object.entries(raw as Record<string, unknown>).map(([k, v]) => [String(k), v == null ? "" : String(v)])
    );
  }
  return {};
}

function parseJsonObject(raw: unknown): Record<string, unknown> {
  if (!raw) return {};
  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw) as unknown;
      return parsed && typeof parsed === "object" && !Array.isArray(parsed)
        ? (parsed as Record<string, unknown>)
        : {};
    } catch {
      return {};
    }
  }
  return raw && typeof raw === "object" && !Array.isArray(raw)
    ? (raw as Record<string, unknown>)
    : {};
}

function toDisplayStrings(raw: unknown, limit = 5): string[] {
  if (!Array.isArray(raw)) return [];
  const result: string[] = [];
  for (const item of raw.slice(0, limit)) {
    if (typeof item === "string") {
      const text = item.trim();
      if (text) result.push(text);
      continue;
    }
    if (!item || typeof item !== "object") continue;
    const obj = item as Record<string, unknown>;
    const parts = [
      "title",
      "metric",
      "name",
      "period",
      "value",
      "timing",
      "impact",
      "detail",
      "note",
      "risk",
    ]
      .map((key) => String(obj[key] ?? "").trim())
      .filter(Boolean);
    if (parts.length > 0) {
      result.push(Array.from(new Set(parts)).join(" / "));
    }
  }
  return result;
}

function parsePreviousReportDateFromDelta(markdown: string): string | null {
  const match = markdown.match(/对比区间：\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s*->/);
  return match?.[1] ?? null;
}

function extractDeltaHighlights(markdown: string): string[] {
  if (!markdown.trim()) return [];
  const lines = markdown
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => /^- /.test(line))
    .map((line) => line.replace(/^- /, "").trim())
    .filter(Boolean);
  return Array.from(new Set(lines)).slice(0, 8);
}

function buildFactPackViewFromRecord(record: {
  collectedAt: string;
  collectedWithWebSearch: boolean;
  payload: Record<string, unknown>;
  summaryMarkdown: string;
  deltaSummaryMarkdown: string;
}): ResearchFactPack {
  const quality = parseJsonObject(record.payload.quality);
  const filings = parseJsonObject(record.payload.filings);
  const news = parseJsonObject(record.payload.news_and_catalysts);
  const valuation = parseJsonObject(record.payload.valuation_and_market);
  const tracking = parseJsonObject(record.payload.tracking);
  return {
    collectedAt: record.collectedAt,
    collectedWithWebSearch: record.collectedWithWebSearch,
    summaryMarkdown: record.summaryMarkdown,
    deltaSummaryMarkdown: record.deltaSummaryMarkdown,
    coverageScore: String(quality.coverage_score ?? "").trim(),
    confidence: String(quality.confidence ?? "").trim(),
    keyFinancials: toDisplayStrings(filings.key_financials, 5),
    recentCatalysts: toDisplayStrings(news.upcoming_catalysts || news.recent_events, 5),
    valuationSnapshot: toDisplayStrings(valuation.market_data || valuation.valuation_multiples, 5),
    topRisks: toDisplayStrings(record.payload.risks, 5),
    trackingItems: toDisplayStrings(tracking.follow_up_items || tracking.minimum_dashboard, 5),
  };
}

function readFactPackFromLocal(dirPath: string): ResearchFactPack | null {
  const factPackPath = path.join(dirPath, "fact_pack.json");
  if (!fs.existsSync(factPackPath)) {
    return null;
  }
  try {
    const raw = JSON.parse(fs.readFileSync(factPackPath, "utf-8")) as Record<string, unknown>;
    return buildFactPackViewFromRecord({
      collectedAt: String(raw.collected_at ?? ""),
      collectedWithWebSearch: Boolean(raw.collected_with_web_search),
      payload: parseJsonObject(raw.payload),
      summaryMarkdown: String(raw.summary_markdown ?? ""),
      deltaSummaryMarkdown: String(raw.delta_summary_markdown ?? ""),
    });
  } catch {
    return null;
  }
}

function buildInsightSummary(docs: ResearchDoc[]): ResearchInsightSummary | null {
  if (docs.length === 0) return null;
  const priorityKeywords = ["一句话判断", "投资备忘录", "结论", "综合"];
  const primaryDoc =
    docs.find((doc) => priorityKeywords.some((keyword) => doc.question.includes(keyword))) || docs[0];
  const lines = primaryDoc.answer
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  const oneLiner =
    lines.find((line) => !/^[-*#>\d]/.test(line) && line.length >= 12) ||
    lines.find((line) => line.length >= 8) ||
    "";
  const keyPoints = lines
    .filter((line) => /^[-*] /.test(line) || /^\d+\./.test(line))
    .map((line) => line.replace(/^[-*]\s+/, "").replace(/^\d+\.\s+/, "").trim())
    .filter(Boolean)
    .slice(0, 5);
  const relatedDocs = docs
    .filter((doc) =>
      ["一句话判断", "投资备忘录", "结论", "风险", "催化剂", "估值"].some((keyword) =>
        doc.question.includes(keyword)
      )
    )
    .slice(0, 5)
    .map((doc) => ({ id: doc.id, question: doc.question }));

  if (!oneLiner && keyPoints.length === 0 && relatedDocs.length === 0) {
    return null;
  }
  return {
    oneLiner,
    keyPoints,
    relatedDocs,
  };
}

function sanitizeRunPart(value: string): string {
  const sanitized = value.trim().replace(/[^\w-]+/g, "_").replace(/^_+|_+$/g, "");
  return sanitized || "untitled";
}

function toRunId(ticker: string, reportDate: string): string {
  return `${sanitizeRunPart(ticker)}_${reportDate}`;
}

function extractPromptId(fileName: string): string {
  const base = fileName.replace(/\.md$/i, "");
  const match = base.match(/^([A-Za-z]+_\d+|\d+)_/);
  if (match) {
    return match[1];
  }
  const firstUnderscore = base.indexOf("_");
  if (firstUnderscore > 0) {
    return base.slice(0, firstUnderscore);
  }
  return base;
}

function parseMeta(lines: string[]): DocMeta {
  const meta: Partial<DocMeta> = {};
  for (const line of lines) {
    if (!line.startsWith("- ")) {
      continue;
    }
    const sep = line.indexOf(":");
    if (sep < 0) {
      continue;
    }
    const key = line.slice(2, sep).trim().toLowerCase();
    const value = line.slice(sep + 1).trim();
    if (key === "company") meta.company = value;
    if (key === "ticker") meta.ticker = value;
    if (key === "date") meta.date = value;
    if (key === "provider") meta.provider = value;
    if (key === "model") meta.model = value;
    if (key === "industry") meta.industry = value;
  }
  return {
    company: meta.company ?? "Unknown",
    ticker: meta.ticker ?? "Unknown",
    date: meta.date ?? "Unknown",
    provider: meta.provider ?? "Unknown",
    model: meta.model ?? "Unknown",
    industry: meta.industry ?? "未分类",
  };
}

function parseSources(lines: string[]): Array<{ title: string; url: string }> {
  const items: Array<{ title: string; url: string }> = [];
  const sourceRegex = /^- \[(.+?)\]\((https?:\/\/.+)\)$/;
  for (const line of lines) {
    const match = line.match(sourceRegex);
    if (match) {
      items.push({ title: match[1], url: match[2] });
    }
  }
  return items;
}

function parseDoc(filePath: string, fileName: string): ResearchDoc {
  const raw = fs.readFileSync(filePath, "utf-8");
  const lines = raw.split(/\r?\n/);
  const title = lines[0]?.replace(/^#\s+/, "").trim() || fileName;
  const meta = parseMeta(lines);

  const answerStart = lines.findIndex((line) => line.trim() === "## Answer");
  const sourcesStart = lines.findIndex((line) => line.trim() === "## Sources");
  const answerLines =
    answerStart >= 0
      ? lines.slice(answerStart + 1, sourcesStart >= 0 ? sourcesStart : undefined)
      : [];
  const answer = answerLines.join("\n").trim();

  const sourceLines = sourcesStart >= 0 ? lines.slice(sourcesStart + 1) : [];
  const sources = parseSources(sourceLines);

  const id = extractPromptId(fileName) || "0";
  return {
    id,
    question: title,
    answer,
    sources,
    meta,
    fileName,
  };
}

function readMarkdownFiles(dirPath: string): string[] {
  return fs
    .readdirSync(dirPath)
    .filter((name) => name.endsWith(".md"))
    .sort((a, b) => a.localeCompare(b, "zh-CN", { numeric: true }));
}

function readTaskLookupFromCsv(): TaskLookup {
  const companyByTicker = new Map<string, string>();
  const industryByTicker = new Map<string, string>();
  if (!fs.existsSync(TASKS_CSV)) {
    return { companyByTicker, industryByTicker };
  }

  const raw = fs.readFileSync(TASKS_CSV, "utf-8");
  const lines = raw.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (lines.length === 0) {
    return { companyByTicker, industryByTicker };
  }

  const headers = parseCsvLine(lines[0]).map((h) => h.trim());
  const companyIdx = headers.findIndex((h) => h === "company");
  const tickerIdx = headers.findIndex((h) => h === "Ticker");
  const industryIdx = headers.findIndex((h) => h === "industry");

  if (companyIdx < 0 || tickerIdx < 0) {
    return { companyByTicker, industryByTicker };
  }

  for (let i = 1; i < lines.length; i += 1) {
    const cells = parseCsvLine(lines[i]);
    const company = (cells[companyIdx] ?? "").trim();
    const ticker = (cells[tickerIdx] ?? "").trim().toUpperCase();
    const industry = industryIdx >= 0 ? (cells[industryIdx] ?? "").trim() : "";
    if (!ticker) continue;
    if (company) {
      companyByTicker.set(ticker, company);
    }
    if (industry) {
      industryByTicker.set(ticker, industry);
    }
  }

  return { companyByTicker, industryByTicker };
}

function chooseDisplayCompanyName(
  ticker: string,
  metaCompany: string,
  taskCompany: string | undefined
): string {
  if (!isChinaMarketTicker(ticker)) {
    return metaCompany;
  }
  const canonicalTicker = ticker.toUpperCase();
  if (taskCompany && hasChinese(taskCompany)) {
    return taskCompany;
  }
  if (hasChinese(metaCompany)) {
    return metaCompany;
  }
  if (CHINA_TICKER_CN_NAME[canonicalTicker]) {
    return CHINA_TICKER_CN_NAME[canonicalTicker];
  }
  return taskCompany || metaCompany;
}

async function getResearchRunsFromLocal(): Promise<ResearchRun[]> {
  if (!fs.existsSync(OUTPUT_ROOT)) {
    return [];
  }

  const runDirs = fs
    .readdirSync(OUTPUT_ROOT, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort((a, b) => b.localeCompare(a, "en"));

  const taskLookup = readTaskLookupFromCsv();
  const runs: ResearchRun[] = [];
  for (const runId of runDirs) {
    const dirPath = path.join(OUTPUT_ROOT, runId);
    const mdFiles = readMarkdownFiles(dirPath);
    if (mdFiles.length === 0) {
      continue;
    }
    const docs = mdFiles.map((fileName) => parseDoc(path.join(dirPath, fileName), fileName));
    const first = docs[0];
    const ticker = first.meta.ticker.toUpperCase();
    const displayCompany = chooseDisplayCompanyName(
      ticker,
      first.meta.company,
      taskLookup.companyByTicker.get(ticker)
    );
    const industry = taskLookup.industryByTicker.get(ticker) || first.meta.industry || "未分类";
    const factPack = readFactPackFromLocal(dirPath);
    const insightSummary = buildInsightSummary(docs);
    runs.push({
      runId,
      company: displayCompany,
      ticker: first.meta.ticker,
      industry,
      date: first.meta.date,
      provider: first.meta.provider,
      model: first.meta.model,
      docs,
      factPack,
      delta: factPack?.deltaSummaryMarkdown
        ? {
            summaryMarkdown: factPack.deltaSummaryMarkdown,
            highlights: extractDeltaHighlights(factPack.deltaSummaryMarkdown),
            previousReportDate: parsePreviousReportDateFromDelta(factPack.deltaSummaryMarkdown),
          }
        : null,
      insightSummary,
    });
  }
  return runs;
}

async function readTaskLookupFromDb(): Promise<TaskLookup> {
  const companyByTicker = new Map<string, string>();
  const industryByTicker = new Map<string, string>();
  const pool = getPostgresPool();
  if (!pool) {
    return { companyByTicker, industryByTicker };
  }

  const result = await pool.query<DbTaskRow>(
    `
      SELECT ticker, company, extra
      FROM rb_company_tasks
    `
  );

  for (const row of result.rows) {
    const ticker = (row.ticker || "").trim().toUpperCase();
    const company = (row.company || "").trim();
    if (!ticker) continue;
    if (company) {
      companyByTicker.set(ticker, company);
    }
    const extra = parseExtraObject(row.extra);
    const industry = (extra.industry || "").trim();
    if (industry) {
      industryByTicker.set(ticker, industry);
    }
  }
  return { companyByTicker, industryByTicker };
}

function parseDbSources(raw: unknown): Array<{ title: string; url: string }> {
  if (!raw) return [];
  let payload: unknown = raw;
  if (typeof raw === "string") {
    try {
      payload = JSON.parse(raw) as unknown;
    } catch {
      return [];
    }
  }
  if (!Array.isArray(payload)) {
    return [];
  }
  const items: Array<{ title: string; url: string }> = [];
  for (const entry of payload) {
    if (!entry || typeof entry !== "object") continue;
    const title = String((entry as { title?: unknown }).title ?? "").trim();
    const url = String((entry as { url?: unknown }).url ?? "").trim();
    if (!url) continue;
    items.push({ title: title || url, url });
  }
  return items;
}

function parseIndustryFromMarkdown(markdown: string): string {
  if (!markdown) return "";
  const lines = markdown.split(/\r?\n/);
  for (const line of lines) {
    const match = line.match(/^- Industry:\s*(.+)$/i);
    if (match) {
      return match[1].trim();
    }
  }
  return "";
}

function buildFactPackMap(rows: DbFactPackRow[]): Map<string, ResearchFactPack> {
  const factByRunId = new Map<string, ResearchFactPack>();
  for (const row of rows) {
    const ticker = (row.ticker || "").trim();
    const reportDate = (row.report_date || "").trim();
    if (!ticker || !reportDate) continue;
    factByRunId.set(
      toRunId(ticker, reportDate),
      buildFactPackViewFromRecord({
        collectedAt: String(row.collected_at || ""),
        collectedWithWebSearch: Boolean(row.collected_with_web_search),
        payload: parseJsonObject(row.payload_json),
        summaryMarkdown: row.summary_markdown || "",
        deltaSummaryMarkdown: row.delta_summary_markdown || "",
      })
    );
  }
  return factByRunId;
}

function buildRunsFromDbRows(
  docsRows: DbDocRow[],
  factRows: DbFactPackRow[],
  taskLookup: TaskLookup
): ResearchRun[] {
  const factByRunId = buildFactPackMap(factRows);
  const byRunId = new Map<string, ResearchRun>();
  for (const row of docsRows) {
    const ticker = (row.ticker || "").trim();
    const reportDate = (row.report_date || "").trim();
    if (!ticker || !reportDate) {
      continue;
    }
    const tickerKey = ticker.toUpperCase();
    const runId = toRunId(ticker, reportDate);
    const markdownIndustry = parseIndustryFromMarkdown(row.markdown || "");
    const runIndustry =
      taskLookup.industryByTicker.get(tickerKey) || markdownIndustry || "未分类";
    const displayCompany = chooseDisplayCompanyName(
      ticker,
      (row.company || "").trim() || ticker,
      taskLookup.companyByTicker.get(tickerKey)
    );

    let run = byRunId.get(runId);
    if (!run) {
      run = {
        runId,
        company: displayCompany,
        ticker,
        industry: runIndustry,
        date: reportDate,
        provider: (row.provider || "").trim() || "Unknown",
        model: (row.model || "").trim() || "Unknown",
        docs: [],
        factPack: factByRunId.get(runId) || null,
        delta: null,
        insightSummary: null,
      };
      byRunId.set(runId, run);
    }

    const fileName = path.basename((row.output_path || "").trim() || `${row.prompt_id}.md`);
    const doc: ResearchDoc = {
      id: (row.prompt_id || "").trim() || "0",
      question: (row.question || "").trim() || fileName.replace(/\.md$/i, ""),
      answer: row.answer_markdown || "",
      sources: parseDbSources(row.sources_json),
      meta: {
        company: displayCompany,
        ticker,
        date: reportDate,
        provider: run.provider,
        model: run.model,
        industry: run.industry,
      },
      fileName,
    };
    run.docs.push(doc);
  }

  const runs = Array.from(byRunId.values());
  for (const run of runs) {
    run.docs.sort((a, b) => a.fileName.localeCompare(b.fileName, "zh-CN", { numeric: true }));
    run.delta = run.factPack?.deltaSummaryMarkdown
      ? {
          summaryMarkdown: run.factPack.deltaSummaryMarkdown,
          highlights: extractDeltaHighlights(run.factPack.deltaSummaryMarkdown),
          previousReportDate: parsePreviousReportDateFromDelta(run.factPack.deltaSummaryMarkdown),
        }
      : null;
    run.insightSummary = buildInsightSummary(run.docs);
  }
  runs.sort((a, b) => {
    if (a.date !== b.date) return b.date.localeCompare(a.date, "en");
    return a.ticker.localeCompare(b.ticker, "en");
  });
  return runs;
}

async function queryFactPacksFromDb(
  pool: NonNullable<ReturnType<typeof getPostgresPool>>,
  whereClause = "",
  params: Array<string> = []
): Promise<DbFactPackRow[]> {
  try {
    const result = await pool.query<DbFactPackRow>(
      `
        SELECT
          company,
          ticker,
          report_date,
          collected_at,
          collected_with_web_search,
          payload_json,
          summary_markdown,
          delta_summary_markdown
        FROM rb_fact_packs
        ${whereClause}
      `,
      params
    );
    return result.rows;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (!message.includes("delta_summary_markdown")) {
      throw error;
    }
    const fallback = await pool.query<Omit<DbFactPackRow, "delta_summary_markdown">>(
      `
        SELECT
          company,
          ticker,
          report_date,
          collected_at,
          collected_with_web_search,
          payload_json,
          summary_markdown
        FROM rb_fact_packs
        ${whereClause}
      `,
      params
    );
    return fallback.rows.map((row) => ({
      ...row,
      delta_summary_markdown: "",
    }));
  }
}

async function getResearchRunsFromDb(): Promise<ResearchRun[]> {
  const pool = getPostgresPool();
  if (!pool) {
    return [];
  }

  const [docsResult, factRows, taskLookup] = await Promise.all([
    pool.query<DbDocRow>(
      `
        SELECT
          company,
          ticker,
          report_date,
          prompt_id,
          question,
          answer_markdown,
          sources_json,
          provider,
          model,
          output_path,
          markdown
        FROM rb_docs
        ORDER BY report_date DESC, ticker ASC, prompt_id ASC
      `
    ),
    queryFactPacksFromDb(pool),
    readTaskLookupFromDb(),
  ]);
  return buildRunsFromDbRows(docsResult.rows, factRows, taskLookup);
}

export async function getResearchRuns(): Promise<ResearchRun[]> {
  const cacheMs = readRunsCacheMs();
  const cache = getRunsCacheState();
  const now = Date.now();
  if (cacheMs > 0 && cache.expiresAt > now) {
    return cache.value;
  }
  if (cache.inFlight) {
    return cache.inFlight;
  }

  cache.inFlight = (async () => {
    if (hasPostgresDsn()) {
      try {
        const pgRuns = await getResearchRunsFromDb();
        if (pgRuns.length > 0) {
          return pgRuns;
        }
        console.warn(
          "Postgres is configured but returned 0 research runs. Falling back to local output."
        );
      } catch (error) {
        console.error("Failed to load research runs from postgres. Falling back to local output.", error);
      }
    } else if (!warnedNoDsn) {
      warnedNoDsn = true;
      console.warn(
        "POSTGRES_DSN/DATABASE_URL is not configured. Research pages will read local output only."
      );
    }

    return getResearchRunsFromLocal();
  })();

  try {
    const value = await cache.inFlight;
    cache.value = value;
    cache.expiresAt = cacheMs > 0 ? Date.now() + cacheMs : 0;
    return value;
  } finally {
    cache.inFlight = undefined;
  }
}

export async function getResearchRun(runId: string): Promise<ResearchRun | null> {
  if (hasPostgresDsn()) {
    try {
      const pool = getPostgresPool();
      if (pool) {
        const reportDate = runId.match(/(\d{4}-\d{2}-\d{2})$/)?.[1] || "";
        if (reportDate) {
          const docsResult = await pool.query<DbDocRow>(
            `
              SELECT
                company,
                ticker,
                report_date,
                prompt_id,
                question,
                answer_markdown,
                sources_json,
                provider,
                model,
                output_path,
                markdown
              FROM rb_docs
              WHERE report_date = $1
              ORDER BY ticker ASC, prompt_id ASC
            `,
            [reportDate]
          );
          const filteredDocs = docsResult.rows.filter(
            (row) => toRunId((row.ticker || "").trim(), (row.report_date || "").trim()) === runId
          );
          if (filteredDocs.length > 0) {
            const ticker = (filteredDocs[0].ticker || "").trim();
            const factRows = await queryFactPacksFromDb(
              pool,
              "WHERE ticker = $1 AND report_date = $2",
              [ticker, reportDate]
            );
            const taskLookup = await readTaskLookupFromDb();
            return buildRunsFromDbRows(filteredDocs, factRows, taskLookup)[0] ?? null;
          }
        }
      }
    } catch (error) {
      console.error("Failed direct getResearchRun lookup from postgres. Falling back to cached runs.", error);
    }
  }
  const runs = await getResearchRuns();
  return runs.find((run) => run.runId === runId) ?? null;
}

export async function getLatestRunByTicker(ticker: string): Promise<ResearchRun | null> {
  const normalized = ticker.trim().toUpperCase();
  if (!normalized) return null;
  if (hasPostgresDsn()) {
    try {
      const pool = getPostgresPool();
      if (pool) {
        const docsResult = await pool.query<DbDocRow>(
          `
            SELECT
              company,
              ticker,
              report_date,
              prompt_id,
              question,
              answer_markdown,
              sources_json,
              provider,
              model,
              output_path,
              markdown
            FROM rb_docs
            WHERE UPPER(ticker) = $1
            ORDER BY report_date DESC, prompt_id ASC
          `,
          [normalized]
        );
        if (docsResult.rows.length > 0) {
          const latestDate = (docsResult.rows[0].report_date || "").trim();
          const latestDocs = docsResult.rows.filter((row) => (row.report_date || "").trim() === latestDate);
          const factRows = await queryFactPacksFromDb(
            pool,
            "WHERE UPPER(ticker) = $1 AND report_date = $2",
            [normalized, latestDate]
          );
          const taskLookup = await readTaskLookupFromDb();
          return buildRunsFromDbRows(latestDocs, factRows, taskLookup)[0] ?? null;
        }
      }
    } catch (error) {
      console.error(
        "Failed direct getLatestRunByTicker lookup from postgres. Falling back to cached runs.",
        error
      );
    }
  }
  const runs = await getResearchRuns();
  const matched = runs.filter((run) => run.ticker.trim().toUpperCase() === normalized);
  if (matched.length === 0) return null;
  matched.sort((a, b) => {
    if (a.date !== b.date) return b.date.localeCompare(a.date, "en");
    return b.runId.localeCompare(a.runId, "en");
  });
  return matched[0];
}

export async function getLatestRunsByTicker(): Promise<ResearchRun[]> {
  const runs = await getResearchRuns();
  const latestByTicker = new Map<string, ResearchRun>();
  for (const run of runs) {
    const key = run.ticker.trim().toUpperCase();
    const prev = latestByTicker.get(key);
    if (!prev) {
      latestByTicker.set(key, run);
      continue;
    }
    if (run.date > prev.date || (run.date === prev.date && run.runId > prev.runId)) {
      latestByTicker.set(key, run);
    }
  }
  return Array.from(latestByTicker.values()).sort((a, b) =>
    a.company.localeCompare(b.company, "zh-CN")
  );
}
