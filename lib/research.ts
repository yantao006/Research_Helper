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

export type ResearchRun = {
  runId: string;
  company: string;
  ticker: string;
  industry: string;
  date: string;
  provider: string;
  model: string;
  docs: ResearchDoc[];
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
    runs.push({
      runId,
      company: displayCompany,
      ticker: first.meta.ticker,
      industry,
      date: first.meta.date,
      provider: first.meta.provider,
      model: first.meta.model,
      docs,
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

async function getResearchRunsFromDb(): Promise<ResearchRun[]> {
  const pool = getPostgresPool();
  if (!pool) {
    return [];
  }

  const [docsResult, taskLookup] = await Promise.all([
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
    readTaskLookupFromDb(),
  ]);

  const byRunId = new Map<string, ResearchRun>();
  for (const row of docsResult.rows) {
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
  }
  runs.sort((a, b) => {
    if (a.date !== b.date) return b.date.localeCompare(a.date, "en");
    return a.ticker.localeCompare(b.ticker, "en");
  });
  return runs;
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
  const runs = await getResearchRuns();
  return runs.find((run) => run.runId === runId) ?? null;
}

export async function getLatestRunByTicker(ticker: string): Promise<ResearchRun | null> {
  const normalized = ticker.trim().toUpperCase();
  if (!normalized) return null;
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
