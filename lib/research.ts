import fs from "fs";
import path from "path";

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

const OUTPUT_ROOT = path.join(process.cwd(), "output");
const TASKS_CSV = path.join(process.cwd(), "tasks.csv");
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

function readTaskCompanyByTicker(): Map<string, string> {
  const mapping = new Map<string, string>();
  if (!fs.existsSync(TASKS_CSV)) {
    return mapping;
  }
  const raw = fs.readFileSync(TASKS_CSV, "utf-8");
  const lines = raw.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (lines.length === 0) {
    return mapping;
  }

  const headers = parseCsvLine(lines[0]).map((h) => h.trim());
  const companyIdx = headers.findIndex((h) => h === "company");
  const tickerIdx = headers.findIndex((h) => h === "Ticker");
  if (companyIdx < 0 || tickerIdx < 0) {
    return mapping;
  }

  for (let i = 1; i < lines.length; i += 1) {
    const cells = parseCsvLine(lines[i]);
    const company = (cells[companyIdx] ?? "").trim();
    const ticker = (cells[tickerIdx] ?? "").trim().toUpperCase();
    if (!company || !ticker) {
      continue;
    }
    mapping.set(ticker, company);
  }
  return mapping;
}

function chooseDisplayCompanyName(ticker: string, metaCompany: string, taskCompany: string | undefined): string {
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

function readMarkdownFiles(dirPath: string): string[] {
  return fs
    .readdirSync(dirPath)
    .filter((name) => name.endsWith(".md"))
    .sort((a, b) => a.localeCompare(b, "zh-CN", { numeric: true }));
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
    industry: meta.industry ?? "未分类"
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
    fileName
  };
}

export function getResearchRuns(): ResearchRun[] {
  if (!fs.existsSync(OUTPUT_ROOT)) {
    return [];
  }

  const runDirs = fs
    .readdirSync(OUTPUT_ROOT, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort((a, b) => b.localeCompare(a, "en"));

  const taskCompanyByTicker = readTaskCompanyByTicker();
  const runs: ResearchRun[] = [];
  for (const runId of runDirs) {
    const dirPath = path.join(OUTPUT_ROOT, runId);
    const mdFiles = readMarkdownFiles(dirPath);
    if (mdFiles.length === 0) {
      continue;
    }
    const docs = mdFiles.map((fileName) => parseDoc(path.join(dirPath, fileName), fileName));
    const first = docs[0];
    const displayCompany = chooseDisplayCompanyName(
      first.meta.ticker,
      first.meta.company,
      taskCompanyByTicker.get(first.meta.ticker.toUpperCase())
    );
    runs.push({
      runId,
      company: displayCompany,
      ticker: first.meta.ticker,
      industry: first.meta.industry,
      date: first.meta.date,
      provider: first.meta.provider,
      model: first.meta.model,
      docs
    });
  }

  return runs;
}

export function getResearchRun(runId: string): ResearchRun | null {
  const runs = getResearchRuns();
  return runs.find((run) => run.runId === runId) ?? null;
}
