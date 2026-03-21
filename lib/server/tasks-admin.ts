import fs from "fs";
import path from "path";

const TASKS_PATH = path.join(process.cwd(), "tasks.csv");

function parseCsvLine(line: string): string[] {
  const out: string[] = [];
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
      out.push(current);
      current = "";
      continue;
    }
    current += ch;
  }
  out.push(current);
  return out;
}

function toCsvCell(value: string): string {
  if (/[",\n\r]/.test(value)) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

export function upsertTaskRow(input: { company: string; ticker: string }): void {
  const ticker = input.ticker.trim().toUpperCase();
  const company = input.company.trim();

  if (!fs.existsSync(TASKS_PATH)) {
    const header = "company,Ticker,analyzed,analyzed_date\n";
    const row = `${toCsvCell(company)},${toCsvCell(ticker)},False,\n`;
    fs.writeFileSync(TASKS_PATH, `${header}${row}`, "utf-8");
    return;
  }

  const raw = fs.readFileSync(TASKS_PATH, "utf-8");
  const lines = raw.split(/\r?\n/).filter((line) => line.length > 0);
  if (lines.length === 0) {
    const header = "company,Ticker,analyzed,analyzed_date";
    lines.push(header);
  }

  const headers = parseCsvLine(lines[0]);
  const companyIdx = headers.indexOf("company");
  const tickerIdx = headers.indexOf("Ticker");
  const analyzedIdx = headers.indexOf("analyzed");
  const analyzedDateIdx = headers.indexOf("analyzed_date");

  if (companyIdx < 0 || tickerIdx < 0 || analyzedIdx < 0 || analyzedDateIdx < 0) {
    throw new Error("tasks.csv missing required headers");
  }

  let updated = false;
  const rowLines = lines.slice(1).map((line) => {
    const cells = parseCsvLine(line);
    while (cells.length < headers.length) cells.push("");
    if ((cells[tickerIdx] ?? "").trim().toUpperCase() === ticker) {
      cells[companyIdx] = company || cells[companyIdx];
      cells[analyzedIdx] = "False";
      cells[analyzedDateIdx] = "";
      updated = true;
    }
    return cells.map(toCsvCell).join(",");
  });

  if (!updated) {
    const cells = Array.from({ length: headers.length }, () => "");
    cells[companyIdx] = company;
    cells[tickerIdx] = ticker;
    cells[analyzedIdx] = "False";
    cells[analyzedDateIdx] = "";
    rowLines.push(cells.map(toCsvCell).join(","));
  }

  const nextRaw = `${lines[0]}\n${rowLines.join("\n")}\n`;
  fs.writeFileSync(TASKS_PATH, nextRaw, "utf-8");
}

