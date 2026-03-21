import { getPostgresPool } from "@/lib/server/postgres";

export type KeywordHit = {
  runId: string;
  company: string;
  ticker: string;
  docId: string;
  question: string;
  snippet: string;
  score: number;
};

type DbRow = {
  ticker: string;
  company: string;
  report_date: string;
  prompt_id: string;
  question: string;
  answer_markdown: string;
  seo_match_count: number;
};

function stripMarkdown(raw: string): string {
  return raw
    .replace(/```[\s\S]*?```/g, " ")
    .replace(/`([^`]+)`/g, "$1")
    .replace(/!\[([^\]]*)\]\([^)]+\)/g, "$1")
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/<[^>]+>/g, " ")
    .replace(/[*_~>#-]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function countOccurrences(text: string, needle: string): number {
  if (!needle) return 0;
  let count = 0;
  let index = 0;
  while (index < text.length) {
    const found = text.indexOf(needle, index);
    if (found < 0) break;
    count += 1;
    index = found + needle.length;
  }
  return count;
}

function buildSnippet(text: string, query: string): string {
  if (!text) return "";
  const lowerText = text.toLowerCase();
  const lowerQuery = query.toLowerCase();
  const at = lowerText.indexOf(lowerQuery);
  if (at < 0) {
    return text.slice(0, 120);
  }
  const start = Math.max(0, at - 42);
  const end = Math.min(text.length, at + query.length + 78);
  const prefix = start > 0 ? "…" : "";
  const suffix = end < text.length ? "…" : "";
  return `${prefix}${text.slice(start, end)}${suffix}`;
}

function sanitizeForRunId(value: string): string {
  const sanitized = value.trim().replace(/[^\w-]+/g, "_").replace(/^_+|_+$/g, "");
  return sanitized || "untitled";
}

function toRunId(ticker: string, reportDate: string): string {
  return `${sanitizeForRunId(ticker)}_${reportDate}`;
}

export async function searchKeywordHitsFromDb(query: string, limit = 20): Promise<KeywordHit[]> {
  const pool = getPostgresPool();
  if (!pool) {
    return [];
  }
  const trimmed = query.trim();
  if (!trimmed) {
    return [];
  }
  const qLower = trimmed.toLowerCase();
  const pattern = `%${qLower}%`;
  const safeLimit = Math.min(Math.max(limit, 1), 100);
  const fetchRows = Math.max(100, safeLimit * 5);

  const sql = `
    SELECT
      d.ticker,
      d.company,
      d.report_date,
      d.prompt_id,
      d.question,
      d.answer_markdown,
      COALESCE(SUM(CASE WHEN sk.keyword_norm LIKE $2 THEN 1 ELSE 0 END), 0)::int AS seo_match_count
    FROM rb_docs d
    LEFT JOIN rb_seo_keywords sk ON sk.sync_key = d.sync_key
    WHERE
      lower(d.company) LIKE $2
      OR lower(d.ticker) LIKE $2
      OR lower(d.question) LIKE $2
      OR lower(d.answer_markdown) LIKE $2
      OR EXISTS (
        SELECT 1
        FROM rb_seo_keywords sk2
        WHERE sk2.sync_key = d.sync_key AND sk2.keyword_norm LIKE $2
      )
    GROUP BY d.sync_key, d.ticker, d.company, d.report_date, d.prompt_id, d.question, d.answer_markdown
    ORDER BY d.report_date DESC
    LIMIT $1
  `;

  const result = await pool.query<DbRow>(sql, [fetchRows, pattern]);
  const items: KeywordHit[] = result.rows.map((row: DbRow) => {
    const company = row.company || "";
    const ticker = row.ticker || "";
    const question = row.question || "";
    const answerPlain = stripMarkdown(row.answer_markdown || "");

    const companyHits = countOccurrences(company.toLowerCase(), qLower);
    const tickerHits = countOccurrences(ticker.toLowerCase(), qLower);
    const questionHits = countOccurrences(question.toLowerCase(), qLower);
    const answerHits = countOccurrences(answerPlain.toLowerCase(), qLower);
    const score =
      companyHits * 8 +
      tickerHits * 7 +
      questionHits * 4 +
      answerHits +
      Math.max(0, row.seo_match_count || 0) * 12;

    return {
      runId: toRunId(ticker, row.report_date || ""),
      company,
      ticker,
      docId: row.prompt_id || "0",
      question,
      snippet: buildSnippet(answerPlain, trimmed),
      score,
    };
  });
  items.sort((a, b) => b.score - a.score);
  return items.slice(0, safeLimit);
}
