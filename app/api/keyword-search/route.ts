import { NextRequest, NextResponse } from "next/server";
import { getResearchRuns } from "@/lib/research";

export const runtime = "nodejs";

type KeywordHit = {
  runId: string;
  company: string;
  ticker: string;
  docId: string;
  question: string;
  snippet: string;
  score: number;
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

export async function GET(request: NextRequest) {
  const q = (request.nextUrl.searchParams.get("q") || "").trim();
  if (!q) {
    return NextResponse.json({ items: [] as KeywordHit[] });
  }

  const runs = getResearchRuns();
  const qLower = q.toLowerCase();
  const hits: KeywordHit[] = [];

  for (const run of runs) {
    for (const doc of run.docs) {
      const answerPlain = stripMarkdown(doc.answer || "");
      const question = doc.question || "";
      const company = run.company || "";
      const ticker = run.ticker || "";
      const haystack = `${company} ${ticker} ${question} ${answerPlain}`.toLowerCase();
      if (!haystack.includes(qLower)) {
        continue;
      }

      const companyHits = countOccurrences(company.toLowerCase(), qLower);
      const tickerHits = countOccurrences(ticker.toLowerCase(), qLower);
      const questionHits = countOccurrences(question.toLowerCase(), qLower);
      const answerHits = countOccurrences(answerPlain.toLowerCase(), qLower);
      const score = companyHits * 8 + tickerHits * 7 + questionHits * 4 + answerHits;

      hits.push({
        runId: run.runId,
        company,
        ticker,
        docId: doc.id,
        question,
        snippet: buildSnippet(answerPlain, q),
        score,
      });
    }
  }

  hits.sort((a, b) => b.score - a.score);
  return NextResponse.json({ items: hits.slice(0, 20) });
}
