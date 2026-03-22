import { NextRequest, NextResponse } from "next/server";
import { searchKeywordHits, type KeywordHit } from "@/lib/server/keyword-search";

export const runtime = "nodejs";

export async function GET(request: NextRequest) {
  const q = (request.nextUrl.searchParams.get("q") || "").trim();
  if (!q) {
    return NextResponse.json({ items: [] as KeywordHit[] });
  }
  const items = await searchKeywordHits(q, 20);
  return NextResponse.json({ items });
}

