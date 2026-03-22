import { NextRequest, NextResponse } from "next/server";
import {
  createResearchAdminSessionToken,
  getResearchAdminCookieName,
  getResearchAdminSessionTtlSeconds,
  isResearchAdminConfigured,
  validateResearchAdminPassword,
} from "@/lib/server/admin-auth";

export const runtime = "nodejs";

export async function POST(request: NextRequest) {
  if (!isResearchAdminConfigured()) {
    return NextResponse.json({ error: "admin auth is not configured" }, { status: 503 });
  }

  const body = (await request.json()) as { password?: string };
  const password = (body.password || "").trim();
  if (!validateResearchAdminPassword(password)) {
    return NextResponse.json({ error: "invalid password" }, { status: 401 });
  }

  const response = NextResponse.json({ ok: true });
  response.cookies.set({
    name: getResearchAdminCookieName(),
    value: createResearchAdminSessionToken(),
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    path: "/",
    maxAge: getResearchAdminSessionTtlSeconds(),
  });
  return response;
}
