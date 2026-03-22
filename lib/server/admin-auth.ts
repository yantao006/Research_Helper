import { createHmac, timingSafeEqual } from "crypto";
import { cookies } from "next/headers";
import { NextRequest } from "next/server";

const ADMIN_COOKIE_NAME = "rh_admin_session";
const SESSION_TTL_SECONDS = 60 * 60 * 24 * 14;

function readSecret(): string {
  return (process.env.RESEARCH_ADMIN_SECRET || "").trim();
}

function readPassword(): string {
  return (process.env.RESEARCH_ADMIN_PASSWORD || "").trim();
}

function sign(value: string, secret: string): string {
  return createHmac("sha256", secret).update(value).digest("base64url");
}

function safeEqual(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);
  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }
  return timingSafeEqual(leftBuffer, rightBuffer);
}

export function isResearchAdminConfigured(): boolean {
  return Boolean(readPassword() && readSecret());
}

export function validateResearchAdminPassword(password: string): boolean {
  const expected = readPassword();
  if (!expected || !password) {
    return false;
  }
  return safeEqual(password, expected);
}

export function createResearchAdminSessionToken(): string {
  const secret = readSecret();
  if (!secret) {
    throw new Error("RESEARCH_ADMIN_SECRET is not configured");
  }
  const payload = JSON.stringify({
    exp: Date.now() + SESSION_TTL_SECONDS * 1000,
  });
  const encoded = Buffer.from(payload, "utf-8").toString("base64url");
  const signature = sign(encoded, secret);
  return `${encoded}.${signature}`;
}

function verifySessionToken(token: string | undefined): boolean {
  const secret = readSecret();
  if (!secret || !token) {
    return false;
  }
  const [encoded, signature] = token.split(".");
  if (!encoded || !signature) {
    return false;
  }
  const expectedSignature = sign(encoded, secret);
  if (!safeEqual(signature, expectedSignature)) {
    return false;
  }
  try {
    const payload = JSON.parse(Buffer.from(encoded, "base64url").toString("utf-8")) as {
      exp?: number;
    };
    return typeof payload.exp === "number" && payload.exp > Date.now();
  } catch {
    return false;
  }
}

export function isResearchAdminRequest(request: NextRequest): boolean {
  return verifySessionToken(request.cookies.get(ADMIN_COOKIE_NAME)?.value);
}

export async function getResearchAdminSession(): Promise<boolean> {
  const cookieStore = await cookies();
  return verifySessionToken(cookieStore.get(ADMIN_COOKIE_NAME)?.value);
}

export function getResearchAdminCookieName(): string {
  return ADMIN_COOKIE_NAME;
}

export function getResearchAdminSessionTtlSeconds(): number {
  return SESSION_TTL_SECONDS;
}
