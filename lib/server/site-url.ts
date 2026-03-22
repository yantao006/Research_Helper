function toAbsoluteUrl(raw: string): string {
  const trimmed = raw.trim();
  if (!trimmed) return "";
  if (/^https?:\/\//i.test(trimmed)) {
    return trimmed;
  }
  return `https://${trimmed}`;
}

function normalizeOrigin(raw: string): string | null {
  const absolute = toAbsoluteUrl(raw);
  if (!absolute) return null;
  try {
    const url = new URL(absolute);
    return url.origin;
  } catch {
    return null;
  }
}

export function getSiteUrl(): string {
  const candidates = [
    process.env.NEXT_PUBLIC_SITE_URL,
    process.env.SITE_URL,
    process.env.VERCEL_PROJECT_PRODUCTION_URL,
    process.env.VERCEL_URL,
  ];
  for (const candidate of candidates) {
    const origin = normalizeOrigin(candidate || "");
    if (origin) {
      return origin;
    }
  }
  if ((process.env.NODE_ENV || "").toLowerCase() === "production") {
    return "https://research-helper.vercel.app";
  }
  return "http://localhost:3000";
}

