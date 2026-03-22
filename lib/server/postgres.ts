import { Pool } from "pg";

function readDsn(): string {
  const dsn = (process.env.POSTGRES_DSN || process.env.DATABASE_URL || "").trim();
  return dsn;
}

function normalizeDsnForNodePg(dsn: string): string {
  // Node pg currently treats sslmode=require as verify-full unless uselibpqcompat=true is set.
  // We normalize to libpq-compatible behavior to avoid surprising cert validation failures.
  try {
    const url = new URL(dsn);
    const sslmode = (url.searchParams.get("sslmode") || "").toLowerCase();
    const hasCompat = url.searchParams.has("uselibpqcompat");
    if (sslmode === "require" && !hasCompat) {
      url.searchParams.set("uselibpqcompat", "true");
      return url.toString();
    }
  } catch {
    // Keep original DSN if it is not URL-parseable.
  }
  return dsn;
}

declare global {
  // eslint-disable-next-line no-var
  var __researchPgPool: Pool | undefined;
}

export function hasPostgresDsn(): boolean {
  return readDsn().length > 0;
}

export function getPostgresPool(): Pool | null {
  const rawDsn = readDsn();
  const dsn = normalizeDsnForNodePg(rawDsn);
  if (!dsn) {
    return null;
  }
  if (!global.__researchPgPool) {
    global.__researchPgPool = new Pool({
      connectionString: dsn,
      max: 5,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 8_000,
    });
  }
  return global.__researchPgPool;
}
