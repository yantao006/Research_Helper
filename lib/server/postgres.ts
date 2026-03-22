import { Pool } from "pg";

function readDsn(): string {
  const dsn = (process.env.POSTGRES_DSN || process.env.DATABASE_URL || "").trim();
  return dsn;
}

function readPositiveInt(name: string, fallback: number): number {
  const raw = (process.env[name] || "").trim();
  if (!raw) return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.floor(n);
}

function defaultPoolMax(): number {
  // Serverless environments should keep per-instance pool tiny to avoid exploding client count.
  if ((process.env.NODE_ENV || "").toLowerCase() === "production") {
    return 1;
  }
  return 5;
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

function summarizeDsnEndpoint(dsn: string): string {
  try {
    const url = new URL(dsn);
    const host = url.hostname || "unknown-host";
    const port = url.port || "5432";
    return `${host}:${port}`;
  } catch {
    return "unparseable-dsn";
  }
}

function shouldWarnSessionModePooler(dsn: string): boolean {
  try {
    const url = new URL(dsn);
    const host = (url.hostname || "").toLowerCase();
    const port = url.port || "5432";
    return host.includes("pooler.supabase.com") && port === "5432";
  } catch {
    return false;
  }
}

declare global {
  // eslint-disable-next-line no-var
  var __researchPgPool: Pool | undefined;
  // eslint-disable-next-line no-var
  var __researchPgPoolEndpointLogged: boolean | undefined;
  // eslint-disable-next-line no-var
  var __researchPgSessionModeWarned: boolean | undefined;
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
  if (!global.__researchPgSessionModeWarned && shouldWarnSessionModePooler(dsn)) {
    global.__researchPgSessionModeWarned = true;
    console.warn(
      "Supabase session-mode pooler detected (:5432). In serverless production this can trigger MaxClientsInSessionMode. Prefer transaction-mode pooler (:6543)."
    );
  }
  if (!global.__researchPgPool) {
    const poolMax = readPositiveInt("PG_POOL_MAX", defaultPoolMax());
    const idleTimeoutMillis = readPositiveInt("PG_IDLE_TIMEOUT_MS", 30_000);
    const connectionTimeoutMillis = readPositiveInt("PG_CONNECT_TIMEOUT_MS", 8_000);
    global.__researchPgPool = new Pool({
      connectionString: dsn,
      max: poolMax,
      idleTimeoutMillis,
      connectionTimeoutMillis,
    });
  }
  if (!global.__researchPgPoolEndpointLogged) {
    global.__researchPgPoolEndpointLogged = true;
    console.info(
      `Postgres pool initialized endpoint=${summarizeDsnEndpoint(dsn)} max=${global.__researchPgPool.options.max}`
    );
  }
  return global.__researchPgPool;
}
