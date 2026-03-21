import { Pool } from "pg";

function readDsn(): string {
  const dsn = (process.env.POSTGRES_DSN || process.env.DATABASE_URL || "").trim();
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
  const dsn = readDsn();
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
