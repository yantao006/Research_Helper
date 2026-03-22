import { spawn } from "child_process";
import fs from "fs";
import { NextRequest, NextResponse } from "next/server";
import path from "path";
import { isResearchAdminRequest } from "@/lib/server/admin-auth";
import { appendLog, createJob, updateJob } from "@/lib/server/research-jobs";
import { isResearchJobsEnabled } from "@/lib/server/runtime-flags";
import { upsertTaskRow } from "@/lib/server/tasks-admin";

export const runtime = "nodejs";

function shanghaiDate(): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
}

const ALLOWED_PROVIDERS = new Set(["openai", "doubao", "siliconflow", "modelscope", "qwen", "zhipu"]);

type RateLimitState = {
  byIpWindow: Map<string, { count: number; windowStartMs: number }>;
};

declare global {
  // eslint-disable-next-line no-var
  var __researchRateLimitState: RateLimitState | undefined;
}

function getRateLimitState(): RateLimitState {
  if (!global.__researchRateLimitState) {
    global.__researchRateLimitState = { byIpWindow: new Map() };
  }
  return global.__researchRateLimitState;
}

function isRateLimited(ip: string | null): boolean {
  if (!ip) return false;
  const now = Date.now();
  const windowMs = 60_000;
  const maxPerWindow = 6;
  const state = getRateLimitState();
  const current = state.byIpWindow.get(ip);
  if (!current || now - current.windowStartMs > windowMs) {
    state.byIpWindow.set(ip, { count: 1, windowStartMs: now });
    return false;
  }
  if (current.count >= maxPerWindow) {
    return true;
  }
  current.count += 1;
  state.byIpWindow.set(ip, current);
  return false;
}

function resolvePythonExecutable(): string {
  const explicit = (process.env.PYTHON_BIN || "").trim();
  if (explicit) {
    return explicit;
  }

  const candidates = [
    path.join(process.cwd(), ".venv", "bin", "python"),
    path.join(process.cwd(), ".venv", "bin", "python3"),
  ];
  for (const candidate of candidates) {
    if (fs.existsSync(candidate)) {
      return candidate;
    }
  }
  return "python3";
}

export async function POST(request: NextRequest) {
  const isAdmin = isResearchAdminRequest(request);
  if (!isAdmin) {
    return NextResponse.json({ error: "admin auth required" }, { status: 401 });
  }
  if (!isResearchJobsEnabled() && !isAdmin) {
    return NextResponse.json(
      { error: "research job endpoint is disabled in this environment" },
      { status: 403 }
    );
  }
  if (isRateLimited(request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ?? null)) {
    return NextResponse.json({ error: "too many requests" }, { status: 429 });
  }

  const body = (await request.json()) as {
    company?: string;
    ticker?: string;
    provider?: string;
    reportDate?: string;
    forceRerun?: boolean;
  };

  const company = (body.company || "").trim();
  const ticker = (body.ticker || "").trim().toUpperCase();
  const provider = (body.provider || "doubao").trim();
  const reportDate = (body.reportDate || shanghaiDate()).trim();
  const forceRerun = Boolean(body.forceRerun);

  if (!company || company.length > 120 || !ticker || ticker.length > 24) {
    return NextResponse.json({ error: "company and ticker are required" }, { status: 400 });
  }
  if (!/^[\w\s\u4e00-\u9fff.\-()&]+$/.test(company)) {
    return NextResponse.json({ error: "invalid company format" }, { status: 400 });
  }
  if (!/^[A-Z0-9.\-]{1,24}$/.test(ticker)) {
    return NextResponse.json({ error: "invalid ticker format" }, { status: 400 });
  }
  if (!ALLOWED_PROVIDERS.has(provider)) {
    return NextResponse.json({ error: "provider is not allowed" }, { status: 400 });
  }

  upsertTaskRow({ company, ticker });
  const job = createJob({ company, ticker, provider, reportDate });
  updateJob(job.id, { status: "running" });

  const pythonBin = resolvePythonExecutable();
  const scriptPath = path.join(process.cwd(), "research_batch", "main.py");
  const cliArgs = [
    scriptPath,
    "--provider",
    provider,
    "--report-date",
    reportDate,
    "--only-ticker",
    ticker,
  ];
  if (forceRerun) {
    cliArgs.push("--force-rerun");
  }
  appendLog(job.id, `Using python executable: ${pythonBin}`);
  const child = spawn(
    pythonBin,
    cliArgs,
    {
      cwd: process.cwd(),
      env: process.env,
    }
  );

  child.stdout.setEncoding("utf-8");
  child.stderr.setEncoding("utf-8");
  child.stdout.on("data", (chunk: string) => {
    chunk
      .split(/\r?\n/)
      .filter((line) => line.trim().length > 0)
      .forEach((line) => appendLog(job.id, line));
  });
  child.stderr.on("data", (chunk: string) => {
    chunk
      .split(/\r?\n/)
      .filter((line) => line.trim().length > 0)
      .forEach((line) => appendLog(job.id, `[stderr] ${line}`));
  });
  child.on("close", (code) => {
    if (code === 0) {
      updateJob(job.id, { status: "success" });
    } else {
      updateJob(job.id, { status: "failed", error: `Process exited with code ${code}` });
    }
  });
  child.on("error", (err) => {
    updateJob(job.id, { status: "failed", error: err.message });
  });

  return NextResponse.json({
    jobId: job.id,
    status: job.status,
  });
}
