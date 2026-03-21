import { spawn } from "child_process";
import { NextRequest, NextResponse } from "next/server";
import path from "path";
import { appendLog, createJob, updateJob } from "@/lib/server/research-jobs";
import { upsertTaskRow } from "@/lib/server/tasks-admin";

function shanghaiDate(): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
}

export async function POST(request: NextRequest) {
  const body = (await request.json()) as {
    company?: string;
    ticker?: string;
    provider?: string;
    reportDate?: string;
  };

  const company = (body.company || "").trim();
  const ticker = (body.ticker || "").trim().toUpperCase();
  const provider = (body.provider || "doubao").trim();
  const reportDate = (body.reportDate || shanghaiDate()).trim();

  if (!company || !ticker) {
    return NextResponse.json({ error: "company and ticker are required" }, { status: 400 });
  }

  upsertTaskRow({ company, ticker });
  const job = createJob({ company, ticker, provider, reportDate });
  updateJob(job.id, { status: "running" });

  const scriptPath = path.join(process.cwd(), "research_batch", "main.py");
  const child = spawn(
    "python3",
    [
      scriptPath,
      "--provider",
      provider,
      "--report-date",
      reportDate,
      "--only-ticker",
      ticker,
    ],
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

