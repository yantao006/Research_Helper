import { randomUUID } from "crypto";

export type ResearchJobStatus = "queued" | "running" | "success" | "failed";

export type ResearchJob = {
  id: string;
  company: string;
  ticker: string;
  provider: string;
  reportDate: string;
  status: ResearchJobStatus;
  createdAt: number;
  updatedAt: number;
  logs: string[];
  error?: string;
};

type JobsState = {
  jobs: Map<string, ResearchJob>;
};

declare global {
  // eslint-disable-next-line no-var
  var __researchJobsState: JobsState | undefined;
}

function getState(): JobsState {
  if (!global.__researchJobsState) {
    global.__researchJobsState = { jobs: new Map() };
  }
  return global.__researchJobsState;
}

export function createJob(input: {
  company: string;
  ticker: string;
  provider: string;
  reportDate: string;
}): ResearchJob {
  const now = Date.now();
  const job: ResearchJob = {
    id: randomUUID(),
    company: input.company,
    ticker: input.ticker,
    provider: input.provider,
    reportDate: input.reportDate,
    status: "queued",
    createdAt: now,
    updatedAt: now,
    logs: [],
  };
  getState().jobs.set(job.id, job);
  return job;
}

export function getJob(id: string): ResearchJob | null {
  return getState().jobs.get(id) ?? null;
}

export function appendLog(id: string, line: string): void {
  const job = getState().jobs.get(id);
  if (!job) return;
  job.logs.push(line);
  if (job.logs.length > 500) {
    job.logs = job.logs.slice(-500);
  }
  job.updatedAt = Date.now();
}

export function updateJob(
  id: string,
  updates: Partial<Pick<ResearchJob, "status" | "error">>
): void {
  const job = getState().jobs.get(id);
  if (!job) return;
  if (updates.status) job.status = updates.status;
  if (updates.error !== undefined) job.error = updates.error;
  job.updatedAt = Date.now();
}

