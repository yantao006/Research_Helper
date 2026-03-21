import { NextRequest, NextResponse } from "next/server";
import { getJob } from "@/lib/server/research-jobs";
import { isResearchJobsEnabled } from "@/lib/server/runtime-flags";

export const runtime = "nodejs";

export async function GET(
  _request: NextRequest,
  context: { params: Promise<{ jobId: string }> }
) {
  if (!isResearchJobsEnabled()) {
    return NextResponse.json(
      { error: "research job endpoint is disabled in this environment" },
      { status: 403 }
    );
  }
  const { jobId } = await context.params;
  const job = getJob(jobId);
  if (!job) {
    return NextResponse.json({ error: "job not found" }, { status: 404 });
  }
  return NextResponse.json(job);
}
