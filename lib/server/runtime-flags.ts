function isTruthy(value: string | undefined): boolean {
  if (!value) return false;
  const v = value.trim().toLowerCase();
  return v === "1" || v === "true" || v === "yes" || v === "on";
}

export function isResearchJobsEnabled(): boolean {
  const explicit = process.env.ENABLE_RESEARCH_JOBS;
  if (explicit !== undefined) {
    return isTruthy(explicit);
  }
  return process.env.NODE_ENV !== "production";
}
