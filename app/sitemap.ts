import type { MetadataRoute } from "next";
import { getResearchRuns } from "@/lib/research";
import { getSiteUrl } from "@/lib/server/site-url";

export const revalidate = 3600;

function toDateOrNow(raw: string): Date {
  const parsed = Date.parse(raw);
  if (Number.isNaN(parsed)) {
    return new Date();
  }
  return new Date(parsed);
}

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const siteUrl = getSiteUrl();
  const runs = await getResearchRuns();
  const items: MetadataRoute.Sitemap = [
    {
      url: `${siteUrl}/`,
      lastModified: new Date(),
      changeFrequency: "daily",
      priority: 1,
    },
  ];

  const seen = new Set<string>();
  for (const run of runs) {
    if (seen.has(run.runId)) {
      continue;
    }
    seen.add(run.runId);
    items.push({
      url: `${siteUrl}/company/${encodeURIComponent(run.runId)}`,
      lastModified: toDateOrNow(run.date),
      changeFrequency: "weekly",
      priority: 0.8,
    });
  }
  return items;
}

