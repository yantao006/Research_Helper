import type { MetadataRoute } from "next";
import { getLatestRunsByTicker } from "@/lib/research";
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
  const runs = await getLatestRunsByTicker();
  const items: MetadataRoute.Sitemap = [
    {
      url: `${siteUrl}/`,
      lastModified: new Date(),
      changeFrequency: "daily",
      priority: 1,
    },
  ];

  const topicSet = new Set<string>();
  for (const run of runs) {
    items.push({
      url: `${siteUrl}/stock/${encodeURIComponent(run.ticker.toUpperCase())}`,
      lastModified: toDateOrNow(run.date),
      changeFrequency: "weekly",
      priority: 0.9,
    });
    for (const doc of run.docs) {
      const lines = (doc.answer || "").split(/\r?\n/);
      const sectionStart = lines.findIndex((line) => line.trim() === "## SEO 关键词");
      if (sectionStart < 0) continue;
      let sectionEnd = lines.length;
      for (let i = sectionStart + 1; i < lines.length; i += 1) {
        if (/^##\s+/.test(lines[i].trim())) {
          sectionEnd = i;
          break;
        }
      }
      for (const line of lines.slice(sectionStart + 1, sectionEnd)) {
        const linked = line.trim().match(/^- \[([^\]]+)\]\([^)]+\)$/);
        const bullet = line.trim().match(/^- (.+)$/);
        const keyword = (linked?.[1] || bullet?.[1] || "").trim();
        if (!keyword) continue;
        topicSet.add(keyword);
      }
    }
  }

  for (const keyword of Array.from(topicSet).slice(0, 300)) {
    items.push({
      url: `${siteUrl}/topic/${encodeURIComponent(keyword)}`,
      lastModified: new Date(),
      changeFrequency: "weekly",
      priority: 0.6,
    });
  }
  return items;
}
