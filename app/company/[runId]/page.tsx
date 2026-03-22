import type { Metadata } from "next";
import { unstable_cache } from "next/cache";
import { notFound } from "next/navigation";
import { getLatestRunByTicker, getResearchRun } from "@/lib/research";
import { getSiteUrl } from "@/lib/server/site-url";
import ReportView from "../report-view";

export const revalidate = 1800;
const getCachedResearchRun = unstable_cache(
  async (runId: string) => getResearchRun(runId),
  ["research-run-by-id"],
  { revalidate }
);
const getCachedLatestRunByTicker = unstable_cache(
  async (ticker: string) => getLatestRunByTicker(ticker),
  ["latest-run-by-ticker"],
  { revalidate }
);

type PageProps = {
  params: Promise<{ runId: string }>;
  searchParams: Promise<{ tab?: string }>;
};

type MetadataProps = {
  params: Promise<{ runId: string }>;
};

function buildMetaDescription(run: Awaited<ReturnType<typeof getResearchRun>>): string {
  if (!run) return "公司调研结果未找到。";
  const firstDoc = run.docs[0];
  const answerPreview = (firstDoc?.answer || "")
    .replace(/[#>*_`~\-]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 120);
  const prefix = `${run.company}（${run.ticker}）调研结论与核心要点。`;
  if (!answerPreview) {
    return `${prefix} 含商业模式、财务质量、风险与估值分析。`;
  }
  return `${prefix} ${answerPreview}`;
}

function toArticleJsonLd(
  run: NonNullable<Awaited<ReturnType<typeof getResearchRun>>>,
  canonicalUrl: string
) {
  return {
    "@context": "https://schema.org",
    "@type": "Article",
    headline: `${run.company}（${run.ticker}）调研报告`,
    description: buildMetaDescription(run),
    inLanguage: "zh-CN",
    datePublished: run.date,
    dateModified: run.date,
    author: {
      "@type": "Organization",
      name: "Research Helper Research Team",
    },
    publisher: {
      "@type": "Organization",
      name: "Research Helper",
    },
    mainEntityOfPage: canonicalUrl,
  };
}

export async function generateMetadata({ params }: MetadataProps): Promise<Metadata> {
  const { runId } = await params;
  const decodedRunId = decodeURIComponent(runId);
  const run = await getCachedResearchRun(decodedRunId);
  const fallbackCanonical = `/company/${encodeURIComponent(decodedRunId)}`;

  if (!run) {
    return {
      title: "公司调研结果未找到",
      description: "该公司调研结果不存在或已被移除。",
      alternates: { canonical: fallbackCanonical },
      robots: { index: false, follow: true },
    };
  }

  const canonicalPath = `/stock/${encodeURIComponent(run.ticker.toUpperCase())}`;
  const latest = await getCachedLatestRunByTicker(run.ticker);
  const isLatest = latest?.runId === run.runId;
  const title = `${run.company}（${run.ticker}）调研报告`;
  const description = buildMetaDescription(run);
  const ogImage = "/og-cover.jpg";

  return {
    title,
    description,
    alternates: { canonical: canonicalPath },
    openGraph: {
      type: "article",
      title,
      description,
      url: canonicalPath,
      locale: "zh_CN",
      images: [ogImage],
    },
    twitter: {
      card: "summary_large_image",
      title,
      description,
      images: [ogImage],
    },
    robots: isLatest ? { index: true, follow: true } : { index: false, follow: true },
  };
}

export default async function CompanyPage({ params, searchParams }: PageProps) {
  const { runId } = await params;
  const query = await searchParams;
  const decodedRunId = decodeURIComponent(runId);
  const run = await getCachedResearchRun(decodedRunId);
  if (!run) notFound();

  const siteUrl = getSiteUrl();
  const canonicalUrl = `${siteUrl}/stock/${encodeURIComponent(run.ticker.toUpperCase())}`;
  const jsonLd = JSON.stringify(toArticleJsonLd(run, canonicalUrl));

  return (
    <>
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: jsonLd }} />
      <ReportView
        run={run}
        activeTab={query.tab}
        basePath={`/company/${encodeURIComponent(run.runId)}`}
        backHref="/"
        backLabel="返回首页"
      />
    </>
  );
}
