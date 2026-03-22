import type { Metadata } from "next";
import { notFound } from "next/navigation";
import { getLatestRunByTicker } from "@/lib/research";
import { getSiteUrl } from "@/lib/server/site-url";
import ReportView from "@/app/company/report-view";

export const dynamic = "force-dynamic";

type PageProps = {
  params: Promise<{ ticker: string }>;
  searchParams: Promise<{ tab?: string }>;
};

type MetadataProps = {
  params: Promise<{ ticker: string }>;
};

function normalizeTicker(raw: string): string {
  const t = decodeURIComponent(raw).trim().toUpperCase();
  if (t.endsWith(".SS")) return `${t.slice(0, -3)}.SH`;
  return t;
}

function buildMetaDescription(company: string, ticker: string, preview: string): string {
  const prefix = `${company}（${ticker}）最新调研报告，覆盖核心结论、风险与估值要点。`;
  if (!preview) return prefix;
  return `${prefix} ${preview.slice(0, 120)}`;
}

function toArticleJsonLd(
  run: NonNullable<Awaited<ReturnType<typeof getLatestRunByTicker>>>,
  canonicalUrl: string
) {
  const preview = (run.docs[0]?.answer || "").replace(/\s+/g, " ").trim().slice(0, 160);
  return {
    "@context": "https://schema.org",
    "@type": "Article",
    headline: `${run.company}（${run.ticker}）最新调研报告`,
    description: buildMetaDescription(run.company, run.ticker, preview),
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
  const { ticker } = await params;
  const normalizedTicker = normalizeTicker(ticker);
  const run = await getLatestRunByTicker(normalizedTicker);
  const canonicalPath = `/stock/${encodeURIComponent(normalizedTicker)}`;

  if (!run) {
    return {
      title: `${normalizedTicker} 调研结果未找到`,
      description: `未找到 ${normalizedTicker} 对应的调研结果。`,
      alternates: { canonical: canonicalPath },
      robots: { index: false, follow: true },
    };
  }

  const preview = (run.docs[0]?.answer || "").replace(/\s+/g, " ").trim();
  const title = `${run.company}（${run.ticker}）最新调研报告`;
  const description = buildMetaDescription(run.company, run.ticker, preview);
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
    },
    twitter: {
      card: "summary_large_image",
      title,
      description,
    },
    robots: { index: true, follow: true },
  };
}

export default async function StockPage({ params, searchParams }: PageProps) {
  const { ticker } = await params;
  const query = await searchParams;
  const normalizedTicker = normalizeTicker(ticker);
  const run = await getLatestRunByTicker(normalizedTicker);
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
        basePath={`/stock/${encodeURIComponent(run.ticker.toUpperCase())}`}
        backHref="/"
        backLabel="返回首页"
      />
    </>
  );
}

