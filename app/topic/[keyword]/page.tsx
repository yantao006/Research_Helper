import type { Metadata } from "next";
import { unstable_cache } from "next/cache";
import Link from "next/link";
import { notFound } from "next/navigation";
import { searchKeywordHits } from "@/lib/server/keyword-search";
import { getSiteUrl } from "@/lib/server/site-url";

export const revalidate = 1800;
const getCachedKeywordHits = unstable_cache(
  async (keyword: string, limit: number) => searchKeywordHits(keyword, limit),
  ["keyword-hits-by-term"],
  { revalidate }
);

type PageProps = {
  params: Promise<{ keyword: string }>;
};

type MetadataProps = {
  params: Promise<{ keyword: string }>;
};

function decodeKeyword(raw: string): string {
  return decodeURIComponent(raw).trim();
}

export async function generateMetadata({ params }: MetadataProps): Promise<Metadata> {
  const { keyword } = await params;
  const kw = decodeKeyword(keyword);
  const canonical = `/topic/${encodeURIComponent(kw)}`;
  if (!kw) {
    return {
      title: "关键词专题未找到",
      description: "该关键词专题不存在。",
      alternates: { canonical },
      robots: { index: false, follow: true },
    };
  }
  const ogImage = "/og-cover.jpg";

  return {
    title: `${kw} 相关公司调研`,
    description: `查看关键词“${kw}”在公司研究报告中的命中内容与关联公司。`,
    alternates: { canonical },
    openGraph: {
      type: "website",
      title: `${kw} 相关公司调研`,
      description: `查看关键词“${kw}”在公司研究报告中的命中内容与关联公司。`,
      url: canonical,
      locale: "zh_CN",
      images: [ogImage],
    },
    twitter: {
      card: "summary_large_image",
      title: `${kw} 相关公司调研`,
      description: `查看关键词“${kw}”在公司研究报告中的命中内容与关联公司。`,
      images: [ogImage],
    },
  };
}

export default async function TopicKeywordPage({ params }: PageProps) {
  const { keyword } = await params;
  const kw = decodeKeyword(keyword);
  if (!kw) notFound();

  const items = await getCachedKeywordHits(kw, 60);
  if (items.length === 0) {
    notFound();
  }

  const siteUrl = getSiteUrl();
  const canonicalUrl = `${siteUrl}/topic/${encodeURIComponent(kw)}`;
  const jsonLd = JSON.stringify({
    "@context": "https://schema.org",
    "@type": "CollectionPage",
    name: `${kw} 相关公司调研`,
    description: `关键词“${kw}”在公司研究报告中的命中内容合集`,
    inLanguage: "zh-CN",
    url: canonicalUrl,
  });

  return (
    <main className="container">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: jsonLd }} />
      <div className="topbar">
        <Link href="/" className="back">
          ← 返回首页
        </Link>
      </div>
      <section className="panel" style={{ padding: "20px" }}>
        <h1 className="title" style={{ marginBottom: "8px" }}>
          关键词专题：{kw}
        </h1>
        <p className="subtitle" style={{ marginBottom: "16px" }}>
          共命中 {items.length} 条研究片段，按相关度排序。
        </p>
        <div style={{ display: "grid", gap: "10px" }}>
          {items.map((item, index) => (
            <article key={`${item.runId}-${item.docId}-${index}`} className="home-keyword-item">
              <div className="home-keyword-title">
                <strong>{item.company}</strong>
                <span className="meta">{item.ticker}</span>
              </div>
              <div className="home-keyword-question">{item.question}</div>
              <div className="home-keyword-snippet">{item.snippet}</div>
              <div style={{ marginTop: "8px", display: "flex", gap: "10px", flexWrap: "wrap" }}>
                <Link href={`/stock/${encodeURIComponent(item.ticker)}?tab=${encodeURIComponent(item.docId)}`}>
                  查看该公司最新报告章节
                </Link>
                <Link href={`/company/${encodeURIComponent(item.runId)}?tab=${encodeURIComponent(item.docId)}`}>
                  查看历史版本原文
                </Link>
              </div>
            </article>
          ))}
        </div>
      </section>
    </main>
  );
}
