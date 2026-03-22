import type { Metadata } from "next";
import { unstable_cache } from "next/cache";
import { getResearchRuns } from "@/lib/research";
import { getResearchAdminSession, isResearchAdminConfigured } from "@/lib/server/admin-auth";
import { isResearchJobsEnabled } from "@/lib/server/runtime-flags";
import { getSiteUrl } from "@/lib/server/site-url";
import HomeDashboard from "./home-dashboard";

export const revalidate = 300;
const getCachedResearchRuns = unstable_cache(
  async () => getResearchRuns(),
  ["research-runs-home"],
  { revalidate }
);

export const metadata: Metadata = {
  title: "上市公司调研结果与搜索",
  description: "快速检索公司代码或名称，查看已生成的上市公司调研结论与历史研究内容。",
  alternates: {
    canonical: "/",
  },
  openGraph: {
    title: "上市公司调研结果与搜索",
    description: "快速检索公司代码或名称，查看已生成的上市公司调研结论与历史研究内容。",
    url: "/",
    images: ["/og-cover.jpg"],
  },
  twitter: {
    card: "summary_large_image",
    title: "上市公司调研结果与搜索",
    description: "快速检索公司代码或名称，查看已生成的上市公司调研结论与历史研究内容。",
    images: ["/og-cover.jpg"],
  },
};

type HomePageProps = {
  searchParams: Promise<{ q?: string; kw?: string }>;
};

export default async function HomePage({ searchParams }: HomePageProps) {
  const query = await searchParams;
  const runs = await getCachedResearchRuns();
  const initialQuery = (query.kw || query.q || "").trim();
  const researchJobsEnabled = isResearchJobsEnabled();
  const adminConfigured = isResearchAdminConfigured();
  const canManageResearch = adminConfigured && (await getResearchAdminSession());
  const siteUrl = getSiteUrl();
  const websiteJsonLd = JSON.stringify({
    "@context": "https://schema.org",
    "@type": "WebSite",
    name: "公司调研看板",
    url: siteUrl,
    inLanguage: "zh-CN",
    potentialAction: {
      "@type": "SearchAction",
      target: `${siteUrl}/?q={search_term_string}`,
      "query-input": "required name=search_term_string",
    },
  });

  return (
    <main className="container">
      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: websiteJsonLd }} />
      {runs.length === 0 ? (
        <div className="empty">
          当前没有找到研究结果。请先运行批量调研脚本，或检查生产环境是否已配置并连通 Postgres 数据源。
        </div>
      ) : (
        <HomeDashboard
          runs={runs}
          initialQuery={initialQuery}
          researchJobsEnabled={researchJobsEnabled}
          canManageResearch={canManageResearch}
          adminConfigured={adminConfigured}
        />
      )}
    </main>
  );
}
