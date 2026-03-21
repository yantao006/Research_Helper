import { getResearchRuns } from "@/lib/research";
import { isResearchJobsEnabled } from "@/lib/server/runtime-flags";
import HomeDashboard from "./home-dashboard";

export const dynamic = "force-dynamic";

type HomePageProps = {
  searchParams: Promise<{ q?: string; kw?: string }>;
};

export default async function HomePage({ searchParams }: HomePageProps) {
  const query = await searchParams;
  const runs = getResearchRuns();
  const initialQuery = (query.kw || query.q || "").trim();
  const researchJobsEnabled = isResearchJobsEnabled();

  return (
    <main className="container">
      {runs.length === 0 ? (
        <div className="empty">当前没有找到研究结果。请先运行批量脚本生成 output/*.md 文件。</div>
      ) : (
        <HomeDashboard
          runs={runs}
          initialQuery={initialQuery}
          researchJobsEnabled={researchJobsEnabled}
        />
      )}
    </main>
  );
}
