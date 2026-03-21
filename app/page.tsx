import { getResearchRuns } from "@/lib/research";
import HomeDashboard from "./home-dashboard";

export const dynamic = "force-dynamic";

export default function HomePage() {
  const runs = getResearchRuns();

  return (
    <main className="container">
      {runs.length === 0 ? (
        <div className="empty">当前没有找到研究结果。请先运行批量脚本生成 output/*.md 文件。</div>
      ) : (
        <HomeDashboard runs={runs} />
      )}
    </main>
  );
}
