import Link from "next/link";
import { redirect } from "next/navigation";
import AdminLogin from "./admin-login";
import { getResearchAdminSession, isResearchAdminConfigured } from "@/lib/server/admin-auth";

type PageProps = {
  searchParams: Promise<{ next?: string }>;
};

export default async function AdminPage({ searchParams }: PageProps) {
  const configured = isResearchAdminConfigured();
  const isAuthed = await getResearchAdminSession();
  const query = await searchParams;
  const nextPath = (query.next || "/").trim() || "/";

  if (!configured) {
    return (
      <main className="container">
        <div className="empty">当前未配置生产管理口令，请先设置 `RESEARCH_ADMIN_PASSWORD` 与 `RESEARCH_ADMIN_SECRET`。</div>
      </main>
    );
  }

  if (isAuthed) {
    redirect(nextPath);
  }

  return (
    <main className="container">
      <div className="panel" style={{ maxWidth: 520, marginInline: "auto", marginTop: "8vh" }}>
        <Link href="/" className="back">
          ← 返回首页
        </Link>
        <h1 className="title" style={{ marginTop: 16, marginBottom: 8 }}>
          管理员登录
        </h1>
        <p className="subtitle" style={{ marginBottom: 20 }}>
          进入后可在生产环境发起新公司调研，并对已有公司执行更新。
        </p>
        <AdminLogin nextPath={nextPath} />
      </div>
    </main>
  );
}
