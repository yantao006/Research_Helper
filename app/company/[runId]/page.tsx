import Link from "next/link";
import { notFound } from "next/navigation";
import { getResearchRun } from "@/lib/research";
import ReactMarkdown from "react-markdown";
import rehypeRaw from "rehype-raw";
import remarkGfm from "remark-gfm";

export const dynamic = "force-dynamic";

type PageProps = {
  params: Promise<{ runId: string }>;
  searchParams: Promise<{ tab?: string }>;
};

export default async function CompanyPage({ params, searchParams }: PageProps) {
  const { runId } = await params;
  const query = await searchParams;
  const run = getResearchRun(decodeURIComponent(runId));

  if (!run) {
    notFound();
  }

  const activeIndex =
    run.docs.findIndex((doc) => doc.id === query.tab) >= 0
      ? run.docs.findIndex((doc) => doc.id === query.tab)
      : 0;
  const activeDoc = run.docs[activeIndex];

  return (
    <main className="container">
      <div className="topbar">
        <Link href="/" className="back">
          ← 返回首页
        </Link>
      </div>

      <h1 className="title">{run.company}</h1>
      <p className="subtitle">
        {run.ticker} · {run.date} · {run.provider} · {run.model}
      </p>

      <section className="panel">
        <div className="tabbar" role="tablist" aria-label="调研类别">
          {run.docs.map((doc, index) => (
            <Link
              key={doc.fileName}
              className={`tab ${index === activeIndex ? "is-active" : ""}`}
              href={`/company/${encodeURIComponent(run.runId)}?tab=${encodeURIComponent(doc.id)}`}
            >
              {doc.id}. {doc.question}
            </Link>
          ))}
        </div>

        <article className="tab-panel">
          <h2>
            {activeDoc.id}. {activeDoc.question}
          </h2>
          <div className="markdown-body">
            <ReactMarkdown
              remarkPlugins={[[remarkGfm, { singleTilde: false }]]}
              rehypePlugins={[rehypeRaw]}
              components={{
                table: ({ children }) => (
                  <div className="table-wrap">
                    <table>{children}</table>
                  </div>
                )
              }}
            >
              {activeDoc.answer || "(无内容)"}
            </ReactMarkdown>
          </div>
          {activeDoc.sources.length > 0 && (
            <div style={{ marginTop: "14px" }}>
              <div className="meta" style={{ marginBottom: "6px" }}>
                Sources
              </div>
              {activeDoc.sources.map((src) => (
                <div key={src.url}>
                  <a href={src.url} target="_blank" rel="noreferrer">
                    {src.title}
                  </a>
                </div>
              ))}
            </div>
          )}
        </article>
      </section>
    </main>
  );
}
