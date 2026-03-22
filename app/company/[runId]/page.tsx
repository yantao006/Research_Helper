import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { getResearchRun } from "@/lib/research";
import ReactMarkdown from "react-markdown";
import rehypeRaw from "rehype-raw";
import rehypeSanitize, { defaultSchema } from "rehype-sanitize";
import remarkGfm from "remark-gfm";
import React from "react";
import DocToc from "./toc-nav";

export const dynamic = "force-dynamic";

type PageProps = {
  params: Promise<{ runId: string }>;
  searchParams: Promise<{ tab?: string }>;
};

type MetadataProps = {
  params: Promise<{ runId: string }>;
};

type TocItem = {
  id: string;
  text: string;
  level: number;
};

const markdownSanitizeSchema = {
  ...defaultSchema,
  attributes: {
    ...defaultSchema.attributes,
    mark: [...(defaultSchema.attributes?.mark || []), "className", "id"],
  },
};

type SeoSection = {
  body: string;
  keywords: string[];
};

function flattenText(node: React.ReactNode): string {
  if (typeof node === "string" || typeof node === "number") {
    return String(node);
  }
  if (Array.isArray(node)) {
    return node.map(flattenText).join("");
  }
  if (React.isValidElement<{ children?: React.ReactNode }>(node)) {
    return flattenText(node.props.children ?? "");
  }
  return "";
}

function normalizeHeadingText(text: string): string {
  return text
    .replace(/`([^`]+)`/g, "$1")
    .replace(/\[(.*?)\]\((.*?)\)/g, "$1")
    .replace(/<[^>]+>/g, "")
    .replace(/[*_~]/g, "")
    .trim();
}

function slugifyHeading(text: string): string {
  const normalized = normalizeHeadingText(text)
    .toLowerCase()
    .replace(/[^\p{Letter}\p{Number}\u4e00-\u9fff\s-]/gu, "")
    .trim()
    .replace(/\s+/g, "-");
  return normalized || "section";
}

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function keywordKey(value: string): string {
  return value.trim().toLocaleLowerCase();
}

function keywordAnchorBase(keyword: string): string {
  const base = keyword
    .toLocaleLowerCase()
    .replace(/[^\p{Letter}\p{Number}\u4e00-\u9fff]+/gu, "-")
    .replace(/^-+|-+$/g, "");
  return `kw-${base || "item"}`;
}

function extractSeoSection(markdown: string): SeoSection {
  const lines = markdown.split(/\r?\n/);
  const start = lines.findIndex((line) => line.trim() === "## SEO 关键词");
  if (start < 0) {
    return { body: markdown, keywords: [] };
  }

  let end = lines.length;
  for (let i = start + 1; i < lines.length; i += 1) {
    if (/^##\s+/.test(lines[i].trim())) {
      end = i;
      break;
    }
  }

  const keywords: string[] = [];
  for (const line of lines.slice(start + 1, end)) {
    const text = line.trim();
    if (!text) continue;
    const linked = text.match(/^- \[([^\]]+)\]\([^)]+\)$/);
    if (linked) {
      keywords.push(linked[1].trim());
      continue;
    }
    const bullet = text.match(/^- (.+)$/);
    if (bullet) {
      keywords.push(bullet[1].trim());
    }
  }

  const bodyLines = [...lines.slice(0, start), ...lines.slice(end)];
  const uniqueKeywords: string[] = [];
  const seen = new Set<string>();
  for (const keyword of keywords) {
    const clean = keyword.trim();
    if (!clean) continue;
    const key = keywordKey(clean);
    if (seen.has(key)) continue;
    seen.add(key);
    uniqueKeywords.push(clean);
  }

  return {
    body: bodyLines.join("\n").trim(),
    keywords: uniqueKeywords,
  };
}

function highlightMarkdownKeywords(
  markdown: string,
  keywords: string[]
): { highlighted: string; firstAnchorByKeyword: Map<string, string> } {
  if (!markdown.trim() || keywords.length === 0) {
    return { highlighted: markdown, firstAnchorByKeyword: new Map() };
  }

  const sorted = [...keywords].sort((a, b) => b.length - a.length);
  const escaped = sorted.map((kw) => escapeRegex(kw));
  if (escaped.length === 0) {
    return { highlighted: markdown, firstAnchorByKeyword: new Map() };
  }
  const pattern = new RegExp(`(${escaped.join("|")})`, "giu");

  const firstAnchorByKeyword = new Map<string, string>();
  const hitCountByKeyword = new Map<string, number>();
  let inCodeFence = false;

  const highlighted = markdown
    .split(/\r?\n/)
    .map((line) => {
      const trimmed = line.trim();
      if (trimmed.startsWith("```")) {
        inCodeFence = !inCodeFence;
        return line;
      }
      if (inCodeFence || !trimmed) {
        return line;
      }
      return line.replace(pattern, (match) => {
        const key = keywordKey(match);
        const nextCount = (hitCountByKeyword.get(key) ?? 0) + 1;
        hitCountByKeyword.set(key, nextCount);
        const id = `${keywordAnchorBase(match)}-${nextCount}`;
        if (!firstAnchorByKeyword.has(key)) {
          firstAnchorByKeyword.set(key, id);
        }
        return `<mark class="kw-hit" id="${id}">${match}</mark>`;
      });
    })
    .join("\n");

  return { highlighted, firstAnchorByKeyword };
}

function extractMarkdownToc(markdown: string): TocItem[] {
  const lines = markdown.split(/\r?\n/);
  const toc: TocItem[] = [];
  const seen = new Map<string, number>();
  let inCodeFence = false;

  for (const line of lines) {
    const text = line.trim();
    if (text.startsWith("```")) {
      inCodeFence = !inCodeFence;
      continue;
    }
    if (inCodeFence) {
      continue;
    }
    const match = text.match(/^(#{1,6})\s+(.+)$/);
    if (!match) {
      continue;
    }
    const level = match[1].length;
    if (level > 4) {
      continue;
    }
    const headingText = normalizeHeadingText(match[2]);
    if (!headingText) {
      continue;
    }
    const base = slugifyHeading(headingText);
    const count = seen.get(base) ?? 0;
    seen.set(base, count + 1);
    const id = count === 0 ? base : `${base}-${count + 1}`;
    toc.push({ id, text: headingText, level });
  }
  return toc;
}

function buildMetaDescription(run: Awaited<ReturnType<typeof getResearchRun>>): string {
  if (!run) {
    return "公司调研结果未找到。";
  }
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

export async function generateMetadata({ params }: MetadataProps): Promise<Metadata> {
  const { runId } = await params;
  const decodedRunId = decodeURIComponent(runId);
  const run = await getResearchRun(decodedRunId);
  const canonicalPath = `/company/${encodeURIComponent(decodedRunId)}`;

  if (!run) {
    return {
      title: "公司调研结果未找到",
      description: "该公司调研结果不存在或已被移除。",
      alternates: {
        canonical: canonicalPath,
      },
      robots: {
        index: false,
        follow: true,
      },
    };
  }

  const title = `${run.company}（${run.ticker}）调研报告`;
  const description = buildMetaDescription(run);
  return {
    title,
    description,
    alternates: {
      canonical: canonicalPath,
    },
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
  };
}

export default async function CompanyPage({ params, searchParams }: PageProps) {
  const { runId } = await params;
  const query = await searchParams;
  const run = await getResearchRun(decodeURIComponent(runId));

  if (!run) {
    notFound();
  }

  const activeIndex =
    run.docs.findIndex((doc) => doc.id === query.tab) >= 0
      ? run.docs.findIndex((doc) => doc.id === query.tab)
      : 0;
  const activeDoc = run.docs[activeIndex];
  const seoSection = extractSeoSection(activeDoc.answer || "");
  const highlightResult = highlightMarkdownKeywords(seoSection.body, seoSection.keywords);
  const toc = extractMarkdownToc(seoSection.body || "");
  const headingSeen = new Map<string, number>();
  const renderHeading = (level: number, children: React.ReactNode) => {
    const text = normalizeHeadingText(flattenText(children));
    const base = slugifyHeading(text);
    const count = headingSeen.get(base) ?? 0;
    headingSeen.set(base, count + 1);
    const id = count === 0 ? base : `${base}-${count + 1}`;
    return React.createElement(`h${level}`, { id }, children);
  };

  return (
    <main className="container">
      <div className="topbar">
        <Link href="/" className="back">
          ← 返回首页
        </Link>
      </div>

      <h1 className="title">{run.company}</h1>
      <p className="subtitle">
        {run.ticker}
        {run.industry && run.industry !== "未分类" ? ` · ${run.industry}` : ""}
        {" · "}
        {run.date} · {run.provider} · {run.model}
      </p>

      <section className="panel docs-surface">
        <div className="docs-layout">
          <aside className="docs-left-nav">
            <div className="docs-nav-title">调研目录</div>
            <div className="docs-nav-list" role="tablist" aria-label="调研类别">
              {run.docs.map((doc, index) => (
                <Link
                  key={doc.fileName}
                  className={`docs-nav-item ${index === activeIndex ? "is-active" : ""}`}
                  href={`/company/${encodeURIComponent(run.runId)}?tab=${encodeURIComponent(doc.id)}`}
                >
                  <span className="docs-nav-id">{doc.id}</span>
                  <span className="docs-nav-text">{doc.question}</span>
                </Link>
              ))}
            </div>
          </aside>

          <article className="tab-panel docs-main">
            <h1 className="docs-main-title">
              {activeDoc.id}. {activeDoc.question}
            </h1>
            {seoSection.keywords.length > 0 ? (
              <nav className="doc-keyword-jumps" aria-label="关键词命中导航">
                <span className="meta">关键词导航</span>
                {seoSection.keywords.map((keyword) => {
                  const anchor = highlightResult.firstAnchorByKeyword.get(keywordKey(keyword));
                  return (
                    <a
                      key={keyword}
                      className="doc-keyword-chip"
                      href={anchor ? `#${anchor}` : "#"}
                    >
                      {keyword}
                    </a>
                  );
                })}
              </nav>
            ) : null}
            <div className="markdown-body">
              <ReactMarkdown
                remarkPlugins={[[remarkGfm, { singleTilde: false }]]}
                rehypePlugins={[rehypeRaw, [rehypeSanitize, markdownSanitizeSchema]]}
                components={{
                  h1: ({ children }) => renderHeading(1, children),
                  h2: ({ children }) => renderHeading(2, children),
                  h3: ({ children }) => renderHeading(3, children),
                  h4: ({ children }) => renderHeading(4, children),
                  table: ({ children }) => (
                    <div className="table-wrap">
                      <table>{children}</table>
                    </div>
                  )
                }}
              >
                {highlightResult.highlighted || "(无内容)"}
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

          <DocToc toc={toc} />
        </div>
      </section>
    </main>
  );
}
