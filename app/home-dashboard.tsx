"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useMemo, useRef, useState } from "react";
import type { ResearchRun } from "@/lib/research";

type Props = {
  runs: ResearchRun[];
  initialQuery?: string;
  researchJobsEnabled: boolean;
};

type SearchItem = {
  company: string;
  ticker: string;
  market: string;
  researched: boolean;
  runId?: string;
};

type KeywordHit = {
  runId: string;
  company: string;
  ticker: string;
  docId: string;
  question: string;
  snippet: string;
  score: number;
};

type JobState = {
  id: string;
  status: "queued" | "running" | "success" | "failed";
  logs: string[];
  error?: string;
};

type QuoteState = {
  price: number | null;
  changePercent: number | null;
  marketTime: number | null;
};

function toDateValue(value: string): number {
  const ts = Date.parse(value);
  return Number.isNaN(ts) ? 0 : ts;
}

function tickerToRunId(ticker: string, reportDate: string): string {
  const safeTicker = ticker.replace(/[^\w-]+/g, "_").replace(/^_+|_+$/g, "") || "untitled";
  return `${safeTicker}_${reportDate}`;
}

function shanghaiDate(): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
}

function formatPrice(value: number | null): string {
  if (value === null) return "--";
  return value.toLocaleString("en-US", { maximumFractionDigits: 3 });
}

function formatPct(value: number | null): string {
  if (value === null) return "--";
  const fixed = value.toFixed(2);
  return value > 0 ? `+${fixed}%` : `${fixed}%`;
}

function inferTickerFromInput(raw: string): string | null {
  const q = raw.trim().toUpperCase();
  if (!q) return null;
  if (/^\d{6}\.(SH|SZ)$/.test(q)) return q;
  if (/^\d{5}\.HK$/.test(q)) return q;
  if (/^[A-Z]{1,6}$/.test(q)) return q;
  if (/^\d{6}$/.test(q)) return /^[689]/.test(q) ? `${q}.SH` : `${q}.SZ`;
  if (/^\d{5}$/.test(q)) return `${q}.HK`;
  if (/^\d{4}$/.test(q)) return `0${q}.HK`;
  return null;
}

function marketFromTicker(ticker: string): string {
  const t = ticker.toUpperCase();
  if (t.endsWith(".HK")) return "港股";
  if (t.endsWith(".SH") || t.endsWith(".SZ")) return "A股";
  return "美股";
}

export default function HomeDashboard({ runs, initialQuery = "", researchJobsEnabled }: Props) {
  const router = useRouter();
  const [query, setQuery] = useState(initialQuery);
  const [searchItems, setSearchItems] = useState<SearchItem[]>([]);
  const [keywordHits, setKeywordHits] = useState<KeywordHit[]>([]);
  const [searching, setSearching] = useState(false);
  const [keywordSearching, setKeywordSearching] = useState(false);
  const [selectedItem, setSelectedItem] = useState<SearchItem | null>(null);
  const [selectedKeywordHit, setSelectedKeywordHit] = useState<KeywordHit | null>(null);
  const [job, setJob] = useState<JobState | null>(null);
  const [isStarting, setIsStarting] = useState(false);
  const [quotes, setQuotes] = useState<Record<string, QuoteState>>({});
  const pollTimerRef = useRef<number | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);

  const latestRuns = useMemo(() => {
    const byTicker = new Map<string, ResearchRun>();
    for (const run of runs) {
      const key = run.ticker.toUpperCase();
      const prev = byTicker.get(key);
      if (!prev || toDateValue(run.date) > toDateValue(prev.date)) {
        byTicker.set(key, run);
      }
    }
    return Array.from(byTicker.values()).sort((a, b) => a.company.localeCompare(b.company, "zh-CN"));
  }, [runs]);

  const recentRuns = useMemo(
    () => [...latestRuns].sort((a, b) => toDateValue(b.date) - toDateValue(a.date)).slice(0, 8),
    [latestRuns]
  );

  const quickChips = useMemo(() => {
    const seed = ["贵州茅台", "NVDA", "泡泡玛特", "AAPL"];
    const dynamic = [
      ...latestRuns.slice(0, 4).map((run) => run.company),
      ...latestRuns.slice(0, 4).map((run) => run.ticker),
    ];
    return Array.from(new Set([...seed, ...dynamic])).filter(Boolean).slice(0, 8);
  }, [latestRuns]);

  const latestDate = useMemo(() => {
    const values = latestRuns.map((run) => toDateValue(run.date)).filter((v) => v > 0);
    if (values.length === 0) return "N/A";
    return new Date(Math.max(...values)).toISOString().slice(0, 10);
  }, [latestRuns]);

  const queryText = query.trim();
  const hasQuery = queryText.length > 0;
  const inferredTicker = useMemo(() => inferTickerFromInput(queryText), [queryText]);

  useEffect(() => {
    setQuery(initialQuery);
  }, [initialQuery]);

  useEffect(() => {
    const q = query.trim();
    if (!q) {
      setSearchItems([]);
      setKeywordHits([]);
      setSelectedItem(null);
      setSelectedKeywordHit(null);
      setSearching(false);
      setKeywordSearching(false);
      return;
    }
    const controller = new AbortController();
    setSearching(true);
    setKeywordSearching(true);
    const timer = window.setTimeout(async () => {
      try {
        const [companyResp, keywordResp] = await Promise.all([
          fetch(`/api/company-search?q=${encodeURIComponent(q)}`, {
            signal: controller.signal,
          }),
          fetch(`/api/keyword-search?q=${encodeURIComponent(q)}`, {
            signal: controller.signal,
          }),
        ]);
        const companyItems = companyResp.ok
          ? (((await companyResp.json()) as { items: SearchItem[] }).items || [])
          : [];
        const keywordItems = keywordResp.ok
          ? (((await keywordResp.json()) as { items: KeywordHit[] }).items || [])
          : [];
        setSearchItems(companyItems);
        setKeywordHits(keywordItems);
        setSelectedItem((prev) => {
          if (!prev) return companyItems[0] ?? null;
          return companyItems.find((item) => item.ticker === prev.ticker) || companyItems[0] || null;
        });
        setSelectedKeywordHit((prev) => {
          if (!prev) return keywordItems[0] ?? null;
          return (
            keywordItems.find(
              (item) => item.runId === prev.runId && item.docId === prev.docId && item.question === prev.question
            ) || keywordItems[0] || null
          );
        });
      } finally {
        setSearching(false);
        setKeywordSearching(false);
      }
    }, 220);
    return () => {
      controller.abort();
      window.clearTimeout(timer);
    };
  }, [query]);

  useEffect(() => {
    if (!job || (job.status !== "queued" && job.status !== "running")) {
      if (pollTimerRef.current) {
        window.clearInterval(pollTimerRef.current);
        pollTimerRef.current = null;
      }
      return;
    }
    pollTimerRef.current = window.setInterval(async () => {
      const response = await fetch(`/api/research-jobs/${job.id}`);
      if (!response.ok) return;
      const payload = (await response.json()) as JobState;
      setJob(payload);
      if (payload.status === "success" || payload.status === "failed") {
        if (pollTimerRef.current) {
          window.clearInterval(pollTimerRef.current);
          pollTimerRef.current = null;
        }
        if (payload.status === "success") {
          router.refresh();
        }
      }
    }, 2000);
    return () => {
      if (pollTimerRef.current) {
        window.clearInterval(pollTimerRef.current);
        pollTimerRef.current = null;
      }
    };
  }, [job, router]);

  useEffect(() => {
    if (latestRuns.length === 0) return;
    const loadQuotes = async () => {
      const tickers = latestRuns.map((run) => run.ticker).join(",");
      const response = await fetch(`/api/quotes?tickers=${encodeURIComponent(tickers)}`);
      if (!response.ok) return;
      const payload = (await response.json()) as {
        items: Array<{
          ticker: string;
          price: number | null;
          changePercent: number | null;
          marketTime: number | null;
        }>;
      };
      const next: Record<string, QuoteState> = {};
      for (const item of payload.items || []) {
        next[item.ticker.toUpperCase()] = {
          price: item.price,
          changePercent: item.changePercent,
          marketTime: item.marketTime,
        };
      }
      setQuotes(next);
    };
    loadQuotes();
  }, [latestRuns]);

  const startResearch = async (target: SearchItem | null = selectedItem) => {
    if (!researchJobsEnabled) return;
    if (!target) return;
    setIsStarting(true);
    try {
      const response = await fetch("/api/research-jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          company: target.company,
          ticker: target.ticker,
          provider: "doubao",
          reportDate: shanghaiDate(),
        }),
      });
      if (!response.ok) throw new Error("启动调研任务失败");
      const payload = (await response.json()) as { jobId: string; status: JobState["status"] };
      setJob({ id: payload.jobId, status: payload.status, logs: [] });
    } catch (err) {
      setJob({
        id: "local-error",
        status: "failed",
        logs: [],
        error: err instanceof Error ? err.message : "启动失败",
      });
    } finally {
      setIsStarting(false);
    }
  };

  const openResearchedRun = (target: SearchItem | null = selectedItem) => {
    if (!target?.researched) return;
    if (target.runId) {
      router.push(`/company/${encodeURIComponent(target.runId)}`);
      return;
    }
    const runId = tickerToRunId(target.ticker, shanghaiDate());
    router.push(`/company/${encodeURIComponent(runId)}`);
  };

  const openKeywordResult = (target: KeywordHit | null = selectedKeywordHit) => {
    if (!target) return;
    router.push(`/company/${encodeURIComponent(target.runId)}?tab=${encodeURIComponent(target.docId)}`);
  };

  const researchedCount = latestRuns.length;
  const companyHitCount = searchItems.length;
  const keywordHitCount = keywordHits.length;
  const hitCount = companyHitCount + keywordHitCount;
  const canStartFromTicker = hasQuery && !selectedItem && Boolean(inferredTicker);

  const primaryButtonLabel = (() => {
    if (!hasQuery) return "开始搜索";
    if (selectedItem?.researched) return "查看调研结果";
    if (selectedItem && !selectedItem.researched) {
      return researchJobsEnabled ? (isStarting ? "启动中..." : "开始调研") : "仅展示已调研";
    }
    if (selectedKeywordHit) return "查看关键词结果";
    if (canStartFromTicker && inferredTicker) {
      return researchJobsEnabled ? (isStarting ? "启动中..." : `调研 ${inferredTicker}`) : "仅展示已调研";
    }
    return "请输入更准确关键词";
  })();

  const handlePrimaryAction = () => {
    if (!hasQuery) {
      inputRef.current?.focus();
      return;
    }
    if (selectedItem?.researched) {
      openResearchedRun(selectedItem);
      return;
    }
    if (selectedItem && !selectedItem.researched) {
      if (researchJobsEnabled) {
        void startResearch(selectedItem);
      }
      return;
    }
    if (selectedKeywordHit) {
      openKeywordResult(selectedKeywordHit);
      return;
    }
    if (canStartFromTicker && inferredTicker) {
      if (researchJobsEnabled) {
        void startResearch({
          company: queryText,
          ticker: inferredTicker,
          market: marketFromTicker(inferredTicker),
          researched: false,
        });
      }
    }
  };

  return (
    <section className="home-shell">
      <div className="home-main">
        <div className="home-search-hero">
          <h1 className="home-title">搜公司，立刻看结论</h1>
          <p className="home-subtitle">
            输入公司名、代码或拼音，快速定位已调研结果；未覆盖公司可立即发起智能调研并自动归档。
          </p>
          <div className="home-search-row">
            <div className="home-center-search">
              <input
                ref={inputRef}
                className="home-main-input"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="搜索公司名、代码或拼音，例如 贵州茅台 / 600519 / AAPL"
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    handlePrimaryAction();
                  }
                }}
              />
              {query ? (
                <button className="home-search-clear" type="button" onClick={() => setQuery("")}>
                  清空
                </button>
              ) : null}
            </div>
            <button
              type="button"
              className="home-search-go"
              disabled={
                isStarting ||
                (!researchJobsEnabled &&
                  !selectedKeywordHit &&
                  ((selectedItem && !selectedItem.researched) || (canStartFromTicker && !selectedItem))) ||
                (hasQuery &&
                  !selectedKeywordHit &&
                  !selectedItem &&
                  !canStartFromTicker &&
                  !searching &&
                  !keywordSearching)
              }
              onClick={handlePrimaryAction}
            >
              {primaryButtonLabel}
            </button>
          </div>
          <div className="home-kpis">
            <span>已调研 {researchedCount} 家</span>
            <span>最新更新 {latestDate}</span>
            {!researchJobsEnabled ? <span>当前站点为公开只读模式</span> : null}
            {hasQuery ? <span>{searching ? "检索中..." : `匹配建议 ${hitCount} 条`}</span> : null}
          </div>
          <div className="home-quick-chips">
            <span className="meta">试试：</span>
            {quickChips.map((chip) => (
              <button key={chip} type="button" className="home-quick-chip" onClick={() => setQuery(chip)}>
                {chip}
              </button>
            ))}
          </div>
        </div>

        {hasQuery ? (
          <div className="home-suggest-panel">
            <div className="home-suggest-head">
              <strong>搜索建议</strong>
              <span className="meta">
                {searching || keywordSearching
                  ? "正在更新..."
                  : `公司 ${companyHitCount} 条 · 关键词 ${keywordHitCount} 条`}
              </span>
            </div>
            {keywordHitCount > 0 ? (
              <div className="home-keyword-panel">
                <div className="home-keyword-head">
                  <strong>关键词命中</strong>
                  <span className="meta">{keywordHitCount} 条</span>
                </div>
                <div className="home-keyword-list">
                  {keywordHits.slice(0, 6).map((hit) => (
                    <button
                      key={`${hit.runId}-${hit.docId}-${hit.question}`}
                      type="button"
                      className={`home-keyword-item ${
                        selectedKeywordHit &&
                        selectedKeywordHit.runId === hit.runId &&
                        selectedKeywordHit.docId === hit.docId &&
                        selectedKeywordHit.question === hit.question
                          ? "is-active"
                          : ""
                      }`}
                      onClick={() => {
                        setSelectedKeywordHit(hit);
                        setSelectedItem(null);
                      }}
                    >
                      <div className="home-keyword-title">
                        <strong>{hit.company}</strong>
                        <span className="meta">{hit.ticker}</span>
                      </div>
                      <div className="home-keyword-question">{hit.question}</div>
                      <div className="home-keyword-snippet">{hit.snippet}</div>
                    </button>
                  ))}
                </div>
                {selectedKeywordHit ? (
                  <div className="home-suggest-action">
                    <span className="meta">
                      命中“{queryText}”的调研内容，可直接打开对应章节。
                    </span>
                    <button type="button" className="home-cta-btn" onClick={() => openKeywordResult(selectedKeywordHit)}>
                      查看命中内容
                    </button>
                  </div>
                ) : null}
              </div>
            ) : null}
            {searchItems.length === 0 && keywordHitCount === 0 ? (
              <div className="home-empty-suggest">
                <div className="meta">未找到匹配公司，可尝试完整股票代码（示例：600519.SH / 09992.HK / AAPL）。</div>
                {inferredTicker && researchJobsEnabled ? (
                  <button
                    type="button"
                    className="home-cta-btn"
                    disabled={isStarting}
                    onClick={() =>
                      startResearch({
                        company: queryText,
                        ticker: inferredTicker,
                        market: marketFromTicker(inferredTicker),
                        researched: false,
                      })
                    }
                  >
                    {isStarting ? "启动中..." : `按代码 ${inferredTicker} 发起调研`}
                  </button>
                ) : null}
              </div>
            ) : searchItems.length > 0 ? (
              <div className="home-suggest-list">
                {searchItems.slice(0, 10).map((item) => (
                  <button
                    key={item.ticker}
                    type="button"
                    className={`home-suggest-item ${selectedItem?.ticker === item.ticker ? "is-active" : ""}`}
                    onClick={() => {
                      setSelectedItem(item);
                      setSelectedKeywordHit(null);
                    }}
                  >
                    <div>
                      <strong>{item.company}</strong>
                      <div className="meta">
                        {item.ticker} · {item.market}
                      </div>
                    </div>
                    <span className={`home-suggest-badge ${item.researched ? "ok" : "new"}`}>
                      {item.researched ? "已调研" : "可发起"}
                    </span>
                  </button>
                ))}
              </div>
            ) : null}
            {selectedItem ? (
              <div className="home-suggest-action">
                <span className="meta">
                  {selectedItem.researched
                    ? `已收录 ${selectedItem.company} 调研结果，可直接查看。`
                    : `尚未收录 ${selectedItem.company}，可一键发起调研。`}
                </span>
                {selectedItem.researched ? (
                  <button type="button" className="home-cta-btn" onClick={() => openResearchedRun(selectedItem)}>
                    查看调研结果
                  </button>
                ) : researchJobsEnabled ? (
                  <button
                    type="button"
                    className="home-cta-btn"
                    disabled={isStarting}
                    onClick={() => startResearch(selectedItem)}
                  >
                    {isStarting ? "启动中..." : "开始调研并同步飞书"}
                  </button>
                ) : (
                  <span className="meta">当前为对外展示环境，暂不开放在线发起调研。</span>
                )}
              </div>
            ) : null}
          </div>
        ) : (
          <div className="home-discover-panel">
            <div className="home-discover-head">
              <strong>最近更新</strong>
              <span className="meta">点击卡片，继续阅读完整调研</span>
            </div>
            <div className="home-discover-grid">
              {recentRuns.slice(0, 6).map((run) => {
                const quote = quotes[run.ticker.toUpperCase()];
                const pct = quote?.changePercent ?? null;
                return (
                  <Link
                    key={`discover-${run.runId}`}
                    href={`/company/${encodeURIComponent(run.runId)}`}
                    className="home-discover-item"
                  >
                    <div className="home-discover-row">
                      <strong>{run.company}</strong>
                      <span className="home-discover-date">{run.date}</span>
                    </div>
                    <div className="home-discover-meta">
                      {run.ticker}
                      {run.industry && run.industry !== "未分类" ? ` · ${run.industry}` : ""}
                    </div>
                    <div className="home-discover-row">
                      <span className="home-discover-price">{formatPrice(quote?.price ?? null)}</span>
                      <span
                        className={`home-side-pct ${
                          pct === null ? "flat" : pct > 0 ? "up" : pct < 0 ? "down" : "flat"
                        }`}
                      >
                        {formatPct(pct)}
                      </span>
                    </div>
                  </Link>
                );
              })}
            </div>
          </div>
        )}

        {job ? (
          <div className="home-job-board">
            <div className="home-job-head">
              <strong>调研进展</strong>
              <span className={`home-job-status ${job.status}`}>{job.status}</span>
            </div>
            {job.error ? <div className="home-job-error">{job.error}</div> : null}
            <div className="home-job-logs">
              {job.logs.length === 0 ? (
                <div className="meta">等待日志输出...</div>
              ) : (
                job.logs.slice(-18).map((line, idx) => (
                  <div className="home-job-line" key={`${line}-${idx}`}>
                    {line}
                  </div>
                ))
              )}
            </div>
          </div>
        ) : null}
      </div>

      <aside className="home-side">
        <div className="home-side-title">已调研公司</div>
        <div className="home-side-list">
          {latestRuns.map((run) => {
            const quote = quotes[run.ticker.toUpperCase()];
            const pct = quote?.changePercent ?? null;
            return (
              <Link
                key={run.runId}
                href={`/company/${encodeURIComponent(run.runId)}`}
                className="home-side-row"
              >
                <div className="home-side-main">
                  <div className="home-side-company">{run.company}</div>
                  <div className="home-side-ticker">{run.ticker}</div>
                  {run.industry && run.industry !== "未分类" ? (
                    <div className="home-side-industry">{run.industry}</div>
                  ) : null}
                </div>
                <div className="home-side-quote">
                  <div className="home-side-price">{formatPrice(quote?.price ?? null)}</div>
                  <div
                    className={`home-side-pct ${
                      pct === null ? "flat" : pct > 0 ? "up" : pct < 0 ? "down" : "flat"
                    }`}
                  >
                    {formatPct(pct)}
                  </div>
                </div>
              </Link>
            );
          })}
        </div>
      </aside>
    </section>
  );
}
