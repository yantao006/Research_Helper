"use client";

import { useEffect, useMemo, useState } from "react";

type TocItem = {
  id: string;
  text: string;
  level: number;
};

type Props = {
  toc: TocItem[];
};

export default function DocToc({ toc }: Props) {
  const [activeId, setActiveId] = useState(toc[0]?.id ?? "");

  const tocIds = useMemo(() => toc.map((item) => item.id), [toc]);

  useEffect(() => {
    if (tocIds.length === 0) return;
    const hash = decodeURIComponent(window.location.hash.slice(1));
    if (hash && tocIds.includes(hash)) {
      setActiveId(hash);
      return;
    }
    setActiveId(tocIds[0]);
  }, [tocIds]);

  useEffect(() => {
    if (tocIds.length === 0) return;
    const onHashChange = () => {
      const hash = decodeURIComponent(window.location.hash.slice(1));
      if (hash && tocIds.includes(hash)) {
        setActiveId(hash);
      }
    };
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, [tocIds]);

  useEffect(() => {
    if (tocIds.length === 0) return;
    const headingElements = tocIds
      .map((id) => document.getElementById(id))
      .filter((el): el is HTMLElement => Boolean(el));
    if (headingElements.length === 0) return;

    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((entry) => entry.isIntersecting)
          .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top);
        if (visible[0]) {
          setActiveId(visible[0].target.id);
        }
      },
      {
        rootMargin: "-92px 0px -65% 0px",
        threshold: [0, 1],
      }
    );

    for (const heading of headingElements) {
      observer.observe(heading);
    }
    return () => observer.disconnect();
  }, [tocIds]);

  return (
    <aside className="docs-right-toc">
      <div className="docs-nav-title">本文结构</div>
      {toc.length === 0 ? (
        <div className="meta">当前内容没有可导航标题</div>
      ) : (
        <nav className="docs-toc-list" aria-label="On this page">
          {toc.map((item) => (
            <a
              key={item.id}
              href={`#${item.id}`}
              className={`docs-toc-item level-${item.level} ${activeId === item.id ? "is-active" : ""}`}
              onClick={() => setActiveId(item.id)}
            >
              {item.text}
            </a>
          ))}
        </nav>
      )}
    </aside>
  );
}
