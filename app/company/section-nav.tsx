"use client";

import { useEffect, useMemo, useState } from "react";

type SectionItem = {
  id: string;
  label: string;
  meta?: string;
};

type Props = {
  items: SectionItem[];
};

export default function SectionNav({ items }: Props) {
  const [activeId, setActiveId] = useState(items[0]?.id ?? "");
  const ids = useMemo(() => items.map((item) => item.id), [items]);

  useEffect(() => {
    if (ids.length === 0) return;
    const hash = decodeURIComponent(window.location.hash.slice(1));
    if (hash && ids.includes(hash)) {
      setActiveId(hash);
      return;
    }
    setActiveId(ids[0]);
  }, [ids]);

  useEffect(() => {
    if (ids.length === 0) return;
    const targets = ids
      .map((id) => document.getElementById(id))
      .filter((node): node is HTMLElement => Boolean(node));
    if (targets.length === 0) return;

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
        rootMargin: "-96px 0px -70% 0px",
        threshold: [0, 1],
      }
    );

    for (const target of targets) observer.observe(target);
    return () => observer.disconnect();
  }, [ids]);

  return (
    <nav className="report-section-nav" aria-label="页面分区导航">
      {items.map((item) => (
        <a
          key={item.id}
          href={`#${item.id}`}
          className={`report-section-link ${activeId === item.id ? "is-active" : ""}`}
          onClick={() => setActiveId(item.id)}
        >
          <span className="report-section-link-label">{item.label}</span>
          {item.meta ? <span className="report-section-link-meta">{item.meta}</span> : null}
        </a>
      ))}
    </nav>
  );
}
