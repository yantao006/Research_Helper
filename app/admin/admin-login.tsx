"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";

type Props = {
  nextPath: string;
};

export default function AdminLogin({ nextPath }: Props) {
  const router = useRouter();
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");

  const onSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setSubmitting(true);
    setError("");
    try {
      const response = await fetch("/api/admin/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ password }),
      });
      const payload = (await response.json()) as { error?: string };
      if (!response.ok) {
        throw new Error(payload.error || "登录失败");
      }
      router.push(nextPath || "/");
      router.refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "登录失败");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <form className="admin-login-form" onSubmit={onSubmit}>
      <label className="admin-login-label" htmlFor="admin-password">
        管理口令
      </label>
      <input
        id="admin-password"
        className="admin-login-input"
        type="password"
        autoComplete="current-password"
        value={password}
        onChange={(event) => setPassword(event.target.value)}
        placeholder="输入仅你知道的生产管理口令"
      />
      {error ? <div className="home-job-error">{error}</div> : null}
      <button className="home-search-go" type="submit" disabled={submitting || !password.trim()}>
        {submitting ? "登录中..." : "进入管理模式"}
      </button>
    </form>
  );
}
