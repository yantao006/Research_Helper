import Link from "next/link";

export default function NotFoundPage() {
  return (
    <main className="container">
      <h1 className="title">页面不存在</h1>
      <p className="subtitle">请返回首页查看已生成的研究结果。</p>
      <Link className="back" href="/">
        ← 返回首页
      </Link>
    </main>
  );
}
