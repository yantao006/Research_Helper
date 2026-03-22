import type { Metadata } from "next";
import { Analytics } from "@vercel/analytics/react";
import { SpeedInsights } from "@vercel/speed-insights/next";
import { getSiteUrl } from "@/lib/server/site-url";
import "./globals.css";

const siteUrl = getSiteUrl();
const ogImagePath = "/og-cover.jpg";
const ogImageUrl = `${siteUrl}${ogImagePath}`;

export const metadata: Metadata = {
  metadataBase: new URL(siteUrl),
  applicationName: "公司调研看板",
  title: {
    default: "公司调研看板",
    template: "%s | 公司调研看板",
  },
  description: "覆盖美股、港股、A股的上市公司调研结论与关键跟踪点，支持快速检索与结构化阅读。",
  openGraph: {
    type: "website",
    locale: "zh_CN",
    siteName: "公司调研看板",
    title: "公司调研看板",
    description: "覆盖美股、港股、A股的上市公司调研结论与关键跟踪点，支持快速检索与结构化阅读。",
    images: [
      {
        url: ogImagePath,
        width: 1200,
        height: 630,
        alt: "公司调研看板",
      },
    ],
  },
  twitter: {
    card: "summary_large_image",
    title: "公司调研看板",
    description: "覆盖美股、港股、A股的上市公司调研结论与关键跟踪点，支持快速检索与结构化阅读。",
    images: [ogImageUrl],
  },
  icons: {
    icon: "/favicon.svg",
    shortcut: "/favicon.svg",
    apple: "/favicon.svg",
  },
  robots: {
    index: true,
    follow: true,
    googleBot: {
      index: true,
      follow: true,
      "max-image-preview": "large",
      "max-snippet": -1,
      "max-video-preview": -1,
    },
  },
};

export default function RootLayout({
  children
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="zh-CN">
      <body>
        {children}
        <Analytics />
        <SpeedInsights />
      </body>
    </html>
  );
}
