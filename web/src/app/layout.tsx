import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'UMALOGI | 競馬AI分析',
  description: '血統解析・レース結果・WIN5予想ダッシュボード',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="ja">
      <body className="min-h-screen antialiased">{children}</body>
    </html>
  )
}
