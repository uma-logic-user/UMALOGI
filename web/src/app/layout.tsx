import type { Metadata, Viewport } from 'next'
import './globals.css'
import SwRegister from '@/components/SwRegister'

export const viewport: Viewport = {
  width: 'device-width',
  initialScale: 1,
  maximumScale: 1,
  userScalable: false,
  themeColor: '#00c8ff',
}

export const metadata: Metadata = {
  title: 'UMALOGI | 競馬AI分析',
  description: 'JRA-VAN×LightGBM 全券種対応・自律型競馬予測プラットフォーム',
  manifest: '/manifest.json',

  // Apple PWA メタタグ
  appleWebApp: {
    capable: true,
    statusBarStyle: 'black-translucent',
    title: 'UMALOGI',
    startupImage: '/apple-touch-icon.png',
  },

  // OGP（URLシェア時のプレビュー）
  openGraph: {
    title: 'UMALOGI | 競馬AI分析',
    description: '自律型競馬予測プラットフォーム',
    type: 'website',
  },

  // アイコン
  icons: {
    icon: [
      { url: '/favicon-32.png', sizes: '32x32', type: 'image/png' },
      { url: '/icon-192.png',   sizes: '192x192', type: 'image/png' },
    ],
    apple: [
      { url: '/apple-touch-icon.png', sizes: '180x180' },
    ],
  },
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="ja">
      <head>
        {/* iOS スタンドアロンモード用 */}
        <meta name="mobile-web-app-capable" content="yes" />
        <meta name="apple-mobile-web-app-capable" content="yes" />
        <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent" />
        <meta name="apple-mobile-web-app-title" content="UMALOGI" />
        {/* MS Tile (Windows/Edge) */}
        <meta name="msapplication-TileColor" content="#02060e" />
        <meta name="msapplication-TileImage" content="/icon-192.png" />
      </head>
      <body className="antialiased" style={{ overflow: 'hidden' }}>
        {children}
        <SwRegister />
      </body>
    </html>
  )
}
