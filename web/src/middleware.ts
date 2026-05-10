import { NextRequest, NextResponse } from 'next/server'

/**
 * UMALOGI Basic 認証ミドルウェア — 二重防御の内側レイヤー
 *
 * 外側: Cloudflare Access (Google OAuth) が未認証アクセスをエッジで遮断
 * 内側: このミドルウェアがトンネル突破への最後の防衛線
 *
 * 環境変数:
 *   BASIC_AUTH_USER  : ユーザー名 (デフォルト: umalogi)
 *   BASIC_AUTH_PASS  : パスワード (未設定時は認証スキップ = 開発モード)
 */

const USER = process.env.BASIC_AUTH_USER ?? 'umalogi'
const PASS = process.env.BASIC_AUTH_PASS ?? ''

// 認証不要なパス（アイコン・マニフェスト・SW・オフラインページ）
const PUBLIC_PATHS = [
  '/manifest.json',
  '/sw.js',
  '/offline.html',
  '/favicon-32.png',
  '/icon-192.png',
  '/icon-512.png',
  '/apple-touch-icon.png',
]

export function middleware(req: NextRequest): NextResponse {
  // BASIC_AUTH_PASS 未設定 = ローカル開発モード（スキップ）
  if (!PASS) return NextResponse.next()

  const { pathname } = req.nextUrl

  // 公開パスはスルー
  if (PUBLIC_PATHS.some((p) => pathname === p)) return NextResponse.next()

  // _next 静的ファイルはスルー
  if (pathname.startsWith('/_next/')) return NextResponse.next()

  const auth = req.headers.get('authorization') ?? ''

  if (auth.startsWith('Basic ')) {
    try {
      const decoded  = Buffer.from(auth.slice(6), 'base64').toString('utf-8')
      const colonIdx = decoded.indexOf(':')
      if (colonIdx > 0) {
        const u = decoded.slice(0, colonIdx)
        const p = decoded.slice(colonIdx + 1)
        if (u === USER && p === PASS) return NextResponse.next()
      }
    } catch {
      // malformed base64 → 認証失敗として扱う
    }
  }

  // 未認証 → 401 + WWW-Authenticate ヘッダ
  return new NextResponse(
    '<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8"><title>UMALOGI — 認証</title>' +
    '<style>body{background:#02060e;color:#00c8ff;font-family:monospace;display:flex;' +
    'align-items:center;justify-content:center;height:100vh;margin:0;}' +
    'h1{font-size:1.2rem;letter-spacing:.1em;}</style></head>' +
    '<body><h1>// UMALOGI — 認証が必要です</h1></body></html>',
    {
      status: 401,
      headers: {
        'Content-Type': 'text/html; charset=utf-8',
        'WWW-Authenticate': 'Basic realm="UMALOGI", charset="UTF-8"',
        // Cloudflare がキャッシュしないよう明示
        'Cache-Control': 'no-store',
      },
    }
  )
}

export const config = {
  // API ルートも保護対象に含める
  matcher: ['/((?!_next/static|_next/image).*)'],
}
