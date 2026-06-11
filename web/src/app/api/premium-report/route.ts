import { NextRequest, NextResponse } from 'next/server'
import fs from 'fs'
import path from 'path'

export const dynamic = 'force-dynamic'

// outputs/marketing/YYYYMMDD/premium_sanren.html を配信する。
// process.cwd() は Next.js 実行時に web/ ディレクトリを指す（lib/db.ts と同様）。
const MARKETING_DIR = path.join(process.cwd(), '..', 'outputs', 'marketing')

const DATE8 = /^\d{8}$/

/** marketing 配下で premium_sanren.html を持つ最新の日付ディレクトリを返す */
function latestReportDate(): string | null {
  if (!fs.existsSync(MARKETING_DIR)) return null
  const dirs = fs.readdirSync(MARKETING_DIR)
    .filter(d => DATE8.test(d))
    .filter(d => fs.existsSync(path.join(MARKETING_DIR, d, 'premium_sanren.html')))
    .sort()
  return dirs.length > 0 ? dirs[dirs.length - 1] : null
}

export async function GET(req: NextRequest) {
  try {
    const { searchParams } = req.nextUrl
    const rawDate = searchParams.get('date')
    // ディレクトリトラバーサル防止: YYYYMMDD 8桁のみ受理
    if (rawDate !== null && !DATE8.test(rawDate)) {
      return NextResponse.json({ error: 'invalid date (expected YYYYMMDD)' }, { status: 400 })
    }
    const date = rawDate ?? latestReportDate()
    if (!date) {
      return NextResponse.json({ error: 'no premium report found' }, { status: 404 })
    }
    const file = path.join(MARKETING_DIR, date, 'premium_sanren.html')
    if (!fs.existsSync(file)) {
      return NextResponse.json({ error: `no report for ${date}` }, { status: 404 })
    }
    const html = fs.readFileSync(file, { encoding: 'utf-8' })
    return new NextResponse(html, {
      status: 200,
      headers: {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store',
        'X-Report-Date': date,
      },
    })
  } catch (e) {
    return NextResponse.json({ error: String(e) }, { status: 500 })
  }
}
