import { NextRequest, NextResponse } from 'next/server'
import fs from 'fs'
import path from 'path'

export const dynamic = 'force-dynamic'

// outputs/marketing/YYYYMMDD/premium_signals.json（premium_pack.py が生成）を配信する。
const MARKETING_DIR = path.join(process.cwd(), '..', 'outputs', 'marketing')

const DATE8 = /^\d{8}$/

// 文字化け検知（CLAUDE.md 16条 TypeScript 簡易版）— 化け文字列は絶対に返さない
function isGarbled(s: string): boolean {
  return /(\?[\x21-\x7e]){2,}/.test(s)
    || /[｡-ﾟ]/.test(s)
    || /[Ͱ-Ͽ]{2,}/.test(s)
}

function cleanStrings(v: unknown): unknown {
  if (typeof v === 'string') {
    const s = v.replace(/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/g, '').trim()
    return isGarbled(s) ? '' : s
  }
  if (Array.isArray(v)) return v.map(cleanStrings)
  if (v !== null && typeof v === 'object') {
    return Object.fromEntries(Object.entries(v as Record<string, unknown>)
      .map(([k, x]) => [k, cleanStrings(x)]))
  }
  return v
}

function latestSignalsDate(): string | null {
  if (!fs.existsSync(MARKETING_DIR)) return null
  const dirs = fs.readdirSync(MARKETING_DIR)
    .filter(d => DATE8.test(d))
    .filter(d => fs.existsSync(path.join(MARKETING_DIR, d, 'premium_signals.json')))
    .sort()
  return dirs.length > 0 ? dirs[dirs.length - 1] : null
}

export async function GET(req: NextRequest) {
  try {
    const { searchParams } = req.nextUrl
    const rawDate = searchParams.get('date')
    if (rawDate !== null && !DATE8.test(rawDate)) {
      return NextResponse.json({ error: 'invalid date (expected YYYYMMDD)' }, { status: 400 })
    }
    const date = rawDate ?? latestSignalsDate()
    if (!date) {
      // シグナル未生成（週末バッチ前）は空構造を返し、UI 側で案内表示する
      return NextResponse.json({
        date: null, n_races: 0, n_signals: 0, max_ev: 0, races: [],
      })
    }
    const file = path.join(MARKETING_DIR, date, 'premium_signals.json')
    if (!fs.existsSync(file)) {
      return NextResponse.json({ error: `no signals for ${date}` }, { status: 404 })
    }
    const data = JSON.parse(fs.readFileSync(file, { encoding: 'utf-8' }))
    return NextResponse.json(cleanStrings(data), {
      headers: { 'Cache-Control': 'no-store', 'X-Report-Date': date },
    })
  } catch (e) {
    return NextResponse.json({ error: String(e) }, { status: 500 })
  }
}
