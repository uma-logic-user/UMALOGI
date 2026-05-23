/**
 * APIレスポンスの文字化け自動検出・サニタイズユーティリティ。
 *
 * 検出対象:
 *   - C0/C1 制御文字 (0x00-0x08, 0x0B-0x0C, 0x0E-0x1F, 0x7F-0x9F)
 *   - Unicode 置換文字の連続 (U+FFFD)
 *   - '?' の3連続以上（SJIS誤デコードの典型パターン）
 */

const CTRL_RE   = /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]/g
const GARBAGE_RE = /(\?{3,})|([�]{2,})/

/** 1文字列をサニタイズして返す。制御文字除去 + 前後トリム。 */
function sanitizeStr(v: string): string {
  return v.replace(CTRL_RE, '').trim()
}

/** 値が文字化けを含むか判定する。 */
function hasGarbage(v: string): boolean {
  return GARBAGE_RE.test(v)
}

/** 任意の値を再帰的にサニタイズする。 */
export function sanitizeDeep(data: unknown): unknown {
  if (typeof data === 'string') return sanitizeStr(data)
  if (Array.isArray(data))      return data.map(sanitizeDeep)
  if (data !== null && typeof data === 'object') {
    return Object.fromEntries(
      Object.entries(data as Record<string, unknown>).map(([k, v]) => [k, sanitizeDeep(v)])
    )
  }
  return data
}

/** 文字化けを検出してサーバーコンソールに警告を出す（再帰）。 */
function warnRecursive(data: unknown, context: string, path: string): void {
  if (typeof data === 'string') {
    if (hasGarbage(data)) {
      console.warn(`[validateResponse] 文字化け検出 ${context} ${path}: ${JSON.stringify(data.slice(0, 60))}`)
    }
    return
  }
  if (Array.isArray(data)) {
    data.forEach((item, i) => warnRecursive(item, context, `${path}[${i}]`))
    return
  }
  if (data !== null && typeof data === 'object') {
    for (const [k, v] of Object.entries(data as Record<string, unknown>)) {
      warnRecursive(v, context, `${path}.${k}`)
    }
  }
}

/**
 * レスポンスデータをサニタイズし、文字化けがあればコンソール警告を出す。
 *
 * 使い方:
 *   return NextResponse.json(validateResponse(output, '[/api/races]'))
 */
export function validateResponse<T>(data: T, context: string): T {
  warnRecursive(data, context, '$')
  return sanitizeDeep(data) as T
}
