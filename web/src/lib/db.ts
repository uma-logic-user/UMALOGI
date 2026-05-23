import Database from 'better-sqlite3'
import path from 'path'

// process.cwd() は Next.js 実行時に web/ ディレクトリを指す
const DB_PATH = path.join(process.cwd(), '..', 'data', 'umalogi.db')

// HMR でモジュールが再ロードされても同一インスタンスを保持するため global に保存する
declare global {
  // eslint-disable-next-line no-var
  var __umalogi_db: Database.Database | undefined
}

export function getDb(): Database.Database {
  if (!global.__umalogi_db) {
    global.__umalogi_db = new Database(DB_PATH, { readonly: true })
    global.__umalogi_db.pragma('journal_mode = WAL')
    global.__umalogi_db.pragma('cache_size = -16384')   // 16MB
    global.__umalogi_db.pragma('mmap_size = 268435456') // 256MB
  }
  return global.__umalogi_db
}
