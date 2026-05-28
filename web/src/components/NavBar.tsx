export default function NavBar() {
  return (
    <header
      className="relative z-10 flex items-center justify-between px-3 py-3 sm:px-6 sm:py-4 border-b border-[var(--border)]"
      style={{ background: 'var(--bg-surface)' }}
    >
      {/* ロゴ */}
      <div className="flex items-center gap-3">
        <div
          className="w-8 h-8 rounded border border-[var(--border)] flex items-center justify-center"
          style={{ background: 'var(--bg-muted)' }}
        >
          <span className="text-xs neon-text font-bold">U</span>
        </div>
        <span
          className="text-lg font-bold tracking-[0.15em] neon-text"
          style={{ fontFamily: 'Georgia, serif' }}
        >
          UMALOGI
        </span>
        <span className="text-xs text-[var(--text-muted)] tracking-widest">v0.1</span>
      </div>

      {/* ナビリンク: モバイルでは非表示 */}
      <nav className="hidden md:flex items-center gap-6 text-xs tracking-widest text-[var(--text-muted)]">
        <span className="neon-text border-b border-[var(--neon-cyan)] pb-0.5 cursor-pointer">
          RACES
        </span>
        <span className="hover:text-[var(--text-primary)] transition-colors cursor-pointer">PEDIGREE</span>
        <span className="hover:text-[var(--text-primary)] transition-colors cursor-pointer">WIN5</span>
        <span className="hover:text-[var(--text-primary)] transition-colors cursor-pointer">ANALYTICS</span>
      </nav>

      {/* ステータス */}
      <div className="flex items-center gap-2">
        <span className="w-2 h-2 rounded-full bg-[var(--neon-green)] pulse-neon" />
        <span className="text-xs text-[var(--text-muted)] tracking-wider hidden sm:inline">LIVE DB</span>
      </div>
    </header>
  )
}
