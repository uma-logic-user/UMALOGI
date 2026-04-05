export default function NavBar() {
  return (
    <header className="relative z-10 flex items-center justify-between px-6 py-4 border-b border-[rgba(0,200,255,0.15)]">
      {/* ロゴ */}
      <div className="flex items-center gap-3">
        <div className="w-8 h-8 rounded border border-[rgba(0,200,255,0.5)] flex items-center justify-center"
          style={{ boxShadow: '0 0 12px rgba(0,200,255,0.4)' }}>
          <span className="text-xs neon-text font-bold">U</span>
        </div>
        <span className="text-lg font-bold tracking-[0.25em] neon-text scanlines">
          UMALOGI
        </span>
        <span className="text-xs text-[var(--text-muted)] tracking-widest">v0.1</span>
      </div>

      {/* ナビリンク */}
      <nav className="flex items-center gap-6 text-xs tracking-widest text-[var(--text-muted)]">
        <span className="neon-text border-b border-[var(--neon-cyan)] pb-0.5 cursor-pointer">
          RACES
        </span>
        <span className="hover:text-[var(--neon-cyan)] transition-colors cursor-pointer">PEDIGREE</span>
        <span className="hover:text-[var(--neon-cyan)] transition-colors cursor-pointer">WIN5</span>
        <span className="hover:text-[var(--neon-cyan)] transition-colors cursor-pointer">ANALYTICS</span>
      </nav>

      {/* ステータス */}
      <div className="flex items-center gap-2">
        <span className="w-2 h-2 rounded-full bg-[var(--neon-green)] pulse-neon"
          style={{ boxShadow: '0 0 8px var(--neon-green)' }} />
        <span className="text-xs text-[var(--text-muted)] tracking-wider">LIVE DB</span>
      </div>
    </header>
  )
}
