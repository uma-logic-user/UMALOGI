import type { NextConfig } from 'next'

const nextConfig: NextConfig = {
  webpack(config) {
    // better-sqlite3 はネイティブ Node モジュールのため externals に追加
    config.externals = [...(config.externals ?? []), 'better-sqlite3']
    return config
  },
}

export default nextConfig
