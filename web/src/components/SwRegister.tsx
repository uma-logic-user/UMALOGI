'use client'

import { useEffect } from 'react'

/**
 * Service Worker 登録コンポーネント
 * Next.js App Router の Client Component として layout.tsx から呼び出す。
 */
export default function SwRegister() {
  useEffect(() => {
    if (typeof window === 'undefined' || !('serviceWorker' in navigator)) return

    const register = async () => {
      try {
        const reg = await navigator.serviceWorker.register('/sw.js', {
          scope: '/',
          updateViaCache: 'none',
        })

        // 新バージョン検出時にユーザーへ通知
        reg.addEventListener('updatefound', () => {
          const newWorker = reg.installing
          if (!newWorker) return
          newWorker.addEventListener('statechange', () => {
            if (
              newWorker.state === 'installed' &&
              navigator.serviceWorker.controller
            ) {
              // 更新を即反映（リロード不要）
              newWorker.postMessage({ type: 'SKIP_WAITING' })
            }
          })
        })
      } catch (err) {
        // SW 登録失敗はサイレントに無視（機能降格）
        console.warn('[SW] 登録失敗:', err)
      }
    }

    // ページロード完了後に登録（FCP を遅らせない）
    if (document.readyState === 'complete') {
      register()
    } else {
      window.addEventListener('load', register, { once: true })
    }
  }, [])

  return null
}
