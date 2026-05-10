# UMALOGI — Windows タスクスケジューラ 自動起動登録スクリプト
# 管理者権限不要でログオン時に自動起動する。
#
# 登録内容:
#   タスク1: UMALOGI-NextJS   — Next.js ダッシュボード (npm run start)
#   タスク2: UMALOGI-Tunnel   — Cloudflare Named Tunnel
#
# 実行方法:
#   PowerShell で: .\scripts\install_tunnel_service.ps1
#   削除する場合: .\scripts\install_tunnel_service.ps1 -Uninstall

param(
    [switch]$Uninstall,
    [int]$Port = 3000
)

$ErrorActionPreference = "Stop"
$ROOT     = (Resolve-Path "$PSScriptRoot\..").Path
$WEB_DIR  = Join-Path $ROOT "web"
$CF_EXE   = Join-Path $ROOT "bin\cloudflared.exe"
$CF_CONF  = Join-Path $ROOT ".cloudflare\config.yml"
$LOG_DIR  = Join-Path $ROOT "data"

Write-Host "UMALOGI 自動起動スクリプト" -ForegroundColor Cyan
Write-Host "ROOT: $ROOT" -ForegroundColor Gray

# ── アンインストール ─────────────────────────────────────────────────────
if ($Uninstall) {
    foreach ($name in @("UMALOGI-NextJS", "UMALOGI-Tunnel")) {
        try {
            Unregister-ScheduledTask -TaskName $name -Confirm:$false -ErrorAction SilentlyContinue
            Write-Host "削除: $name" -ForegroundColor Yellow
        } catch {}
    }
    Write-Host "アンインストール完了" -ForegroundColor Green
    exit 0
}

# ── 前提チェック ──────────────────────────────────────────────────────────
if (-not (Test-Path $WEB_DIR)) {
    Write-Error "web/ ディレクトリが見つかりません: $WEB_DIR"
}
if (-not (Test-Path $CF_CONF)) {
    Write-Host "⚠️  Cloudflare 設定ファイルが見つかりません: $CF_CONF" -ForegroundColor Yellow
    Write-Host "   先に py scripts/setup_named_tunnel.py --create を実行してください。" -ForegroundColor Yellow
    Write-Host "   Next.js タスクのみ登録します..." -ForegroundColor Yellow
}

# ── タスク1: UMALOGI-NextJS ──────────────────────────────────────────────
Write-Host "`n[1/2] UMALOGI-NextJS タスクを登録中..." -ForegroundColor Cyan

$nextAction = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument "/c cd /d `"$WEB_DIR`" && npm run build && npm run start -- --port $Port > `"$LOG_DIR\nextjs.log`" 2>&1" `
    -WorkingDirectory $WEB_DIR

$trigger    = New-ScheduledTaskTrigger -AtLogOn
$settings   = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 2) `
    -StartWhenAvailable

# 既存タスクを一旦削除
Unregister-ScheduledTask -TaskName "UMALOGI-NextJS" -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask `
    -TaskName    "UMALOGI-NextJS" `
    -Description "UMALOGI ダッシュボード Next.js サーバー (port $Port)" `
    -Action      $nextAction `
    -Trigger     $trigger `
    -Settings    $settings `
    -RunLevel    Limited | Out-Null

Write-Host "✅ UMALOGI-NextJS 登録完了 (ログオン時自動起動)" -ForegroundColor Green

# ── タスク2: UMALOGI-Tunnel ──────────────────────────────────────────────
if (Test-Path $CF_CONF) {
    Write-Host "`n[2/2] UMALOGI-Tunnel タスクを登録中..." -ForegroundColor Cyan

    # Next.js 起動後 30 秒待ってからトンネルを開く
    $tunnelAction = New-ScheduledTaskAction `
        -Execute "cmd.exe" `
        -Argument "/c timeout /t 30 /nobreak && `"$CF_EXE`" tunnel --config `"$CF_CONF`" run umalogi > `"$LOG_DIR\tunnel.log`" 2>&1" `
        -WorkingDirectory $ROOT

    $tunnelTrigger  = New-ScheduledTaskTrigger -AtLogOn
    $tunnelSettings = New-ScheduledTaskSettingsSet `
        -ExecutionTimeLimit ([TimeSpan]::Zero) `
        -RestartCount 5 `
        -RestartInterval (New-TimeSpan -Minutes 1) `
        -StartWhenAvailable

    Unregister-ScheduledTask -TaskName "UMALOGI-Tunnel" -Confirm:$false -ErrorAction SilentlyContinue

    Register-ScheduledTask `
        -TaskName    "UMALOGI-Tunnel" `
        -Description "UMALOGI Cloudflare Named Tunnel (自動復旧: 5回/1分間隔)" `
        -Action      $tunnelAction `
        -Trigger     $tunnelTrigger `
        -Settings    $tunnelSettings `
        -RunLevel    Limited | Out-Null

    Write-Host "✅ UMALOGI-Tunnel 登録完了 (ログオン30秒後に起動)" -ForegroundColor Green
} else {
    Write-Host "[2/2] Cloudflare 設定なし — Tunnel タスクをスキップ" -ForegroundColor Yellow
}

# ── 登録確認 ─────────────────────────────────────────────────────────────
Write-Host "`n=== 登録済みタスク ===" -ForegroundColor Cyan
Get-ScheduledTask | Where-Object { $_.TaskName -like "UMALOGI-*" } |
    Select-Object TaskName, State, Description |
    Format-Table -AutoSize

Write-Host @"

=== 次のステップ ===
  1. 今すぐ起動テスト:
     Start-ScheduledTask -TaskName 'UMALOGI-NextJS'
     Start-ScheduledTask -TaskName 'UMALOGI-Tunnel'

  2. 停止:
     Stop-ScheduledTask  -TaskName 'UMALOGI-NextJS'
     Stop-ScheduledTask  -TaskName 'UMALOGI-Tunnel'

  3. アンインストール:
     .\scripts\install_tunnel_service.ps1 -Uninstall

  4. ログ確認:
     data\nextjs.log  / data\tunnel.log
"@ -ForegroundColor Gray
