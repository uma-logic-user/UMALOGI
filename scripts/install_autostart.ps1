# UMALOGI — ログオン時自動起動登録スクリプト（Tailscale版）
# 管理者権限不要。タスクスケジューラにログオントリガーで登録する。
#
# 実行: .\scripts\install_autostart.ps1
# 削除: .\scripts\install_autostart.ps1 -Uninstall

param([switch]$Uninstall)

$ROOT    = (Resolve-Path "$PSScriptRoot\..").Path
$WEB_DIR = Join-Path $ROOT "web"
$LOG_DIR = Join-Path $ROOT "data"
$TASK    = "UMALOGI-Dashboard"

if ($Uninstall) {
    Unregister-ScheduledTask -TaskName $TASK -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "✅ $TASK タスクを削除しました" -ForegroundColor Yellow
    exit 0
}

# Next.js をビルド済みの状態で 0.0.0.0:3000 で起動
$action = New-ScheduledTaskAction `
    -Execute       "cmd.exe" `
    -Argument      "/c cd /d `"$WEB_DIR`" && npm run start > `"$LOG_DIR\nextjs.log`" 2>&1" `
    -WorkingDirectory $WEB_DIR

$trigger  = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 2) `
    -StartWhenAvailable

Unregister-ScheduledTask -TaskName $TASK -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask `
    -TaskName    $TASK `
    -Description "UMALOGI Next.js ダッシュボード (0.0.0.0:3000 / Tailscale対応)" `
    -Action      $action `
    -Trigger     $trigger `
    -Settings    $settings `
    -RunLevel    Limited | Out-Null

Write-Host "✅ $TASK を登録しました (ログオン時に自動起動)" -ForegroundColor Green
Write-Host ""
Write-Host "今すぐ起動する場合:" -ForegroundColor Cyan
Write-Host "  Start-ScheduledTask -TaskName '$TASK'" -ForegroundColor White
Write-Host ""
Write-Host "ログ確認:" -ForegroundColor Cyan
Write-Host "  data\nextjs.log" -ForegroundColor White
