# UMALOGI — ログオン時自動起動登録スクリプト（Tailscale版）
# 管理者権限不要。HKCU\Run レジストリにログオントリガーで登録する。
#
# 実行: .\scripts\install_autostart.ps1
# 削除: .\scripts\install_autostart.ps1 -Uninstall

param([switch]$Uninstall)

$ROOT    = (Resolve-Path "$PSScriptRoot\..").Path
$WEB_DIR = Join-Path $ROOT "web"
$LOG_DIR = Join-Path $ROOT "data"
$REG_PATH = "HKCU:\SOFTWARE\Microsoft\Windows\CurrentVersion\Run"
$REG_NAME = "UMALOGI-Dashboard"

if ($Uninstall) {
    Remove-ItemProperty -Path $REG_PATH -Name $REG_NAME -ErrorAction SilentlyContinue
    Write-Host "✅ $REG_NAME を自動起動から削除しました" -ForegroundColor Yellow
    exit 0
}

# Next.js をビルド済みの状態で 0.0.0.0:3000 で起動
# HKCU\Run は管理者権限不要・現在ユーザーのログオン時に実行される
$cmd = "cmd.exe /c `"cd /d `"$WEB_DIR`" && npm run start >> `"$LOG_DIR\nextjs.log`" 2>&1`""
Set-ItemProperty -Path $REG_PATH -Name $REG_NAME -Value $cmd

Write-Host "✅ $REG_NAME を登録しました (ログオン時に自動起動)" -ForegroundColor Green
Write-Host ""
Write-Host "登録された起動コマンド:" -ForegroundColor Cyan
Write-Host "  $cmd" -ForegroundColor White
Write-Host ""
Write-Host "今すぐ起動する場合:" -ForegroundColor Cyan
Write-Host "  Start-Process cmd.exe -ArgumentList '/c cd /d `"$WEB_DIR`" && npm run start'" -ForegroundColor White
Write-Host ""
Write-Host "ログ確認:" -ForegroundColor Cyan
Write-Host "  $LOG_DIR\nextjs.log" -ForegroundColor White
