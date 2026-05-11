# UMALOGI — TCP 3000番ポート ファイアウォール開放スクリプト
# 【実行方法】PowerShellを「管理者として実行」して:
#   .\scripts\open_firewall_3000.ps1
#
# または Claude Code のプロンプトで:
#   ! powershell -ExecutionPolicy Bypass -File .\scripts\open_firewall_3000.ps1

#Requires -RunAsAdministrator

$RuleName = 'UMALOGI Dashboard (TCP 3000)'

# 既存ルールがあれば削除して再作成
Remove-NetFirewallRule -DisplayName $RuleName -ErrorAction SilentlyContinue

$rule = New-NetFirewallRule `
    -DisplayName  $RuleName `
    -Description  'Tailscale経由のUMALOGIダッシュボードアクセス (Next.js port 3000)' `
    -Direction    Inbound `
    -Protocol     TCP `
    -LocalPort    3000 `
    -Action       Allow `
    -Profile      Any `
    -Enabled      True

Write-Host "✅ ファイアウォールルール作成完了:" -ForegroundColor Green
Write-Host "   名前    : $($rule.DisplayName)" -ForegroundColor Cyan
Write-Host "   方向    : 受信 (Inbound)" -ForegroundColor Cyan
Write-Host "   プロトコル: TCP" -ForegroundColor Cyan
Write-Host "   ポート  : 3000" -ForegroundColor Cyan
Write-Host "   アクション: 許可 (Allow)" -ForegroundColor Cyan
Write-Host ""
Write-Host "スマホからのアクセス URL:" -ForegroundColor Yellow
$tsIP = (Get-NetIPAddress -InterfaceAlias 'Tailscale' -AddressFamily IPv4 -ErrorAction SilentlyContinue).IPAddress
if ($tsIP) {
    Write-Host "  http://$($tsIP):3000" -ForegroundColor Green
} else {
    Write-Host "  http://<TailscaleのIPアドレス>:3000" -ForegroundColor Yellow
    Write-Host "  ※ Tailscaleインストール後、'tailscale ip -4' で確認できます" -ForegroundColor Gray
}
