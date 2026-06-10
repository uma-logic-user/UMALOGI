# ============================================================
#  UMALOGI 日次自動タスクの Windows タスクスケジューラ登録
#
#  登録タスク:
#    1) UMALOGI_BootStart      — PC 起動時に start_umalogi.bat を実行
#                                （オートパイロット/watchdog/ダッシュボード常駐）
#    2) UMALOGI_DailyMarketing — 毎日 21:00 に SNS 集客アセットを自動生成
#                                （py -m src.marketing.sns_generator）
#
#  使い方（管理者 PowerShell で 1 回実行）:
#    powershell -ExecutionPolicy Bypass -File scripts\bat\register_daily_tasks.ps1
#
#  解除:
#    Unregister-ScheduledTask -TaskName UMALOGI_BootStart -Confirm:$false
#    Unregister-ScheduledTask -TaskName UMALOGI_DailyMarketing -Confirm:$false
# ============================================================

$ErrorActionPreference = "Stop"
$Root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Write-Host "UMALOGI ルート: $Root"

# ── 1) ブート時: 本番スタック一括起動 ─────────────────────────────
$bootAction = New-ScheduledTaskAction `
    -Execute (Join-Path $Root "scripts\bat\start_umalogi.bat") `
    -WorkingDirectory $Root
$bootTrigger = New-ScheduledTaskTrigger -AtLogOn
Register-ScheduledTask `
    -TaskName "UMALOGI_BootStart" `
    -Action $bootAction `
    -Trigger $bootTrigger `
    -Description "UMALOGI 本番スタック（オートパイロット/watchdog/ダッシュボード）をログオン時に自動起動" `
    -Force | Out-Null
Write-Host "✅ UMALOGI_BootStart 登録完了（ログオン時起動）"

# ── 2) 毎日 21:00: SNS マーケティングアセット生成 ─────────────────
$mkCmd = "/c cd /d `"$Root`" && py -m src.marketing.sns_generator >> logs\marketing_daily.log 2>&1"
$mkAction = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $mkCmd -WorkingDirectory $Root
$mkTrigger = New-ScheduledTaskTrigger -Daily -At "21:00"
Register-ScheduledTask `
    -TaskName "UMALOGI_DailyMarketing" `
    -Action $mkAction `
    -Trigger $mkTrigger `
    -Description "SNS 無料予想・的中実績・動画台本を outputs/marketing/ へ日次自動生成" `
    -Force | Out-Null
Write-Host "✅ UMALOGI_DailyMarketing 登録完了（毎日 21:00）"

Write-Host ""
Write-Host "登録済みタスクの確認: Get-ScheduledTask -TaskName 'UMALOGI_*'"
