# UMALOGI watchdog をタスクスケジューラに登録する PowerShell スクリプト
# 実行: powershell -ExecutionPolicy Bypass -File scripts\_register_task.ps1

param(
    [string]$Python = "",
    [string]$Root   = "C:\dev\horse-racing-ai",
    [string]$Task   = "UMALOGI_Watchdog"
)

if (-not $Python) {
    # py launcher で現在のデフォルト Python を使用
    $Python = (Get-Command py -ErrorAction SilentlyContinue).Source
    if (-not $Python) {
        $Python = (Get-Command python -ErrorAction SilentlyContinue).Source
    }
}

$Script = "$Root\scripts\watchdog.py"
$LogDir = "$Root\logs"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

Write-Host "=== UMALOGI_Watchdog タスク登録 ===" -ForegroundColor Cyan
Write-Host "Python : $Python"
Write-Host "Script : $Script"
Write-Host "Root   : $Root"

$ErrorActionPreference = "Stop"

# 5分おき + 起動時 の繰り返しトリガー
$triggerBoot = New-ScheduledTaskTrigger -AtStartup
$triggerBoot.Delay = "PT2M"   # 起動2分後から開始

# 繰り返し設定（XML直接編集）
$repXml = @"
<Repetition xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Interval>PT5M</Interval>
  <StopAtDurationEnd>false</StopAtDurationEnd>
</Repetition>
"@
$repNode = ([xml]$repXml).DocumentElement
$triggerBoot.RepetitionDuration   = [System.TimeSpan]::Zero
$triggerBoot.RepetitionInterval   = [System.TimeSpan]::FromMinutes(5)

$ArgStr = '"' + $Script + '" --interval 5'
$action = New-ScheduledTaskAction `
    -Execute $Python `
    -Argument $ArgStr `
    -WorkingDirectory $Root

$settings = New-ScheduledTaskSettingsSet `
    -MultipleInstances        IgnoreNew `
    -ExecutionTimeLimit       ([System.TimeSpan]::Zero) `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -RestartCount             999 `
    -RestartInterval          (New-TimeSpan -Minutes 1) `
    -StartWhenAvailable

$principal = New-ScheduledTaskPrincipal `
    -UserId      "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType   Interactive `
    -RunLevel    Highest

# 既存タスクを削除
try {
    Unregister-ScheduledTask -TaskName $Task -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "既存タスクを削除しました" -ForegroundColor Yellow
} catch {}

# 登録
$registered = Register-ScheduledTask `
    -TaskName    $Task `
    -Action      $action `
    -Trigger     $triggerBoot `
    -Settings    $settings `
    -Principal   $principal `
    -Description "UMALOGI 自己修復ウォッチドッグ: 5分おきにOdds欠損を監視・自動補完"

if ($registered) {
    Write-Host "✅ タスク登録成功: $Task" -ForegroundColor Green
    Write-Host ""
    Write-Host "詳細:" -ForegroundColor Cyan
    Write-Host "  起動条件: PC起動2分後 + 5分おき繰り返し"
    Write-Host "  権限    : HighestAvailable (最上位)"
    Write-Host "  失敗時  : 1分後に自動再起動 (999回)"
    Write-Host "  電源条件: バッテリー動作でも継続"
    Write-Host ""
    # 即時起動
    Start-ScheduledTask -TaskName $Task
    Write-Host "✅ watchdog を今すぐ起動しました" -ForegroundColor Green
} else {
    Write-Host "❌ タスク登録失敗" -ForegroundColor Red
    exit 1
}
