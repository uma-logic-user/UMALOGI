# JVLinkAgent.exe タスクスケジューラ登録スクリプト（管理者権限自動昇格）
# 使い方: powershell.exe -ExecutionPolicy Bypass -File scripts\register_task_scheduler_admin.ps1
#
# 管理者権限がない場合は UAC プロンプトを自動表示して昇格する。
# 社長は「はい」を押すだけで登録が完了する。

param([switch]$Elevated)

$TaskName  = "UMALOGI_TargetFrontierJV"
$ExePath   = "C:\Program Files (x86)\JRA-VAN\Data Lab\JVLinkAgent.exe"

# ── 管理者チェック ─────────────────────────────────────────────────────
function Test-Admin {
    $currentUser = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal   = New-Object Security.Principal.WindowsPrincipal($currentUser)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
}

# 管理者でなければ UAC で自己昇格
if (-not (Test-Admin)) {
    Write-Host "[INFO] 管理者権限が必要です。UACプロンプトを表示します..."
    $scriptPath = $MyInvocation.MyCommand.Path
    Start-Process powershell.exe `
        -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`" -Elevated" `
        -Verb RunAs `
        -Wait
    exit
}

Write-Host ""
Write-Host "======================================================"
Write-Host "  UMALOGI タスクスケジューラ完全登録（管理者権限）"
Write-Host "======================================================"
Write-Host ""

# ── 実行ファイル存在確認 ───────────────────────────────────────────────
if (-not (Test-Path $ExePath)) {
    Write-Host "[ERROR] 実行ファイルが見つかりません: $ExePath" -ForegroundColor Red
    Write-Host "  JRA-VAN Data Lab が正しくインストールされているか確認してください。"
    Read-Host "Enterで終了"
    exit 1
}
Write-Host "[OK] 実行ファイル確認: $ExePath" -ForegroundColor Green

# ── 既存タスク削除（冪等性）──────────────────────────────────────────
$existing = schtasks /Query /TN $TaskName 2>&1
if ($LASTEXITCODE -eq 0) {
    schtasks /Delete /TN $TaskName /F | Out-Null
    Write-Host "[INFO] 既存タスクを削除しました"
}

# ── タスク登録（RUNLEVEL HIGHEST = 管理者として実行）────────────────
Write-Host "[INFO] タスクスケジューラに登録中..."

$result = schtasks /Create `
    /TN  $TaskName `
    /TR  "`"$ExePath`"" `
    /SC  ONLOGON `
    /RL  HIGHEST `
    /DELAY "0001:00" `
    /F 2>&1

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "[SUCCESS] タスクスケジューラ登録完了！" -ForegroundColor Green
    Write-Host "  タスク名  : $TaskName"
    Write-Host "  実行ファイル: $ExePath"
    Write-Host "  トリガー  : ログオン時（1分遅延）"
    Write-Host "  実行レベル: 最高（管理者）"
    Write-Host ""
    Write-Host "→ 次回 Windows ログオン時から JVLinkAgent が自動起動します。"
} else {
    Write-Host "[ERROR] 登録失敗:" -ForegroundColor Red
    Write-Host $result
}

# ── 疎通確認 ─────────────────────────────────────────────────────────
Write-Host ""
Write-Host "[INFO] JVLink 疎通確認中..."
$jvTest = py -3-32 -c @"
import win32com.client, sys
try:
    jv  = win32com.client.Dispatch('JVDTLab.JVLink.1')
    ret = jv.JVInit('UNKNOWN')
    print(f'JVInit={ret}')
    jv.JVClose()
    sys.exit(0 if ret == 0 else 1)
except Exception as e:
    print(f'ERROR: {e}')
    sys.exit(1)
"@ 2>&1

Write-Host "[JVLink] $jvTest"

Write-Host ""
Read-Host "Enterで終了"
