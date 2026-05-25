$desktop = [Environment]::GetFolderPath('Desktop')
$wsh     = New-Object -ComObject WScript.Shell

$lnk = $wsh.CreateShortcut("$desktop\UMALOGI_UI.lnk")
$lnk.TargetPath       = 'C:\dev\horse-racing-ai\start_ui.bat'
$lnk.WorkingDirectory = 'C:\dev\horse-racing-ai'
$lnk.Description      = 'UMALOGI Next.js UI サーバー起動'
$lnk.Save()

$lnk2 = $wsh.CreateShortcut("$desktop\UMALOGI_AI.lnk")
$lnk2.TargetPath       = 'C:\dev\horse-racing-ai\start_ai.bat'
$lnk2.WorkingDirectory = 'C:\dev\horse-racing-ai'
$lnk2.Description      = 'UMALOGI AI スケジューラー起動'
$lnk2.Save()

Write-Host "ショートカット作成完了: $desktop\UMALOGI_UI.lnk / UMALOGI_AI.lnk"
