import win32com.client

def test_jvlink():
    # JV-Linkオブジェクトの呼び出し（成功済み！）
    jv = win32com.client.Dispatch("JVDTLab.JVLink.1")
    # 初期化（成功済み！）
    status = jv.JVInit("UNKNOWN")
    
    if status == 0:
        print("JV-Link 開通成功！JRA公式データサーバーに接続しました！")
    else:
        print("初期化エラー: status=", status)

if __name__ == "__main__":
    test_jvlink()