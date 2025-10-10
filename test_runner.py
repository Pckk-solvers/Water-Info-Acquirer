#!/usr/bin/env python3
"""
テスト実行用スクリプト
"""
import subprocess
import sys
from pathlib import Path


def run_tests():
    """テストを実行"""
    print("=== テスト環境の確認 ===")
    
    # pytest がインストールされているか確認
    try:
        result = subprocess.run([sys.executable, "-m", "pytest", "--version"], 
                              capture_output=True, text=True)
        print(f"pytest version: {result.stdout.strip()}")
    except Exception as e:
        print(f"pytest が見つかりません: {e}")
        return False
    
    print("\n=== 基本テストの実行 ===")
    
    # 基本テストを実行
    cmd = [
        sys.executable, "-m", "pytest", 
        "tests/", 
        "-v", 
        "--tb=short"
    ]
    
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        print("\n✅ 基本テストが成功しました")
    else:
        print("\n❌ テストが失敗しました")
        return False
    
    print("\n=== カバレッジ付きテストの実行 ===")
    
    # カバレッジ付きテストを実行
    cmd_cov = [
        sys.executable, "-m", "pytest", 
        "tests/", 
        "--cov=src/wia", 
        "--cov-report=term-missing",
        "--cov-report=html"
    ]
    
    result = subprocess.run(cmd_cov)
    
    if result.returncode == 0:
        print("\n✅ カバレッジ付きテストが成功しました")
        print("📊 HTMLカバレッジレポートが htmlcov/ に生成されました")
    else:
        print("\n❌ カバレッジ付きテストが失敗しました")
        return False
    
    return True


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)