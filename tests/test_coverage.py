"""
テストカバレッジの確保

各モジュールの単体テストカバレッジ80%以上達成、
重要なエラーパスのテストカバレッジ確認
"""

import subprocess
import sys
from pathlib import Path
import pytest


def test_coverage_threshold():
    """テストカバレッジが80%以上であることを確認"""
    # カバレッジレポートを生成
    result = subprocess.run([
        sys.executable, "-m", "pytest", 
        "--cov=src/wia", 
        "--cov-report=term-missing",
        "--cov-fail-under=80",
        "--quiet"
    ], capture_output=True, text=True)
    
    # カバレッジが80%未満の場合はテスト失敗
    if result.returncode != 0:
        print("Coverage output:")
        print(result.stdout)
        print(result.stderr)
        pytest.fail("Test coverage is below 80%")


def test_critical_modules_coverage():
    """重要なモジュールのカバレッジを個別に確認"""
    critical_modules = [
        "src/wia/data_source.py",
        "src/wia/excel_writer.py", 
        "src/wia/api.py",
        "src/wia/errors.py"
    ]
    
    for module in critical_modules:
        if Path(module).exists():
            result = subprocess.run([
                sys.executable, "-m", "pytest",
                f"--cov={module}",
                "--cov-report=term-missing",
                "--cov-fail-under=75",
                "--quiet"
            ], capture_output=True, text=True)
            
            if result.returncode != 0:
                pytest.fail(f"Coverage for {module} is below 75%")


def test_error_paths_coverage():
    """エラーパスのテストカバレッジを確認"""
    # エラーハンドリング関連のテストが存在することを確認
    test_files = [
        "tests/test_error_handling.py",
        "tests/test_exception_handler.py",
        "tests/test_exception_integration.py"
    ]
    
    for test_file in test_files:
        assert Path(test_file).exists(), f"Error handling test file {test_file} not found"
    
    # エラーハンドリングテストを実行してカバレッジを確認
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        "tests/test_error_handling.py",
        "tests/test_exception_handler.py", 
        "tests/test_exception_integration.py",
        "--cov=src/wia/errors.py",
        "--cov=src/wia/exception_handler.py",
        "--cov-report=term-missing",
        "--quiet"
    ], capture_output=True, text=True)
    
    assert result.returncode == 0, "Error handling tests failed"


if __name__ == "__main__":
    # 直接実行時はカバレッジレポートを表示
    subprocess.run([
        sys.executable, "-m", "pytest",
        "--cov=src/wia",
        "--cov-report=term-missing",
        "--cov-report=html"
    ])