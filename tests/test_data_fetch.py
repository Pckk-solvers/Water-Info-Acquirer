import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.main_datetime import (
    EmptyExcelWarning as HourlyEmptyExcelWarning,
    process_data_for_code,
)
from src.datemode import (
    EmptyExcelWarning as DailyEmptyExcelWarning,
    process_period_date_display_for_code,
)


META_HTML = """
<html>
  <body>
    <table border="1" cellpadding="2" cellspacing="1">
      <tr><td>idx</td><td>name</td></tr>
      <tr><td>001</td><td>TestStation</td></tr>
    </table>
  </body>
</html>
"""

DATA_HTML = """
<html>
  <body>
    <td><font>1</font></td>
    <td><font>2</font></td>
    <td><font>3</font></td>
    <td><font>4</font></td>
    <td><font>5</font></td>
  </body>
</html>
"""

MISSING_TABLE_HTML = "<html><body>NO DATA</body></html>"


class DummyResponse:
    def __init__(self, text: str):
        self.text = text
        self.encoding = "utf-8"


class DataFetchTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = Path(tempfile.mkdtemp(prefix="wia-tests-"))
        original_writer = pd.ExcelWriter

        def _excel_writer(file_name, *args, **kwargs):
            target = self.tmpdir / file_name
            target.parent.mkdir(parents=True, exist_ok=True)
            return original_writer(target, *args, **kwargs)

        self._excel_patcher = patch.object(pd, "ExcelWriter", new=_excel_writer)
        self._excel_patcher.start()

    def tearDown(self):
        self._excel_patcher.stop()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_process_data_for_code_writes_excel(self):
        responses = [DummyResponse(META_HTML), DummyResponse(DATA_HTML)]
        with patch("requests.get", side_effect=responses):
            result = process_data_for_code(
                code="123456789012345",
                Y1="2024",
                Y2="2024",
                M1="1月",
                M2="1月",
                mode_type="S",
                single_sheet=False,
            )

        self.assertTrue(result.endswith("_WH.xlsx"))
        self.assertTrue((self.tmpdir / result).exists())

    def test_process_period_date_display_for_code_writes_excel(self):
        responses = [DummyResponse(META_HTML), DummyResponse(DATA_HTML)]
        with patch("requests.get", side_effect=responses):
            result = process_period_date_display_for_code(
                code="123456789012345",
                Y1="2024",
                Y2="2024",
                M1="1月",
                M2="1月",
                mode_type="S",
                single_sheet=False,
            )

        self.assertTrue(result.endswith("_WD.xlsx"))
        self.assertTrue((self.tmpdir / result).exists())

    def test_process_data_for_code_missing_table(self):
        responses = [DummyResponse(MISSING_TABLE_HTML)]
        with patch("requests.get", side_effect=responses):
            with self.assertRaises(HourlyEmptyExcelWarning):
                process_data_for_code(
                    code="999999999999999",
                    Y1="2024",
                    Y2="2024",
                    M1="1月",
                    M2="1月",
                    mode_type="S",
                    single_sheet=False,
                )

    def test_process_period_date_display_for_code_missing_table(self):
        responses = [DummyResponse(MISSING_TABLE_HTML)]
        with patch("requests.get", side_effect=responses):
            with self.assertRaises(DailyEmptyExcelWarning):
                process_period_date_display_for_code(
                    code="999999999999999",
                    Y1="2024",
                    Y2="2024",
                    M1="1月",
                    M2="1月",
                    mode_type="S",
                    single_sheet=False,
                )


if __name__ == "__main__":
    unittest.main()
