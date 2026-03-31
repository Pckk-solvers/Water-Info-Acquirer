from __future__ import annotations

import json

from hydrology_graphs.io.threshold_store import group_thresholds, load_thresholds


def test_load_thresholds_sorts_by_priority_and_excludes_disabled(tmp_path):
    csv_text = (
        "source,station_key,graph_type,line_name,value,unit,line_color,line_style,line_width,priority,enabled\n"
        "jma,111,hyetograph,low,10,mm,#111111,solid,1.0,1,true\n"
        "jma,111,hyetograph,high,20,mm,#222222,dashed,1.5,5,true\n"
        "jma,111,hyetograph,disabled,30,mm,#333333,solid,1.0,9,false\n"
        "jma,111,hyetograph,same_priority_tail,40,mm,#444444,dotted,2.0,5,true\n"
    )
    path = tmp_path / "thresholds.csv"
    path.write_text(csv_text, encoding="utf-8")

    result = load_thresholds(path)

    assert [line.line_name for line in result.lines] == ["high", "same_priority_tail", "low"]
    grouped = group_thresholds(result.lines)
    assert grouped["jma|111|hyetograph"][0].line_name == "high"
    assert all(line.enabled for line in result.lines)


def test_load_thresholds_json(tmp_path):
    payload = [
        {
            "source": "jma",
            "station_key": "111",
            "graph_type": "annual_max_rainfall",
            "line_name": "基準",
            "value": 10,
            "unit": "mm",
        }
    ]
    path = tmp_path / "thresholds.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    result = load_thresholds(path)

    assert len(result.lines) == 1
    assert result.lines[0].graph_type == "annual_max_rainfall"
