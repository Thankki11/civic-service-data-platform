"""
test_dag_parse.py — Kiểm tra cú pháp tất cả DAG files không bị lỗi import/syntax.
Chạy bởi Jenkins Stage 'Unit Tests'.
"""
import ast
import glob

import pytest


def _get_dag_files():
    return glob.glob("orchestration/dags/*.py")


@pytest.mark.parametrize("dag_file", _get_dag_files())
def test_dag_syntax(dag_file):
    """Mỗi file DAG phải parse được qua ast mà không có SyntaxError."""
    with open(dag_file, encoding="utf-8") as f:
        source = f.read()
    try:
        tree = ast.parse(source)
        assert tree is not None, f"{dag_file}: ast.parse trả về None"
    except SyntaxError as e:
        pytest.fail(f"{dag_file} có lỗi syntax: {e}")


@pytest.mark.parametrize("dag_file", _get_dag_files())
def test_dag_has_dag_definition(dag_file):
    """Mỗi file DAG phải chứa khai báo DAG (có chuỗi 'dag_id')."""
    with open(dag_file, encoding="utf-8") as f:
        content = f.read()
    assert "dag_id" in content, (
        f"{dag_file} không chứa 'dag_id' — có thể không phải file DAG hợp lệ"
    )
