"""
test_registry_yaml.py — Kiểm tra cấu trúc registry.yaml và tính toàn vẹn
của các code_path khai báo trong Pipeline Registry.
Chạy bởi Jenkins Stage 'Unit Tests'.
"""
import os

import pytest
import yaml

REGISTRY_PATH = "platform/pipeline-api/registry.yaml"


@pytest.fixture(scope="module")
def registry():
    with open(REGISTRY_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def test_registry_parseable():
    """registry.yaml phải parse được mà không lỗi YAML."""
    with open(REGISTRY_PATH, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert data is not None, "registry.yaml rỗng hoặc lỗi parse"


def test_registry_has_pipelines_key(registry):
    """registry.yaml phải có key 'pipelines' ở cấp cao nhất."""
    assert "pipelines" in registry, "Thiếu key 'pipelines' trong registry.yaml"


@pytest.mark.parametrize("pipeline_name", [
    "bronze_master",
    "bronze_xml",
    "bronze_api",
    "silver",
    "gold_dims",
    "gold",
])
def test_required_pipelines_exist(registry, pipeline_name):
    """Tất cả pipeline bắt buộc phải được khai báo trong registry."""
    pipelines = registry.get("pipelines", {})
    assert pipeline_name in pipelines, (
        f"Pipeline '{pipeline_name}' bị thiếu trong registry.yaml"
    )


def test_all_code_paths_exist(registry):
    """Mỗi pipeline version phải trỏ tới file .py tồn tại trong repo."""
    pipelines = registry.get("pipelines", {})
    missing = []
    for name, info in pipelines.items():
        versions = info.get("versions", {})
        for ver, vinfo in versions.items():
            code_path = vinfo.get("code_path", "")
            if code_path and not os.path.isfile(code_path):
                missing.append(f"{name}@{ver}: '{code_path}' không tồn tại")
    assert not missing, "Các code_path sau không tồn tại:\n" + "\n".join(missing)


def test_all_pipelines_have_latest_field(registry):
    """Mỗi pipeline phải có trường 'latest' chỉ rõ version mặc định."""
    pipelines = registry.get("pipelines", {})
    for name, info in pipelines.items():
        assert "latest" in info, f"Pipeline '{name}' thiếu trường 'latest'"


def test_all_pipelines_have_spark_conf(registry):
    """Mỗi pipeline version phải có 'spark_conf' để CustomSparkSubmitOperator lấy config."""
    pipelines = registry.get("pipelines", {})
    for name, info in pipelines.items():
        for ver, vinfo in info.get("versions", {}).items():
            assert "spark_conf" in vinfo, (
                f"Pipeline '{name}' version '{ver}' thiếu 'spark_conf'"
            )
