"""Azure OpenAI 接続テスト + モデルデプロイスクリプト。"""

import json
import os
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

API_KEY = os.environ.get("AZURE_OPENAI_API_KEY", "")
ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
RG = "rg-ic-test-evaluation"
ACCOUNT = "keiba-openai"
API_VERSION = "2024-12-01-preview"

print(f"Endpoint: {ENDPOINT}")
print(f"API Key: {API_KEY[:10]}...{API_KEY[-4:]}")


def test_gpt4o():
    """gpt-4o デプロイ済みモデルの接続テスト。"""
    from openai import AzureOpenAI

    client = AzureOpenAI(
        api_key=API_KEY,
        azure_endpoint=ENDPOINT,
        api_version=API_VERSION,
    )
    print("\n--- gpt-4o 接続テスト ---")
    try:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": "1+1=? 数字のみ回答"}],
            max_tokens=10,
        )
        print(f"OK: {resp.choices[0].message.content}")
        print(f"Model: {resp.model}")
        print(f"Usage: {resp.usage}")
        return True
    except Exception as e:
        print(f"ERROR: {e}")
        return False


def deploy_model(deployment_name, model_name, model_version):
    """Azure CLIでモデルをデプロイ。"""
    print(f"\n--- {model_name} デプロイ中 ---")
    az = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"
    cmd = [
        az, "cognitiveservices", "account", "deployment", "create",
        "--name", ACCOUNT,
        "--resource-group", RG,
        "--deployment-name", deployment_name,
        "--model-name", model_name,
        "--model-version", model_version,
        "--model-format", "OpenAI",
        "--sku-capacity", "1",
        "--sku-name", "GlobalStandard",
        "-o", "none",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0:
            print(f"OK: {deployment_name} デプロイ成功")
            return True
        else:
            print(f"ERROR: {result.stderr.strip()[:300]}")
            return False
    except subprocess.TimeoutExpired:
        print("TIMEOUT: 120秒超過")
        return False


def list_deployments():
    """既存のデプロイメント一覧。"""
    print("\n--- 既存デプロイメント ---")
    az = r"C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd"
    cmd = [
        az, "cognitiveservices", "account", "deployment", "list",
        "--name", ACCOUNT,
        "--resource-group", RG,
        "--query", "[].{name:name, model:properties.model.name, version:properties.model.version}",
        "-o", "table",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            print(result.stdout)
        else:
            print(f"ERROR: {result.stderr.strip()[:300]}")
    except subprocess.TimeoutExpired:
        print("TIMEOUT")


if __name__ == "__main__":
    action = sys.argv[1] if len(sys.argv) > 1 else "test"

    if action == "test":
        test_gpt4o()
    elif action == "deploy":
        deploy_model("gpt-52", "gpt-5.2", "2025-12-01")
        deploy_model("gpt-5-nano", "gpt-5-nano", "2025-12-01")
    elif action == "list":
        list_deployments()
    elif action == "all":
        list_deployments()
        deploy_model("gpt-52", "gpt-5.2", "2025-12-01")
        deploy_model("gpt-5-nano", "gpt-5-nano", "2025-12-01")
        test_gpt4o()
