# ローカル環境で dlt を使用して MySQL -> BigQuery にテーブルをエクスポートする手順

## 前提条件

- Python 3.10 系と `pip`
- `gcloud` CLI（プロジェクトにアクセスできること）
- BigQuery と MySQL への接続情報（`dlt_project/.dlt/secrets.toml` に記載）

## セットアップ

1. 必要なら仮想環境を作成し、requirements をインストールする

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. MySQL の接続情報と BigQuery の設定を `.dlt/secrets.toml` に記入する
3. サービスアカウントをなりすましで利用できるよう、呼び出し元に `Service Account Token Creator` ロールを付与しておく
4. アプリケーションデフォルト認証情報 (ADC) をサービスアカウントとして取得する

```bash
gcloud auth application-default login --impersonate-service-account {service_account_email}
```

## 実行手順

1. dlt パイプラインを実行して MySQL → BigQuery へのエクスポートを行う

```bash
python full_export_tables.py
```
