# Daily Literature Update — GitHub Actions + Gemini + Slack

指定雑誌をEurope PMCで毎日確認し、Geminiで5カテゴリーに分類・要約し、GitHub PagesとSlackを更新します。

## 生成物

- `docs/latest.html`：今回の日次更新
- `docs/daily/`：日ごとのページ
- `docs/weeks/`：月曜〜日曜の週次アーカイブ
- `.weekly-literature/`：処理状態・重複投稿防止情報

## 5カテゴリー

1. 腸内細菌
2. 腸管神経
3. 超硫黄分子・超硫黄修飾
4. 翻訳後修飾
5. 神経免疫

## 最短セットアップ

### 1. GitHubリポジトリを作成

GitHubで新しいリポジトリを作成します。最初はPublicが簡単です。

### 2. ファイルを配置

ZIPを展開し、中身をリポジトリ直下へ配置します。

```text
repository/
├── .github/workflows/daily-literature.yml
├── config.json
├── requirements.txt
├── run_daily.py
└── weekly_literature_pipeline.py
```

`.weekly-literature/`と`docs/`は初回実行時に生成・更新されます。

ブラウザから設定する場合、通常ファイルをアップロードした後、
`Add file` → `Create new file` でファイル名を
`.github/workflows/daily-literature.yml` と入力し、同梱YAMLの内容を貼り付けます。

### 3. GitHub Secretsを登録

Repository → Settings → Secrets and variables → Actions → New repository secret

- `GEMINI_API_KEY`
- `SLACK_WEBHOOK_URL`

`GITHUB_TOKEN`の追加は不要です。

### 4. Actionsの書き込み権限

Repository → Settings → Actions → General → Workflow permissions で
`Read and write permissions`を選びます。

### 5. GitHub Pagesを設定

Repository → Settings → Pages → Build and deployment → Source で
`GitHub Actions`を選びます。

この版はPages用Artifactを同じWorkflow内で直接デプロイします。

### 6. 初回手動実行

Repository → Actions → Daily literature update → Run workflow → Run workflow

成功すると、以下が行われます。

- Europe PMC検索
- Geminiスクリーニング・要約
- HTML生成
- Slack投稿
- `docs/`と`.weekly-literature/`の自動コミット
- GitHub Pagesへのデプロイ

## 毎日の実行時刻

標準は日本時間の毎朝8:15です。

```yaml
- cron: "15 23 * * *"
```

GitHub ActionsのcronはUTCです。変更する場合は
`.github/workflows/daily-literature.yml`を編集してください。

## 雑誌と検索設定

`config.json`を編集します。

- `journals`：確認する雑誌
- `keywords`：キーワードヒット表示・ルール判定
- `search_keywords_outside_journals`：`false`なら指定誌外を検索しない
- `max_screening_per_run`：1回の最大スクリーニング数
- `max_summaries_per_run`：1回の最大要約数

## 公開URL

通常は次の形式です。

```text
https://<GitHubユーザー名>.github.io/<リポジトリ名>/
```

- 最新：`latest.html`
- 日次一覧：`daily/`
- 週次一覧：`weeks/`

## トラブル時

Actionsの実行名を開き、赤くなったStepを展開してエラー全文を確認します。

- `Required environment variable is missing`：Secret名または値を確認
- `403`やpush失敗：Workflow permissions、branch protectionを確認
- Gemini `429`：クォータ超過。翌日再試行、または件数上限を下げる
- Slack webhook失敗：Webhook URL、投稿先、Slack App設定を確認
