# Board Game Web

`gui` の機能をブラウザ向けに移植した Web アプリです。

## Run

```bash
python webapp/server.py
```

Windows では:

```bat
webapp\open.bat
```

## Data

- 既定では repo root の `*.parquet` を自動検出します
- 別ディレクトリを使う場合は `--data-dir` を指定します
- parquet は Git に含めなくても、配置されていれば再生できます

## Features

- リプレイファイル切り替え
- 板表示
- 再生、停止、前後ステップ、`x5`
- 約定一覧
- チャートからのシーク
- 売買シミュレーション
