# Board Game

板情報のリプレイ用アプリを置くリポジトリです。

現在は既存の PyQt デスクトップ版と、新しく追加する Web アプリ版を管理します。

## Contents

- [gui/app.py](/C:/Users/masato/Desktop/板/itayomikun/game/gui/app.py)
  - 既存のデスクトップ GUI
- [gui/trade.py](/C:/Users/masato/Desktop/板/itayomikun/game/gui/trade.py)
  - デスクトップ版の売買まわり
- [webapp/server.py](/C:/Users/masato/Desktop/板/itayomikun/game/webapp/server.py)
  - parquet を読み込んで配信する軽量サーバ
- [webapp/static](/C:/Users/masato/Desktop/板/itayomikun/game/webapp/static)
  - 板、再生、約定、チャート、売買シミュレーションを持つブラウザ版 UI

## Run

Web 版:

```bash
python webapp/server.py
```

デスクトップ版:

```bash
python gui/app.py
```

Windows で Web 版を開く場合:

```bat
webapp\open.bat
```

## Data

- 既定では repo root の `*.parquet` を自動検出します
- 別ディレクトリを使う場合は `python webapp/server.py --data-dir <path>` を使います
- parquet は Git に含めなくても、配置されていれば再生できます

## License

コードは MIT License です。
