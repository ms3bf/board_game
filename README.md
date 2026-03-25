# Board Game

公開URL: https://board-game-omega.vercel.app/

板リプレイ風のデモトレ Web アプリです。ブラウザ上で板、チャート、歩み値を見ながら売買シミュレーションができます。

## 重要

このアプリで再生している板は、実市場のリアルタイム板配信ではありません。

- 表示している板と約定は、公開用に生成した疑似板データです
- 実際の取引所配信や証券会社の気配配信をそのまま表示するものではありません
- 投資判断や実売買の根拠として使うことは想定していません

## Web App

- 公開版: https://board-game-omega.vercel.app/
- ローカル起動:

```bash
python webapp/server.py
```

- Windows で開く:

```bat
webapp\open.bat
```

## 主な機能

- 板表示
- チャート表示
- 歩み値表示
- 買い / 売りシミュレーション
- X 共有
- 音声フィードバック

## データ構成

- `demo_trade.parquet`
  - 板・約定の本体データ
- `demo_trade.chart.parquet`
  - チャート専用の軽量データ

本番配信では、重い parquet は Blob から読み、フロントは必要な chunk だけ取得する構成です。

## 開発メモ

- Web サーバ: [`webapp/server.py`](/C:/Users/masato/Desktop/板/itayomikun/game/webapp/server.py)
- Vercel API: [`api/index.py`](/C:/Users/masato/Desktop/板/itayomikun/game/api/index.py)
- フロント: [`webapp/static/app.js`](/C:/Users/masato/Desktop/板/itayomikun/game/webapp/static/app.js)

## クレジット

- 音声: VOICEVOX ずんだもん

## License

コードは MIT License です。
