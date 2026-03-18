# Synthetic Board GUI

`game/*.parquet` を再生する専用 GUI です。`data/` は参照しません。

## 起動

```bash
python game/gui/app.py
```

または:

```bat
game\gui\open.bat
```

## 仕様

- `game` 直下の parquet を自動検出
- 上部の `Replay File` で再生ファイルを切り替え
- 板表示、再生、停止、前後ステップ、`x5` 再生
- `Show Trades` で約定一覧
- `Show Chart` で価格チャート
- `Show Trading` で簡易売買 UI

## Trading

- 板の `AskQ` をダブルクリックすると BUY 指値
- 板の `BidQ` をダブルクリックすると SELL 指値
- 保留注文価格は青で表示

## 依存

- `PyQt6`
- `numpy`
- `pandas`
- `pyarrow`
