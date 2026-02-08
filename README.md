# Avro Viewer White

ローカルの `.avro` (Avro OCF) をブラウザだけで読み込み、表形式で確認する静的Webアプリです。

## 起動方法

```bash
python -m http.server 8000
```

その後、`http://localhost:8000` を開いてください。  
`index.html` を直接開くこともできますが、ブラウザ設定によってはモジュール読み込みやファイル挙動が変わるため、簡易HTTPサーバを推奨します。

## 使い方

1. **ファイル読み込み**
   - ヘッダの「ファイル選択」またはドラッグ&ドロップで `.avro` を読み込みます。
2. **Schema表示**
   - 左の `Schema` にファイル埋め込み schema が表示されます。
3. **レコード閲覧**
   - 先頭N件（初期200件）をテーブル表示します。
4. **列切替**
   - 左の `Columns` で表示/非表示を切替します。
5. **検索・フィルタ・ソート**
   - 上部検索ボックス: 部分一致検索。
   - Filter: `contains`, `==`, `exists`。
   - テーブルヘッダクリック: 単一列の昇順/降順ソート。
6. **詳細表示**
   - 行をクリックすると `Row Detail` にJSON整形表示します。
7. **エクスポート**
   - `Export JSON`: 現在の絞り込み結果を JSON 配列でダウンロード。
   - `Export CSV`: 表示列のみで CSV をダウンロード。

## 制限事項

- Avroのデコードに `vendor/avsc.js` を同梱しています（オフライン利用可）。
- 大容量ファイル対応として、初期表示は「先頭N件」のみを保持・描画します。
- ページングは1ページ100件です。
- 複雑な logicalType の人間可読変換は現状未対応です（値は文字列化して表示）。

## 依存ライブラリ

- [`avsc` v5.7.7](https://www.npmjs.com/package/avsc) (同梱: `vendor/avsc.js`)
  - License: MIT (`vendor/avsc-LICENSE.txt`)

## ファイル構成

- `index.html`
- `styles.css`
- `app.js`
- `README.md`
- `vendor/avsc.js`
- `vendor/avsc-LICENSE.txt`
