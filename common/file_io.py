from typing import Dict, Any
from context import spark, dbutils

# =========================
# ファイル出力
# =========================
def export(df, path: str, **options):
    """
    DataFrameを単一ファイルとしてエクスポートするユーティリティ

    ・csv / json / parquet に対応
    ・.gz を付けた場合はgzip圧縮を自動適用
    ・単一ファイル出力（coalesce(1)）
    ・overwrite固定

    パラメータ:
        df: 書き出すSpark DataFrame
        path: 出力ファイルの完全パス（拡張子で形式と圧縮を指定）
        options: 書き出し時の追加オプション（Spark writer options）

    戻り値:
        最終的なファイルパス（str）
    """

    base_dir = path.rsplit("/", 1)[0]
    file_name = path.split("/")[-1]

    # gzip判定（拡張子ベース）
    use_gzip = file_name.endswith(".gz")

    # format判定用に.gzを除去（安全なsuffix処理）
    if use_gzip:
        file_name = file_name.removesuffix(".gz")

    tmp_dir = f"{base_dir}/{file_name}_tmp"

    # gzipはデフォルトで自動付与（外部optionsでoverride可能）
    if use_gzip and "compression" not in options:
        print("compression")
        options["compression"] = "gzip"

    writer = df.coalesce(1).write.mode("overwrite").options(**options)

    # CSV出力
    if file_name.endswith(".csv"):
        writer.csv(tmp_dir)

    # JSON出力
    elif file_name.endswith(".json"):
        writer.json(tmp_dir)

    # PARQUET出力（gzipは無視、Spark標準圧縮に任せる）
    elif file_name.endswith(".parquet"):
        writer.parquet(tmp_dir)

    else:
        raise ValueError(f"unsupported format: {path}")

    # partファイル取得
    files = dbutils.fs.ls(tmp_dir)
    parts = [f.path for f in files if f.name.startswith("part")]

    if not parts:
        raise FileNotFoundError(f"partファイル未找到: {tmp_dir}")

    # 最終出力ファイル名（gzipの場合は戻す）
    final_path = f"{base_dir}/{file_name}"
    if use_gzip:
        final_path += ".gz"

    # 移動＆クリーンアップ
    dbutils.fs.mv(parts[0], final_path)
    dbutils.fs.rm(tmp_dir, True)

    print(f"ファイル出力が成功しました。出力ファイルパス: {final_path}")

    return final_path

# =========================
# ファイルリスト取得
# =========================
def list(path: str):
    """
    指定したパス配下の全てのファイル・フォルダ情報を取得します。

    パラメータ:
        path: 対象ディレクトリのパス（str）。

    戻り値:
        ファイル・フォルダ情報のリスト。
    """
    return dbutils.fs.ls(path)


# =========================
# パス存在チェック
# =========================
def exists(path: str) -> bool:
    """
    指定したパスが存在するかどうかをチェックします。

    パラメータ:
        path: チェック対象のパス（str）。

    戻り値:
        存在する場合はTrue、存在しない場合はFalse。
    """
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


# =========================
# パス削除
# =========================
def delete(path: str, recursive: bool = True):
    """
    指定したパスを削除します。

    パラメータ:
        path: 削除対象のパス（str）。
        recursive: サブディレクトリやファイルを再帰的に削除するかどうか（bool、デフォルトはTrue）。

    戻り値:
        なし
    """
    dbutils.fs.rm(path, recursive)
    print(f"パス削除が成功しました。削除パス: {path}")

# =========================
# ファイル移動
# =========================
def move(src: str, dest: str):
    """
    指定したパスのファイルを別のパスへ移動します（DBFS / Volume / S3対応）。

    パラメータ:
        src: 移動元ファイルのパス（str）。
        dest: 移動先ファイルのパス（str）。

    処理内容:
        - 移動元パスの存在確認を行います。
        - dbutils.fs.mv を使用してファイルを移動します。

    例外:
        FileNotFoundError: 移動元ファイルが存在しない場合。
    """

    try:
        dbutils.fs.ls(src)
    except Exception as e:
        raise FileNotFoundError(f"移動元ファイルが存在しません: {src}") from e

    dbutils.fs.mv(src, dest)
    print(f"ファイル移動が成功しました。移動先パス: {dest}")



    