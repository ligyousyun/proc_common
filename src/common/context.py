"""
Databricks 実行コンテキストユーティリティ
Sparkおよびdbutilsをモジュール全体で共有します
"""

from pyspark.sql import SparkSession

_spark = None
_dbutils = None


def _get_spark():
    """
    SparkSessionを取得します。

    Returns:
        SparkSession
    """
    global _spark

    if _spark is not None:
        return _spark

    spark = SparkSession.getActiveSession()

    if spark is None:
        raise RuntimeError("SparkSessionが取得できません")

    _spark = spark
    return _spark


def _get_dbutils():
    """
    dbutilsインスタンスを取得します。

    Returns:
        dbutils
    """
    global _dbutils

    if _dbutils is not None:
        return _dbutils

    # notebook環境
    try:
        import builtins
        if hasattr(builtins, "dbutils"):
            _dbutils = builtins.dbutils
            return _dbutils
    except Exception:
        pass

    # Spark経由
    try:
        from pyspark.dbutils import DBUtils

        spark = _get_spark()
        _dbutils = DBUtils(spark)
        return _dbutils

    except Exception:
        pass

    raise RuntimeError("dbutilsが利用できる環境ではありません")


# モジュール共有インスタンス
spark = _get_spark()
dbutils = _get_dbutils()