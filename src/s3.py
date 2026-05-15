from context import spark, dbutils

# =========================
# Spark/Hadoop の S3A 認証情報設定関数 ⭐
# =========================
def init(
    access_id_key: str,
    secret_key_key: str,
    scope: str = "adw_da-marketing-dmp-prod_japaneast_SCOPE"
):
    """
    Databricks環境にS3A認証情報を設定します。

    引数:
        access_id_key (str): Databricksシークレットに保存されたAWSアクセスキーIDのキー名。
        secret_key_key (str): Databricksシークレットに保存されたAWSシークレットアクセスキーのキー名。
        scope (str): シークレットスコープ名（デフォルト: "adw_da-marketing-dmp-prod_japaneast_SCOPE"）。

    処理内容:
        - dbutils.secrets.getでAWS認証情報を取得し、Spark/Hadoopの設定に反映します。
        - S3Aアクセス用の認証情報をHadoop/Spark両方に設定します。

    """

    aws_access_key_id = dbutils.secrets.get(scope=scope, key=access_id_key)
    aws_secret_access_key = dbutils.secrets.get(scope=scope, key=secret_key_key)

    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.access.key", aws_access_key_id)
    hconf.set("fs.s3a.secret.key", aws_secret_access_key)
    hconf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    spark.conf.set("fs.s3a.access.key", aws_access_key_id)
    spark.conf.set("fs.s3a.secret.key", aws_secret_access_key)
    spark.conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

    print("S3認証情報の設定が完了しました。")