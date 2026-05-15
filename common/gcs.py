from context import spark, dbutils

# =========================
# Spark/Hadoop の GCS 認証情報設定関数 ⭐
# =========================
def init(
    client_email_key: str,
    project_id_key: str,
    private_key_key: str,
    private_key_id_key: str,
    scope: str = "adw_da-marketing-dmp-prod_japaneast_SCOPE"
):
    """
    Databricks環境にGCS認証情報を設定します。

    引数:
        client_email_key (str): Databricksシークレットに保存されたGCSサービスアカウントのメールアドレスのキー名。
        project_id_key (str): Databricksシークレットに保存されたGCSプロジェクトIDのキー名。
        private_key_key (str): Databricksシークレットに保存されたGCSサービスアカウントの秘密鍵のキー名。
        private_key_id_key (str): Databricksシークレットに保存されたGCSサービスアカウントの秘密鍵IDのキー名。
        scope (str): シークレットスコープ名（デフォルト: "adw_da-marketing-dmp-prod_japaneast_SCOPE"）。

    処理内容:
        - dbutils.secrets.getでGCS認証情報を取得し、Spark/Hadoopの設定に反映します。
        - GCSアクセス用の認証情報をHadoop/Spark両方に設定します。

    """

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("google.cloud.auth.service.account.enable", "true")
    hadoop_conf.set(
        "fs.gs.auth.service.account.email",
        dbutils.secrets.get(scope=scope, key=client_email_key)
    )
    hadoop_conf.set(
        "fs.gs.project.id",
        dbutils.secrets.get(scope=scope, key=project_id_key)
    )
    hadoop_conf.set(
        "fs.gs.auth.service.account.private.key",
        dbutils.secrets.get(scope=scope, key=private_key_key)
    )
    hadoop_conf.set(
        "fs.gs.auth.service.account.private.key.id",
        dbutils.secrets.get(scope=scope, key=private_key_id_key)
    )
    print("GCS接続設定が完了しました（Hadoop Configuration経由）")

    