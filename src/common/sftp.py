# %pip install paramiko 
# pyファイル内では%pipや!pipは使用できません。paramikoはノートブックやクラスタのライブラリ設定でインストールしてください。

from context import spark, dbutils

class SFTPFileIO:
    # =========================
    # 初期化 ⭐
    # =========================
    def __init__(
        self,
        host: str,
        username_key: str,
        password_key: str = None,
        key_key: str = None,
        port: int = 22,
        scope: str = "adw_da-marketing-dmp-prod_japaneast_SCOPE"
    ):
        """
        SFTPFileIOのインスタンスを初期化します。

        Args:
            host (str): SFTPホスト名。
            username_key (str): SFTPユーザー名のSecret Key。
            password_key (str): SFTPパスワードのSecret Key。
            key_key (str): 秘密鍵のSecret Key。デフォルトはNone。
            port (int): SFTPポート番号。デフォルトは22。
            scope (str): Databricks Secret Scope。デフォルトはadw_da-marketing-dmp-prod_japaneast_SCOPE。
        """
        import paramiko
        import io
        self.host = host  # SFTPホスト名
        self.port = port  # SFTPポート番号
        self.key_key = key_key  # 秘密鍵のSecret Key
        self.transport = None  # SFTPトランスポートオブジェクト
        self.sftp = None  # SFTPクライアントオブジェクト
        self.scope = scope  # Databricks Secret Scope
        
        # ユーザー名とパスワードをSecretから取得
        self.username = dbutils.secrets.get(scope=self.scope, key=username_key)
        if password_key:
            self.password = dbutils.secrets.get(scope=self.scope, key=password_key)
        else:
            self.password = None
        self.private_key = None  # 秘密鍵オブジェクト

        if self.key_key:
            # 秘密鍵をSecretから取得し、改行を整形
            raw_key = dbutils.secrets.get(scope=self.scope, key=self.key_key)
            formatted_key = raw_key.replace("\\n", "\n")
            self.private_key = paramiko.RSAKey.from_private_key(io.StringIO(formatted_key))
        print("✅ SFTP接続情報設定完了")
        print(f"接続先: {host}")

    # =========================
    # 内部接続チェック ⭐
    # =========================
    def _check_connection(self):
        """
        SFTP接続が確立されているかをチェックします。未接続の場合は例外を投げます。
        """
        if not self.sftp or not self.transport or not self.transport.is_active():
            raise RuntimeError("SFTP接続が確立されていません。connect()を先に呼び出してください。")

    # =========================
    # 接続 ⭐
    # =========================
    def connect(self):
        """
        SFTPサーバーへ接続します。

        Args:
            なし

        Returns:
            なし
        """
        import paramiko

        self.transport = paramiko.Transport((self.host, self.port))
        if self.private_key:
            self.transport.connect(username=self.username, pkey=self.private_key)
        else:
            self.transport.connect(username=self.username, password=self.password)
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        print("SFTP接続成功")
        print(f"接続先: {self.host}")

    # =========================
    # ファイル一覧取得 ⭐
    # =========================
    def listdir(self, path: str):
        """
        指定パス内のファイル一覧を取得します。

        Args:
            path (str): SFTP上のパス。

        Returns:
            list: ファイル名のリスト。
        """
        self._check_connection()
        return self.sftp.listdir(path)


    def listdir_attr(self, path: str):
        """
        ファイル詳細情報を取得します。

        Returns:
            list[dict]: ファイル情報のリスト
        """

        import datetime

        self._check_connection()

        files = self.sftp.listdir_attr(path)

        result = []

        for f in files:
            result.append({
                "name": f.filename,
                "size": f.st_size,
                "modified_time": f.st_mtime,
                "path": f"{path.rstrip('/')}/{f.filename}"
            })

        return result

    # =========================
    # ダウンロード ⭐
    # =========================
    def get(self, remote_path: str, target_path: str):
        """
        SFTPからファイルをダウンロードし、一時ローカルパス経由でtarget_pathに移動し、保存先パスを返します。

        Args:
            remote_path (str): SFTP上のファイルパス。
            target_path (str): 保存先パス。

        Returns:
            str: 保存先パス（target_path）。
        """
        import os
        import shutil

        self._check_connection()

        # ファイル存在確認
        try:
            self.sftp.stat(remote_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"SFTP上のファイルが存在しません: {remote_path}")

        # 一時ローカルパス
        local_dir = "/dbfs/tmp/sftp/"
        os.makedirs(local_dir, exist_ok=True)
        filename = os.path.basename(remote_path)
        local_path = os.path.join(local_dir, filename)

        # target_pathのディレクトリが存在しない場合は作成
        target_dir = os.path.dirname(target_path)
        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)

        # SFTPから一時ローカルパスへダウンロード
        self.sftp.get(remote_path, local_path)

        # target_pathへ移動
        shutil.move(local_path, target_path)
        print(f"ファイルダウンロード完了: {remote_path} → {target_path}")
        return target_path

    # =========================
    # アップロード ⭐
    # =========================
    def put(self, source_path, remote_path, remove_source: bool = True):
        """
        指定パス（volumeやその他）からDBFSの一時パスへファイルをコピーし、その後SFTPサーバーへアップロードします。

        Args:
            source_path (str): コピー元ファイルパス（volumeやその他）。
            remote_path (str): SFTP上の保存先ファイルパス。
            remove_source (bool): コピー元ファイルを削除するかどうか。デフォルトはTrue。

        Returns:
            str: SFTP上の保存先ファイルパス（remote_path）。
        """

        import os
        import shutil

        # DBFS上の一時ディレクトリとファイルパス
        filename = os.path.basename(remote_path)
        dbfs_temp_dir = "/dbfs/tmp/sftp/"
        os.makedirs(dbfs_temp_dir, exist_ok=True)
        dbfs_file_path = os.path.join(dbfs_temp_dir, filename)

        # source_path存在チェックとコピー        
        if not os.path.exists(source_path):
            raise FileNotFoundError(f"コピー元ファイルが存在しません: {source_path}")
        if remove_source:
            shutil.move(source_path, dbfs_file_path)
        else:
            shutil.copy(source_path, dbfs_file_path)

        try:
            remote_dir = os.path.dirname(remote_path)

            # SFTP上のディレクトリが存在しない場合は作成
            try:
                self.sftp.stat(remote_dir)
            except FileNotFoundError:
                self.sftp.mkdir(remote_dir)
                print(f"SFTP上のディレクトリを作成しました: {remote_dir}")

            # ファイルをSFTPへアップロード
            self.sftp.put(dbfs_file_path, remote_path)
            print(f"SFTPアップロードが完了しました: {remote_path}")

        except Exception as e:
            print(f"SFTPアップロード中にエラーが発生しました: {e}")
            raise

        # DBFS上の一時ファイルを削除
        os.remove(dbfs_file_path)

        return remote_path

    # =========================
    # ファイル削除 ⭐
    # =========================
    def delete(self, remote_path: str):
        """
        SFTP上のファイルを削除します。

        Args:
            remote_path (str): SFTP上のファイルパス。

        Returns:
            なし
        """
        self._check_connection()
        self.sftp.remove(remote_path)

    # =========================
    # クローズ ⭐
    # =========================
    def close(self):
        """
        SFTP接続をクローズします。

        Args:
            なし

        Returns:
            なし
        """
        if self.sftp:
            self.sftp.close()
        if self.transport:
            self.transport.close()
        print("SFTP接続をクローズしました")