from pathlib import Path
from io import StringIO
import os
from dotenv import load_dotenv
import boto3

load_dotenv()


class Downloader:

    def __init__(self):
        self.data_dir = Path().resolve() / "src/data"
        self.session = boto3.Session()
        self.s3 = self.session.resource(service_name='s3')
        self.bucket = self.s3.Bucket(os.environ.get("S3_BUCKET_NAME"))

    def read_object(self, key: str, decoding: str = "utf-8"):
        """
        read csv file from s3 bucket
        """
        csv_obj = self.bucket.Object(key=key)\
            .get()\
            .get("Body")\
            .read()\
            .decode(decoding)
        data = StringIO(csv_obj)
        return data

    def download_data(self):
        """
        download files in src/data
        """
        all_keys = [obj.key for obj in self.bucket.objects.all()]
        for key in all_keys:
            obj = self.read_object(key)
            path = self.data_dir / key.replace("cdc/users/", "")
            path.write_text(obj.read())


if __name__ == "__main__":
    l = Downloader()
    l.download_data()
