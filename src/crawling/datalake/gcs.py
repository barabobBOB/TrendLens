import logging
import os

from google.cloud.storage import transfer_manager

from src.config.gcp.config import StorageClientSingleton
from src.config.logger.logger_setup import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def upload_many_blobs_with_transfer_manager(
    bucket_name, filenames, source_directory="", workers=8
):
    """
    여러 파일을 GCS 버킷에 동시에 업로드합니다.

    Args:
        bucket_name (str): GCS 버킷 이름.
        filenames (list[str]): 업로드할 파일 이름 리스트.
        source_directory (str): 업로드할 파일이 저장된 로컬 디렉터리 경로.
        workers (int): 업로드 작업에 사용할 최대 워커 수.

    Returns:
        None
    """
    storage_client = StorageClientSingleton()
    bucket = storage_client.bucket(bucket_name)

    # Transfer Manager를 이용하여 파일 업로드
    results = transfer_manager.upload_many_from_filenames(
        bucket, filenames, source_directory=source_directory, max_workers=workers
    )

    # 업로드 결과 출력
    for name, result in zip(filenames, results):
        if isinstance(result, Exception):
            logger.debug(f"Failed to upload {name} due to exception: {result}")
        else:
            logger.debug(f"Uploaded {name} to {bucket.name}.")


if __name__ == "__main__":
    # GCS 버킷 이름
    bucket_name = "trend_lens"

    # 업로드할 파일들이 저장된 로컬 디렉토리
    source_directory = "data"

    # 업로드할 파일 이름 리스트 (디렉토리 내 모든 파일 선택)
    filenames = [file for file in os.listdir(source_directory) if file.endswith(".parquet")]

    # GCS로 파일 업로드
    upload_many_blobs_with_transfer_manager(bucket_name, filenames, source_directory=source_directory)
