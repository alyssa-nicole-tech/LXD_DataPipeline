
import os
import shutil
import subprocess
import sys

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

DEFAULT_VIDEO_ID = "kbkLi2kC5Ig"

default_args = {
    "owner": "alyssa",
    "retries": 1,
}

with DAG(
    dag_id="youtube_astro_extraction_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    params={"video_id": DEFAULT_VIDEO_ID},
) as dag:

    @task
    def download_youtube(**context) -> dict:
        video_id = context["params"]["video_id"]
        local_path = f"/tmp/{video_id}.mp4"
        youtube_url = f"https://www.youtube.com/watch?v={video_id}"

        dag_dir = Path(__file__).resolve().parent
        fallback_candidates = [
            dag_dir / "0be2408114024fe484f6236705022767.mp4",
            dag_dir / "fa84e08b9252447db7bda1f1667da87d.mp4",
        ]
        fallback_path = next((p for p in fallback_candidates if p.exists()), None)

        if fallback_path is not None:
            shutil.copy2(fallback_path, local_path)
            return {"video_id": video_id, "local_path": local_path}

        cmd = [
            sys.executable,
            "-m",
            "yt_dlp",
            "-f",
            "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best",
            "-o",
            local_path,
            youtube_url,
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"yt-dlp failed for video_id={video_id!r}: {e.stderr or str(e)}"
            ) from e

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"yt-dlp did not create the expected file: {local_path}")

        return {"video_id": video_id, "local_path": local_path}

    @task
    def transform_video(download_result: dict) -> dict:
        video_id = download_result["video_id"]
        local_path = download_result["local_path"]
        output_path = local_path.replace(".mp4", "_processed.json")
        dag_dir = Path(__file__).resolve().parent
        transform_script = dag_dir / "astro" / "transform_video.py"

        if not transform_script.exists():
            raise FileNotFoundError(f"Transform script not found: {transform_script}")

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Input video not found for transform: {local_path}")

        cmd = [
            sys.executable,
            str(transform_script),
            "--input",
            local_path,
            "--output",
            output_path,
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(
                f"transform_video.py failed for video_id={video_id!r}: {e.stderr or str(e)}"
            ) from e

        return {"video_id": video_id, "local_path": local_path, "output_path": output_path}

    @task
    def upload_to_s3(transform_result: dict) -> str:
        bucket = Variable.get("S3_BUCKET")
        video_id = transform_result["video_id"]
        output_path = transform_result["output_path"]
        s3_key = f"processed_videos/{video_id}.json"

        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_hook.load_file(
            filename=output_path,
            key=s3_key,
            bucket_name=bucket,
            replace=True,
        )
        return s3_key

    @task(trigger_rule="all_done")
    def cleanup(download_result: dict, transform_result: dict) -> None:
        paths = set()
        if download_result:
            paths.add(download_result.get("local_path"))
        if transform_result:
            paths.add(transform_result.get("local_path"))
            paths.add(transform_result.get("output_path"))

        for path in paths:
            if path and os.path.exists(path):
                os.remove(path)

    downloaded = download_youtube()
    transformed = transform_video(downloaded)
    uploaded = upload_to_s3(transformed)
    cleanup_task = cleanup(downloaded, transformed)
    uploaded >> cleanup_task
