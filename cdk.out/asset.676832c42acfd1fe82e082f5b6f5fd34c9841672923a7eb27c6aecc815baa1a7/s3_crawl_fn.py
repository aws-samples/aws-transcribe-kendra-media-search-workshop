import boto3
import os
import uuid
from urllib.parse import unquote_plus
import json

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TRANSCRIPTION_BUCKET = os.environ["TRANSCRIPTION_BUCKET"]
TRANSCRIPTION_OUTPUT_PREFIX = os.environ["TRANSCRIPTION_OUTPUT_PREFIX"]
TRANSCRIBE_ROLE = os.environ["TRANSCRIBE_ROLE"]
transcribe_client = boto3.client("transcribe")


def validate_filetype(filetype):
    return filetype in ["mp3", "mp4", "wav"]


def start_transcribe_job(bucket_name, key):
    logger.info("Starting Transcribe Job")
    media_url = f"s3://{bucket_name}/{key}"
    file_name = key.split("/")[-1]
    filetype = file_name.split(".")[-1]
    job_name = 'workshop_' + file_name.split(".")[0] + str(uuid.uuid4())
    transcription_file = os.path.join(
        TRANSCRIPTION_OUTPUT_PREFIX, file_name.split(".")[0] + ".json"
    )

    if validate_filetype(filetype):
        try:
            _ = transcribe_client.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={"MediaFileUri": media_url},
                MediaFormat=filetype,
                LanguageCode="en-US",
                OutputBucketName=TRANSCRIPTION_BUCKET,
                OutputKey=transcription_file,
                JobExecutionSettings={
                    "AllowDeferredExecution": True,
                    "DataAccessRoleArn": TRANSCRIBE_ROLE,
                },
            )
        except Exception as e:
            logger.error("Error has occured while starting the transcribe job")
            logger.error(e)
    else:
        logger.info(f"{key} not a valid file for transcribe")


def lambda_handler(event, context):
    logger.info("Received event: %s" % json.dumps(event))

    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        start_transcribe_job(bucket_name, key)