import boto3
from botocore.exceptions import ClientError
import json
import textwrap
import os
import tempfile

import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

transcribe_client = boto3.client("transcribe")
s3 = boto3.client("s3")

TRANSCRIPTION_BUCKET = os.environ["TRANSCRIPTION_BUCKET"]
TRANSCRIPTION_PREFIX = os.environ["TRANSCRIPTION_PREFIX"]


def error_response(msg="Error occured, check logs"):
    return {"statusCode": 400, "body": json.dumps({"error": msg})}

def get_bucket_region(bucket):
    # get bucket location.. buckets in us-east-1 return None, otherwise region is identified in LocationConstraint
    try:
        region = s3.get_bucket_location(Bucket=bucket)["LocationConstraint"] or 'us-east-1' 
        logger.info(f"Identified region is {region}")
    except Exception as e:
        logger.info(f"Unable to retrieve bucket region (bucket owned by another account?).. defaulting to us-east-1. Bucket: {bucket} - Message: " + str(e))
        region = 'us-east-1'
    return region

def get_transcription_job(job_name):
    try:
        response = transcribe_client.get_transcription_job(
            TranscriptionJobName=job_name
        )
    except Exception as e:
        logger.error("Exception getting transcription job: " + job_name)
        logger.error(e)
        return None
    return response


# function to download file from s3 using boto3
def download_file_from_s3(bucket, file_name):
    try:
        with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as temp_file:
            s3.download_fileobj(bucket, file_name, temp_file)
            temp_file_path = temp_file.name
            logger.info(f"Downloading file from s3: {bucket}, {file_name} in {temp_file_path}")
        return temp_file_path
    except ClientError as e:
        logger.error(e)


def prepare_transcript(transcript_uri):
    logger.info(f"Preparing transcript for: {transcript_uri}")
    duration_secs = 0
    file_path = download_file_from_s3(
        transcript_uri.split("/")[3], "/".join(transcript_uri.split("/")[4:])
    )
    if file_path is None:
        return None
    logger.info("file downloaded")
    with open(file_path, "r") as file:
        transcript = json.load(file)
    os.unlink(file_path)
    # if output bucket and prefix not mentioned while creating the job
    # response = urllib.request.urlopen(transcript_uri)
    # transcript = json.loads(response.read())

    items = transcript["results"]["items"]
    txt = ""
    sentence = ""
    for i in items:
        if i["type"] == "punctuation":
            sentence = sentence + i["alternatives"][0]["content"]
            if i["alternatives"][0]["content"] == ".":
                # sentence completed
                txt = txt + " " + sentence + " "
                sentence = ""
        else:
            if sentence == "":
                sentence = "[" + i["start_time"] + "]"
            sentence = sentence + " " + i["alternatives"][0]["content"]
            duration_secs = i["end_time"]
    if sentence != "":
        txt = txt + " " + sentence + " "
    out = textwrap.fill(txt, width=70)
    # return [duration_secs, out]
    return out


def prepare_metadata(media_s3url):
    logger.info(f"prepare metadata {media_s3url}")
    title = media_s3url.split("/")[-1]
    category = "Transcript"
    bucket = media_s3url.split("/")[2]
    key = "/".join(media_s3url.split("/")[3:])
    logger.info(f"bucket: {bucket}, key: {key}")
    region = get_bucket_region(bucket)
    source_uri = f'https://s3.{region}.amazonaws.com/{bucket}/{key}'
    return {
        "Attributes": {"_category": category, "_source_uri": source_uri},
        "Title": title,
        "ContentType": "TXT",
    }


def upload_transcript_files_metadata(transcripts, metadata, filename):
    logger.info(
        f"Uploading transcript files to s3 as {filename}"
    )
    transcript_file_key = f"{TRANSCRIPTION_PREFIX}/{filename}.txt"
    metadata_file_key = f"{TRANSCRIPTION_PREFIX}/{filename}.txt.metadata.json"
    try:
        # upload transcripts in s3
        s3.put_object(
            Body=transcripts, Bucket=TRANSCRIPTION_BUCKET, Key=transcript_file_key
        )
        # upload metadata in s3
        s3.put_object(
            Body=json.dumps(metadata),
            Bucket=TRANSCRIPTION_BUCKET,
            Key=metadata_file_key,
        )
    except Exception as e:
        logger.error("Error while uploading transcript files to s3")
        logger.error(e)
        return False
    return True


def lambda_handler(event, context):
    logger.info("Received event: %s" % json.dumps(event))

    job_name = event["detail"]["TranscriptionJobName"]
    logger.info(f"Transcription job name: {job_name}")

    # get results of Amazon Transcribe job
    transcription_job = get_transcription_job(job_name)

    if transcription_job == None or ("TranscriptionJob" not in transcription_job):
        logger.error("Unable to retrieve transcription from job.")
        return error_response()

    else:
        job_status = transcription_job["TranscriptionJob"]["TranscriptionJobStatus"]
        media_s3url = transcription_job["TranscriptionJob"]["Media"]["MediaFileUri"]

        if job_status == "FAILED":
            # job failed
            failure_reason = transcription_job["TranscriptionJob"]["FailureReason"]
            logger.error(
                f"Transcribe job failed: {job_status} - Reason {failure_reason}"
            )

        else:
            transcript_uri = transcription_job["TranscriptionJob"]["Transcript"][
                "TranscriptFileUri"
            ]
            text = prepare_transcript(transcript_uri)
            if text:
                media_metadata = prepare_metadata(media_s3url)
                if media_metadata:
                    file_name = media_s3url.split("/")[-1].split(".")[0]
                    upload_status = upload_transcript_files_metadata(
                        text, media_metadata, file_name
                    )
                    if upload_status:
                        logger.info("Transcript files uploaded successfully")
                    else:
                        return error_response()
                else:
                    return error_response()
            else:
                return error_response()

