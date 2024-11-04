from constructs import Construct
from aws_cdk import (
    Stack,
    aws_lambda as lambda_,
    aws_s3 as s3,
    Duration,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    Aspects,
)
import cdk_nag


class MediaIndexingStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        TRANSCRIPTION_JOB_NAMES = [
            f"arn:aws:transcribe:{self.region}:{self.account}:transcription-job/workshop_AWS_Podcast_Episode_432",
            f"arn:aws:transcribe:{self.region}:{self.account}:transcription-job/workshop_How_can_I_log_into_my_Amazon_EC2_instance_if_I_receive_an_error_that_the_server_refused_our_key_"
        ] 

        transcription_bucket = s3.Bucket(
            self,
            "transcription_bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )


        # role to assign to Transcribe to get media file access
        transcribe_role = iam.Role(
            self,
            "transcribe_role",
            assumed_by=iam.ServicePrincipal("transcribe.amazonaws.com"),
        )
        # policies to grant Transcribe read and write access to transcription bucket
        transcribe_role.add_to_policy(
            iam.PolicyStatement(
                resources=[transcription_bucket.arn_for_objects("media/AWS_Podcast_Episode_432.mp3"),
                           transcription_bucket.arn_for_objects("media/How_can_I_log_into_my_Amazon_EC2_instance_if_I_receive_an_error_that_the_server_refused_our_key_.mp4"),
                           transcription_bucket.arn_for_objects("transcribe_job_output/.write_access_check_file.temp"),
                           transcription_bucket.arn_for_objects("transcribe_job_output/AWS_Podcast_Episode_432.json"),
                           transcription_bucket.arn_for_objects("transcribe_job_output/How_can_I_log_into_my_Amazon_EC2_instance_if_I_receive_an_error_that_the_server_refused_our_key_.json"),
                           ],
                actions=["s3:PutObject", "s3:GetObject"],
            )
        )

        s3_crawl_fn = lambda_.Function(
            self,
            "s3_crawl_fn",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset("lambda/s3_crawl_fn"),
            handler="s3_crawl_fn.lambda_handler",
            environment={
                "TRANSCRIPTION_BUCKET": transcription_bucket.bucket_name,
                "TRANSCRIPTION_OUTPUT_PREFIX": "transcribe_job_output",
                "TRANSCRIBE_ROLE": transcribe_role.role_arn,
            },
            timeout=Duration.seconds(45),
        )
        


        # permission for s3_crawl_fn to call transcribe
        s3_crawl_fn.role.add_to_policy(
            iam.PolicyStatement(
                resources=TRANSCRIPTION_JOB_NAMES,
                actions=["transcribe:StartTranscriptionJob"],
                conditions={
                        "ArnEquals": {
                            "aws:PrincipalArn": s3_crawl_fn.role.role_arn
                        }
                    }
            )
        )

        # permission to pass role to transcribe
        s3_crawl_fn.role.add_to_policy(
            iam.PolicyStatement(
                resources=[transcribe_role.role_arn],
                actions=["iam:PassRole"],
            )
        )

        transcribe_process_fn = lambda_.Function(
            self,
            "transcribe_process_fn",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset("lambda/transcribe_process_fn"),
            handler="transcribe_process_fn.lambda_handler",
            environment={
                "TRANSCRIPTION_BUCKET": transcription_bucket.bucket_name,
                "TRANSCRIPTION_PREFIX": "transcriptions",
            },
            timeout=Duration.seconds(45),
        )

        # permission for transcribe_process_fn to get transcription job details
        transcribe_process_fn.role.add_to_policy(
            iam.PolicyStatement(
                resources=TRANSCRIPTION_JOB_NAMES,
                actions=["transcribe:GetTranscriptionJob"],
                conditions={
                    "ArnEquals": {
                            "aws:PrincipalArn": transcribe_process_fn.role.role_arn
                    }
                }
                )
            )

        # permission for transcribe_process_fn to get and put object in the transcription_bucket
        transcribe_process_fn.role.add_to_policy(
            iam.PolicyStatement(
                resources=[transcription_bucket.arn_for_objects("transcribe_job_output/AWS_Podcast_Episode_432.json"),
                           transcription_bucket.arn_for_objects("transcribe_job_output/How_can_I_log_into_my_Amazon_EC2_instance_if_I_receive_an_error_that_the_server_refused_our_key_.json"),
                           transcription_bucket.arn_for_objects("transcriptions/AWS_Podcast_Episode_432.txt"),
                           transcription_bucket.arn_for_objects("transcriptions/AWS_Podcast_Episode_432.txt.metadata.json"),
                           transcription_bucket.arn_for_objects("transcriptions/How_can_I_log_into_my_Amazon_EC2_instance_if_I_receive_an_error_that_the_server_refused_our_key_.txt"),
                           transcription_bucket.arn_for_objects("transcriptions/How_can_I_log_into_my_Amazon_EC2_instance_if_I_receive_an_error_that_the_server_refused_our_key_.txt.metadata.json"),
                           ],
                actions=["s3:PutObject", "s3:GetObject"],
            )
        )

        # permission for transcribe_process_fn to get s3 bucket location
        # needed in the code to create s3 object link
        transcribe_process_fn.role.add_to_policy(
            iam.PolicyStatement(
                resources=[transcription_bucket.bucket_arn],
                actions=["s3:ListBucket", "s3:GetBucketLocation"],
            )
        )
        
        # eventbridge trigger on transcription job status change
        transcribe_rule = events.Rule(
            self,
            "TranscribeJobStatusRule",
            description="Capture Transcribe job status changes",
            event_pattern=events.EventPattern(
                source=["aws.transcribe"],
                detail_type=["Transcribe Job State Change"],
                detail={"TranscriptionJobStatus": ["COMPLETED", "FAILED"]},
            ),
        )

        transcribe_rule.add_target(targets.LambdaFunction(transcribe_process_fn))

        # Add suppressions for remaining issues
        cdk_nag.NagSuppressions.add_stack_suppressions(
            self,
            [
                cdk_nag.NagPackSuppression(
                    id="AwsSolutions-IAM4",
                    reason="Using AWSLambdaBasicExecutionRole is acceptable for this use case",
                ),
                cdk_nag.NagPackSuppression(
                    id="AwsSolutions-IAM5",
                    reason="Specific permissions are used where possible, wildcards are minimized",
                ),
                cdk_nag.NagPackSuppression(
                    id="AwsSolutions-S1",
                    reason="Access logs bucket is not required.",
                ),
            ],
        )

        Aspects.of(self).add(cdk_nag.AwsSolutionsChecks())