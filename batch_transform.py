response = client.create_transform_job(
    TransformJobName=transform_jobname,
    ModelName=model_name,
    MaxConcurrentTransforms=3,
    ModelClientConfig={"InvocationsTimeoutInSeconds": 60, "InvocationsMaxRetries": 3},
    MaxPayloadInMB=10,
    BatchStrategy="SingleRecord",
    # Environment={
    #    'string': 'string'
    # },
    TransformInput={
        "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": s3uri}},
        "ContentType": "application/jsonlines",
        "CompressionType": "None",
        "SplitType": "Line",
    },
    TransformOutput={
        "S3OutputPath": s3_output,
        "Accept": "application/jsonlines",
        "AssembleWith": "Line",
        #'KmsKeyId': 'string'
    },
    # DataCaptureConfig={
    #    'DestinationS3Uri': 'string',
    #    'KmsKeyId': 'string',
    #    'GenerateInferenceId': True|False
    # },
    TransformResources={
        "InstanceType": "ml.m4.xlarge",
        "InstanceCount": 1,
        #'VolumeKmsKeyId': 'string'
    },
    # DataProcessing={
    #    'InputFilter': 'string',
    #    'OutputFilter': 'string',
    #    'JoinSource': 'Input'|'None'
    # },
    Tags=[
        {"Key": "project", "Value": "deep_ar"},
    ],
    # ExperimentConfig={
    #    'ExperimentName': 'string',
    #    'TrialName': 'string',
    #    'TrialComponentDisplayName': 'string',
    #    'RunName': 'string'
    # }
)
