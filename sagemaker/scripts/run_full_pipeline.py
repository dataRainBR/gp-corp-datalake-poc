import sagemaker
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.parameters import ParameterString, ParameterFloat
from sagemaker.workflow.steps import ProcessingStep, TrainingStep, CreateModelStep
from sagemaker.workflow.condition_step import ConditionStep
from sagemaker.workflow.conditions import ConditionLessThanOrEqualTo
from sagemaker.workflow.properties import PropertyFile
from sagemaker.workflow.model_step import RegisterModel

from sagemaker.processing import ScriptProcessor, ProcessingOutput, ProcessingInput
from sagemaker.estimator import Estimator
from sagemaker.model import Model
import sagemaker.image_uris

# ==========================================================
# CONFIG
# ==========================================================
region = "us-east-1"
role = sagemaker.get_execution_role()
pipeline_session = PipelineSession()
bucket = pipeline_session.default_bucket()

# ==========================================================
# PARÂMETROS
# ==========================================================
feature_group_param = ParameterString(
    name="FeatureGroupName",
    default_value="coamo-ml-forecasting-v3",
)

processing_instance_param = ParameterString(
    name="ProcessingInstanceType",
    default_value="ml.t3.medium",
)

training_instance_param = ParameterString(
    name="TrainingInstanceType",
    default_value="ml.m5.large",
)

rmse_threshold_param = ParameterFloat(
    name="RMSEThreshold",
    default_value=10.0,
)

# ==========================================================
# PROCESSOR BASE
# ==========================================================
sklearn_image = sagemaker.image_uris.retrieve(
    framework="sklearn",
    region=region,
    version="1.2-1",
    instance_type="ml.m5.large",
)

processor = ScriptProcessor(
    image_uri=sklearn_image,
    command=["python3"],
    instance_type=processing_instance_param,
    instance_count=1,
    role=role,
    sagemaker_session=pipeline_session,
)

# ==========================================================
# STEP 1 — INGESTÃO + FEATURE ENGINEERING
# ==========================================================
step_ingest_fe = ProcessingStep(
    name="IngestAndFeatureEngineering",
    processor=processor,
    code="step1_processing_featurestore.py",
    job_arguments=[
        "--feature-group-name",
        feature_group_param,
        "--region-name",
        region,
        "--output-dir",
        "/opt/ml/processing/output",
    ],
    outputs=[
        ProcessingOutput(
            output_name="processed_data",
            source="/opt/ml/processing/output",
        )
    ],
)

# ==========================================================
# STEP 2 — TRAIN / TEST SPLIT
# ==========================================================
step_split = ProcessingStep(
    name="TrainTestSplit",
    processor=processor,
    code="step2_split.py",
    inputs=[
        ProcessingInput(
            source=step_ingest_fe.properties.ProcessingOutputConfig.Outputs[
                "processed_data"
            ].S3Output.S3Uri,
            destination="/opt/ml/processing/input",
        )
    ],
    outputs=[
        ProcessingOutput(
            output_name="train_data",
            source="/opt/ml/processing/train",
        ),
        ProcessingOutput(
            output_name="test_data",
            source="/opt/ml/processing/test",
        ),
    ],
)

# ==========================================================
# STEP 3 — TRAINING
# ==========================================================
estimator = Estimator(
    image_uri=sklearn_image,
    role=role,
    instance_count=1,
    instance_type=training_instance_param,
    output_path=f"s3://{bucket}/model",
    sagemaker_session=pipeline_session,
)

step_train = TrainingStep(
    name="ModelTraining",
    estimator=estimator,
    inputs={
        "train": sagemaker.inputs.TrainingInput(
            s3_data=step_split.properties.ProcessingOutputConfig.Outputs[
                "train_data"
            ].S3Output.S3Uri
        )
    },
)

# ==========================================================
# STEP 4 — EVALUATION
# ==========================================================
evaluation_report = PropertyFile(
    name="EvaluationReport",
    output_name="evaluation",
    path="evaluation.json",
)

step_eval = ProcessingStep(
    name="EvaluateModel",
    processor=processor,
    code="evaluate.py",
    inputs=[
        ProcessingInput(
            source=step_train.properties.ModelArtifacts.S3ModelArtifacts,
            destination="/opt/ml/processing/model",
        ),
        ProcessingInput(
            source=step_split.properties.ProcessingOutputConfig.Outputs[
                "test_data"
            ].S3Output.S3Uri,
            destination="/opt/ml/processing/test",
        ),
    ],
    outputs=[
        ProcessingOutput(
            output_name="evaluation",
            source="/opt/ml/processing/evaluation",
        )
    ],
    property_files=[evaluation_report],
)

# ==========================================================
# STEP 5 — CONDITION (BEST MODEL)
# ==========================================================
step_condition = ConditionStep(
    name="CheckRMSE",
    conditions=[
        ConditionLessThanOrEqualTo(
            left=evaluation_report,
            right=rmse_threshold_param,
        )
    ],
    if_steps=[],
    else_steps=[],
)

# ==========================================================
# STEP 6 — REGISTER MODEL
# ==========================================================
model = Model(
    image_uri=sklearn_image,
    model_data=step_train.properties.ModelArtifacts.S3ModelArtifacts,
    role=role,
    sagemaker_session=pipeline_session,
)

step_register = RegisterModel(
    name="RegisterBestModel",
    model=model,
    content_types=["text/csv"],
    response_types=["text/csv"],
    inference_instances=["ml.m5.large"],
    transform_instances=["ml.m5.large"],
    model_package_group_name="CoamoForecastModels",
)

# ==========================================================
# STEP 7 — CREATE MODEL + ENDPOINT
# ==========================================================
step_create_model = CreateModelStep(
    name="CreateSageMakerModel",
    model=model,
)

# ==========================================================
# AMARRAÇÃO DO CONDITION
# ==========================================================
step_condition.if_steps = [step_register, step_create_model]

# ==========================================================
# PIPELINE
# ==========================================================
pipeline = Pipeline(
    name="CoamoEndToEndForecastPipeline",
    parameters=[
        feature_group_param,
        processing_instance_param,
        training_instance_param,
        rmse_threshold_param,
    ],
    steps=[
        step_ingest_fe,
        step_split,
        step_train,
        step_eval,
        step_condition,
    ],
    sagemaker_session=pipeline_session,
)

pipeline.upsert(role_arn=role)