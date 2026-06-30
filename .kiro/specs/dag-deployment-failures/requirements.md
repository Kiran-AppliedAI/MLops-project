# Requirements Document

## Introduction

DAG/Code Deployment failure scenarios for the Macy's Cloud Composer demo environment. These scenarios replicate real deployment incidents that account for 55-60% of production incidents at Macy's (~700-760 out of ~1520 total). Each scenario produces a distinct failure pattern that triggers the existing alerting chain (Cloud Monitoring → Pub/Sub → Cloud Function → Jira → OpsRabbit AI RCA) and yields a unique root cause analysis.

The scenarios cover four categories of deployment failure: broken DAG syntax on upload, missing Python module dependencies, breaking changes in shared libraries, and Airflow API version incompatibilities after Composer upgrades.

## Glossary

- **Composer**: Google Cloud Composer, the managed Apache Airflow service hosting the `inventory-pipeline` environment
- **DAG**: Directed Acyclic Graph — an Airflow workflow definition file uploaded to the Composer DAGs bucket
- **DAG_Processor**: The Airflow scheduler component that parses uploaded DAG files and registers them; produces "Broken DAG" errors for files that fail to parse
- **Alerting_Chain**: The existing pipeline: Cloud Monitoring log-based alert → Pub/Sub topic → Cloud Function → Jira ticket creation → OpsRabbit AI RCA
- **MFM_Module**: A shared Python module (Macy's Fulfillment Module) that multiple DAGs import for common inventory operations
- **MAXIS_Framework**: Macy's Analytics and Integration Services framework — provides base classes and utilities that DAGs extend
- **Broken_DAG**: An Airflow state where the DAG Processor cannot parse a DAG file due to syntax or import errors; the file appears in the "Broken DAGs" list in the Airflow UI
- **OpsRabbit**: The AI agent that performs automated Root Cause Analysis on Jira tickets created by the alerting chain
- **RCA**: Root Cause Analysis — the diagnostic output produced by OpsRabbit for each incident

## Requirements

### Requirement 1: Bad DAG Deploy — Syntax Error on Upload

**User Story:** As a demo operator, I want to upload a DAG file with a realistic Python syntax error, so that Composer marks it as a Broken DAG and the alerting chain produces an RCA identifying a bad code deployment.

#### Acceptance Criteria

1. WHEN the DAG file is uploaded to the Composer DAGs bucket, THE DAG_Processor SHALL fail to parse the file due to a Python syntax error
2. WHEN the DAG_Processor fails to parse the file, THE Composer environment SHALL log an error containing "Broken DAG" and the file path to Cloud Logging with severity ERROR
3. WHEN the Broken DAG error is logged, THE Alerting_Chain SHALL detect the log entry and create a Jira ticket within 5 minutes
4. THE DAG file SHALL contain a realistic syntax error that resembles an incomplete merge conflict or truncated deployment artifact
5. THE DAG file SHALL use production-style naming, structure, and business logic context consistent with Macy's inventory operations
6. THE DAG file SHALL NOT contain words indicating intentional failure such as "demo", "test", "simulation", "fake", or "intentional"

### Requirement 2: Missing Module — Undeployed Dependency

**User Story:** As a demo operator, I want to upload a DAG that imports a Python module not present in the Composer environment, so that the DAG fails with a ModuleNotFoundError and the alerting chain produces an RCA identifying a missing dependency deployment.

#### Acceptance Criteria

1. WHEN the DAG file is uploaded to the Composer DAGs bucket, THE DAG_Processor SHALL fail to import the file due to a ModuleNotFoundError for the missing module
2. WHEN the ModuleNotFoundError occurs, THE Composer environment SHALL log an error containing the module name and "No module named" to Cloud Logging with severity ERROR
3. WHEN the import error is logged, THE Alerting_Chain SHALL detect the log entry and create a Jira ticket within 5 minutes
4. THE DAG file SHALL import a module with a name that reflects a plausible internal Macy's package (e.g., `mfm_inventory_utils`) that would normally be deployed alongside the DAG
5. THE DAG file SHALL contain complete, realistic business logic that would function correctly if the missing module were present
6. THE DAG file SHALL NOT contain words indicating intentional failure such as "demo", "test", "simulation", "fake", or "intentional"

### Requirement 3: Breaking Change in Shared Library

**User Story:** As a demo operator, I want to trigger a DAG that calls a function with an outdated signature from a shared module, so that the DAG fails at runtime with a TypeError and the alerting chain produces an RCA identifying a breaking change in a shared library update.

#### Acceptance Criteria

1. THE DAG file SHALL parse and register successfully in the Airflow scheduler without errors
2. WHEN the DAG is triggered manually, THE task SHALL fail at runtime with a TypeError indicating incorrect function arguments for a shared module function
3. WHEN the task fails, THE Composer environment SHALL log the TypeError including the expected vs provided function signature to Cloud Logging with severity ERROR
4. WHEN the task failure is logged, THE Alerting_Chain SHALL detect the log entry and create a Jira ticket within 5 minutes
5. THE DAG file SHALL include a local stub module that provides the function with a changed signature, simulating a shared library that was updated independently of the DAG
6. THE error message SHALL clearly indicate a function signature mismatch (e.g., "got an unexpected keyword argument" or "missing required positional argument") to provide OpsRabbit with sufficient context for a breaking-change RCA
7. THE DAG file SHALL NOT contain words indicating intentional failure such as "demo", "test", "simulation", "fake", or "intentional"

### Requirement 4: Version Incompatibility — Deprecated Airflow API

**User Story:** As a demo operator, I want to trigger a DAG that uses a removed or deprecated Airflow API method, so that the DAG fails at runtime with an AttributeError and the alerting chain produces an RCA identifying a version incompatibility after a Composer upgrade.

#### Acceptance Criteria

1. THE DAG file SHALL parse and register successfully in the Airflow scheduler without errors
2. WHEN the DAG is triggered manually, THE task SHALL fail at runtime with an AttributeError or ImportError indicating a removed or relocated Airflow API
3. WHEN the task fails, THE Composer environment SHALL log the error including the deprecated API path and the Airflow version context to Cloud Logging with severity ERROR
4. WHEN the task failure is logged, THE Alerting_Chain SHALL detect the log entry and create a Jira ticket within 5 minutes
5. THE DAG file SHALL reference an Airflow API that existed in Airflow 1.x or early 2.x but was removed or relocated in Airflow 2.6+ (the version running in Cloud Composer 2)
6. THE error message SHALL clearly indicate a version mismatch (e.g., referencing `airflow.contrib` modules or removed `BaseOperator` methods) to provide OpsRabbit with sufficient context for a version-incompatibility RCA
7. THE DAG file SHALL NOT contain words indicating intentional failure such as "demo", "test", "simulation", "fake", or "intentional"

### Requirement 5: Alerting Chain Compatibility

**User Story:** As a demo operator, I want all deployment failure scenarios to trigger the existing alerting chain without modifications to infrastructure, so that no Terraform changes are required.

#### Acceptance Criteria

1. THE log entries produced by each deployment failure scenario SHALL match at least one pattern in the existing Cloud Monitoring alert policy filter (severity>=ERROR with matching text patterns)
2. THE Cloud Function SHALL detect the DAG name from the log entry for each deployment failure scenario and include it in the Jira ticket title
3. IF the DAG_Processor produces the error (Requirement 1 and 2), THEN THE error log SHALL contain the DAG file name so the Cloud Function can extract the DAG identifier
4. IF the task produces the error at runtime (Requirement 3 and 4), THEN THE error log SHALL contain the dag_id so the Cloud Function can extract the DAG identifier
5. THE existing alert policy filter SHALL NOT require modifications to detect deployment failure scenarios

### Requirement 6: Distinct RCA Patterns

**User Story:** As a demo operator, I want each deployment failure scenario to produce a distinct error pattern, so that OpsRabbit generates a different root cause analysis for each scenario.

#### Acceptance Criteria

1. THE Broken DAG scenario (Requirement 1) SHALL produce error logs referencing syntax parsing failure and the specific line/character of the error
2. THE Missing Module scenario (Requirement 2) SHALL produce error logs referencing a ModuleNotFoundError with the specific package name
3. THE Breaking Change scenario (Requirement 3) SHALL produce error logs referencing a TypeError with function signature details indicating an API contract violation
4. THE Version Incompatibility scenario (Requirement 4) SHALL produce error logs referencing an AttributeError or ImportError with the deprecated module path and version context
5. WHEN OpsRabbit analyzes any two deployment failure scenarios, THE RCA outputs SHALL identify different root causes (bad deploy vs missing dependency vs breaking change vs version incompatibility)
