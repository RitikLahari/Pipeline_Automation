# BDB Pipeline Automation

Safe, local-first automation scaffolding for BDB Data Engineering Pipelines and a Claude Code skill that knows the verified capability boundary.

The project can be a sibling of `bdbservices`, which was inspected as an implementation reference. It has no runtime import dependency on that folder.

## Current status

The existing BDB SDK exposes Data Engineering **Job** and job-canvas workflow APIs through `consumerName=BIZVIZPIPELINE`. Browser captures and the deployed editor source now verify `createPipeline`, `createPipelineWorkflow`, `getPipelineById`, `getAllComponentsInfo`, and `checkPipelineAvailableNewVersions` for the separate **Pipelines** canvas.

Consequently:

- Empty standalone pipeline creation is available through the verified two-call create/workflow sequence; component configuration and activation remain dry-run only.
- A workflow can be persisted for an already-created pipeline ID through the Python service's verified `create_pipeline_workflow` method.
- Existing standalone pipelines can be read and verified through the captured `getPipelineById` contract.
- Visible standalone pipelines can be listed through the captured `loadPipelinePrivilege` contract.
- Pipeline component templates can be discovered through the captured `getAllComponentsInfo` contract.
- Components already instantiated in a pipeline can be read through `getComponentConfigForPipeline`.
- Saved visual workflow, links, and events can be read through `getPipelineWorkflow`.
- Standalone pipeline run and execution-status operations remain unavailable.
- Verified Job read, workflow, activation, and log operations are available as a Python service for explicit Job use.
- No endpoint, payload, or success response is guessed.

## Architecture

```text
Claude request
    -> .claude/skills/bdb-pipeline/SKILL.md
    -> scripts/*.py
    -> PipelineService (safe standalone-pipeline boundary)
    -> BDBClient / AuthenticationService (when a verified live call exists)

Explicit Job request
    -> JobService
    -> BDBClient
    -> verified /cxf/bizviz/pluginService Job operations
```

## Folder structure

```text
bdb-pipeline-automation/
|-- .claude/skills/bdb-pipeline/
|   |-- SKILL.md
|   `-- references/
|       |-- authentication.md
|       |-- bdb-sdk.md
|       |-- examples.md
|       `-- pipeline.md
|-- scripts/
|   |-- activate_pipeline.py
|   |-- configure_pipeline.py
|   |-- create_pipeline.py
|   |-- list_components.py
|   |-- list_pipeline_components.py
|   |-- inspect_pipeline_workflow.py
|   `-- verify_pipeline.py
|-- src/bdb_pipeline/
|   |-- auth.py
|   |-- client.py
|   |-- job_service.py
|   |-- pipeline_service.py
|   `-- utils.py
|-- tests/
|-- .env.example
|-- .gitignore
|-- pyproject.toml
`-- requirements.txt
```

## Relationship to `bdbservices`

The following implementation patterns were adapted from `../bdbservices/bdb-platform-sdk-python/bdb_platform_sdk/` and are now implemented locally:

- Required `BDB_URL` and `BDB_SPACEKEY` configuration.
- Form-URL-encoded POST client behavior.
- Customer-space discovery and password authentication endpoints.
- `authtoken` and `spacekey` header conventions.
- The `pluginService` envelope for verified Job operations.
- Job ID to `docid` conversion.

The full SDK was not copied. Semantic models, data services, agents, uploads, notebook provisioning, job creation builders, and deployment-specific constants remain in the original SDK.

## Installation

Python 3.9 or later is required.

```bash
python -m venv .venv
```

Activate the virtual environment, then install the package and test dependencies:

```bash
python -m pip install -e ".[dev]"
```

For runtime dependencies only:

```bash
python -m pip install -r requirements.txt
```

The scripts add the local `src` directory to their import path, so dry-run commands can also be executed directly from the project root.

## Environment configuration

Copy `.env.example` to `.env`:

```text
BDB_URL=https://app.bdb.ai
BDB_SPACEKEY=1111
BDB_USER_ID=
BDB_USER_EMAIL=
BDB_CUSTOMERKEY=
BDB_PASSWORD=
BDB_AUTH_TOKEN=
```

| Variable | Required | Source |
|---|---:|---|
| `BDB_URL` | For live BDB calls | Base URL for the target BDB deployment. |
| `BDB_SPACEKEY` | For live BDB calls | Space/workspace identifier for the target tenant. |
| `BDB_USER_ID` | Recommended | User ID sent in `loggedUser` and `userid`. |
| `BDB_USER_EMAIL` | Password flow | BDB email/user name. |
| `BDB_CUSTOMERKEY` | Optional password-flow override | Leave blank to discover it from `BDB_SPACEKEY`. |
| `BDB_PASSWORD` | Password flow | Secret stored only in the local ignored `.env`. |
| `BDB_AUTH_TOKEN` | Optional alternative | Existing session token; expires and requires replacement. |

The existing SDK has an optional `BDB_WORKSPACEKEY` for sandbox-storage uploads. This project does not upload files, so that variable is not included.

`.env` is ignored by Git and may contain the credentials supplied by the local user. It is plain text: restrict access to it and never commit, paste, print, or attach it. `.env.example` contains placeholders only.

## Authentication

The discovered password flow is:

1. Call `/cxf/auth/getCustomerSpaces` with a user email/ID.
2. Select the unique password-login entry whose `spaceKey` matches `BDB_SPACEKEY` and read its `customerKey`. The live response uses `spaces.spaces[]`; the parser also accepts the SDK's documented nested variants.
3. Call `/cxf/auth/authenticateuser` with user ID, password, the discovered customer key, and `authType=ep`.
4. Read `authToken` from the response.
5. Put that token in the `authtoken` header for subsequent service calls.

BDB does not use a Bearer header in the inspected SDK. No API-key requirement or refresh-token endpoint was found. Token validity is checked through `/cxf/auth/getUserInfoByToken`; authenticate again after expiration.

The sibling SDK also implements configured-space SSO. This CLI does not provide a web callback listener, so use the original SDK or a host application for SSO token exchange.

## Localhost testing

Dry-run planning does not make network calls and does not require credentials:

```bash
python scripts/create_pipeline.py --name test-pipeline --dry-run
```

Live standalone-pipeline create/configure/activate operations intentionally return exit code `2` until their endpoint and payload contracts are verified. The captured read/verify operation is available.

## Creating a pipeline

```bash
python scripts/create_pipeline.py \
  --name test-pipeline \
  --description "Local pipeline test" \
  --resource-allocation low \
  --dry-run
```

The dry-run output previews `createPipeline`, derived `createPipelineWorkflow`, and verification. After reviewing the preview and authorizing the mutation, omit `--dry-run` to create the empty pipeline.

## Configuring a pipeline

Prepare a reviewed JSON object and run:

```bash
python scripts/configure_pipeline.py \
  --pipeline-id PIPELINE_ID \
  --config reviewed-config.json \
  --dry-run
```

The project validates that the file contains a non-empty JSON object. It does not claim that this object is a valid BDB component/event graph until that schema is captured from BDB.

## Activating a pipeline

```bash
python scripts/activate_pipeline.py --pipeline-id PIPELINE_ID --dry-run
```

Activation is planning-only for standalone pipelines. Do not substitute the Job `changeStatusJob` call: that endpoint activates a Job resource.

## Verifying a pipeline

Report discovered capabilities without a network call:

```bash
python scripts/verify_pipeline.py --pipeline-id PIPELINE_ID --capabilities
```

To verify an existing pipeline, run:

```bash
python scripts/verify_pipeline.py --pipeline-id dp_PIPELINE_ID --user-id USER_ID
```

The script reads configuration and credentials from `.env`. With `BDB_USER_EMAIL`, `BDB_PASSWORD`, and `BDB_CUSTOMERKEY`, it authenticates for a fresh token. Alternatively it uses `BDB_AUTH_TOKEN`; if neither is configured, it prompts using hidden input. It calls `getPipelineById` and reports the pipeline name, active/running/batch flags, resource limit, and component count without printing credentials.

List the pipelines visible to the current user:

```bash
python scripts/list_pipelines.py --user-id USER_ID
```

## Existing registered model

The current automation target assumes the model already exists and is registered. Model creation and training are not part of this skill. The next component-level API to capture is saving a DS Lab Runner configured with that model in either real-time or batch mode.

Inspect the current DS Lab Runner template:

```bash
python scripts/list_components.py --user-id USER_ID --name "DSLab Runner"
```

Inspect components already added to a pipeline:

```bash
python scripts/list_pipeline_components.py --pipeline-id dp_PIPELINE_ID --user-id USER_ID
```

Add `--full` for the complete component objects. The captured real-time DS Lab Runner instance did not include selected project/model values, so its presence alone must not be reported as completed model configuration.

The captured schema exposes `dsLabModelRunner` for an existing registered model and requires the DS Lab project and model selections. The component ID is discovered dynamically rather than hardcoded.

## Verified Job operations

`bdb_pipeline.job_service.JobService` provides these explicit Job calls:

- `get_job_by_id`
- `get_job_workflow`
- `change_status_job`
- `get_job_ui_logs`
- `get_advance_logs`
- `verify_job`

They are not invoked by the standalone pipeline scripts. The existing full SDK should remain the preferred implementation for end-to-end Python Job creation because it already handles project provisioning, notebook registration, job creation/update, and the separate job workflow store.

## Running unit tests

```bash
python -m pytest
```

The tests use fake clients and sessions. They do not call BDB services.

## Integration testing

No automatic integration tests are included because they would create external resources. Any future integration test must:

1. Be separated from unit tests.
2. Skip unless `BDB_INTEGRATION_TEST=true` is explicitly set.
3. Obtain secrets at runtime.
4. Use a non-production workspace.
5. Avoid printing tokens or passwords.
6. Clean up only resources created by that test and only after confirming their IDs.

`BDB_INTEGRATION_TEST` is a test safety switch, not BDB platform configuration, so it is intentionally absent from `.env.example`.

## Claude Code skill

The project skill is at `.claude/skills/bdb-pipeline/SKILL.md`. Invoke it as `/bdb-pipeline` in a Claude Code environment that discovers project skills.

The entrypoint routes detailed work to focused references instead of embedding the complete SDK and every payload in one file.

## Troubleshooting

### Missing `BDB_URL` or `BDB_SPACEKEY`

Live client construction raises a configuration error. Confirm the target workspace values; do not copy credentials from `bdbservices`.

### Pipeline command says the operation is unsupported

This is expected for component configuration and activation. Empty pipeline creation is supported; use `--dry-run` to preview it before an approved live mutation.

### A Job exists but its canvas is empty

The existing Job implementation documents a separate workflow store. `createJob`/`updateJob` populate execution tasks, while `createJobWorkflow` and `updateJobWorkflow` persist the job canvas and form values. Use the full sibling SDK and verify with `getJobWorkflow`.

### Confusing Jobs with Pipelines

The name `BIZVIZPIPELINE` is the consumer used by verified Job APIs, but that name alone is not evidence that those requests operate on the standalone Pipelines list or editor.

### Browser automation

The inspected `.playwright-mcp` folder contains only console logs, not reusable configuration or pipeline network captures. Browser clicking is not implemented. API automation remains preferred after the actual contract is captured.

## Next implementation milestone

Capture the DS Lab Runner save request with selected project/model values, followed by the standalone activation request. Those contracts are the next milestones after verified empty-pipeline creation.
