# BDB Pipeline Automation

Safe, local-first automation scaffolding for BDB Data Engineering Pipelines and a Claude Code skill that knows the verified capability boundary.

The project can be a sibling of `bdbservices`, which was inspected as an implementation reference. It has no runtime import dependency on that folde

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
