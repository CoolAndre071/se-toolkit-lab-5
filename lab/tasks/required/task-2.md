# Analytics Endpoints

<h4>Time</h4>

~45 min

<h4>Purpose</h4>

Implement four analytics endpoints that perform `SQL` aggregation queries on the data populated by the ETL pipeline. Use pre-written tests to verify your implementation.

<h4>Context</h4>

The ETL pipeline from Task 1 has populated the database with check results. Now the team needs endpoints that aggregate this data for dashboards: score distributions, per-task pass rates, submission timelines, and per-group performance.

Pre-written tests in [`backend/tests/unit/test_analytics.py`](../../../backend/tests/unit/test_analytics.py) define the expected behavior. Your job is to implement the endpoints so that all tests pass.

<h4>Table of contents</h4>

- [1. Steps](#1-steps)
  - [1.1. Follow the `Git workflow`](#11-follow-the-git-workflow)
  - [1.2. Create a `Lab Task` issue](#12-create-a-lab-task-issue)
  - [1.3. Read the tests and stubs](#13-read-the-tests-and-stubs)
  - [1.4. Run the tests (expect failures)](#14-run-the-tests-expect-failures)
  - [1.5. Implement the endpoints](#15-implement-the-endpoints)
    - [1.5.1. Scores histogram](#151-scores-histogram)
    - [1.5.2. Pass rates](#152-pass-rates)
    - [1.5.3. Timeline](#153-timeline)
    - [1.5.4. Groups](#154-groups)
  - [1.6. Run the tests (all should pass)](#16-run-the-tests-all-should-pass)
  - [1.7. Commit and push your work](#17-commit-and-push-your-work)
  - [1.8. Deploy and verify](#18-deploy-and-verify)
  - [1.9. Finish the task](#19-finish-the-task)
  - [1.10. Check the task using the autochecker](#110-check-the-task-using-the-autochecker)
- [2. Acceptance criteria](#2-acceptance-criteria)

## 1. Steps

### 1.1. Follow the `Git workflow`

Follow the [`Git workflow`](../../../wiki/git-workflow.md) to complete this task.

### 1.2. Create a `Lab Task` issue

Title: `[Task] Analytics Endpoints`

### 1.3. Read the tests and stubs

1. [Open the file](../../../wiki/vs-code.md#open-the-file):
   [`backend/tests/unit/test_analytics.py`](../../../backend/tests/unit/test_analytics.py).

   This file contains 17 tests organized into four test classes:

   | Test class | Endpoint | What it tests |
   |------------|----------|---------------|
   | `TestScores` | `GET /analytics/scores?lab=lab-04` | Score histogram with 4 buckets |
   | `TestPassRates` | `GET /analytics/pass-rates?lab=lab-04` | Average score and attempt count per task |
   | `TestTimeline` | `GET /analytics/timeline?lab=lab-04` | Submission count per day |
   | `TestGroups` | `GET /analytics/groups?lab=lab-04` | Average score and student count per group |

> [!NOTE]
> The tests create an in-memory database with fixture data and send requests to the endpoints.
> You do not need to modify the tests — only the endpoint implementations.

2. [Open the file](../../../wiki/vs-code.md#open-the-file):
   [`backend/app/routers/analytics.py`](../../../backend/app/routers/analytics.py).

   This file contains four endpoint stubs. Each endpoint currently raises `NotImplementedError`.

   Read the TODO comments to understand the expected query logic.

### 1.4. Run the tests (expect failures)

1. [Check that the current directory is `se-toolkit-lab-5`](../../../wiki/shell.md#check-the-current-directory-is-directory-name).

2. To run the unit tests,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   uv run poe test
   ```

   You should see 17 analytics tests failing and 3 interaction tests passing:

   ```terminal
   17 failed, 3 passed
   ```

   The failures are expected — the endpoints are not implemented yet.

### 1.5. Implement the endpoints

You can implement the endpoints manually or use an AI coding agent. Each endpoint performs an `SQL` aggregation query.

> [!TIP]
> If using an AI agent, give it a prompt like:
>
> "Read the tests in `backend/tests/unit/test_analytics.py` and the stubs in `backend/app/routers/analytics.py`. Implement all four endpoints to make the tests pass. Use SQLAlchemy/SQLModel queries."
>
> The agent will have the test expectations and the TODO comments to guide it.

The `lab` query parameter is a lab identifier like `"lab-04"`. Transform it to match the title format (e.g. `"lab-04"` → `"Lab 04"`) and find items whose title contains that string.

<!-- no toc -->
- [1.5.1. Scores histogram](#151-scores-histogram)
- [1.5.2. Pass rates](#152-pass-rates)
- [1.5.3. Timeline](#153-timeline)
- [1.5.4. Groups](#154-groups)

#### 1.5.1. Scores histogram

`GET /analytics/scores?lab=lab-04`

Returns the distribution of scores in four buckets:

```json
[
  {"bucket": "0-25", "count": 12},
  {"bucket": "26-50", "count": 8},
  {"bucket": "51-75", "count": 15},
  {"bucket": "76-100", "count": 25}
]
```

Query logic:

1. Find the lab item whose title contains the `lab` parameter (e.g. `"lab-04"` → match `"Lab 04"` in the title).
2. Find all task items that belong to this lab (`parent_id = lab.id`).
3. Query interactions for these tasks that have a `score`.
4. Group scores into 4 buckets using `CASE WHEN` expressions.
5. Always return all four buckets, even if the count is 0.

#### 1.5.2. Pass rates

`GET /analytics/pass-rates?lab=lab-04`

Returns per-task statistics:

```json
[
  {"task": "Repository Setup", "avg_score": 92.3, "attempts": 150}
]
```

Query logic:

1. Find the lab item and its child task items.
2. For each task, compute the average score (rounded to 1 decimal) and total number of interactions.
3. Order by task title.

#### 1.5.3. Timeline

`GET /analytics/timeline?lab=lab-04`

Returns submissions per day:

```json
[
  {"date": "2026-02-28", "submissions": 45}
]
```

Query logic:

1. Find the lab item and its child task items.
2. Group interactions by date (cast `created_at` to date).
3. Count submissions per day.
4. Order by date ascending.

#### 1.5.4. Groups

`GET /analytics/groups?lab=lab-04`

Returns per-group performance:

```json
[
  {"group": "B23-CS-01", "avg_score": 78.5, "students": 25}
]
```

Query logic:

1. Find the lab item and its child task items.
2. Join interactions with learners to get `student_group`.
3. For each group, compute the average score (rounded to 1 decimal) and count of distinct learners.
4. Order by group name.

### 1.6. Run the tests (all should pass)

1. To run the full test suite,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   uv run poe test
   ```

   All 20 tests should pass:

   ```terminal
   ===================== 20 passed in X.XXs =====================
   ```

   <details><summary>Troubleshooting</summary>

   <h4>Some tests still fail</h4>

   Read the failing test name and assertion message carefully. The test names describe what they expect. For example, `test_scores_counts_are_correct` checks specific count values for each bucket.

   To run a single test class to focus on one endpoint,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   uv run pytest backend/tests/unit/test_analytics.py::TestScores -v
   ```

   <h4><code>NotImplementedError</code></h4>

   You haven't implemented the endpoint yet. Check that you replaced `raise NotImplementedError` with the query logic.

   </details>

### 1.7. Commit and push your work

1. [Commit](../../../wiki/git-workflow.md#commit-changes) your changes.

   Use this commit message:

   ```text
   feat: implement analytics endpoints (scores, pass-rates, timeline, groups)
   ```

2. To push your task branch,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   git push -u origin <task-branch>
   ```

   Replace [`<task-branch>`](../../../wiki/git-workflow.md#task-branch).

### 1.8. Deploy and verify

1. To pull your branch and restart the services on your VM,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   cd se-toolkit-lab-5
   git fetch origin && git checkout <task-branch> && git pull
   docker compose --env-file .env.docker.secret up --build -d
   ```

   Replace [`<task-branch>`](../../../wiki/git-workflow.md#task-branch).

2. [Open `Swagger UI`](../../../wiki/swagger.md#open-swagger-ui) at `http://<your-vm-ip-address>:42002/docs`.

3. [Authorize in `Swagger UI`](../../../wiki/swagger.md#authorize-in-swagger-ui) with your [`API_KEY`](../../../wiki/dotenv-docker-secret.md#api_key) in [`.env.docker.secret`](../../../wiki/dotenv-docker-secret.md#what-is-envdockersecret).

4. Try each analytics endpoint with `lab=lab-04` (or any lab that has data). Verify that each returns a `200` response with a `JSON` array.

### 1.9. Finish the task

1. [Create a PR](../../../wiki/git-workflow.md#create-a-pr-to-the-main-branch-in-your-fork) with your changes.
2. [Get a PR review](../../../wiki/git-workflow.md#get-a-pr-review) and complete the subsequent steps in the `Git workflow`.

### 1.10. Check the task using the autochecker

[Check the task using the autochecker `Telegram` bot](../../../wiki/autochecker.md#check-the-task-using-the-autochecker-bot).

---

## 2. Acceptance criteria

- [ ] Issue has the correct title.
- [ ] `uv run poe test` passes all 20 tests (17 analytics + 3 interaction).
- [ ] `GET /analytics/scores?lab=<lab>` returns `200` with a `JSON` array of 4 bucket objects.
- [ ] `GET /analytics/pass-rates?lab=<lab>` returns `200` with a `JSON` array of task objects.
- [ ] `GET /analytics/timeline?lab=<lab>` returns `200` with a `JSON` array of date objects.
- [ ] `GET /analytics/groups?lab=<lab>` returns `200` with a `JSON` array of group objects.
- [ ] PR is approved.
- [ ] PR is merged.
