# Lab 5 — Data Pipeline & Analytics Dashboard

The lab gets updated regularly, so do [sync your fork with the upstream](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/syncing-a-fork#syncing-a-fork-branch-from-the-command-line) from time to time.

<h2>Table of contents</h2>

- [Lab story](#lab-story)
- [Learning advice](#learning-advice)
- [Learning outcomes](#learning-outcomes)
- [Tasks](#tasks)
  - [Prerequisites](#prerequisites)
  - [Required](#required)
  - [Optional](#optional)

## Lab story

Today's tasks will be about **Data engineering**.
Same project – **Learning Management Service**.

The system has an API and front-end in place. Now the team needs to populate the database with real data from an external source, build analytics on top of it, and visualize the results.

A senior data engineer explains your next assignment:

> 1. Build an ETL (extract-transform-load) pipeline that fetches data from the autochecker API and loads it into the database.
> 2. Implement analytics endpoints that aggregate the data for dashboards.
> 3. Add dashboard charts to the front-end to visualize the analytics.

> [!IMPORTANT]
> Communicate through issues and PRs and deliver a working deployment.

## Learning advice

Read the tasks, do the setup properly, work with an agent to help you:

> What do we need to do in Task x? Explain, I want to maximize learning.

> Why is this important? What exactly do we need to do?

You need an agent that has access to the whole repo to work effectively.

## Learning outcomes

By the end of this lab, you should be able to:

- Build an ETL pipeline that fetches data from an external API.
- Handle pagination, incremental sync, and idempotent upserts.
- Write SQL aggregation queries (GROUP BY, COUNT, AVG, CASE WHEN).
- Implement REST API endpoints that return computed analytics.
- Use pre-written tests to validate your implementation.
- Integrate Chart.js into a React front-end for data visualization.
- Use an AI coding agent for ETL and front-end development.

In simple words, you should be able to say:
>
> 1. I built a pipeline that fetches data from an external API and loads it into the database!
> 2. I implemented analytics endpoints and made all tests pass!
> 3. I added charts to the front-end to visualize the analytics!

## Tasks

### Prerequisites

1. Complete the [lab setup](./lab/tasks/setup-simple.md#lab-setup)

> **Note**: If this is the first lab you are attempting in this course, you need to do the [full version of the setup](./lab/tasks/setup.md#lab-setup)

### Required

1. [Build the data pipeline](./lab/tasks/required/task-1.md#build-the-data-pipeline)
2. [Analytics endpoints](./lab/tasks/required/task-2.md#analytics-endpoints)
3. [Dashboard frontend](./lab/tasks/required/task-3.md#dashboard-frontend)

### Optional

1. [Grafana dashboard](./lab/tasks/optional/task-1.md#grafana-dashboard)
