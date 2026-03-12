# Dashboard Frontend

<h4>Time</h4>

~45 min

<h4>Purpose</h4>

Add charts to the frontend to visualize the analytics data from Task 2, and learn to integrate a chart library into a `React` application.

<h4>Context</h4>

The analytics endpoints are returning data. Now the team wants a visual dashboard so users can see score distributions, submission timelines, and group performance at a glance.

You will use `Chart.js` (via `react-chartjs-2`) to create bar charts, line charts, or tables.
An AI coding agent can help with the `Chart.js` integration.

<h4>Diagram</h4>

<!-- TODO fill in this section -->

<h4>Table of contents</h4>

- [1. Steps](#1-steps)
  - [1.1. Follow the `Git workflow`](#11-follow-the-git-workflow)
  - [1.2. Create a `Lab Task` issue](#12-create-a-lab-task-issue)
  - [1.3. Install the chart library](#13-install-the-chart-library)
  - [1.4. Create the dashboard component (AI)](#14-create-the-dashboard-component-ai)
  - [1.5. Add navigation](#15-add-navigation)
  - [1.6. Run the type checker](#16-run-the-type-checker)
  - [1.7. Verify locally](#17-verify-locally)
  - [1.8. Commit and push your work](#18-commit-and-push-your-work)
  - [1.9. Deploy to the VM](#19-deploy-to-the-vm)
  - [1.10. Finish the task](#110-finish-the-task)
  - [1.11. Check the task using the autochecker](#111-check-the-task-using-the-autochecker)
- [2. Acceptance criteria](#2-acceptance-criteria)

## 1. Steps

### 1.1. Follow the `Git workflow`

Follow the [`Git workflow`](../../../wiki/git-workflow.md#create-a-lab-task-issue) to complete this task.

### 1.2. Create a `Lab Task` issue

Title: `[Task] Dashboard Frontend`

### 1.3. Install the chart library

1. To navigate to the frontend directory,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   cd frontend
   ```

2. To install `Chart.js` and the React wrapper,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   npm install chart.js react-chartjs-2
   ```

3. To go back to the project root,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   cd ..
   ```

### 1.4. Create the dashboard component (AI)

> [!NOTE]
> The dashboard should display at least two of the following visualizations:
>
> - **Bar chart** — score distribution from `/analytics/scores`
> - **Line chart** — submissions over time from `/analytics/timeline`
> - **Table** — pass rates from `/analytics/pass-rates` or group performance from `/analytics/groups`
>
> You can use an AI agent to generate the `Chart.js` integration code.

1. Open the [coding agent](../../../wiki/coding-agents.md#what-is-a-coding-agent) in the `frontend/` directory.
2. Give it a prompt like:

   > "Create a Dashboard component in `frontend/src/Dashboard.tsx` that:
   > 1. Fetches data from `/analytics/scores?lab=<lab-id>`, `/analytics/timeline?lab=<lab-id>`, and `/analytics/pass-rates?lab=<lab-id>` using the Bearer token from localStorage (key: `api_key`).
   > 2. Shows a bar chart of score buckets using `react-chartjs-2`.
   > 3. Shows a line chart of submissions per day.
   > 4. Shows a table of pass rates per task.
   > 5. Includes a dropdown to select different labs.
   > 6. The code must pass `npm run typecheck` (TypeScript strict mode). Use proper types for all API responses — no `any`."

   Replace `<lab-id>` (without `<` and `>`) with a valid lab ID, such as `lab-04`, or any other lab that has data in your environment.

3. Review the generated code. Make sure it:

   - Imports from `react-chartjs-2` and registers `Chart.js` components.
   - Reads the API token from `localStorage` (key: `api_key`) for the `Authorization: Bearer` header.
   - Renders at least one `<canvas>` element (this is how `Chart.js` renders charts).
   - Handles loading and error states.
   - Uses proper `TypeScript` types for API responses (no `any` types).

> [!TIP]
> If you prefer to implement manually, here is the minimal setup for a bar chart:
>
> ```tsx
> import { Bar } from 'react-chartjs-2'
> import { Chart as ChartJS, CategoryScale, LinearScale, BarElement, Title, Tooltip } from 'chart.js'
>
> ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip)
> ```
>
> Then render `<Bar data={chartData} />` where `chartData` has the `Chart.js` data format.

### 1.5. Add navigation

1. Update `frontend/src/App.tsx` to include navigation between the Items page and the Dashboard.

   You can use an AI agent or implement it manually. Complete these steps:

   1. Add a state variable for the current page (e.g., `"items"` or `"dashboard"`).
   2. Add buttons or links in the header to switch between pages.
   3. Render the Items table or the Dashboard component based on the current page.

### 1.6. Run the type checker

> [!IMPORTANT]
> AI coding agents often generate code with type errors. `TypeScript` strict mode catches bugs like `undefined is not a function` **before** they reach the browser.

1. To navigate to the frontend directory,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   cd frontend
   ```

2. To run the type checker,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   npm run typecheck
   ```

3. Fix any errors reported by the type checker. Common issues:

   - Missing type annotations on function parameters or return values.
   - Using `any` instead of a proper interface for API responses.
   - Accessing properties on possibly `undefined` values without null checks.

   > 🟩 **Tip**
   >
   > If you used an AI agent, give it the error output and ask it to fix the type errors. Include the instruction "Do not use `any` types" in your prompt.

4. To go back to the project root,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   cd ..
   ```

### 1.7. Verify locally

1. To navigate to the frontend directory,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   cd frontend
   ```

2. Configure the environment. Complete these steps:

   1. Open the file [`frontend/.env.example`](../../../frontend/.env.example) ([how to open a file](../../../wiki/vs-code.md#open-the-file)).
   2. Copy it to `frontend/.env`.
   3. Set `VITE_API_TARGET` to the URL of your backend API, for example `http://<your-vm-ip-address>:42002`.

      If you changed [`CADDY_HOST_PORT`](../../../wiki/dotenv-docker-secret.md#caddy_host_port) in [`.env.docker.secret`](../../../wiki/dotenv-docker-secret.md#what-is-envdockersecret), use your value instead of `42002`.

3. To install dependencies,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   npm install
   ```

4. To start the dev server,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   npm run dev
   ```

5. Open the URL shown in the terminal output in a browser.
6. Connect with your API key.
7. Navigate to the Dashboard page.

   You should see charts rendering with data from the analytics endpoints.

   > 🟦 **Note**
   >
   > Make sure you have run `POST /pipeline/sync` at least once (from Task 1) so there is data for the analytics endpoints to return.

### 1.8. Commit and push your work

1. [Commit](../../../wiki/git-workflow.md#commit-changes) your changes.

   Use this commit message:

   ```text
   feat: add analytics dashboard with charts
   ```

2. To push your task branch,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   git push -u origin <task-branch>
   ```

   Replace [`<task-branch>`](../../../wiki/git-workflow.md#task-branch).

### 1.9. Deploy to the VM

1. To navigate to the project directory on your VM,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   cd se-toolkit-lab-5
   ```

2. To fetch updates from the remote,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   git fetch origin
   ```

3. To switch to your task branch,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   git checkout <task-branch>
   ```

   Replace [`<task-branch>`](../../../wiki/git-workflow.md#task-branch).

4. To pull the latest changes,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   git pull
   ```

5. To restart the services,

   [run in the `VS Code Terminal`](../../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   docker compose --env-file .env.docker.secret up --build caddy -d
   ```

6. Open in a browser: `http://<your-vm-ip-address>:42002`.

   If you changed [`CADDY_HOST_PORT`](../../../wiki/dotenv-docker-secret.md#caddy_host_port) in [`.env.docker.secret`](../../../wiki/dotenv-docker-secret.md#what-is-envdockersecret), use your value instead of `42002`.

7. Connect with your API key.
8. Navigate to the Dashboard page.

   You should see charts rendering with data from the analytics endpoints.

   <details><summary><b>Troubleshooting (click to open)</b></summary>

   <h4>Charts do not render</h4>

   Open the browser developer tools console and check for errors. Common issues: missing `Chart.js` component registration, incorrect data format, API returning errors.

   <h4>Container build fails</h4>

   Check that `frontend/package.json` includes `chart.js` and `react-chartjs-2` in dependencies (not devDependencies).

   </details>

### 1.10. Finish the task

1. [Create a PR](../../../wiki/git-workflow.md#create-a-pr-to-the-main-branch-in-your-fork) with your changes.
2. [Get a PR review](../../../wiki/git-workflow.md#get-a-pr-review) and complete the subsequent steps in the `Git workflow`.

### 1.11. Check the task using the autochecker

[Check the task using the autochecker `Telegram` bot](../../../wiki/autochecker.md#check-the-task-using-the-autochecker-bot).

---

## 2. Acceptance criteria

- [ ] Issue has the correct title.
- [ ] `react-chartjs-2` is listed in `frontend/package.json` dependencies.
- [ ] The Dashboard component imports from `Chart.js`.
- [ ] The frontend renders at least one `<canvas>` element (chart).
- [ ] Navigation exists between the Items page and the Dashboard.
- [ ] `npm run typecheck` passes with no errors.
- [ ] The production build is deployed on the VM.
- [ ] PR is approved.
- [ ] PR is merged.
