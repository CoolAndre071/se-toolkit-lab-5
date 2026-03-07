# Lab setup

- [1. Required steps](#1-required-steps)
  - [1.1. Clean up Lab 4 (on your VM)](#11-clean-up-lab-4-on-your-vm)
  - [1.2. Set up your fork (in github)](#12-set-up-your-fork-in-github)
    - [1.2.1. Fork the course instructors' repo](#121-fork-the-course-instructors-repo)
    - [1.2.2. Go to your fork](#122-go-to-your-fork)
    - [1.2.3. Enable issues](#123-enable-issues)
    - [1.2.4. Add a classmate as a collaborator](#124-add-a-classmate-as-a-collaborator)
    - [1.2.5. Protect your `main` branch](#125-protect-your-main-branch)
  - [1.3. Clone your fork and set up the environment (on your laptop)](#13-clone-your-fork-and-set-up-the-environment-on-your-laptop)
  - [1.4. Start the services locally (on your laptop)](#14-start-the-services-locally-on-your-laptop)
  - [1.5. Deploy to your VM](#15-deploy-to-your-vm)
    - [1.5.1. Connect to your VM and get the repo there](#151-connect-to-your-vm-and-get-the-repo-there)
    - [1.5.2. Prepare the environment (on the VM)](#152-prepare-the-environment-on-the-vm)
    - [1.5.3. Start the services (on the VM)](#153-start-the-services-on-the-vm)
  - [1.6. Verify the deployment](#16-verify-the-deployment)
  - [1.7 Set up Qwen Code](#17-set-up-qwen-code)

## 1. Required steps

> [!NOTE]
> This lab builds on the same tools and setup from Lab 4.
> If you completed Lab 4, most tools are already installed.
> The main changes are: a new repo, new environment variables, and cleaning up old containers.

> [!NOTE]
> This lab needs your university email, github alias, and VM IP in Autochecker bot <https://t.me/auchebot>. If you haven't entered, then do so. If you want to change something contact your TA.

### 1.1. Clean up Lab 4 (on your VM)

> [!IMPORTANT]
> Remove Lab 4 containers and volumes to free up ports and disk space on your VM.

1. [Connect to your VM](../../wiki/vm.md#connect-to-the-vm).
2. Navigate to the Lab 4 project directory,

   run in the VM:

   ```terminal
   cd se-toolkit-lab-4
   ```

3. Stop and remove all Lab 4 containers and volumes,

   run in the VM:

   ```terminal
   docker compose --env-file .env.docker.secret down -v
   ```

   you should see something like:

   ```
   [+] down 6/6
   ✔ Container se-toolkit-lab-4-caddy-1    Removed                                                                            1.3ss
   ✔ Container se-toolkit-lab-4-pgadmin-1  Removed                                                                            3.4ss
   ✔ Container se-toolkit-lab-4-app-1      Removed                                                                            11.1s
   ✔ Container se-toolkit-lab-4-postgres-1 Removed                                                                            1.5s
   ✔ Volume se-toolkit-lab-4_postgres_data Removed                                                                            0.1s
   ✔ Network se-toolkit-lab-4_default      Removed                                                                            0.2s
   ```

4. Go back to the home directory:

   ```terminal
   cd ~
   ```

### 1.2. Set up your fork (in github)

#### 1.2.1. Fork the course instructors' repo

1. Fork the [lab's repo](https://github.com/inno-se-toolkit/se-toolkit-lab-5).

We refer to your fork as `fork` and to the original repo as `upstream` (выше по течению).

#### 1.2.2. Go to your fork

1. Go to your fork, it should look like `https://github.com/<your-github-username>/se-toolkit-lab-5`.

#### 1.2.3. Enable issues

1. [Enable issues](../../wiki/github.md#enable-issues).

#### 1.2.4. Add a classmate as a collaborator

1. [Add a collaborator](../../wiki/github.md#add-a-collaborator) — your partner from Lab 4.
2. Your partner should add you as a collaborator in their repo.

#### 1.2.5. Protect your `main` branch

1. [Protect a branch](../../wiki/github.md#protect-a-branch).

### 1.3. Clone your fork and set up the environment (on your laptop)

1. Clone your fork to your local machine.

  ```terminal
  git clone https://github.com/<your-github-username>/se-toolkit-lab-5
  ```

1. Open the forked repo in `VS Code`.

2. Go to `VS Code Terminal`, [check that the current directory is `se-toolkit-lab-5`](../../wiki/shell.md#check-the-current-directory-is-directory-name), and install `Python` dependencies:

   ```terminal
   uv sync --dev
   ```

3. Create the environment files:

   ```terminal
   cp .env.docker.example .env.docker.secret
   ```

4. Configure the autochecker API credentials.

   The ETL pipeline fetches data from the autochecker dashboard API.
   You need to set your credentials in both environment files.

   Open `.env.docker.secret` (created from `.env.docker.example`) and set:

   ```text
   AUTOCHECKER_EMAIL=<your-email>@innopolis.university
   AUTOCHECKER_PASSWORD=<your-github-username><your-telegram-alias>
   ```

   Example: if your GitHub username is `johndoe` and your Telegram alias is `jdoe`, the password is `johndoejdoe`.

   Open `.env.docker.secret` and set the same values:

   ```text
   AUTOCHECKER_EMAIL=<your-email>@innopolis.university
   AUTOCHECKER_PASSWORD=<your-github-username><your-telegram-alias>
   ```

   > [!IMPORTANT]
   > The credentials must match your autochecker bot registration.
   > If you haven't registered with the autochecker bot, see [step 1.7](#17-set-up-the-autochecker).

### 1.4. Start the services locally (on your laptop)

1. [Start `Docker`](../../wiki/docker.md#start-docker).

2. To start the services,

   run in the `VS Code Terminal`:

   ```terminal
   docker compose --env-file .env.docker.secret up --build
   ```

   Wait for the services to start. You should see log output from the `app`, `postgres`, `pgadmin`, and `caddy` containers.

   <details><summary>Troubleshooting</summary>

   <h4>Port conflict (<code>port is already allocated</code>)</h4>

   Stop the process that uses the port, then retry.

   <h4>Containers exit immediately</h4>

   To rebuild all containers from scratch,

   run in the `VS Code Terminal`:

   ```terminal
   docker compose --env-file .env.docker.secret down -v
   docker compose --env-file .env.docker.secret up --build
   ```

   </details>

3. Open in a browser: [http://localhost:42002/docs](http://localhost:42002/docs).

   You should see the [`Swagger UI`](../../wiki/swagger.md#what-is-swagger-ui) page with the API documentation.

4. [Authorize](../../wiki/swagger.md#authorize-in-swagger-ui) with the [`API_KEY`](../../wiki/dotenv-docker-secret.md#api_key) from `.env.docker.secret`.

> [!NOTE]
> The database starts empty — there is no seed data.
> All data will be populated by the ETL pipeline in Task 1.

### 1.5. Deploy to your VM

#### 1.5.1. Connect to your VM and get the repo there

1. Connect to your VM through `VS Code Terminal`.

   ```terminal
   ssh <vm-user>@<vm-ip>
   ```

   e.g. ssh my-vm-user@10.93.1.1

   If unable, see [how to connect to vm](../../wiki/vm.md#connect-to-the-vm)

2. To clone your fork on the VM,

   replace <github-username> and run in the `VS Code Terminal`:

   ```terminal
   cd ~
   git clone https://github.com/<github-username>/se-toolkit-lab-5.git
   ```

3. To navigate to the project directory,

   run in the `VS Code Terminal`:

   ```terminal
   cd se-toolkit-lab-5
   ```

#### 1.5.2. Prepare the environment (on the VM)

1. To create the `Docker` environment file,

   run in the `VS Code Terminal` connected to your VM:

   ```terminal
   cp .env.docker.example .env.docker.secret
   ```

2. Edit `.env.docker.secret` and set your autochecker API credentials:

    Open the file with `nano`:

    ```terminal
    nano .env.docker.secret 
    ```
  
    Edit the two fields:

     ```text
     AUTOCHECKER_EMAIL=<your-email>@innopolis.university
     AUTOCHECKER_PASSWORD=<your-github-username><your-telegram-alias>
     ```

    Also set your API key, so that only authorized users can access it.

    ```text
    API_KEY=set-it-to-something-and-remember-it
    ```
  
    Then save the edit by pressing `CTRL X` then type letter `y` and press `ENTER`.

    > It is useful to remember how to view and edit files with `nano` or another similar tool, it's a common operation.

#### 1.5.3. Start the services (on the VM)

1. To start the services in [background](../../wiki/operating-system.md#background-process), run in the `VS Code Terminal`:

   ```terminal
   docker compose --env-file .env.docker.secret up --build -d
   ```

2. To check that the containers are running,

   run in the `VS Code Terminal`:

   ```terminal
   docker compose --env-file .env.docker.secret ps --format "table {{.Service}}\t{{.Status}}"
   ```

   You should see all four services running with the status `Up`:

   ```terminal
   SERVICE    STATUS
   app        Up 50 seconds
   caddy      Up 49 seconds
   pgadmin    Up 50 seconds
   postgres   Up 55 seconds (healthy)
   ```

   <details><summary>Troubleshooting</summary>

   <h4>Port conflict (<code>port is already allocated</code>)</h4>

   [Clean up `Docker`](../../wiki/docker.md#clean-up-docker), then run the `docker compose up` command again.

   <h4>Containers exit immediately</h4>

   To rebuild all containers from scratch,

   [run in the `VS Code Terminal`](../../wiki/vs-code.md#run-a-command-in-the-vs-code-terminal):

   ```terminal
   docker compose --env-file .env.docker.secret down -v
   docker compose --env-file .env.docker.secret up --build -d
   ```

   </details>

### 1.6. Verify the deployment

1. Open in a browser: `http://<your-vm-ip-address>:<caddy-port>/docs`.

   e.g. [http://10.93.x.x:42002/docs](http://10.93.x.x:42002/docs)

   > You set caddy port in `.env.docker.secret`, by default it is 42002.

   You should see the `Swagger UI` page with endpoints including `/pipeline/sync` and `/analytics/`.

2. [Authorize in Swagger](../../wiki/swagger.md#authorize-in-swagger-ui) with `API_KEY` you have in `.env.docker.secret`.

   > You can check both the `API_KEY` and `caddy-port` by opening the env file on VM:
   > `nano .env.docker.secret`, then when done close it with `CLTR X`

3. Try the `GET /items/` endpoint.

   You should get an empty array `[]` — the database has no data yet.

### 1.7 Set up Qwen Code

`Qwen Code` is a coding agent we'll use to make your life easier. It works in Russia without VPN and has a free tier.

1. Create an account in [Qwen Chat](https://chat.qwen.ai/?mode=register)
2. Install `Qwen code` on your local machine.

  Linux / macOS:

  ```bash
  curl -fsSL https://qwen-code-assets.oss-cn-hangzhou.aliyuncs.com/installation/install-qwen.sh | bash
  ```
  
  Windows (Run as Administrator CMD):

  ```cmd
  curl -fsSL -o %TEMP%\install-qwen.bat https://qwen-code-assets.oss-cn-hangzhou.aliyuncs.com/installation/install-qwen.bat && %TEMP%\install-qwen.bat
  ```
  
  > **Note**: It's recommended to restart your terminal after installation to ensure environment variables take effect.

1. Now launch `Qwen code` in Terminal on your laptop:

  ```terminal
  qwen
  ```
  
  And authenticate with your Qwen Chat OAuth option following the instructions.
  
  You can now ask it, for example:

  ```
  What is this lab supposed to teach me?
  ```
  
  > **Note**: It is my personal preference to use terminal version of the agent, yet there are other ways to use it as an VS Code extension, read more here if interested: [AI coding agent setup](../../wiki/coding-agents.md)

----

🎉 Congrats! You are done with the setup! Now the tasks should go smoothly. Go to [tasks](https://github.com/inno-se-toolkit/se-toolkit-lab-5/tree/main?tab=readme-ov-file#tasks).
