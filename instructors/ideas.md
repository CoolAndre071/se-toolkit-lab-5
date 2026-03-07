## This lab

### This lab - TODO

1. install nodejs via nix
2. update caddy port - should be the biggest
3. goal: interact with the database, not just observe
4. rename: the autochecker -> `Autochecker`
5. describe full vm setup
6. use mdsh for nixpkgs hashes
7. connect by remote ssh - check your ip to understand where you are
8. [?] Connect by ssh - can't find ssh config in Linux
9. should a section in a sequence of steps assume the previous step?
10. lab-prompts.md? - prompts for agents
    - bundle all instructions for task 1 in a readable doc
11. don't use claude-specific words in skills
12. Use consistent API token (or key?) naming
13. Rename app -> backend
14. Rename `APP_` -> `BACK_`
15. Add `FRONT_` suffix for front-end variables
16. Always provide links to variables from .env.docker.secret
17. `.env.local.example`
18. Student: ask agent why these tests
19. move generic troubleshooting sections to wiki
20. autochecker: check file submissions size
21. Is this true? "# This solution won't work outside the University network."
22. fix adjacent links
23. GitHub Pages with good full-text search
24. `Remote-SSH: Connect to Host...`
25. what is X -> About X?
26. setup-simple.md - a simpler version of setup.md
27. Move constants with `CONST_` prefix from `.env.docker.example` to `.env.const`
28. conventions: prompt engineering steps:
    - don't provide a ready prompt first.
    - hint at what to think about when writing a prompt.
    - provide the prompt under a spoiler.
29. Move ideas to the instructors/ideas.md.
30. Use instructors/meetings just for storing meeting notes, not for the lab design.
31. skill /issue
32. russian version
33. Prompt students to use `skill commit`
34. setup: The instructions aren't guaranteed to work outside of Linux or macOS. This is why we require to use WSL
35. coding-agents.md 1000 requests - each file read is a request
36. Fix config after the migration from the older repo
37. Migrate relevant parts of inno-se/the-guide
38. grafana later when we have multiple apps
39. use pnpm or bun
40. qwen and ssh
41. conventions: new sentence on a new line
42. line break between instructions
43. line break after curl command
44. fix /fix-file-by-conventions skill: write title instead of cross-out in the task report.
45. skill: terms not explained in the wiki

### This lab - DONE

1. [x] CRLF - fixed via .gitattributes
2. [x] switch to typescript
3. [x] kebab case most of the time - via claude conventions
4. [x] direnv allow
5. [x] caddy container has frontend?
6. [x] in git-workflow.md, explain how to git pull
7. [x] update db name: lab-4 to db-lab-4
8. [x] update server name: lab-4 to postgres-lab-4
9. [x] reuse claude skills for qwen

       Symlinked QWEN.md to CLAUDE.md and .qwen to .claude.
       However, the qwen extension can't use skills.
       Only the CLI version can.
10. [x] api.md
11. [x] use postgres 18
12. [x] conventions include self-checks
13. [?] add and remove cors
14. [x] Rename `API_TOKEN` -> `API_KEY`
15. [x] install extensions not in WSL but earlier and then reopen in WSL
        Mentioned in the docs on instaling the recommended extensions
16. [x] move from testing.md to quality-assurance.md
17. [x] Conventions:
        Tasks must create controlled environment.
        Even AI steps.
18. [x] Conventions - outcomes must be verifiable using the autochecker
19. [x] Move constants to `.env.docker.example` with `CONST_` prefix
20. [x] convention - visualize task
21. [x] use a more neutral IP address in examples
        Use `192.0.2.1`
22. [x] review via conventions - check conceptual problems from the educational and practical point of view.
23. [x] conventions:

      files must stay in sync:
      - dotenv-docker-secret.md with .env.docker.example
      - dotenv-tests-unit-secret.md with .env.tests.unit.example
      - dotenv-tests-e2e-secret.md with .env.tests.e2e.example
      - pyproject-toml.md with pyproject.toml
24. [x] report "Which conceptual problems are in ..." then which conventions are violated
        Have the skill /review-task-conceptual
25. [x] Run through autochecker note at the end of each task
        Have a convention now

## Next labs

1. test front
2. Implement a Status page <https://status.claude.com/>
   Must be a separate service that checks health. Grafana?
3. caddy https
4. include logging
5. script for database backup
6. deploy via github actions
