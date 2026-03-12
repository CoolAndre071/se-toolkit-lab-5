"""ETL pipeline: fetch data from the autochecker API and load it into the database.

The autochecker dashboard API provides two endpoints:
- GET /api/items — lab/task catalog
- GET /api/logs  — anonymized check results (supports ?since= and ?limit= params)

Both require HTTP Basic Auth (email + password from settings).
"""

from datetime import datetime

import httpx
from sqlmodel.ext.asyncio.session import AsyncSession

from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/items
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - The response is a JSON array of objects with keys:
      lab (str), task (str | null), title (str), type ("lab" | "task")
    - Return the parsed list of dicts
    - Raise an exception if the response status is not 200
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API.

    TODO: Implement this function.
    - Use httpx.AsyncClient to GET {settings.autochecker_api_url}/api/logs
    - Pass HTTP Basic Auth using settings.autochecker_email and
      settings.autochecker_password
    - Query parameters:
      - limit=500 (fetch in batches)
      - since={iso timestamp} if provided (for incremental sync)
    - The response JSON has shape:
      {"logs": [...], "count": int, "has_more": bool}
    - Handle pagination: keep fetching while has_more is True
      - Use the submitted_at of the last log as the new "since" value
    - Return the combined list of all log dicts from all pages
    """
    all_logs: list[dict] = []

    async with httpx.AsyncClient() as client:
        while True:
            params: dict[str, str | int] = {"limit": 500}
            if since is not None:
                params["since"] = since.isoformat()

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            if not logs:
                break

            last_submitted_at = logs[-1].get("submitted_at")
            if not isinstance(last_submitted_at, str):
                break

            since = datetime.fromisoformat(last_submitted_at.replace("Z", "+00:00"))

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database.

    TODO: Implement this function.
    - Import ItemRecord from app.models.item
    - Process labs first (items where type="lab"):
      - For each lab, check if an item with type="lab" and matching title
        already exists (SELECT)
      - If not, INSERT a new ItemRecord(type="lab", title=lab_title)
      - Build a dict mapping the lab's short ID (the "lab" field, e.g.
        "lab-01") to the lab's database record, so you can look up
        parent IDs when processing tasks
    - Then process tasks (items where type="task"):
      - Find the parent lab item using the task's "lab" field (e.g.
        "lab-01") as the key into the dict you built above
      - Check if a task with this title and parent_id already exists
      - If not, INSERT a new ItemRecord(type="task", title=task_title,
        parent_id=lab_item.id)
    - Commit after all inserts
    - Return the number of newly created items
    """
    from sqlalchemy import select

    from app.models.item import ItemRecord

    new_count = 0
    lab_map: dict[str, ItemRecord] = {}

    # Pass 1: labs
    for item in items:
        if item.get("type") != "lab":
            continue

        lab_title = item.get("title")
        lab_short_id = item.get("lab")
        if not isinstance(lab_title, str) or not isinstance(lab_short_id, str):
            continue

        stmt = select(ItemRecord).where(
            ItemRecord.type == "lab",
            ItemRecord.title == lab_title,
        )
        result = await session.exec(stmt)
        existing_lab = result.scalars().first()

        if existing_lab is None:
            existing_lab = ItemRecord(type="lab", title=lab_title)
            session.add(existing_lab)
            await session.flush()
            new_count += 1

        lab_map[lab_short_id] = existing_lab

    # Pass 2: tasks
    for item in items:
        if item.get("type") != "task":
            continue

        task_title = item.get("title")
        parent_lab_short_id = item.get("lab")
        if not isinstance(task_title, str) or not isinstance(parent_lab_short_id, str):
            continue

        parent_lab = lab_map.get(parent_lab_short_id)
        if parent_lab is None or parent_lab.id is None:
            continue

        stmt = select(ItemRecord).where(
            ItemRecord.type == "task",
            ItemRecord.title == task_title,
            ItemRecord.parent_id == parent_lab.id,
        )
        result = await session.exec(stmt)
        existing_task = result.scalars().first()

        if existing_task is None:
            session.add(
                ItemRecord(
                    type="task",
                    title=task_title,
                    parent_id=parent_lab.id,
                )
            )
            new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database.

    Args:
        logs: Raw log dicts from the API (each has lab, task, student_id, etc.)
        items_catalog: Raw item dicts from fetch_items() — needed to map
            short IDs (e.g. "lab-01", "setup") to item titles stored in the DB.
        session: Database session.

    TODO: Implement this function.
    - Import Learner from app.models.learner
    - Import InteractionLog from app.models.interaction
    - Import ItemRecord from app.models.item
    - Build a lookup from (lab_short_id, task_short_id) to item title
      using items_catalog. For labs, the key is (lab, None). For tasks,
      the key is (lab, task). The value is the item's title.
    - For each log dict:
      1. Find or create a Learner by external_id (log["student_id"])
         - If creating, set student_group from log["group"]
      2. Find the matching item in the database:
         - Use the lookup to get the title for (log["lab"], log["task"])
         - Query the DB for an ItemRecord with that title
         - Skip this log if no matching item is found
      3. Check if an InteractionLog with this external_id already exists
         (for idempotent upsert — skip if it does)
      4. Create InteractionLog with:
         - external_id = log["id"]
         - learner_id = learner.id
         - item_id = item.id
         - kind = "attempt"
         - score = log["score"]
         - checks_passed = log["passed"]
         - checks_total = log["total"]
         - created_at = parsed log["submitted_at"]
    - Commit after all inserts
    - Return the number of newly created interactions
    """
    from sqlalchemy import select

    from app.models.interaction import InteractionLog
    from app.models.item import ItemRecord
    from app.models.learner import Learner

    # (lab_short_id, task_short_id|None) -> title
    item_title_lookup: dict[tuple[str, str | None], str] = {}
    for item in items_catalog:
        lab_short_id = item.get("lab")
        title = item.get("title")
        if not isinstance(lab_short_id, str) or not isinstance(title, str):
            continue
        task_short_id = item.get("task")
        if task_short_id is not None and not isinstance(task_short_id, str):
            continue
        item_title_lookup[(lab_short_id, task_short_id)] = title

    new_count = 0

    for log in logs:
        student_id = log.get("student_id")
        if not isinstance(student_id, str):
            continue

        # 1) Find or create learner
        learner_stmt = select(Learner).where(Learner.external_id == student_id)
        learner_result = await session.exec(learner_stmt)
        learner = learner_result.scalars().first()

        if learner is None:
            student_group = log.get("group")
            learner = Learner(
                external_id=student_id,
                student_group=student_group if isinstance(student_group, str) else "",
            )
            session.add(learner)
            await session.flush()

        if learner.id is None:
            continue

        # 2) Resolve item by API short IDs -> title -> DB item
        lab_short_id = log.get("lab")
        task_short_id = log.get("task")
        if not isinstance(lab_short_id, str):
            continue
        if task_short_id is not None and not isinstance(task_short_id, str):
            continue

        item_title = item_title_lookup.get((lab_short_id, task_short_id))
        if item_title is None:
            continue

        if task_short_id is None:
            item_stmt = select(ItemRecord).where(
                ItemRecord.title == item_title,
                ItemRecord.type == "lab",
            )
            item_result = await session.exec(item_stmt)
            item = item_result.scalars().first()
        else:
            parent_lab_title = item_title_lookup.get((lab_short_id, None))
            if parent_lab_title is None:
                continue

            parent_lab_stmt = select(ItemRecord).where(
                ItemRecord.title == parent_lab_title,
                ItemRecord.type == "lab",
            )
            parent_lab_result = await session.exec(parent_lab_stmt)
            parent_lab = parent_lab_result.scalars().first()
            if parent_lab is None or parent_lab.id is None:
                continue

            item_stmt = select(ItemRecord).where(
                ItemRecord.title == item_title,
                ItemRecord.type == "task",
                ItemRecord.parent_id == parent_lab.id,
            )
            item_result = await session.exec(item_stmt)
            item = item_result.scalars().first()

        if item is None or item.id is None:
            continue

        # 3) Idempotent upsert by external log ID
        external_log_id = log.get("id")
        if not isinstance(external_log_id, int):
            continue

        existing_stmt = select(InteractionLog).where(
            InteractionLog.external_id == external_log_id
        )
        existing_result = await session.exec(existing_stmt)
        if existing_result.scalars().first() is not None:
            continue

        # 4) Insert interaction log
        submitted_at = log.get("submitted_at")
        if not isinstance(submitted_at, str):
            continue

        created_at = datetime.fromisoformat(submitted_at.replace("Z", "+00:00"))
        if created_at.tzinfo is not None:
            created_at = created_at.replace(tzinfo=None)

        interaction = InteractionLog(
            external_id=external_log_id,
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log.get("score"),
            checks_passed=log.get("passed"),
            checks_total=log.get("total"),
            created_at=created_at,
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline.

    TODO: Implement this function.
    - Step 1: Fetch items from the API (keep the raw list) and load them
      into the database
    - Step 2: Determine the last synced timestamp
      - Query the most recent created_at from InteractionLog
      - If no records exist, since=None (fetch everything)
    - Step 3: Fetch logs since that timestamp and load them
      - Pass the raw items list to load_logs so it can map short IDs
        to titles
    - Return a dict: {"new_records": <number of new interactions>,
                      "total_records": <total interactions in DB>}
    """
    from sqlalchemy import func, select

    from app.models.interaction import InteractionLog

    # Step 1: fetch and load item catalog
    items_catalog = await fetch_items()
    await load_items(items_catalog, session)

    # Step 2: find latest imported interaction timestamp
    last_synced_result = await session.exec(select(func.max(InteractionLog.created_at)))
    last_synced_raw = last_synced_result.first()
    if last_synced_raw is None:
        since = None
    elif isinstance(last_synced_raw, datetime):
        since = last_synced_raw
    else:
        since = last_synced_raw[0]

    # Step 3: fetch and load logs incrementally
    logs = await fetch_logs(since=since)
    new_records = await load_logs(logs, items_catalog, session)

    total_result = await session.exec(select(func.count(InteractionLog.id)))
    total_raw = total_result.first()
    if total_raw is None:
        total_records = 0
    elif isinstance(total_raw, int):
        total_records = total_raw
    else:
        total_records = int(total_raw[0])

    return {"new_records": new_records, "total_records": total_records}
