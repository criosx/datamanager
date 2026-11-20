# datamanager — Module Context for Broker Refactor

## Core Responsibility

`roadmap_datamanager` is the **data lifecycle layer** for the ROADMAP system. It is
responsible for:

1. Creating and maintaining a versioned, hierarchical directory tree of experimental
   data using **DataLad** (git-annex + git under the hood).
2. Attaching **JSON-LD / Schema.org metadata** to every file, folder, and dataset
   within that tree.
3. **Publishing** the tree (or subtrees) to a **GIN** (G-Node Infrastructure) remote
   for long-term archival and collaboration.

It does **not** control instruments, dispatch tasks, schedule work, or carry any
real-time responsibilities. It is entirely orthogonal to the AMQP message flow.

---

## Directory Hierarchy

```
{dm_root}/                           ← DataLad root dataset (keyed by user_name)
  {project}/                         ← DataLad subdataset
    {campaign}/                      ← DataLad subdataset
      {experiment}/                  ← DataLad subdataset
        autocontrol/                 ← gitignored — SQLite state, NOT versioned
        raw/                         ← versioned measurement data
        reduced/
        analysis/
        measurement/
        model/
        ...
```

Each level (`root`, `project`, `campaign`, `experiment`) is its own DataLad
subdataset. The `autocontrol/` folder is explicitly **gitignored** at the experiment
level — the three SQLite TaskContainers (queue / active / history) and
`channel_po.json` are never committed to git.

### Dataset Types (used in metadata keys)
- `root` — the user's top-level DataLad dataset
- `project`
- `campaign`
- `experiment`
- `below-experiment` — files/folders *inside* an experiment that are not a dataset

---

## Key Classes and Methods

### `DataManager` (`datamanager.py`)
The main entry point. All public methods are idempotent where possible.

| Method | Purpose |
|---|---|
| `init_tree(project, campaign, experiment)` | Create/verify the nested DataLad dataset tree |
| `install_into_tree(source, …, category)` | Copy/move a file or folder into `{experiment}/{category}/` and attach metadata |
| `save_meta(ds_path, path, extra)` | Attach JSON-LD metadata to a path and commit |
| `load_meta(ds_path, path)` | Read back stored metadata |
| `publish_gin_sibling(…)` | Create GIN sibling and push the tree |
| `publish_lazy_to_remote(…)` | Climb to the nearest ancestor with a GIN sibling and push recursively |
| `remove_from_tree(dataset, path)` | `datalad drop` content (after remote verification) |
| `clone_from_remote(dest, …)` | Clone a GIN tree locally |

### `Metadata` (`metadata.py`)
Reads/writes a `metadata.json` file at the dataset root. Records are keyed by POSIX
relative path. Each record is a JSON-LD envelope:

```json
{
  "type": "file | dataset",
  "extractor_name": "...",
  "extraction_time": "2026-05-14T...",
  "agent_name": "...",
  "dataset_id": "...",
  "dataset_version": "...",
  "extracted_metadata": {
    "@context": {"@vocab": "https://schema.org/"},
    "@type": "CreativeWork | Dataset | Collection",
    "@id": "datalad:{dataset_type}{dataset_id}:{relposix}",
    "identifier": "{relposix}",
    ...payload fields...
  }
}
```

### `configuration.py`
- `BaseConfig` dataclass — fields: `user_name`, `user_email`, `project`, `campaign`,
  `experiment`, `use_datalad`, `GIN_url/repo/user`, `dm_root`, `extractor_name`, etc.
- `DataManagerConfig` extends `BaseConfig` for the standalone datamanager package.
- `load_persistent_cfg()` / `save_persistent_cfg()` — read/write from `platformdirs`
  user config dir (env var override via `ROADMAP_DM_CONFIG`).

### `datalad_gin_api.py`
Low-level wrappers: `push_to_remotes`, `pull_from_remotes`, `save_branch`,
`get_git_sync_status`, `clone_from_remote`, `get_dataset_nodetype`, SSH utilities.

---

## Integration with Autocontrol (Current State)

Autocontrol subclasses `BaseConfig` into its own `DataManagerConfig`
(`autocontrol/support/configuration.py`) adding: `autocontrol_dir`, `atc_address`,
`autocontrol_startup`.

### Startup Flow (`autocontrol/streamlit/Main.py`)
1. Streamlit UI starts; sets `autocontrol_startup = True` (startup lock).
2. Checks `storage_path`:
   - Outside a DataLad dataset → user provides path manually.
   - At dataset level → error (must be *below* experiment).
   - Below-experiment → calls `bootstrap_config(path, cfg)` which walks the tree
     upward to recover `project`, `campaign`, `experiment`, `user_name`, `user_email`,
     and `dm_root` from stored metadata.
3. User confirms in `pages/01_File_System.py` → calls `start_server()` → sets
   `autocontrol_startup = False`, releasing the lock.
4. Only after this lock is released does the Flask (REST) server start.

The datamanager is **not consulted again** during task execution. The `autocontrol/`
folder inside the experiment tree holds all runtime state (SQLite) and is gitignored.

---

## What Is NOT Changed by the Broker Refactor

- The DataLad / git-annex internals — these are unchanged.
- The `metadata.json` format — no schema change needed.
- The startup flow in Streamlit — path selection and tree discovery are UI-level and
  pre-execution; they are unrelated to AMQP.
- The gitignore of `autocontrol/` — SQLite state remains gitignored.
- The GIN publishing logic — entirely offline/manual, not triggered by broker events.

---

## Broker-Relevant Constraints

1. **No real-time role** — datamanager operations (DataLad save, GIN push) are slow
   (seconds to minutes). They must never block the AMQP message loop.

2. **Large data output** — measurement results installed via `install_into_tree()` are
   exactly the kind of payload subject to the **Claim Check pattern**. The broker
   carries a `retrieval_uri` pointing to the installed path; the data itself stays on
   disk inside the experiment tree.

3. **No channel affinity concern** — datamanager has no concept of instrument channels.

4. **Thread/async safety** — `DataManager` and `datalad` calls are synchronous and
   blocking. Any broker-triggered datamanager call must run in a thread pool
   (`asyncio.get_event_loop().run_in_executor`) to avoid blocking the aio-pika event
   loop.
