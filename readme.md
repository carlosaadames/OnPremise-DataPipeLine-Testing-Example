# Usage:
This proof of concept focuses on usage of airflow v.3.x on premise to orchestrate a pyspark data pipeline. Before running, populate your settings in `dags\config.py`. The intent is for on premise testing before moving to a production environment.

## CLI Execution:
```bash
python main.py --ticker NVDA --period max --interval 1d
```
---
## Changes for production:
To elevate your local PySpark and Airflow setup into a portfolio piece that mirrors a production pipeline, you can seamlessly map your current logic to any cloud platform. In 2026, Google Cloud's Always Free tier provides enough resources to build this exact architecture at zero cost. This demonstrations will be in another repository, `GCP-DataPipeLine`.

## I. The 0-Cost GCP Engine (Replacing Local Infrastructure)

Current PoC relies on local directories and a SQLite database. To make this production-ready, you will shift these storage and compute components to managed cloud services.

* **Storage (The Data Lake):** Replace local `data_lake/` directories with **Google Cloud Storage (GCS)**. The free tier provides 5 GB-months of Standard Storage, which is perfect for isolating the Bronze, Silver, and Gold Delta tables.


* **Serving Layer (The Mirror):** Swap the local `pipeline.db` SQLite mirror for **Google BigQuery**. BigQuery acts as an enterprise data warehouse and offers 10 GiB of storage and 1 TiB of querying per month at no cost.


* **Compute & Orchestration:** Deploy your `orchestrator.py` and `pipeline.py` onto a **Compute Engine `e2-micro` VM**. You get one free instance per month, which is sufficient to run a lightweight Airflow scheduler and execute PySpark in local mode for small-scale PoC data.


* **Configuration Management:** Instead of loading secrets from a local `.env` file, migrate your configuration to **Secret Manager**, which allows 6 free secret versions per month.

--- 
## Airflow 3 Concepts — Chapter Reference

The orchestration patterns in this repo (`config.py`, `pipeline.py`, `orchestrator.py`, `main.py`) line up with *Data Pipelines with Apache Airflow, Second Edition* (Manning) — fully revised for Airflow 3. Use this as a jumping-off point if you want to go deeper on any pattern used here.

### Concepts already in use

| Chapter | Topic | Where it lives in this repo |
|---|---|---|
| 2 | Anatomy of an Airflow DAG | `orchestrator.py` → `spark_orchestrator()` |
| 6 | Defining dependencies between tasks | `orchestrator.py` → TaskFlow return-value chaining |
| 10 | Testing | `main.py` → `airflow dags test ...` |
| 12 | Best practices | `pipeline.py` + `config.py` |

#### Chapter 2 — Anatomy of an Airflow DAG
`spark_orchestrator()`, decorated with `@dag`, is the entry point the DAG processor discovers and parses. Task order (`extract → silver → gold`) isn't wired up with `>>`; it falls out of the dependency graph Airflow builds from the TaskFlow calls below.

#### Chapter 6 — Defining dependencies between tasks (TaskFlow API)
Each task (`extract_task`, `transform_silver_task`, `transform_gold_task`) is a plain Python function wrapped in `@task`, not a manually built Operator. Dependencies are inferred by passing one task's return value into the next — `transform_silver_task(bronze)`, `transform_gold_task(silver)` — instead of explicit XCom pulls or `.set_downstream()`.

#### Chapter 10 — Testing
`main.py` shells out to `airflow dags test spark_pipeline_orchestrator <date> --conf '<json>'`. This is the CLI equivalent of this chapter's `dag.test()` walkthrough: it runs every task in-process against the local metadata DB, no scheduler or webserver required. With `schedule=None` and `catchup=False`, every run here is a manual, on-demand test — the right mode for local development on a laptop.

#### Chapter 12 — Best practices
Two practices from this chapter map directly to fixes made earlier in this project:

- **Avoid computation in your DAG definition.** `pipeline.DataPipeLine()` builds a full SparkSession, so it originally lived at module scope in `orchestrator.py` — meaning every DAG-folder scan paid for JVM/Spark startup, and the DAG never made it into `airflow dags list`. The fix moved `import pipeline` and `DataPipeLine()` instantiation inside each `@task` function, so parsing the file is cheap and Spark only spins up when a task actually runs.
- **Manage configuration centrally, and keep persistence logic in one place.** `config.py`'s `load_configuration()` is the single source for Spark/Delta tuning, `.env` secrets, and every data-lake path (bronze, silver, gold, and now the SQLite mirror). All the actual writing — Delta *and* the SQLite mirror — stays inside `pipeline.py`'s tier methods, so `orchestrator.py` never touches storage details directly.

### Concepts worth adopting next

These chapters describe patterns this project doesn't use yet, but are natural next steps as the pipeline matures:

- **Chapter 4 — Asset-aware scheduling.** Replace `schedule=None` with Airflow 3's asset-based scheduling so a fresh `yfinance` pull can automatically retrigger `bronze → silver → gold`, instead of waiting on a manual `dags test` or trigger.
- **Chapter 7 — Triggering workflows with external input.** Instead of a fixed `ticker` param, `extract_task` could be kicked off by an external event (a file landing, a webhook) — decoupling "new data exists" from "someone remembered to run the DAG."
- **Chapter 8 — Communicating with external systems.** The project already leans on this chapter's core idea — Airflow as control plane, Spark/Delta as the compute engine — so this is about reinforcing the pattern as more external systems get added, rather than adopting something new.

---
*Reference: Julian de Ruiter, Ismael Cabral, Kris Geusebroek, Daniel van der Ende, and Bas Harenslak — **Data Pipelines with Apache Airflow, Second Edition** (Manning). ISBN 9781633436374.*

## PySpark & Delta Lake Concepts — Chapter Reference

`pipeline.py` is almost entirely PySpark, so this section pairs with the Airflow mapping above: Airflow orchestrates, this is what it's orchestrating. Mapped against *Learning Apache Spark with Python* by Wenqiang Feng.

### Concepts already in use

| Chapter | Topic | Where it lives in this repo |
|---|---|---|
| 3 (§3.2, §3.7) | Configure Running Platform | Local Mac/no-Docker setup; `SparkJob._spark_builder()` |
| 4 (§4.2) | Spark Components | Driver / BlockManager configs in `_spark_builder()` |
| 5 (§5.3) | `rdd.DataFrame` vs `pd.DataFrame` | `_data_fetch`, `_mirror_to_sqlite`, `_gold_tier` window functions |

#### Chapter 3 — Configure Running Platform
§3.2 ("Configure Spark on Mac and Ubuntu") walks through exactly the local-machine setup this project assumes — Java and Python installed directly on the laptop, no cluster manager, no Docker. §3.7 introduces the `SparkSession.builder.appName(...).config(...).getOrCreate()` shape used throughout the book; `SparkJob._spark_builder()` is the same pattern with real Delta/network configs swapped in for the book's placeholder `"spark.some.config.option"`. `getOrCreate()` itself already returns an existing session instead of building a new one — the same idea `SparkJob.__new__`'s singleton enforces one level up, at the Python-object level, so calling `DataPipeLine()` again inside another task doesn't rebuild Spark within that process.

#### Chapter 4 — An Introduction to Apache Spark
§4.2 ("Spark Components") defines the Driver and the BlockManager — the exact two pieces `_spark_builder()` configures via `spark.driver.host`, `spark.driver.bindAddress`, `spark.driver.port`, and `spark.blockManager.port`. Running fully local means there's no cluster manager negotiating these automatically, so pinning them to `127.0.0.1` and fixed ports is what stops Spark from trying to bind addresses meant for a real cluster.

#### Chapter 5 — Programming with RDDs (§5.3 `rdd.DataFrame` vs `pd.DataFrame`)
- `_data_fetch`'s `self._spark.createDataFrame(finance_data)` is the pandas→Spark half of this section's side-by-side comparison; `_mirror_to_sqlite`'s `.toPandas()` runs the same conversion in reverse.
- `_gold_tier`'s `Window.orderBy("trade_date").rowsBetween(-6, 0)` / `.rowsBetween(-29, 0)`, combined with `avg`, `stddev`, `min`, `max`, is a direct application of §5.3.16 "Window" — same `Window.orderBy(...).over(...)` shape as the book's `rank()` example, with a rolling frame and aggregate functions in place of a ranking function.

**One gap worth flagging:** this edition's §26.4 ("delta format") and §25.3 ("JDBC write") are both literally marked `TODO...` in the source — unwritten. So the Delta Lake writes in `_bronze_tier` / `_silver_tier` / `_gold_tier`, and the new SQLite mirror, go beyond what this particular book documents. Delta Lake's own docs (`docs.delta.io`) and Python's `sqlite3` docs are the better reference for those specifics.

### Concepts worth adopting next

- **Chapter 17 — Monte Carlo Simulation, §17.2 "Simulating a Random Walk."** Works from the exact OHLCV shape `_data_fetch` already pulls (Date/Open/High/Low/Close/Volume), computes CAGR and annualized volatility from historical closes (`returns.std() * sqrt(252)`) — conceptually the same figure `volatility_30d` already produces — then uses both to simulate future price paths. A natural continuation once `gold_tier`'s features exist.

---
*Reference: Wenqiang Feng — **Learning Apache Spark with Python** (self-published tutorial/reference, December 2021 edition).*

---
