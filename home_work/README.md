# Airflow Practice: Three Real-World Pipelines

Three exercises, each a little harder than the last. In each one you build a complete DAG, end to end, on "dirty" data like the data you get at work.

| # | Topic | Level | What you practice |
|---|-------|-------|-------------------|
| 1 | Support tickets (ETL) | Basic | `PythonOperator`, XCom, Variables, data cleaning |
| 2 | Weather sensors, 3 cities | Intermediate | Parallel tasks, data-quality checks, failures and `upstream_failed`, `trigger_rule` |
| 3 | Shipments, "production-style" pipeline | Challenging | Retries, quality gate, reject file, idempotency, notify and cleanup that always run |

---

## Setup (once)

1. In the project root (next to `docker-compose.yaml`) run:
   ```bash
   python3 home_work/generate_exercise_data.py
   ```
   This creates the following files in `source_data/`:
   `support_tickets.csv`, `sensors_tel_aviv.csv`, `sensors_haifa.csv`, `sensors_jerusalem.csv`, `shipments.csv`
2. Inside the containers this folder appears as `/tmp/source_data`, and the output folder is `/tmp/target_data` (mounted from `target_data/` on your machine).
3. Write each DAG as a `.py` file in `airflow_data/dags/`. It shows up in the UI within about 30 seconds.
4. Create Variables in the UI: **Admin → Variables → +**.

### Rules for all exercises

- Use `with DAG(...)` and `PythonOperator`.
- **Pass file paths between tasks through XCom, not tables.** This is how it is done in real projects. XCom is for small values.
- Every Variable is **optional**: use `Variable.get("name", default_var=...)` so the DAG does not break when the Variable does not exist.
- Use `print` so the logs tell the story (how many rows were read, how many were removed, and so on).
- Use `catchup=False` and `schedule_interval="@daily"`.

---

## Exercise 1: Support tickets (basic)

### The story
The support team of a SaaS company exports a ticket file every day. The support manager wants a summary table per category every morning. The file comes out of the system in a messy state.

### The data: `support_tickets.csv`
| Column | Notes |
|---|---|
| `ticket_id` | Ticket identifier. **There are duplicates** (a bug in the ticketing system) |
| `created_at`, `customer_id` | |
| `category` | billing / technical / account / shipping, **in mixed case and with spaces** (`" Billing "`, `"TECHNICAL"`) |
| `priority` | low / medium / high / urgent, **sometimes empty**, also in mixed case |
| `status` | open / closed / pending, in mixed case |
| `resolution_hours` | Hours until resolved. Empty for tickets that are not closed |

### What to build
A DAG named `ex1_support_tickets_etl` with three tasks in a chain: `extract >> transform >> load`.

1. **extract**: reads `/tmp/source_data/support_tickets.csv`, saves a raw copy to `<output_dir>/staging/tickets_raw.csv`, and returns the path.
2. **transform**: gets the path from XCom and cleans the data:
   - Strips spaces and lower-cases the columns `category`, `priority`, `status`.
   - Removes duplicate tickets by `ticket_id` (keep the first).
   - Fills a missing `priority` with the value of the Variable `default_priority`.
   - Adds a column `is_high_priority` (true when `priority` is `high` or `urgent`).
   - Saves `tickets_clean.csv` and returns the path.
3. **load**: builds a summary per category with the number of tickets, the number of high-priority tickets and the average resolution hours (`ticket_summary.csv` in `<output_dir>`).

Settings: `retries=2`, `retry_delay` = 1 minute.

### Variables
| Name | Default | Used for |
|---|---|---|
| `output_dir` | `/tmp/target_data` | Output folder |
| `default_priority` | `medium` | Priority for tickets that have none |

### How to know you succeeded
- The `transform` log shows **123 rows before, 120 after**, and **8 missing priorities** filled.
- `ticket_summary.csv` has **4 rows** (4 clean categories) and the ticket counts add up to **120**.
- Change `default_priority` to `low`, run again, and check that the log changes without touching the code.

### Hints
- `df[col].astype("string").str.strip().str.lower()`
- `df.drop_duplicates(subset="ticket_id")`
- `df.groupby("category").agg(...)`
- To read from XCom: `ti.xcom_pull(task_ids="extract")`. The function needs a parameter named `ti`.

---

## Exercise 2: Weather sensors in 3 cities (intermediate)

### The story
An energy company receives sensor files from three cities. When a sensor breaks, its data is full of holes. Sending customers a summary based on a faulty sensor is not acceptable, so each city needs a **quality gate** before the data is merged.

### The data: `sensors_tel_aviv.csv`, `sensors_haifa.csv`, `sensors_jerusalem.csv`
Columns: `reading_time` (one reading per hour, 3 days), `temperature_c`, `humidity_pct`.

- Missing values appear **as an empty field** and also as **`-999`** (the device writes it when a measurement failed). `-999` is not a real temperature!
- One sensor is more faulty than the others.

### What to build
A DAG named `ex2_sensor_quality_pipeline`:

```
start ─┬─ extract_tel_aviv ── quality_check_tel_aviv ─┐
       ├─ extract_haifa ───── quality_check_haifa ────┼─ merge_and_load ── finish
       └─ extract_jerusalem ─ quality_check_jerusalem ┘
```

1. **extract_`<city>`** (three cities in parallel): reads the file, replaces `-999` with `NaN`, calculates the **percentage of missing values** in the two measurement columns, saves a clean file to staging, and returns a small dictionary: `{"city", "path", "rows", "null_pct"}`.
2. **quality_check_`<city>`**: if `null_pct` is greater than the Variable `max_null_pct`, raise a `ValueError` with a clear message. Otherwise print "passed".
3. **merge_and_load**: merges the three cities and calculates, per city and per day: number of readings, average / minimum / maximum temperature and average humidity. Writes `daily_weather_summary.csv`.
4. **finish**: prints a message, and **must run even if something failed** (`trigger_rule`).

Settings: `retries=1`, `retry_delay` = 30 seconds.

### Variables
| Name | Default | Used for |
|---|---|---|
| `max_null_pct` | `25` | Maximum allowed percentage of missing values |
| `output_dir` | `/tmp/target_data` | Output folder |

### How to know you succeeded
- Missing-value percentages in the log: **tel_aviv 4.2%**, **haifa 7.6%**, **jerusalem 18.8%**.
- The summary file has **9 rows** (3 cities × 3 days), each with **24 readings**.
- **The important experiment:** set `max_null_pct` to **10** and run.
  - What should happen? Check the Grid: which task is `failed`, which is `upstream_failed`, and which is still `success`?
  - Why does `finish` run anyway?
  - Does the whole DAG Run look green or red? Why?

### Hints
- `df[["temperature_c","humidity_pct"]].replace(-999, np.nan)`
- Missing percentage: `df[cols].isna().sum().sum() / df[cols].size * 100`. Convert it to a plain `float` before returning it to XCom.
- Three cities means the same code three times. Use a `for city in [...]` loop that creates the tasks, and pass `op_kwargs={"city": city}`.
- To pull from several tasks at once: `ti.xcom_pull(task_ids=["extract_haifa", "extract_tel_aviv", ...])`
- `from airflow.utils.trigger_rule import TriggerRule`, then `TriggerRule.ALL_DONE`

---

## Exercise 3: Shipments, a production-style pipeline (challenging)

### The story
A logistics company wants a daily report: how long does each carrier take to deliver, and what percentage of shipments are late? The file comes from an old system and is full of errors. The team has three requirements:
**(1)** Bad rows are not silently deleted. They are saved to a separate file for audit.
**(2)** If too many rows are bad, the pipeline stops and does not publish a wrong report.
**(3)** Running it again for the same day must not create duplicates.

### The data: `shipments.csv`
| Column | Notes |
|---|---|
| `shipment_id` | **Sometimes empty** |
| `shipped_date`, `delivered_date` | `delivered_date` is empty for a shipment that is still on its way (that is valid!). **Some rows have a delivery date before the shipping date** |
| `carrier` | DHL / FedEx / Israel Post / UPS, with inconsistent spelling |
| `destination_city` | |
| `weight_kg` | **Sometimes `N/A`, sometimes negative** |

### What to build
A DAG named `ex3_shipments_production_pipeline`, in a single chain:

```
check_source_file >> extract_and_validate >> quality_gate >> transform
   >> load_report >> save_run_metadata >> notify >> cleanup
```

| Task | What it does |
|---|---|
| `check_source_file` | Fails immediately if the file does not exist or is empty |
| `extract_and_validate` | Splits **valid rows** from **rejected rows**, with a reason for each rejected one: `missing_shipment_id` / `invalid_weight` / `invalid_dates`. Saves `valid_<ds>.csv` and `rejected_<ds>.csv`. Returns statistics (total, valid, rejected, reject percentage, paths) |
| `quality_gate` | Fails if the reject percentage is greater than `max_reject_pct` |
| `transform` | Per carrier: number of delivered shipments, average delivery days, number and percentage of **late** shipments (more than `sla_days`). A shipment still on its way is not counted. Saves `carrier_report_<ds>.csv` |
| `load_report` | "Loads" the report to the final destination `<output_dir>/carrier_report_<ds>.csv`. **Fails on purpose on the first attempt** (simulating an unreliable system) and succeeds on the retry |
| `save_run_metadata` | Saves the run date in a Variable named `shipments_last_success_date` |
| `notify` | Prints whether the pipeline finished successfully or with problems (and which tasks failed). **Always runs** |
| `cleanup` | Deletes the temporary `valid` file. **Always runs** |

Settings: `retries=2`, `retry_delay` = 20 seconds.

> `ds` is the run date in the format `YYYY-MM-DD`. You get it as a parameter named `ds` in your function: `def extract_and_validate(ds): ...`

### Variables
| Name | Default | Used for |
|---|---|---|
| `max_reject_pct` | `10` | Quality gate threshold |
| `sla_days` | `5` | A delivery slower than this many days counts as late |
| `simulate_flaky_load` | `true` | Whether the load fails on the first attempt |
| `output_dir` | `/tmp/target_data` | Output folder |
| `shipments_last_success_date` | (written by the DAG) | |

### How to know you succeeded
- **Validation:** 150 rows, **12 rejected (8.0%)**: 7 `invalid_weight`, 3 `missing_shipment_id`, 2 `invalid_dates`.
- **Report:** 4 carriers, **13 shipments still on their way** that are not counted, and the shipments in the report add up to **125**.
- **Retry:** the log of `load_report` has two attempt files: `attempt=1` failed, `attempt=2` succeeded.
- **Variable:** `shipments_last_success_date` holds the run date.

### Three experiments (required)
1. **Quality gate:** set `max_reject_pct` to **5** and run. Which tasks failed, which got `upstream_failed`, and which ran anyway? What does the `notify` log say? Why is the whole DAG Run green?
2. **Retry:** set `simulate_flaky_load` to `false` and run. What changed in the logs?
3. **Idempotency:** run the same day again (**Clear** the Run). Was a second report file created, or was the existing file overwritten? Why does this matter?

### Hints
- Parse dates and numbers without crashing: `pd.to_datetime(col, errors="coerce")`, `pd.to_numeric(col, errors="coerce")`. An invalid value becomes `NaN` / `NaT`.
- Several conditions, one reason for each: `np.select([cond1, cond2, cond3], ["reason1", "reason2", "reason3"], default="")`
- Delivery days: `(delivered - shipped).dt.days`
- To know which attempt is running inside a task: `ti.try_number` (it is 1 on the first attempt)
- "Atomic" writing: write to a temporary file `...tmp`, then `os.replace(tmp, final)`. This way nobody reads a half-written file.
- The list of failures in `notify`: add a parameter `dag_run` to the function, then use `dag_run.get_task_instances()` and check `t.state`.
- `Variable.set("name", value)` to save a value.

---

## Bonus (for those who finish early)

- In Exercise 3, replace `raise ValueError` with `AirflowFailException` in the quality gate. What is the difference in behavior with `retries`?
- In Exercise 2, add a fourth city by changing a single line.
- In Exercise 1, write the same pipeline with TaskFlow (`@dag`, `@task`).

## Checklist before you hand in

- [ ] All DAGs appear in the UI with no import errors
- [ ] Every task prints something useful to the log
- [ ] No Variable is required (there is a `default_var`)
- [ ] No DataFrame is passed through XCom, only paths and small dictionaries
- [ ] You ran all the experiments and can explain what you saw in the Grid
