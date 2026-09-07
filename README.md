# 🧱 Databricks Lakeflow Designer — End-to-End Data Engineering Pipeline

> **A real-world, no-code/low-code Spark data pipeline** built entirely inside **Databricks Lakeflow Designer** — from raw ingestion (tables + REST APIs) all the way to enriched, analytics-ready Delta tables, including AI-powered sentiment analysis.

This repo is a hands-on demonstration of how a modern **Lakehouse ETL pipeline** is designed visually, generates real PySpark code under the hood, and can be extended on demand using natural-language prompts — exactly the way a professional Data Engineer would build it in production.

---

## 🎯 What This Project Proves

By the end of this pipeline, the following skills are clearly demonstrated end-to-end:

| Skill Area | Demonstrated Through |
|---|---|
| **No-code/low-code Spark pipeline design** | Full DAG built visually in Lakeflow Designer, auto-compiled to PySpark |
| **Multi-source ingestion** | Delta tables + 3 independent REST API sources (CSV over HTTP) |
| **Custom Python transforms** | Parameterized ingestion nodes with input-fallback logic |
| **Data quality / messy data handling** | Fixing malformed CSV rows with embedded commas in a text field |
| **Data modeling** | Building a denormalized **One Big Table (OBT)** from 4 sources |
| **Aggregation & analytics logic** | `GROUP BY` + `SUM` / `COUNT` with custom SQL expressions |
| **Business-requirement translation** | A plain-English ask → auto-generated aggregation + sort logic |
| **Generative AI in the Lakehouse** | Native **AI Functions** (`ai_analyze_sentiment`) applied at scale, no ML pipeline needed |
| **Medallion architecture thinking** | Clean separation of `raw` sources → `enr` (enriched) output schema |

---

## 🗺️ Pipeline Architecture

The full DAG spans two logical zones — **ingestion & modeling** on the left, and **three parallel analytics branches** on the right.

### 1️⃣ Ingestion → Joins → One Big Table (OBT)

![Ingestion, Joins and OBT](./Snap2.png)

### 2️⃣ Analytics Branches: City Sales · Monthly Order Status · Review Sentiment

![Aggregations, Sort and AI Sentiment branches](./Snap1.png)

```mermaid
flowchart LR
    subgraph Sources
        A[orders\nDelta Table]
        B[order_items\nDelta Table]
        C[API_Customers\nPython + REST CSV]
        D[API_Shipments\nPython + REST CSV]
    end

    A --> J1[OrdersJoinOrdersItems\nLEFT JOIN on order_id]
    B --> J1
    J1 --> J2[OrderItemsJoinCustomers\nLEFT JOIN on customer_id]
    C --> J2
    J2 --> OBT[OBT\nLEFT JOIN on order_id]
    D --> OBT

    OBT --> AGG1[OrdersByCity\nCOUNT + SUM]
    AGG1 --> SORT1[Sorted\nDESC by TotalAmount]
    SORT1 --> OUT1[(AggregatedOrders\nworkspace.enr)]

    OBT --> PREP[ExtractOrderMonth\nMONTH(order_date)]
    PREP --> AGG2[CountStatusPerMonth\nGROUP BY month, status]
    AGG2 --> SORT2[SortByOrderMonthDesc]
    SORT2 --> OUT2[(OrderStatus\nworkspace.enr)]

    E[Reviews_API\nPython + REST CSV\n+ data cleansing] --> SENT[Sentiment\nai_analyze_sentiment]
    SENT --> OUT3[(Sentiment\nworkspace.enr)]
```

---

## 🔎 Walkthrough — Node by Node

### 🟦 Stage 1: Multi-Source Ingestion

The pipeline pulls data from **two different kinds of sources at once** — this is the first sign of real-world engineering thinking, since production pipelines rarely have all their data sitting neatly in one place.

| Node | Type | What it does |
|---|---|---|
| **`orders`** | Delta Table Source | Reads all data from `workspace.raw.orders` |
| **`order_items`** | Delta Table Source | Reads all data from `workspace.raw.order_items` |
| **`API_Customers`** | Custom Python | Pulls `customers.csv` live from a GitHub URL via `requests`, parses it with `pandas`, and converts it into a Spark DataFrame |
| **`API_Shipments`** | Custom Python | Same REST-to-Spark pattern, for `shipments.csv` |

**The engineering detail that stands out:** both API nodes are written with a **reusable fallback pattern**:

```python
if inputs.get("data"):
    result = inputs["data"][0]      # use upstream data if the node is re-wired
else:
    # otherwise, self-fetch from the source URL
    ...
    result = spark.createDataFrame(df)
```

This means each node works standalone **or** as a plug-in-place step in a larger graph — exactly how you'd design a transform meant to be reused across pipelines.

---

### 🧹 Stage 2: Real-World Data Cleansing — `Reviews_API`

This is the most advanced custom node in the project. The raw `reviews.csv` file has a broken structure: some `review_title` values contain **unescaped commas**, which silently shifts every column after it out of position — a classic messy real-world data problem.

Rather than letting Spark mis-parse the file, the node:
1. Reads the header to determine the **expected column count**.
2. Parses every row with Python's `csv` module.
3. Detects rows with **extra columns** (a sign the title field got split).
4. **Re-merges** the overflow fragments back into a single `review_title` value.
5. Silently drops truly broken rows (fewer columns than expected).
6. Loads the repaired data into a clean `pandas` → `Spark` DataFrame.

This is exactly the kind of defensive data-quality logic a data engineer needs when ingesting messy, human-generated CSV exports.

---

### 🔗 Stage 3: Building the One Big Table (OBT)

Three sequential **LEFT JOINs** stitch every source into a single denormalized table — the backbone for both downstream analytics branches:

```
orders  ⟕  order_items        → OrdersJoinOrdersItems   (on order_id)
        ⟕  API_Customers      → OrderItemsJoinCustomers (on customer_id)
        ⟕  API_Shipments      → OBT                      (on order_id)
```

Each join keeps **all rows from the left side** and enriches with matches from the right, and duplicate join keys (like the right-side `order_id`) are explicitly dropped from the output — a clean, intentional OBT design rather than a sloppy `SELECT *` join.

---

### 📊 Branch A — Sales Performance by City

| Step | Logic |
|---|---|
| **`OrdersByCity`** (Aggregate) | `GROUP BY city` → `COUNT(order_id) AS TotalOrders`, `ROUND(SUM(unit_price), 2) AS TotalAmount` |
| **`Sorted`** (Sort) | Orders the result set `DESC` by `TotalAmount` |
| **`AggregatedOrders`** (Output) | Overwrites `workspace.enr.AggregatedOrders` |

**Business answer produced:** *"Which cities generate the most revenue, ranked highest to lowest?"*

---

### 📅 Branch B — Monthly Order Status Trends *(built from a plain-English business ask)*

This branch is the standout proof point of the project — it shows the pipeline being **extended on demand from a natural-language requirement**, not just hand-built once:

> **Prompt given:** *"I want to create one task after OBT and find the count of each order status for each month. Sort the data in DESC by order date."*

Lakeflow Designer translated that requirement into three new nodes automatically:

| Step | Logic |
|---|---|
| **`ExtractOrderMonth`** (Prepare/Formula) | Derives `order_month = MONTH(order_date)` |
| **`CountStatusPerMonth`** (Aggregate) | `GROUP BY order_month, order_status` → `COUNT(order_id) AS StatusCount` |
| **`SortByOrderMonthDesc`** (Sort) | Orders the result `DESC` by `order_month` |
| **`OrdersStatus`** (Output) | Overwrites `workspace.enr.OrderStatus` |

**Business answer produced:** *"How many orders landed in each status (Delivered, Cancelled, Pending, etc.) each month — most recent month first?"*

This demonstrates a very practical, in-demand skill: **turning a stakeholder's plain-English ask into a working, production-shaped transformation chain.**

---

### 🤖 Branch C — Generative AI Sentiment Analysis on Customer Reviews

| Step | Logic |
|---|---|
| **`Reviews_API`** (Custom Python) | Loads and repairs the raw reviews CSV (see Stage 2 above) |
| **`Sentiment`** (AI Function) | Applies `ai_analyze_sentiment(review_body)` across every row, keeping all original columns |
| **`Sentiments`** (Output) | Overwrites `workspace.enr.Sentiment` |

No external ML model, no custom NLP code, no notebook training loop — this uses Databricks' **native, governed AI Functions** to run LLM-powered inference directly inside the Spark pipeline as a declarative step. This is a genuinely modern Lakehouse skill: **blending classic ETL with GenAI inference in the same governed pipeline.**

---

## 🏗️ Output Layer — Medallion-Style Enrichment Schema

All three branches converge on the same target schema, `workspace.enr`, keeping raw and enriched data cleanly separated:

| Output Table | Produced By | Grain |
|---|---|---|
| `workspace.enr.AggregatedOrders` | Branch A | One row per city |
| `workspace.enr.OrderStatus` | Branch B | One row per (month, order status) |
| `workspace.enr.Sentiment` | Branch C | One row per review, enriched with sentiment |

Every output node uses `write_mode: overwrite`, making the whole pipeline **idempotent and safely re-runnable** — a small but important production-readiness detail.

---

## 🧠 Why This Project Matters

This isn't a toy tutorial — it's a compact but complete simulation of a real Lakehouse engineering workflow:

- ✅ Ingest from **heterogeneous sources** (tables + live REST APIs)
- ✅ Write **defensive, reusable ingestion code**
- ✅ **Clean genuinely messy data** instead of assuming it's tidy
- ✅ Model a proper **OBT** through deliberate, key-based joins
- ✅ Deliver **two distinct business-facing aggregates**
- ✅ **Respond to a stakeholder requirement in natural language** and extend the pipeline correctly
- ✅ Layer in **GenAI-powered enrichment** using governed platform primitives
- ✅ Land everything into a clean, **query-ready enriched schema**

Together, these steps reflect a solid, practical grasp of **modern Databricks Lakehouse engineering** — from raw ingestion to AI-enriched, analytics-ready data — using Lakeflow Designer's visual, PySpark-generating canvas.

---

## 🖼️ Reference Snapshots

| Snapshot | Shows |
|---|---|
| `assets/pipeline-part2.png` | Sources (`orders`, `order_items`) → API ingestion (`API_Customers`, `API_Shipments`) → sequential joins → OBT → start of the aggregation branch |
| `assets/pipeline-part1.png` | The three parallel output branches: City aggregation & sort, Monthly order-status aggregation & sort, and the Reviews → AI Sentiment → output chain |

---

*Built with 🧱 Databricks Lakeflow Designer — visual, no-code Spark pipeline authoring with auto-generated PySpark under the hood.*
