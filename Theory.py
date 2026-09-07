# Databricks notebook source
# MAGIC %md
# MAGIC # 🧱 Databricks Lakeflow Designer — End-to-End Data Engineering Pipeline
# MAGIC
# MAGIC > **A real-world, no-code/low-code Spark data pipeline** built entirely inside **Databricks Lakeflow Designer** — from raw ingestion (tables + REST APIs) all the way to enriched, analytics-ready Delta tables, including AI-powered sentiment analysis.
# MAGIC
# MAGIC This repo is a hands-on demonstration of how a modern **Lakehouse ETL pipeline** is designed visually, generates real PySpark code under the hood, and can be extended on demand using natural-language prompts — exactly the way a professional Data Engineer would build it in production.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 What This Project Proves
# MAGIC
# MAGIC By the end of this pipeline, the following skills are clearly demonstrated end-to-end:
# MAGIC
# MAGIC | Skill Area | Demonstrated Through |
# MAGIC |---|---|
# MAGIC | **No-code/low-code Spark pipeline design** | Full DAG built visually in Lakeflow Designer, auto-compiled to PySpark |
# MAGIC | **Multi-source ingestion** | Delta tables + 3 independent REST API sources (CSV over HTTP) |
# MAGIC | **Custom Python transforms** | Parameterized ingestion nodes with input-fallback logic |
# MAGIC | **Data quality / messy data handling** | Fixing malformed CSV rows with embedded commas in a text field |
# MAGIC | **Data modeling** | Building a denormalized **One Big Table (OBT)** from 4 sources |
# MAGIC | **Aggregation & analytics logic** | `GROUP BY` + `SUM` / `COUNT` with custom SQL expressions |
# MAGIC | **Business-requirement translation** | A plain-English ask → auto-generated aggregation + sort logic |
# MAGIC | **Generative AI in the Lakehouse** | Native **AI Functions** (`ai_analyze_sentiment`) applied at scale, no ML pipeline needed |
# MAGIC | **Medallion architecture thinking** | Clean separation of `raw` sources → `enr` (enriched) output schema |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🗺️ Pipeline Architecture
# MAGIC
# MAGIC The full DAG spans two logical zones — **ingestion & modeling** on the left, and **three parallel analytics branches** on the right.
# MAGIC
# MAGIC ### 1️⃣ Ingestion → Joins → One Big Table (OBT)
# MAGIC
# MAGIC ![Ingestion, Joins and OBT](./assets/pipeline-part2.png)
# MAGIC
# MAGIC ### 2️⃣ Analytics Branches: City Sales · Monthly Order Status · Review Sentiment
# MAGIC
# MAGIC ![Aggregations, Sort and AI Sentiment branches](./assets/pipeline-part1.png)
# MAGIC
# MAGIC ```mermaid
# MAGIC flowchart LR
# MAGIC     subgraph Sources
# MAGIC         A[orders\nDelta Table]
# MAGIC         B[order_items\nDelta Table]
# MAGIC         C[API_Customers\nPython + REST CSV]
# MAGIC         D[API_Shipments\nPython + REST CSV]
# MAGIC     end
# MAGIC
# MAGIC     A --> J1[OrdersJoinOrdersItems\nLEFT JOIN on order_id]
# MAGIC     B --> J1
# MAGIC     J1 --> J2[OrderItemsJoinCustomers\nLEFT JOIN on customer_id]
# MAGIC     C --> J2
# MAGIC     J2 --> OBT[OBT\nLEFT JOIN on order_id]
# MAGIC     D --> OBT
# MAGIC
# MAGIC     OBT --> AGG1[OrdersByCity\nCOUNT + SUM]
# MAGIC     AGG1 --> SORT1[Sorted\nDESC by TotalAmount]
# MAGIC     SORT1 --> OUT1[(AggregatedOrders\nworkspace.enr)]
# MAGIC
# MAGIC     OBT --> PREP[ExtractOrderMonth\nMONTH(order_date)]
# MAGIC     PREP --> AGG2[CountStatusPerMonth\nGROUP BY month, status]
# MAGIC     AGG2 --> SORT2[SortByOrderMonthDesc]
# MAGIC     SORT2 --> OUT2[(OrderStatus\nworkspace.enr)]
# MAGIC
# MAGIC     E[Reviews_API\nPython + REST CSV\n+ data cleansing] --> SENT[Sentiment\nai_analyze_sentiment]
# MAGIC     SENT --> OUT3[(Sentiment\nworkspace.enr)]
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🔎 Walkthrough — Node by Node
# MAGIC
# MAGIC ### 🟦 Stage 1: Multi-Source Ingestion
# MAGIC
# MAGIC The pipeline pulls data from **two different kinds of sources at once** — this is the first sign of real-world engineering thinking, since production pipelines rarely have all their data sitting neatly in one place.
# MAGIC
# MAGIC | Node | Type | What it does |
# MAGIC |---|---|---|
# MAGIC | **`orders`** | Delta Table Source | Reads all data from `workspace.raw.orders` |
# MAGIC | **`order_items`** | Delta Table Source | Reads all data from `workspace.raw.order_items` |
# MAGIC | **`API_Customers`** | Custom Python | Pulls `customers.csv` live from a GitHub URL via `requests`, parses it with `pandas`, and converts it into a Spark DataFrame |
# MAGIC | **`API_Shipments`** | Custom Python | Same REST-to-Spark pattern, for `shipments.csv` |
# MAGIC
# MAGIC **The engineering detail that stands out:** both API nodes are written with a **reusable fallback pattern**:
# MAGIC
# MAGIC ```python
# MAGIC if inputs.get("data"):
# MAGIC     result = inputs["data"][0]      # use upstream data if the node is re-wired
# MAGIC else:
# MAGIC     # otherwise, self-fetch from the source URL
# MAGIC     ...
# MAGIC     result = spark.createDataFrame(df)
# MAGIC ```
# MAGIC
# MAGIC This means each node works standalone **or** as a plug-in-place step in a larger graph — exactly how you'd design a transform meant to be reused across pipelines.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🧹 Stage 2: Real-World Data Cleansing — `Reviews_API`
# MAGIC
# MAGIC This is the most advanced custom node in the project. The raw `reviews.csv` file has a broken structure: some `review_title` values contain **unescaped commas**, which silently shifts every column after it out of position — a classic messy real-world data problem.
# MAGIC
# MAGIC Rather than letting Spark mis-parse the file, the node:
# MAGIC 1. Reads the header to determine the **expected column count**.
# MAGIC 2. Parses every row with Python's `csv` module.
# MAGIC 3. Detects rows with **extra columns** (a sign the title field got split).
# MAGIC 4. **Re-merges** the overflow fragments back into a single `review_title` value.
# MAGIC 5. Silently drops truly broken rows (fewer columns than expected).
# MAGIC 6. Loads the repaired data into a clean `pandas` → `Spark` DataFrame.
# MAGIC
# MAGIC This is exactly the kind of defensive data-quality logic a data engineer needs when ingesting messy, human-generated CSV exports.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔗 Stage 3: Building the One Big Table (OBT)
# MAGIC
# MAGIC Three sequential **LEFT JOINs** stitch every source into a single denormalized table — the backbone for both downstream analytics branches:
# MAGIC
# MAGIC ```
# MAGIC orders  ⟕  order_items        → OrdersJoinOrdersItems   (on order_id)
# MAGIC         ⟕  API_Customers      → OrderItemsJoinCustomers (on customer_id)
# MAGIC         ⟕  API_Shipments      → OBT                      (on order_id)
# MAGIC ```
# MAGIC
# MAGIC Each join keeps **all rows from the left side** and enriches with matches from the right, and duplicate join keys (like the right-side `order_id`) are explicitly dropped from the output — a clean, intentional OBT design rather than a sloppy `SELECT *` join.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📊 Branch A — Sales Performance by City
# MAGIC
# MAGIC | Step | Logic |
# MAGIC |---|---|
# MAGIC | **`OrdersByCity`** (Aggregate) | `GROUP BY city` → `COUNT(order_id) AS TotalOrders`, `ROUND(SUM(unit_price), 2) AS TotalAmount` |
# MAGIC | **`Sorted`** (Sort) | Orders the result set `DESC` by `TotalAmount` |
# MAGIC | **`AggregatedOrders`** (Output) | Overwrites `workspace.enr.AggregatedOrders` |
# MAGIC
# MAGIC **Business answer produced:** *"Which cities generate the most revenue, ranked highest to lowest?"*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 📅 Branch B — Monthly Order Status Trends *(built from a plain-English business ask)*
# MAGIC
# MAGIC This branch is the standout proof point of the project — it shows the pipeline being **extended on demand from a natural-language requirement**, not just hand-built once:
# MAGIC
# MAGIC > **Prompt given:** *"I want to create one task after OBT and find the count of each order status for each month. Sort the data in DESC by order date."*
# MAGIC
# MAGIC Lakeflow Designer translated that requirement into three new nodes automatically:
# MAGIC
# MAGIC | Step | Logic |
# MAGIC |---|---|
# MAGIC | **`ExtractOrderMonth`** (Prepare/Formula) | Derives `order_month = MONTH(order_date)` |
# MAGIC | **`CountStatusPerMonth`** (Aggregate) | `GROUP BY order_month, order_status` → `COUNT(order_id) AS StatusCount` |
# MAGIC | **`SortByOrderMonthDesc`** (Sort) | Orders the result `DESC` by `order_month` |
# MAGIC | **`OrdersStatus`** (Output) | Overwrites `workspace.enr.OrderStatus` |
# MAGIC
# MAGIC **Business answer produced:** *"How many orders landed in each status (Delivered, Cancelled, Pending, etc.) each month — most recent month first?"*
# MAGIC
# MAGIC This demonstrates a very practical, in-demand skill: **turning a stakeholder's plain-English ask into a working, production-shaped transformation chain.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🤖 Branch C — Generative AI Sentiment Analysis on Customer Reviews
# MAGIC
# MAGIC | Step | Logic |
# MAGIC |---|---|
# MAGIC | **`Reviews_API`** (Custom Python) | Loads and repairs the raw reviews CSV (see Stage 2 above) |
# MAGIC | **`Sentiment`** (AI Function) | Applies `ai_analyze_sentiment(review_body)` across every row, keeping all original columns |
# MAGIC | **`Sentiments`** (Output) | Overwrites `workspace.enr.Sentiment` |
# MAGIC
# MAGIC No external ML model, no custom NLP code, no notebook training loop — this uses Databricks' **native, governed AI Functions** to run LLM-powered inference directly inside the Spark pipeline as a declarative step. This is a genuinely modern Lakehouse skill: **blending classic ETL with GenAI inference in the same governed pipeline.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏗️ Output Layer — Medallion-Style Enrichment Schema
# MAGIC
# MAGIC All three branches converge on the same target schema, `workspace.enr`, keeping raw and enriched data cleanly separated:
# MAGIC
# MAGIC | Output Table | Produced By | Grain |
# MAGIC |---|---|---|
# MAGIC | `workspace.enr.AggregatedOrders` | Branch A | One row per city |
# MAGIC | `workspace.enr.OrderStatus` | Branch B | One row per (month, order status) |
# MAGIC | `workspace.enr.Sentiment` | Branch C | One row per review, enriched with sentiment |
# MAGIC
# MAGIC Every output node uses `write_mode: overwrite`, making the whole pipeline **idempotent and safely re-runnable** — a small but important production-readiness detail.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧠 Why This Project Matters
# MAGIC
# MAGIC This isn't a toy tutorial — it's a compact but complete simulation of a real Lakehouse engineering workflow:
# MAGIC
# MAGIC - ✅ Ingest from **heterogeneous sources** (tables + live REST APIs)
# MAGIC - ✅ Write **defensive, reusable ingestion code**
# MAGIC - ✅ **Clean genuinely messy data** instead of assuming it's tidy
# MAGIC - ✅ Model a proper **OBT** through deliberate, key-based joins
# MAGIC - ✅ Deliver **two distinct business-facing aggregates**
# MAGIC - ✅ **Respond to a stakeholder requirement in natural language** and extend the pipeline correctly
# MAGIC - ✅ Layer in **GenAI-powered enrichment** using governed platform primitives
# MAGIC - ✅ Land everything into a clean, **query-ready enriched schema**
# MAGIC
# MAGIC Together, these steps reflect a solid, practical grasp of **modern Databricks Lakehouse engineering** — from raw ingestion to AI-enriched, analytics-ready data — using Lakeflow Designer's visual, PySpark-generating canvas.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🖼️ Reference Snapshots
# MAGIC
# MAGIC | Snapshot | Shows |
# MAGIC |---|---|
# MAGIC | `assets/pipeline-part2.png` | Sources (`orders`, `order_items`) → API ingestion (`API_Customers`, `API_Shipments`) → sequential joins → OBT → start of the aggregation branch |
# MAGIC | `assets/pipeline-part1.png` | The three parallel output branches: City aggregation & sort, Monthly order-status aggregation & sort, and the Reviews → AI Sentiment → output chain |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC *Built with 🧱 Databricks Lakeflow Designer — visual, no-code Spark pipeline authoring with auto-generated PySpark under the hood.*