# Problem Statement 2 — Cross-League Player Intelligence Platform

**Focus:** Elaborate design for Wyscout-based player analytics  
**Orchestration angle:** **Prefect** drives the Apache compute stack and **guarantees dashboard freshness**  
**Dataset:** Wyscout public soccer-logs (CC BY 4.0) — Pappalardo et al., *Nature Scientific Data* (2019)  
[doi:10.1038/s41597-019-0247-7](https://doi.org/10.1038/s41597-019-0247-7)  
(~1,941 matches · ~3.25M events · ~4,300 players · top-5 leagues 2017/18 + WC 2018 + Euro 2016)

---

## Broad problem

Player comparison across leagues is biased by role, minutes played, opponent quality, and tactical context. Form is also **non-stationary**: a midfielder’s progressive-pass rate can collapse after a role change or injury return.  

Build an end-to-end **player-intelligence platform** on the Wyscout open research release that:

1. Unifies competitions, teams, players, and event streams in a lakehouse  
2. Computes **role-aware, minutes-normalized** performance features at scale  
3. Produces **cross-league comparable rankings** and similarity neighbourhoods  
4. Detects **concept drift / form breaks** on rolling windows  
5. Exposes **everything a scout needs on a single live dashboard**, refreshed by a Prefect-orchestrated Apache pipeline  

Submissions must demonstrate the full path: **raw open JSON → Prefect flows → Apache processing → curated marts → dashboard panels**—not a static notebook export.

---

## Why Prefect in this stack

Prefect is the **control plane**; Apache tools remain the **data plane**.

| Concern | Prefect role | Apache / serving role |
|---|---|---|
| Schedule & retries | Flows, deployments, retries, SLAs | — |
| Dependency graph | Task graph: land → validate → Spark → publish → refresh | Spark / Flink / Kafka jobs invoked as tasks |
| Observability | Flow run UI, task logs, failure alerts | Job logs inside Spark/Flink |
| Dashboard freshness | Final tasks: mart build + **dataset/cache refresh** + health check | Hive/Iceberg marts → Superset (or equivalent) |
| Backfills | Parameterized flow runs per league / matchweek | Idempotent Spark writes to Iceberg |

**Design rule:** no dashboard panel may depend on a manual notebook step. If a metric is on the UI, a Prefect task must be able to regenerate it.

---

## Integrated architecture (Prefect + Apache → Dashboard)

```text
                    ┌─────────────────────────────────────┐
                    │     Prefect  (flows · deploys · UI) │
                    │  land → validate → batch → stream   │
                    │  → marts → dashboard refresh → QA   │
                    └───────────────┬─────────────────────┘
                                    │ triggers / monitors
          ┌─────────────────────────┼─────────────────────────┐
          ▼                         ▼                         ▼
 ┌─────────────────┐     ┌─────────────────┐       ┌─────────────────┐
 │ Wyscout open    │     │ Apache Kafka    │       │ Stream compute  │
 │ JSON (Figshare) │────►│ topics:         │──────►│ Apache Flink    │
 │ competitions ·  │land │  events.raw     │replay │ (rolling form · │
 │ matches ·       │     │  events.curated │       │  drift alarms)  │
 │ players · events│     └────────┬────────┘       └────────┬────────┘
 └────────┬────────┘              │                         │
          │                       ▼                         │
          │              ┌─────────────────┐                │
          └─────────────►│ Lakehouse       │◄───────────────┘
                         │ HDFS + Parquet  │
                         │ / Iceberg/Hive  │
                         │ bronze·silver·  │
                         │ gold            │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐
                         │ Apache Spark    │
                         │ ETL · features  │
                         │ Spark MLlib     │
                         │ rankings / kNN  │
                         └────────┬────────┘
                                  │
                                  ▼
                         ┌─────────────────┐         ┌──────────────────────┐
                         │ Gold marts      │────────►│ Scout Dashboard      │
                         │ player_match    │  JDBC / │ Apache Superset      │
                         │ player_season   │  SQL    │ (or Streamlit+API)   │
                         │ rankings        │         │                      │
                         │ similarity      │         │ panels below MUST    │
                         │ drift_events    │         │ all be wired         │
                         │ data_quality    │         │                      │
                         └─────────────────┘         └──────────────────────┘
```

---

## Elaborate pipeline stages

### 1. Land & validate (Prefect flow: `wyscout_land`)
- Download / sync public Wyscout JSON (competitions, matches, teams, players, events)
- Write **bronze** objects (raw JSON or raw Parquet) to HDFS/object store
- Validation tasks: schema presence, referential integrity (player ∈ match lineup), event timestamp monotonicity per match
- Fail the flow (and block dashboard refresh) if critical DQ gates fail; write rows to `data_quality` mart

### 2. Curate events (Prefect flow: `wyscout_curate` → Spark)
- Flatten nested events → **silver** typed tables: passes, shots, duels, fouls, substitutions, etc.
- Join player, team, competition, and match metadata
- Publish curated batches to Kafka `events.curated` for streaming consumers and match replay demos

### 3. Feature & ranking batch (Prefect flow: `player_features` → Spark + MLlib)
- Build **player–match** and **player–season** feature tables (per-90, role tags, progressive actions, duel success, shot involvement, defensive volume, sequence participation)
- Position-normalize and shrink noisy low-minute samples
- Fit ranking / similarity models (e.g. regularized score + kNN / ALS-style neighbourhoods)
- Write **gold** marts: `player_match_features`, `player_season_rankings`, `player_similarity`

### 4. Streaming form & drift (Prefect flow: `form_stream` → Kafka + Flink)
- Chronologically replay (or incrementally append) curated events on Kafka
- Flink keyed state per player: rolling windows (e.g. last 5 matches) for form metrics
- Emit **drift_events** when distribution shift / Page-Hinkley / ADWIN-style detectors fire
- Persist alarms back to Iceberg/Hive so the dashboard can show “form break” timelines

### 5. Mart publish & dashboard refresh (Prefect flow: `dashboard_publish`) — critical
This stage is what makes “everything on the dashboard” reliable:

1. Build/refresh gold SQL views expected by the BI tool  
2. Run row-count & null-rate checks vs previous successful run  
3. **Trigger dashboard dataset refresh** (Superset `/api/v1/dataset/refresh` or cache warm)  
4. Hit a **smoke-query checklist** (one SQL per panel); fail the flow if any panel query returns empty when data should exist  
5. Emit a Prefect artifact / notification: “Dashboard green @ matchweek W” or list broken panels  

---

## Dashboard contract — what must appear

Design the UI so every panel maps 1:1 to a gold table/view and a Prefect smoke check.

| Panel | Source mart | Scout question answered |
|---|---|---|
| League & competition filter + coverage KPI | `data_quality`, matches | Is the open dataset fully loaded? |
| Cross-league ranking leaderboard (by position) | `player_season_rankings` | Who leads after normalization? |
| Player profile (per-90 radar / bars) | `player_match_features` agg | What is this player’s fingerprint? |
| Similar players | `player_similarity` | Who are the nearest replacements? |
| Form sparkline (last N matches) | streaming → `player_match_features` | Is form rising or falling? |
| Drift / alarm timeline | `drift_events` | When did behaviour shift? |
| Club / league comparison | rankings + team dims | Does tournament form transfer to clubs? |
| Pipeline health strip | Prefect run metadata + `data_quality` | Are ranks stale? last success time? |

**Completeness rule:** if a metric is mentioned in the report, it must have a panel **and** a Prefect-validated query. No orphan CSVs.

### Suggested Superset layout (minimal but complete)
1. **Overview** — coverage KPIs, last Prefect success, open issues from DQ  
2. **Rankings** — filters: league, position, minutes threshold  
3. **Player deep-dive** — profile + form + drift for one `player_id`  
4. **Scouting shortlist** — similarity + rising form slice  
5. **Ops** — flow run history, Kafka lag (if exposed), Flink job status  

Optional: thin FastAPI over Iceberg/Hive if you prefer Streamlit/Dash; Prefect still owns refresh + smoke tests.

---

## Analytics questions (address ≥4 on the dashboard)

- After position and minutes normalization, which players lead **progressive-action value** in each top-5 league?  
- Do World Cup / Euro signals **transfer** to club-season rankings, or are they inflated?  
- Where do drift alarms fire, and do they precede ranking drops within 3–5 matches?  
- Can similarity neighbourhoods recover known archetypes (deep-lying playmaker vs ball-carrier)?  
- Which features are most unstable week-to-week (highest drift rate) by position?

---

## Deliverables

1. **Prefect project** with deployed flows: land, curate, features, stream, `dashboard_publish`  
2. Apache jobs: Spark ETL + MLlib, Kafka topics, Flink (or Spark Structured Streaming) form/drift job, Iceberg/Hive gold marts  
3. **Scout dashboard** with all panels in the contract above wired to marts  
4. Evidence that Prefect `dashboard_publish` refreshes data **and** fails when a panel would be empty/stale  
5. Short research write-up answering the analytics questions with dashboard screenshots  
6. Runbook: bootstrap from public Wyscout data → green dashboard in one command path  

---

## Out of scope

Proprietary club APIs, transfer-fee market models, private scouting notes, and non-public tracking/video as primary sources.

---

## Evaluation focus

Pipeline completeness, Prefect→dashboard reliability (freshness + smoke checks), correctness of features/joins, interpretability of rankings and drift, and whether a scout can answer the listed questions **without leaving the dashboard**.
