# Web Front-End Specification

## 1. Overview & Audience
The goal of the web front-end is to provide actionable insights, historical data visualization, and efficiency tracking for your connected electric vehicle based on the ingested BigQuery dataset.
- **Audience:** Small private group (2 users: you and your wife).
- **Platform:** Fully responsive web application (optimized for Mobile and Desktop).

## 2. Architecture & Tech Stack
To keep costs low and align with the "occasional use" requirement, the architecture will rely on serverless components that scale to zero when not in use.
- **Backend/API:** Google Cloud Functions (Python) or Google Cloud Run to query BigQuery and serve data. 
- **Frontend:** 
  - *Lightweight Python* A Streamlit or Dash app containerized and hosted on Cloud Run. This is heavily optimized for data dashboards and requires very little boilerplate.
- **Authentication:** Google SSO. Implementing Google Identity-Aware Proxy (IAP) in front of Cloud Run/Functions is highly recommended. It allows you to strictly whitelist specific Google accounts (yours and your wife's) without having to write any custom login, password, or session management code.
- **External APIs:** Google Maps Elevation API or Open-Elevation API to backfill missing elevation data based on ingested GPS coordinates.

## 3. Core Features & Views
The UI should be organized into the following analytical views:

**1. High-Level Dashboard (The Landing Page)**
- **Current Status:** Live (or last known) State of Charge (SoC), estimated range, parked location (mini-map), online status, and active charging status.
- **Quick Stats:** Trailing 7-day and 30-day summaries (total km driven, total time, average efficiency, total kWh).

**2. Trip Logs & Drilldowns**
- **Trip History:** A chronological, paginated list of automatically detected trips. This should show Date, Start time, duration, distance, kwh consumed, and efficiency (KWh/100km)
- **Trip Details:** Clicking a trip reveals an interactive map showing the route, start/end times, distance, duration, energy consumed, efficiency, temperature, altitude change, and average speed.

**3. Efficiency & Environmental Analytics**
- **Efficiency Scatter Plots:** Interactive visualizations comparing average trip efficiency against:
  - *Outside temperature:* To visualize battery performance/range loss in winter vs. summer.
  - *Average speed:* Highway vs. city driving efficiency.
  - *Elevation change:* Net elevation gain/loss calculation (requires enriching the dataset with the Elevation API).

**4. Charging & Battery Health Insights**
- **Charging Sessions:** Log of all charging events, showing start/end SoC, duration, max charging power, and charging type (AC/DC / Fast vs. Slow).
- **Cost Estimation:** A configuration tab to input the location of home chargers, as well as electricity rates (e.g., euros/kWh) for both home charging and on-the-road charging, to estimate charging costs over time based on where the car is being charged.
- **Phantom Drain Tracker:** Analysis of battery percentage lost while the car is parked and asleep over extended periods (e.g., overnight or at the airport), as well as phantom loss when charge levels are recalculated when the car starts charging.

## 4. Data Pipeline Enhancements Needed
To support the fast loading of the front-end, the following backend/database tasks will need to be addressed:
- **Trip & Session Inference Logic:** Because the raw BigQuery table only stores 5-minute time-series snapshots, we will need to create BigQuery SQL Views or Scheduled Queries to group continuous "driving" or "charging" rows into discrete `trips` and `charge_sessions` tables.
- **Energy Consumption Logic:** Energy usage is reported as SoC (charge percentage), which is only precise to 1%. We'll need to convert this to kWh based on the size of the battery in the car (77kwh). This will also require some clever logic to try to infer greater precision of the actual consumption beyond a 0.77kwh precision (1% of 77kwh) for shorter trips, most likely using the number of km driven and typical efficiency rates.
- **Elevation Backfill Strategy:** A script to periodically scan the `vehicle_status` table for new latitude/longitude pairs, query an Elevation API, and update the existing schema.