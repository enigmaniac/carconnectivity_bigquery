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
- **Trip History:** A chronological, paginated list of automatically detected trips (which may consist of multiple driving segments). This should show Date, Start time, destination (user-friendly city & country code), duration, distance, kwh consumed, and efficiency (KWh/100km).
- **Trip Details:** Clicking a trip reveals an interactive map showing the route and overall stats. It should include a header with the trip date and start/end times, and breakdown of total time into **Driving**, **Charging**, and **Parked** durations.

The trip details should have a **trip overview**, displayed as a vertical line segment on the left representing, with dots representing the start/stop locations of the trip-segments and/or charging (showing the user-friendly location with city & country code, and time), and segments between the dots showing a summary of the key events of the trip (driving, charging, parked -- along with duration, distance, and KWH for the segment -- depending on the event type) The trip should start at the top of the line with the trip departure, and end at the bottom of the line with the final arrival.

 When clicking on any of these trip components (other than "parked"), display a detailed view of the individual **Trip Segment** (start and end location in user-friendly format (city, 2-letter country code), distance, time, average speed, average temperature, KWH consumed, efficiency -- if the `is_consumption_estimated` flag is true, display a warning tooltip/icon indicating the kWh consumption is an estimate) or mid-trip **Charge Sessions** (location, AC or DC, time, kWh added, max power).    

**3. Efficiency & Environmental Analytics**
- **Efficiency Scatter Plots:** Interactive visualizations comparing average trip efficiency against:
  - *Outside temperature:* To visualize battery performance/range loss in winter vs. summer.
  - *Average speed:* Highway vs. city driving efficiency.
  - *Elevation change:* Net elevation gain/loss calculation (requires enriching the dataset with the Elevation API).
  Allow this data to be filtered based on various factors like date, trip time, trip duration, starting or ending location, etc.

**4. Charging & Battery Health Insights**
- **Charging Sessions:** Log of all charging events, showing start/end SoC, duration, max charging power, and charging type (AC/DC / Fast vs. Slow).
- **Cost Estimation:** A configuration tab to input the location of home chargers, as well as electricity rates (e.g., euros/kWh) for both home charging and on-the-road charging, to estimate charging costs over time based on where the car is being charged.
- **Phantom Drain Tracker:** Analysis of battery percentage lost while the car is parked and asleep over extended periods (e.g., overnight or at the airport), as well as phantom loss when charge levels are recalculated when the car starts charging.

## 4. Data Pipeline Enhancements Needed (COMPLETE!)
To support the fast loading of the front-end, the following backend/database tasks will need to be addressed:
- **Trip & Session Inference Logic:** Because the raw BigQuery table only stores 5-minute time-series snapshots, we will need to create BigQuery SQL Views to group continuous "driving" rows into `segments`, and then group those segments into holistic `trips` based on idle periods of less than 2 hours.
- **Energy Consumption Logic:** Energy usage is reported as SoC, which lacks precision for small movements. We use a **4% SoC drop threshold at the Trip level**. For trips <4%, we fallback to an assumed efficiency (16kWh/100km). For individual trip segments <4%, we estimate consumption using the segment's distance multiplied by the overall trip's average efficiency. Segments >=4% use their actual battery math.
- **Elevation Backfill Strategy:** A script to periodically scan the `vehicle_status` table for new latitude/longitude pairs, query an Elevation API, and update the existing schema.