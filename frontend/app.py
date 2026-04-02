import os
import datetime
import calendar
import streamlit as st
import pandas as pd
import plotly.express as px
import folium
from streamlit_folium import st_folium
from google.cloud import bigquery
import google.auth

# --- CONFIGURATION ---
st.set_page_config(
    page_title="EV Analytics Dashboard",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Attempt to get project ID from environment, or fallback to default auth
PROJECT_ID = os.environ.get("GCP_PROJECT")
if not PROJECT_ID:
    try:
        _, PROJECT_ID = google.auth.default()
    except Exception:
        PROJECT_ID = "your-project-id"
DATASET_ID = "car_data"

# --- BQ CLIENT SETUP ---
@st.cache_resource
def get_bq_client():
    """Initializes and caches the BigQuery client."""
    return bigquery.Client(project=PROJECT_ID)

client = get_bq_client()

# --- DATA FETCHING ---
@st.cache_data(ttl=300)  # Cache for 5 minutes (matches ingestion schedule)
def get_latest_vehicle_status():
    """Fetches the most recent row from the vehicle_status table."""
    query = f"""
        SELECT ingestion_timestamp, soc, mileage, external_temperature, 
               latitude, longitude, is_charging, is_online, state
        FROM `{PROJECT_ID}.{DATASET_ID}.vehicle_status`
        ORDER BY ingestion_timestamp DESC
        LIMIT 1
    """
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Failed to fetch data from BigQuery: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_trips_data():
    """Fetches the recent trips summary."""
    query = f"""
        SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.trips` 
        ORDER BY start_time DESC LIMIT 100
    """
    try:
        return client.query(query).to_dataframe()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_charging_data():
    """Fetches recent charging sessions."""
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.charging_sessions` ORDER BY start_time DESC LIMIT 100"
    try:
        return client.query(query).to_dataframe()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_route_coordinates(vehicle_id, start_time, end_time):
    """Fetches GPS coordinates for a specific time window to draw a route map."""
    query = f"""
        SELECT latitude, longitude
        FROM `{PROJECT_ID}.{DATASET_ID}.vehicle_status`
        WHERE vehicle_id = '{vehicle_id}' 
          AND ingestion_timestamp BETWEEN TIMESTAMP_SUB(CAST('{start_time.isoformat()}' AS TIMESTAMP), INTERVAL 30 MINUTE) AND TIMESTAMP_ADD(CAST('{end_time.isoformat()}' AS TIMESTAMP), INTERVAL 30 MINUTE)
          AND latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY ingestion_timestamp ASC
    """
    try:
        return client.query(query).to_dataframe()
    except Exception:
        return pd.DataFrame()

# --- HELPER FUNCTIONS ---
def parse_bq_array(arr):
    """Safely extracts lists of structs from BigQuery array columns."""
    if arr is None: return []
    if isinstance(arr, float) and pd.isna(arr): return []
    if hasattr(arr, 'tolist'): arr = arr.tolist()
    if isinstance(arr, list): return [x for x in arr if x and isinstance(x, dict) and x.get('start_time')]
    return []

def render_timeline(events):
    """Reusable function to render a beautiful UI timeline of driving/charging events."""
    events.sort(key=lambda x: pd.to_datetime(x['start_time']))
    
    for i, event_data in enumerate(events):
        ev_type = event_data['type']
        start_t = pd.to_datetime(event_data['start_time'])
        end_t = pd.to_datetime(event_data['end_time'])
        
        if ev_type == 'segment':
            seg = event_data['data']
            trip_context = event_data.get('trip_details', {})
            
            start_city = seg.get('start_city') or 'Unknown'
            start_country = seg.get('start_country') or 'XX'
            end_city = seg.get('end_city') or 'Unknown'
            end_country = seg.get('end_country') or 'XX'
            
            st.markdown(f"**🟢 {start_t.strftime('%H:%M')}** - Depart {start_city}, {start_country}")
            with st.expander(f"🚗 Driving ({seg['duration_minutes']} min, {seg['distance_km']:.1f} km)"):
                est_warn = " ⚠️ *(Estimated)*" if trip_context.get('is_consumption_estimated') else ""
                eff = (seg['kwh_consumed'] / seg['distance_km'] * 100) if seg['distance_km'] > 0 else 0
                avg_speed = seg['distance_km'] / (seg['duration_minutes'] / 60) if seg['duration_minutes'] > 0 else 0
                
                start_alt = seg.get('start_altitude')
                start_alt = start_alt if start_alt is not None else 0
                end_alt = seg.get('end_altitude')
                end_alt = end_alt if end_alt is not None else 0
                alt_change = end_alt - start_alt
                
                avg_temp = seg.get('avg_external_temp')
                avg_temp = avg_temp if avg_temp is not None else 0
                
                st.write(f"- **Distance:** {seg['distance_km']:.1f} km")
                st.write(f"- **Avg Speed:** {avg_speed:.0f} km/h")
                st.write(f"- **Elevation Change:** {alt_change:.0f} m (Start: {start_alt:.0f}m ➡️ End: {end_alt:.0f}m)")
                st.write(f"- **Avg Temp:** {avg_temp:.1f} °C")
                st.write(f"- **Energy:** {seg['kwh_consumed']:.1f} kWh{est_warn}")
                st.write(f"- **Efficiency:** {eff:.1f} kWh/100km")
                st.write(f"- **Battery:** {seg['start_soc']}% ➡️ {seg['end_soc']}%")
            
            # Arrival Icon logic: Use a checkered flag for the final event, otherwise a pin for an intermediate stop
            icon = "🏁" if i == len(events) - 1 else "📍"
            st.markdown(f"**{icon} {end_t.strftime('%H:%M')}** - Arrive {end_city}, {end_country}")
            
        elif ev_type == 'charge':
            charge = event_data['data']
            charge_city = charge.get('city') or 'Unknown'
            charge_country = charge.get('country') or 'XX'
            st.markdown(f"**⚡ {start_t.strftime('%H:%M')}** - Plug in at {charge_city}, {charge_country}")
            with st.expander(f"🔋 Charging ({charge['duration_minutes']} min, +{charge['kwh_added']:.1f} kWh)"):
                st.write(f"- **Type:** {charge['charging_type']}")
                st.write(f"- **Max Power:** {charge['max_charging_power']} kW")
                st.write(f"- **Battery:** {charge['start_soc']}% ➡️ {charge['end_soc']}%")
            
            icon = "🏁" if i == len(events) - 1 else "🔌"
            st.markdown(f"**{icon} {end_t.strftime('%H:%M')}** - Unplugged")
        
        # Render Parked Gap between events
        if i < len(events) - 1:
            next_start = pd.to_datetime(events[i+1]['start_time'])
            gap_mins = (next_start - end_t).total_seconds() / 60
            if gap_mins > 5:
                st.markdown(f"<div style='margin-left: 10px; border-left: 3px dashed #ccc; padding-left: 15px; margin-top: 5px; margin-bottom: 5px; color: #888;'>🅿️ <i>Parked for {int(gap_mins)} min</i></div>", unsafe_allow_html=True)

# --- NAVIGATION ---
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", [
    "Dashboard", 
    "Trips & Logs", 
    "Calendar View",
    "Efficiency Analytics", 
    "Charging Insights"
])

# --- PAGE: DASHBOARD ---
if page == "Dashboard":
    st.title("🚗 High-Level Dashboard")
    st.markdown("Welcome to your connected EV analytics platform.")
    
    status_df = get_latest_vehicle_status()
    
    if not status_df.empty:
        current = status_df.iloc[0]
        
        st.subheader("Current Status")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("State of Charge (SoC)", f"{current['soc']}%" if pd.notnull(current['soc']) else "N/A")
        col2.metric("Odometer", f"{current['mileage']:,.0f} km" if pd.notnull(current['mileage']) else "N/A")
        col3.metric("Vehicle State", str(current['state']).replace('_', ' ').title())
        col4.metric("Outside Temp", f"{current['external_temperature']:.1f} °C" if pd.notnull(current['external_temperature']) else "N/A")
        
        st.caption(f"*Last updated: {current['ingestion_timestamp'].strftime('%Y-%m-%d %H:%M:%S UTC')}*")
        
        st.markdown("---")
        st.subheader("📍 Last Known Parked Location")
        if pd.notnull(current['latitude']) and pd.notnull(current['longitude']):
            m = folium.Map(location=[current['latitude'], current['longitude']], zoom_start=15)
            folium.Marker(
                [current['latitude'], current['longitude']], 
                popup="Vehicle Location",
                icon=folium.Icon(color="blue", icon="car", prefix='fa')
            ).add_to(m)
            st_folium(m, width=800, height=350, returned_objects=[])
        else:
            st.info("Location data not available.")
    else:
        st.warning("No vehicle data found. Please ensure the backend data pipeline is running.")

elif page == "Trips & Logs":
    st.title("🗺️ Trip Logs & Drilldowns")
    
    trips_df = get_trips_data()
    if trips_df.empty:
        st.info("No trips recorded yet. Go for a drive!")
    else:
        # Calculate efficiency for display and limit to 20 rows
        display_df = trips_df.head(20).copy()
        # Safely calculate efficiency, handle divide by zero
        display_df['efficiency (kWh/100km)'] = display_df.apply(
            lambda row: (row['kwh_consumed'] / row['distance_km']) * 100 if row['distance_km'] > 0 else 0, axis=1
        )
        
        display_df['Date'] = display_df['start_time'].dt.strftime('%Y-%m-%d')
        display_df['Departure'] = display_df['start_time'].dt.strftime('%H:%M')
        display_df['Arrival'] = display_df['end_time'].dt.strftime('%H:%M')
        display_df['Origin'] = display_df.apply(lambda r: f"{r.get('start_city', 'Unknown')}, {r.get('start_country', 'XX')}" if pd.notnull(r.get('start_city')) else "Unknown", axis=1)
        display_df['Destination'] = display_df.apply(lambda r: f"{r.get('end_city', 'Unknown')}, {r.get('end_country', 'XX')}" if pd.notnull(r.get('end_city')) else "Unknown", axis=1)
        display_df['Distance (km)'] = display_df['distance_km'].round(0)
        display_df['Duration (min)'] = display_df['total_duration_minutes']
        display_df['Energy (kWh)'] = display_df['kwh_consumed'].round(1)
        
        # Add warning icon for estimated consumption directly to the string
        display_df['Efficiency'] = display_df.apply(
            lambda r: f"{r['efficiency (kWh/100km)']:.1f} ⚠️" if r['is_consumption_estimated'] else f"{r['efficiency (kWh/100km)']:.1f}", axis=1
        )
        
        display_df.insert(0, 'Action', '🔍 View')

        cols_to_show = ['Action', 'Date', 'Departure', 'Arrival', 'Origin', 'Destination', 'Distance (km)', 'Duration (min)', 'Energy (kWh)', 'Efficiency']
        
        st.markdown("👆 **Select a trip below to view details:**")
        event = st.dataframe(
            display_df[cols_to_show],
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun"
        )
        
        selected_rows = event.selection.rows
        if selected_rows:
            selected_trip = display_df.iloc[selected_rows[0]]['trip_id']
        else:
            selected_trip = None

        if selected_trip:
            st.markdown("---")
            trip_details = trips_df[trips_df['trip_id'] == selected_trip].iloc[0]
            
            st.subheader(f"📍 Trip Details: {trip_details['start_time'].strftime('%b %d, %Y')}")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Distance", f"{trip_details['distance_km']:.0f} km")
            col2.metric("Driving", f"{trip_details['driving_duration_minutes']} min")
            col3.metric("Charging", f"{trip_details['charging_duration_minutes']} min")
            col4.metric("Parked", f"{trip_details['parked_duration_minutes']} min")
            
            st.markdown("### Timeline")
            
            events = []
            for seg in parse_bq_array(trip_details.get('segments')):
                events.append({'type': 'segment', 'start_time': seg['start_time'], 'end_time': seg['end_time'], 'data': seg, 'trip_details': trip_details})
            for charge in parse_bq_array(trip_details.get('charge_sessions')):
                events.append({'type': 'charge', 'start_time': charge['start_time'], 'end_time': charge['end_time'], 'data': charge})
            
            render_timeline(events)
            
            st.markdown("### Route Map")
            route_df = get_route_coordinates(trip_details['vehicle_id'], trip_details['start_time'], trip_details['end_time'])
            if not route_df.empty:
                m = folium.Map(location=[route_df['latitude'].mean(), route_df['longitude'].mean()], zoom_start=11)
                coords = route_df[['latitude', 'longitude']].values.tolist()
                folium.PolyLine(coords, color="blue", weight=5, opacity=0.8).add_to(m)
                folium.Marker(coords[0], popup="Start", icon=folium.Icon(color="green", icon="play")).add_to(m)
                folium.Marker(coords[-1], popup="End", icon=folium.Icon(color="red", icon="flag")).add_to(m)
                st_folium(m, use_container_width=True, height=400, returned_objects=[])
            else:
                # Fallback to direct start/end coordinates if continuous route path is missing
                start_lat = trip_details.get('start_latitude')
                start_lon = trip_details.get('start_longitude')
                end_lat = trip_details.get('end_latitude')
                end_lon = trip_details.get('end_longitude')
                
                if pd.notnull(start_lat) and pd.notnull(start_lon) and pd.notnull(end_lat) and pd.notnull(end_lon):
                    m = folium.Map(location=[(start_lat + end_lat)/2, (start_lon + end_lon)/2], zoom_start=11)
                    coords = [[start_lat, start_lon], [end_lat, end_lon]]
                    folium.PolyLine(coords, color="blue", weight=5, opacity=0.8, dash_array='10').add_to(m)
                    folium.Marker(coords[0], popup="Start", icon=folium.Icon(color="green", icon="play")).add_to(m)
                    folium.Marker(coords[-1], popup="End", icon=folium.Icon(color="red", icon="flag")).add_to(m)
                    st_folium(m, use_container_width=True, height=400, returned_objects=[])
                else:
                    st.info("Route map details not available for this trip.")

elif page == "Calendar View":
    st.title("📅 Monthly Activity Calendar")
    
    trips_df = get_trips_data()
    charging_df = get_charging_data()
    
    if not trips_df.empty or not charging_df.empty:
        trips_df['date'] = trips_df['start_time'].dt.date
        charging_df['date'] = charging_df['start_time'].dt.date
        
        col1, col2, col3 = st.columns([1, 1, 2])
        year = col1.selectbox("Year", [2023, 2024, 2025, 2026], index=3)
        month = col2.selectbox("Month", list(range(1, 13)), index=datetime.datetime.now().month - 1)
        metric = col3.radio("Activity Metric", ["Driving (km)", "Charging (kWh)"], horizontal=True)
        
        daily_driving = trips_df.groupby('date')['distance_km'].sum().to_dict()
        daily_charging = charging_df.groupby('date')['kwh_added'].sum().to_dict()
        daily_data = daily_driving if metric == "Driving (km)" else daily_charging
        
        max_val = max(daily_data.values()) if daily_data else 1
        
        st.write("")
        cols = st.columns(7)
        for i, day_name in enumerate(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]):
            cols[i].markdown(f"**{day_name}**")
        
        # Render the calendar blocks
        for week in calendar.monthcalendar(year, month):
            cols = st.columns(7)
            for i, day in enumerate(week):
                if day == 0:
                    continue
                d = datetime.date(year, month, day)
                val = daily_data.get(d, 0)
                
                with cols[i].container(border=True):
                    st.markdown(f"**{day}**")
                    if val > 0:
                        intensity = min(val / max_val, 1.0)
                        color = f"rgba(0, 150, 255, {max(intensity, 0.2)})"
                        st.markdown(f"<div style='background-color:{color}; width:100%; height:8px; border-radius:4px;'></div>", unsafe_allow_html=True)
                        st.caption(f"{val:.0f} {'km' if metric == 'Driving (km)' else 'kWh'}")
                    else:
                        st.markdown(f"<div style='background-color:rgba(200, 200, 200, 0.2); width:100%; height:8px; border-radius:4px;'></div>", unsafe_allow_html=True)
                        st.caption("-")
                        
                    if st.button("🔍 View", key=f"btn_{d}", use_container_width=True):
                        st.session_state.selected_date = d
                        
        if "selected_date" in st.session_state:
            d = st.session_state.selected_date
            st.markdown("---")
            st.subheader(f"Detailed Day View: {d.strftime('%A, %b %d, %Y')}")
            
            day_trips = trips_df[trips_df['date'] == d]
            day_charges = charging_df[charging_df['date'] == d]
            
            if day_trips.empty and day_charges.empty:
                st.info("No driving or charging activity on this day.")
            else:
                events = []
                for _, trip in day_trips.iterrows():
                    for seg in parse_bq_array(trip.get('segments')):
                        events.append({'type': 'segment', 'start_time': seg['start_time'], 'end_time': seg['end_time'], 'data': seg, 'trip_details': trip})
                for _, charge in day_charges.iterrows():
                    events.append({'type': 'charge', 'start_time': charge['start_time'], 'end_time': charge['end_time'], 'data': charge})
                
                render_timeline(events)
    else:
        st.info("Not enough data to construct calendar.")

elif page == "Efficiency Analytics":
    st.title("📈 Efficiency & Environmental Analytics")
    
    trips_df = get_trips_data()
    if not trips_df.empty:
        st.sidebar.subheader("Analytics Filters")
        min_dist = st.sidebar.slider("Minimum Trip Distance (km)", 1, 150, 5)
        
        eff_df = trips_df[trips_df['distance_km'] >= min_dist].copy()
        eff_df['efficiency'] = (eff_df['kwh_consumed'] / eff_df['distance_km']) * 100
        eff_df['avg_speed'] = eff_df['distance_km'] / (eff_df['driving_duration_minutes'] / 60)
        eff_df['elevation_change'] = eff_df['end_altitude'] - eff_df['start_altitude']
        
        if not eff_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Efficiency vs. Temperature")
                fig1 = px.scatter(eff_df, x="avg_external_temp", y="efficiency", size="distance_km", hover_data=["start_time"], labels={"avg_external_temp": "Average Temp (°C)", "efficiency": "Efficiency (kWh/100km)"})
                st.plotly_chart(fig1, use_container_width=True)
                
                st.subheader("Efficiency vs. Avg Speed")
                fig2 = px.scatter(eff_df, x="avg_speed", y="efficiency", size="distance_km", hover_data=["start_time"], labels={"avg_speed": "Avg Speed (km/h)", "efficiency": "Efficiency (kWh/100km)"})
                st.plotly_chart(fig2, use_container_width=True)
            
            with col2:
                st.subheader("Efficiency vs. Elevation Change")
                fig3 = px.scatter(eff_df, x="elevation_change", y="efficiency", size="distance_km", hover_data=["start_time"], labels={"elevation_change": "Net Elevation Change (m)", "efficiency": "Efficiency (kWh/100km)"})
                st.plotly_chart(fig3, use_container_width=True)
        else:
            st.warning("No trips match the selected filters.")
    else:
        st.info("Not enough trip data to display analytics.")

elif page == "Charging Insights":
    st.title("⚡ Charging & Battery Health Insights")
    
    charging_df = get_charging_data()
    if not charging_df.empty:
        display_charge_df = charging_df.copy()
        display_charge_df.insert(0, 'Action', '🔍 View')
        display_charge_df['Date'] = display_charge_df['start_time'].dt.strftime('%Y-%m-%d %H:%M')
        display_charge_df['Location'] = display_charge_df.apply(lambda r: f"{r.get('city') or 'Unknown'}, {r.get('country') or 'XX'}", axis=1)
        
        cols = ['Action', 'Date', 'Location', 'duration_minutes', 'start_soc', 'end_soc', 'kwh_added', 'max_charging_power', 'charging_type']
        
        st.dataframe(
            display_charge_df[cols].style.format({
                'kwh_added': '{:.1f}',
                'max_charging_power': '{:.1f}'
            }), 
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No charging sessions recorded yet.")