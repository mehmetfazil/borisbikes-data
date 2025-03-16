from tfl import fetch_cycle_hire_data, get_changed_or_new_stations
from db import insert_stations, get_conn, get_latest_states
from datetime import datetime, timezone

def main():
    with get_conn() as conn:
        _, stations_from_tfl = fetch_cycle_hire_data()
        last_update = datetime.now(timezone.utc)
        stations_from_db = get_latest_states(conn)
        
        changed_or_new_stations = get_changed_or_new_stations(
            stations_from_db, stations_from_tfl
        )
        
        insert_stations(conn, last_update, changed_or_new_stations)
        print(f"Successfully processed {len(changed_or_new_stations)} cycle hire data")

if __name__ == "__main__":
    main()
