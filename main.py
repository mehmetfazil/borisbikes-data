import time
import logging
from tfl import fetch_cycle_hire_data, get_changed_or_new_stations
from db import insert_stations, get_conn, get_latest_states
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)

def main():
    while True:
        try:
            conn = get_conn()
            _, stations_from_tfl = fetch_cycle_hire_data()
            last_update = datetime.now(timezone.utc)
            stations_from_db = get_latest_states(conn)

            changed_or_new_stations = get_changed_or_new_stations(
                stations_from_db, stations_from_tfl
            )
            
            insert_stations(conn, last_update, changed_or_new_stations)
        
        except Exception as e:
            logging.exception("Error in the data pipeline loop: %s", e)
            # Optionally do some cleanup or logic here (e.g. sleep longer).
        
        finally:
            # Make sure we close the connection if it was opened
            try:
                conn.close()
            except:
                pass
        
        # Sleep 5 seconds before the next iteration
        time.sleep(5)

if __name__ == "__main__":
    main()
