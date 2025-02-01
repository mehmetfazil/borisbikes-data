import requests
import xml.etree.ElementTree as ET
from datetime import datetime

def fetch_cycle_hire_data():
    """
    Fetches live cycle hire update data from the TfL feed and parses it into:
      - a Python datetime object representing the 'last_update'
      - a list of tuples, where each tuple has the structure:
        (terminal_name, installed, locked, temporary,
         nb_bikes, nb_standard_bikes, nb_ebikes, nb_empty_docks, nb_docks)
    """
    url = "https://tfl.gov.uk/tfl/syndication/feeds/cycle-hire/livecyclehireupdates.xml"
    response = requests.get(url)
    response.raise_for_status()  # Raise HTTPError if the request was not successful
    
    # Parse the XML
    root = ET.fromstring(response.content)
    
    # Convert the lastUpdate (milliseconds) to a Python datetime
    last_update_millis = int(root.attrib["lastUpdate"])
    last_update_timestamp = datetime.fromtimestamp(last_update_millis / 1000.0)
    
    stations_data = []
    for station in root.findall("station"):
        terminal_name = station.find("terminalName").text
        
        # Convert boolean-like strings ("true"/"false") to 1/0
        installed = 1 if station.find("installed").text.lower() == "true" else 0
        locked = 1 if station.find("locked").text.lower() == "true" else 0
        temporary = 1 if station.find("temporary").text.lower() == "true" else 0
        
        nb_bikes = int(station.find("nbBikes").text)
        nb_standard_bikes = int(station.find("nbStandardBikes").text)
        nb_ebikes = int(station.find("nbEBikes").text)
        nb_empty_docks = int(station.find("nbEmptyDocks").text)
        nb_docks = int(station.find("nbDocks").text)
        
        # Append as a tuple matching your column requirements:
        # (terminal_name, installed, locked, temporary,
        #  nb_bikes, nb_standard_bikes, nb_ebikes, nb_empty_docks, nb_docks)
        stations_data.append(
            (terminal_name, installed, locked, temporary,
             nb_bikes, nb_standard_bikes, nb_ebikes, nb_empty_docks, nb_docks)
        )
    
    return last_update_timestamp, stations_data

def get_changed_or_new_stations(stations_from_db, stations_from_tfl):
    """
    Returns a list of stations where the station status (tuple data) 
    is different between the DB list and the TFL list, or the station 
    is entirely missing from the DB list.
    """
    # If the DB list is empty, all TFL stations are "new" or "changed"
    if not stations_from_db:
        return stations_from_tfl

    # Convert DB stations into a dictionary keyed by station name
    db_dict = {station[0]: station[1:] for station in stations_from_db}
    
    changed_stations = []
    
    # Check each station from TFL
    for tfl_station in stations_from_tfl:
        station_name = tfl_station[0]
        tfl_data = tfl_station[1:]  # The status values
        
        db_data = db_dict.get(station_name)  # None if station not in DB
        
        # If the station does not exist in DB, or its status has changed
        if db_data != tfl_data:
            changed_stations.append(tfl_station)
    
    return changed_stations
