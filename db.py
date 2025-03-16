def create_table(conn):
    query = """
    CREATE TABLE IF NOT EXISTS livecyclehireupdates (
    last_update TIMESTAMP NOT NULL,
    terminal_name VARCHAR(10) NOT NULL,
    installed BOOLEAN NOT NULL,
    locked BOOLEAN NOT NULL,
    temporary BOOLEAN NOT NULL,
    nb_bikes INT NOT NULL,
    nb_standard_bikes INT NOT NULL,
    nb_ebikes INT NOT NULL,
    nb_empty_docks INT NOT NULL,
    nb_docks INT NOT NULL,
    PRIMARY KEY (last_update, terminal_name)  
    );"""

    conn.execute(query)

def drop_table(conn):
    query = """DROP TABLE livecyclehireupdates;"""
    conn.execute(query)

def get_latest_states(conn):
    query = """
    SELECT
        l.terminal_name,
        l.installed,
        l.locked,
        l.temporary,
        l.nb_bikes,
        l.nb_standard_bikes,
        l.nb_ebikes,
        l.nb_empty_docks,
        l.nb_docks
    FROM livecyclehireupdates AS l
    JOIN (
        SELECT
            terminal_name,
            MAX(last_update) AS max_ts
        FROM livecyclehireupdates
        GROUP BY terminal_name
    ) AS latest
        ON l.terminal_name = latest.terminal_name
    AND l.last_update   = latest.max_ts;"""


    cursor = conn.execute(query)
    rows = cursor.fetchall()
    return rows

def insert_stations(conn, last_update, stations):
    if not stations:
        return
    # We'll dynamically build the placeholders and values
    values = []
    placeholders = []

    # Each row will look like (?,?,?,?,?,?,?,?,?,?)
    # We must have the same number of placeholders repeated for each row
    for station in stations:
        placeholders.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        # row is (terminal_name, installed, locked, temporary, nb_bikes, nb_standard_bikes, nb_ebikes, nb_empty_docks, nb_docks)
        # We need to prepend last_update
        row_values = (last_update,) + station
        values.extend(row_values)

    # Join all placeholder groups with commas
    placeholders_str = ", ".join(placeholders)

    query = f"""
        INSERT INTO livecyclehireupdates (
            last_update,
            terminal_name,
            installed,
            locked,
            temporary,
            nb_bikes,
            nb_standard_bikes,
            nb_ebikes,
            nb_empty_docks,
            nb_docks
        )
        VALUES {placeholders_str};"""

    conn.execute("BEGIN;")
    try:
        conn.execute(query, values)
        conn.execute("COMMIT;")
    except:
        conn.execute("ROLLBACK;")
        raise

def get_conn():
    import os
    from dotenv import load_dotenv
    import sqlitecloud

    load_dotenv()

    sqlite_conn_str = os.environ.get("SQLITECLOUD_CONN_STR")
    conn = sqlitecloud.connect(sqlite_conn_str)

    return conn
