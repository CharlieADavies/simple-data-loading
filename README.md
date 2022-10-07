# Data Engineering Challenge
* Language: Python
* Database: Postgres

## Running the whole thing
0. Add the data to `./json_events/`
1. Set up postgres instance (I used docker, via `docker run --name posty -e POSTGRES_PASSWORD=password -d -p 5432:5432 --rm postgres`)
    * If not running via this method, connection params can be set via environment variables, see `get_connection.py`
2. Set up python environment (`python3 -m venv venv && . ./venv/bin/activate && pip install -r requirements.txt`)
3. `python setup_database.py` will setup the simple database schema, and create the views
4. `python load_data.py` will load data into the database

# Task 1
* `./load_data.py` contains the code used to load data into the database
* `./setup_tables.sql` contains the very minimal schema which we load data into

# Task 2
* `./setup_view.sql` contains the view definition

## Output view
![example output](https://i.imgur.com/7tK4KzD.png)
