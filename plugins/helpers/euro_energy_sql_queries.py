class EuroEnergyQueries:
    energy_loads_table_insert = """
    SELECT
        g.event_date,
        g.country_id,
        g.generation_type,
        p.day_ahead_price,
        l.total_demand as demand_load,
        g.generation_load
    FROM staging_energy_generation as g ON
    LEFT JOIN staging_energy_loads as l ON
        g.ts = l.ts,
        g.country_id=l.country_id
    LEFT JOIN staging_day_ahead_prices as p ON 
        l.ts=p.ts AND
        l.country_id=p.country_id
    """

    installed_capacity_insert = """
    SELECT DISTINCT 
        production_type as generation_type,
        name as station_name,
        country_id as country,
        current_installed_capacity as installed_capacity,
        voltage_connection_level as connection_voltage,
        commissioning_date as commission_date,
        SPLIT_PART(area_date, ' / ', 1) as control_area,
        code
    FROM staging_installed_cap
    """

    times_table_insert = """
    SELECT DISTINCT
        stel.event_ts,
        EXTRACT(year from stel.event_ts) as year,
        EXTRACT(month from stel.event_ts) as month,
        EXTRACT(day from stel.event_ts) as day,
        EXTRACT(hour from stel.event_ts) as hour,
        EXTRACT(minute from stel.event_ts
        EXTRACT(WEEKDAY from stel.event_ts) as dayofweek,
        CASE EXTRACT(WEEKDAY from stel.event_ts) IN (5, 6) THEN true ELSE false
    FROM (
        SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as event_ts
        FROM staging_energy_loads
    ) as stel
    """

    ## countries table is inserted directly from csv as own dimension task
    countries_table_insert = ()


class EuroEnergyMakeTables:

    stage_installed_capacity = """
        CREATE TABLE IF NOT EXISTS staging_installed_cap (
            area_date VARCHAR,
            production_type VARCHAR,	
            code VARCHAR,
            name VARCHAR,
            installed_capacity_year_start INT,
            current_installed_capacity	INT,
            location VARCHAR,
            voltage_connection_level INT,
            commissioning_date	VARCHAR,
            decommissioning_date VARCHAR,
            country_id VARCHAR(3)
        );
    """

    stage_energy_loads = """
    CREATE TABLE IF NOT EXISTS staging_energy_loads (
        event_date TIMESTAMP
        total_demand FLOAT
        ts VARCHAR,
        country_id VARCHAR(3)
    )
    """

    stage_energy_generation = """
    CREATE TABLE IF NOT EXISTS staging_energy_generation (
        event_date TIMESTAMP,
        generation_type VARCHAR,
        generation_load FLOAT,
        ts VARCHAR,
        country_id VARCHAR(3)
    )
    """

    stage_day_ahead_prices = """
    CREATE TABLE IF NOT EXISTS staging_day_ahead_prices (
        event_date TIMESTAMP,
        day_ahead_price FLOAT,
        ts VARCHAR,
        country_id VARCHAR(2)
    """

    create_energy_loads = """
    CREATE TABLE IF NOT EXISTS energy_loads (
        event_it INT IDENTITY(0,1),
        event_date TIMESTAMP NOT NULL,
        country_id INT4,
        generation_type INT4,
        day_ahead_price FLOAT,
        demand_load FLOAT,
        generation_load FLOAT
        );
    """

    create_times = """
        CREATE TABLE IF NOT EXISTS times (
            event_date TIMESTAMP PRIMARY KEY,
            year INT4,
            month INT2,
            day INT2,
            hour INT2,
            minute INT2,
            dayofweek INT2,
            weekend BOOLEAN
        );
    """

    create_countries = """
        CREATE TABLE IF NOT EXISTS countries (
            country_id VARCHAR(3) PRIMARY KEY,
            country_name VARCHAR(45),
            latitude FLOAT,
            longitude FLOAT
        );
    """

    create_installed_capacity = """
        CREATE TABLE IF NOT EXISTS installed_capacity (
            generation_type VARCHAR,
            station_name VARCHAR,
            country_id VARCHAR(3),
            installed_capacity INT4,
            connection_voltage INT4,
            commission_date DATE,
            control_area VARCHAR(6),
            code VARCHAR(32)
        );
    """