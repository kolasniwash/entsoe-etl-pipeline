class EuroEnergyQueries:
    energy_loads_table_insert = """
    SELECT
        cast(g.event_date as timestamp),
        g.country_id,
        g.generation_type,
        p.day_ahead_price,
        l.total_demand as demand_load,
        g.generation_load
    FROM staging_energy_generation as g
    LEFT JOIN staging_energy_loads as l ON
        g.ts = l.ts AND
        g.country_id = l.country_id
    LEFT JOIN staging_day_ahead_prices as p ON 
        l.ts = p.ts AND
        l.country_id = p.country_id
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
        EXTRACT(minute from stel.event_ts) as minute,
        EXTRACT(WEEKDAY from stel.event_ts) as dayofweek
    FROM (
        SELECT
            TIMESTAMP 'epoch' + ts/1000000 *INTERVAL '1 second' as event_ts
        FROM staging_energy_loads
    ) as stel
    """
    #
    # FROM(
    #     SELECT
    #         TIMESTAMP 'epoch' + ts / 1000 * interval '1 second' AS start_time,
    #         *
    #     FROM staging_events
    #     WHERE page = 'NextSong') as events
    #


    ## countries table is inserted directly from csv as own dimension task
    countries_table_insert = ()


    stage_installed_capacity = """
        CREATE TABLE IF NOT EXISTS staging_installed_cap (
            area_date VARCHAR(256),
            production_type VARCHAR(256),	
            code VARCHAR(256),
            name VARCHAR(256),
            installed_capacity_year_start NUMERIC(10,1),
            current_installed_capacity	NUMERIC(10,1),
            location VARCHAR(256),
            voltage_connection_level INT4,
            commissioning_date	VARCHAR(256),
            decommissioning_date VARCHAR(256),
            country_id VARCHAR(256)
        );
    """

    stage_energy_loads = """
    CREATE TABLE IF NOT EXISTS staging_energy_loads (
        event_date VARCHAR(256),
        total_demand NUMERIC(12,2),
        ts int8,
        country_id VARCHAR(256)
    )
    """

    stage_energy_generation = """
    CREATE TABLE IF NOT EXISTS staging_energy_generation (
        event_date VARCHAR(256),
        ts int8,
        country_id VARCHAR(256),
        area VARCHAR(256),
        generation_type VARCHAR(256),
        generation_load NUMERIC(12,2)
    )
    """

    stage_day_ahead_prices = """
    CREATE TABLE IF NOT EXISTS staging_day_ahead_prices (
        event_date VARCHAR(256),
        day_ahead_price NUMERIC(8,2),
        country_id VARCHAR(256),
        ts int8
        );
    """

    create_energy_loads = """
    CREATE TABLE IF NOT EXISTS energy_loads (
        event_date TIMESTAMP NOT NULL,
        country_id VARCHAR(256),
        generation_type VARCHAR(256),
        day_ahead_price NUMERIC(8,2),
        demand_load NUMERIC(12,2),
        generation_load NUMERIC(12,2),
        CONSTRAINT event_key PRIMARY KEY (event_date)
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
            country_id VARCHAR(256) NOT NULL,
            latitude NUMERIC(18,0),
            longitude NUMERIC(18,0),
            country_name VARCHAR(256),
            CONSTRAINT country_key PRIMARY KEY (country_id)
        );
    """

    create_installed_capacity = """
        CREATE TABLE IF NOT EXISTS installed_capacity (
            generation_type VARCHAR(256),
            station_name VARCHAR(256),
            country_id VARCHAR(256),
            installed_capacity INT4,
            connection_voltage INT4,
            commission_date VARCHAR(256),
            control_area VARCHAR(256),
            code VARCHAR(256),
            PRIMARY KEY (country_id, generation_type, station_name)
        );
    """