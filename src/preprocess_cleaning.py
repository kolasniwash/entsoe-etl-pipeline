import os
import pandas as pd
from pathlib import Path
import datetime


def traverse_path(path, split_idx):
    """
    Generates a dictionary of the path to each country.

    Input:
        path: str. The path to the root directory
        split_idx: int. split point to capture country name. I.e. -2 splits one directory up.
    """

    pathlist = Path(path).glob('**/*.csv')

    country_paths = dict()

    for p in pathlist:
        path_in_str = str(p)
        country = path_in_str.split('/')[split_idx]
        country_paths[country] = path_in_str

    return country_paths


def process_capacity_demand(country_paths, output_path, name, new_cols=None):
    """
    Prepares capacity and demand csvs from the ENTOSE API for the data warehouse.
    Appends country information and renames columns

    country_paths: dict. country and path to csvs with installed capcity data
    new_cols: list. list of new columns headers
    """

    for country, path_in_str in country_paths.items():

        # load dataframe
        df = pd.read_csv(path_in_str)

        # add country name
        df['country_id'] = country

        # rename columns
        if new_cols is not None:
            if 'Unnamed: 0' in df.columns:
                df.drop('Unnamed: 0', axis=1, inplace=True)

            assert len(new_cols) == len(df.columns), f'new_cols must be length {len(df.columns)}'
            df.columns = new_cols

        # save dataframe
        df.to_csv(os.path.join(output_path, f'{name}-{country}-{datetime.datetime.now().year}.csv'),
                  index=False)
        print(f'Saved: {country}')

def process_total_generation(country_paths, output_path):
    """
    Prepares the total generation csvs from the ENTOSE database for the data warehouse.

    Input:
        country_paths: dict. dict. country and path to csvs with total generation data
        output_path: str. path to save
    """

    for country, path_in_str in country_paths.items():
        # load dataframe
        df = pd.read_csv(path_in_str)

        # add country name
        df['country_id'] = country

        ## clean column headers
        df.columns = [x[0].strip().lower() for x in df.columns.str.split("-")]

        ## parse datetime and set index as dt object
        df['event_date'] = df['mtu'].apply(lambda x: pd.to_datetime(x.split("-")[0]))
        df = df.drop('mtu', axis=1)
        df = df.set_index('event_date')
        df.index = df.index.tz_localize(tz='Europe/Brussels',
                                        ambiguous='infer',
                                        nonexistent='shift_backward')

        ## Add a timestamp column
        df['ts'] = df.index.asi8

        # get date ranges
        start = df.index.min().strftime('%Y%m%d')
        end = df.index.max().strftime('%Y%m%d')

        ## unpivot data into long format
        df = df.reset_index().melt(id_vars=['event_date', 'ts', 'country_id', 'area'],
                                   var_name='generation_type',
                                   value_name='generation_load')

        ## fill mising values
        df['generation_load'] = df['generation_load'].replace('n/e', 0).astype('float')
        df['generation_load'].fillna(0, inplace=True)

        # save dataframe
        df.to_csv(os.path.join(output_path, f'generation-{country}-{start}-{end}'),
                  index=False)
        print(f'Saved: {country}')


def process_day_ahead_prices(country_paths, output_path):
    """
    Prepares the day ahead prices csvs from the ENTOSE database for the data warehouse.

    Input:
        country_paths: dict. dict. country and path to csvs with total generation data
        output_path: str. path to save

    """

    for country, path_in_str in country_paths.items():
        # load dataframe
        df = pd.read_csv(path_in_str)

        # add country name
        df['country_id'] = country

        ## clean column headers
        df.columns = [x[0].strip().lower() for x in df.columns.str.split(" ")]
        df.rename(columns={'day-ahead': 'day_ahead_price'}, inplace=True)

        ## parse datetime and set index as dt object
        df['event_date'] = df['mtu'].apply(lambda x: pd.to_datetime(x.split("-")[0]))
        df = df.drop('mtu', axis=1)
        df = df.set_index('event_date')
        df.index = df.index.tz_localize(tz='Europe/Brussels',
                                        ambiguous='infer',
                                        nonexistent='shift_backward')

        ## Add a timestamp column
        df['ts'] = df.index.asi8

        ## fill missing values
        df.fillna(0, inplace=True)

        # get date ranges
        start = df.index.min().strftime('%Y%m%d')
        end = df.index.max().strftime('%Y%m%d')

        # save dataframe
        df = df.reset_index()
        df.to_csv(os.path.join(output_path, f'day-ahead-prices-{country}-{start}-{end}'),
                  index=False)
        print(f'Saved: {country}')




if __name__=='__main__':

    root_path = '/Users/ns/github-repos/entsoe-etl-pipeline/data/'
    output_path = '/Users/ns/github-repos/entsoe-etl-pipeline/data/{}/processed'

    print('Preprocessing total demand')
    country_paths = traverse_path(os.path.join(root_path, 'total_demand'), -2)

    new_total_demand_cols = ['event_date', 'total_demand', 'ts', 'country_id']

    process_capacity_demand(country_paths,
                            output_path.format('total_demand'),
                            'demand',
                            new_total_demand_cols)


    print('Preprocessing installed capacity')
    country_paths = traverse_path(os.path.join(root_path, 'installed_capacity'), -3)

    new_install_capacity_cols = ['event_date', 'production_type', 'code',
                                 'name', 'installed_capacity_year_start',
                                 'current_installed_capacity', 'location',
                                 'voltage_connection_level', 'commissioning_date',
                                 'decommissioning_date', 'country_id']

    process_capacity_demand(country_paths,
                            output_path.format('installed_capacity'),
                            'capacity',
                            new_install_capacity_cols)

    print('Preprocessing total generation')
    country_paths = traverse_path(os.path.join(root_path, 'total_generation'), -2)

    process_total_generation(country_paths, output_path.format('total_generation'))


    print('Preprocessing day ahead prices')
    country_paths = traverse_path(os.path.join(root_path, 'day_ahead_prices'), -2)

    process_day_ahead_prices(country_paths, output_path.format('day_ahead_prices'))
