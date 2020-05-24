import time
import pandas as pd
from entsoe import EntsoePandasClient
import os
from pathlib import Path
from entsoe.exceptions import NoMatchingDataError


"""
Script to download generation data per country and generation type from ENTOSE API.

"""



client = EntsoePandasClient(api_key=os.environ['ENTSOE_TOKEN'])

def make_total_generation_query(country, start, end, psr_type):
    start = pd.Timestamp(start, tz='Europe/Brussels')
    end = pd.Timestamp(end, tz='Europe/Brussels')
    data = client.query_generation(country, start=start, end=end, psr_type=psr_type)
    return data


def get_5_years_generation(country, years, start_year, psr_type):
    generation = dict()

    for year in range(years):
        for part in range(2):
            if part == 0:
                start = f'20{start_year + year}0101'
                end = f'20{start_year + year}0701'
            elif part == 1:
                start = f'20{start_year + year}0701'
                end = f'20{start_year + year + 1}0101'
            print(f'year_{year}_{part} start: {start} end: {end} psr_type: {psr_type}')
            try:
                generation[f'{psr_type}_year_{year}_{part}'] = make_total_generation_query(country, start, end, psr_type)
                time.sleep(90)
            except NoMatchingDataError as error:
                print(f'No data for {country} {psr_type} {start} {end}')
                print(error)
                time.sleep(90)
    # combine values into one column
    return pd.concat([df for df in generation.values()], axis=0)


def get_all_generation_types(country, psr_types):
    all_generation = dict()

    for psr_type in psr_types.keys():
        try:
            all_generation[psr_type] = get_5_years_generation(country, 5, 15, psr_type)
        except:
            time.sleep(700)

    return pd.concat([df for df in all_generation.values()], axis=1)


def total_generation_load_save(country, psr_types, start='20150101', end='20200101'):
    time.sleep(90)

    print(f'Downloading {country}')

    data = get_all_generation_types(country, psr_types)

    data['timestamp'] = data.index.asi8
    data.reset_index(inplace=True)

    path = f'/Users/ns/github-repos/entsoe-etl-pipeline/data/total_generation/{country}'
    Path(path).mkdir(parents=True, exist_ok=True)

    file_name = f'total_generation_{country}_{start}_{end}.csv'

    print(f'Downloaded: {country}')
    print(f'Total rows: {len(data)}')

    data.to_csv(os.path.join(path, file_name), index=False)
    print(f'Saved as: {file_name}')
    print("")


def main():
    generation_countries = ['FR', 'BE', 'PL', 'NL', 'IE', 'FL', 'NO', 'PT']

    psr_types = {'B01': 'Biomass',
                 'B02': 'Fossil Brown coal/Lignite',
                 'B03': 'Fossil Coal-derived gas',
                 'B04': 'Fossil Gas',
                 'B05': 'Fossil Hard coal',
                 'B06': 'Fossil Oil',
                 'B07': 'Fossil Oil shale',
                 'B08': 'Fossil Peat',
                 'B09': 'Geothermal',
                 'B10': 'Hydro Pumped Storage',
                 'B11': 'Hydro Run-of-river and poundage',
                 'B12': 'Hydro Water Reservoir',
                 'B13': 'Marine',
                 'B14': 'Nuclear',
                 'B15': 'Other renewable',
                 'B16': 'Solar',
                 'B17': 'Waste',
                 'B18': 'Wind Offshore',
                 'B19': 'Wind Onshore',
                 'B20': 'Other'}


    time.sleep(90)

    for country in generation_countries:
        try:
            total_generation_load_save(country, psr_types)
            time.sleep(90)
        except:
            print(f'Error downloading {country}')
            time.sleep(900)


if __name__ == '__main__':
    main()
