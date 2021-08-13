import os
import pathlib
import datetime as dt
import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd

county_population_file_name = 'covid_county_population_usafacts.csv'
covid_file_path = 'covid_data_url.txt'
source = 'usafacts.org'
download_text = 'Download Data'
headers = {'User-Agent': 'Mozilla/5.0'}
county_file_path = 'county_coordinates_url.txt'
county_file_outpath = 'county_coordinates.csv'
private_key_file = 'tomorrowio_api_key.txt'
# weather_api_url = 'https://api.tomorrow.io/v4/timelines?location={},{}&fields={}&units={}&apikey={}'

def read_and_update_covid_files(covid_file_path):
    with open(covid_file_path, 'r') as in_file:
        covid_url = in_file.read().strip()

    response = requests.get(covid_url)

    if response.status_code != 200:
        raise Exception("Oh no, non-200 status code from {}".format(source))

    bsobj = BeautifulSoup(response.content, 'html.parser')

    potential_section_list = bsobj.find_all('div', {'class': 'jss154'})

    links = []
    for line in potential_section_list:
        if download_text in line.text:
            potential_links = line.find_all('a', href=True)
            for a_ in potential_links:
                links.append(a_['href'])
            break

    for l in links:
        response = requests.get(l, headers=headers)
        out_file_name = l.split('/')[-1]
        assert os.path.exists(out_file_name), f'No such file: {out_file_name}'
        if out_file_name != county_population_file_name and \
                dt.datetime.fromtimestamp(pathlib.Path(out_file_name).stat().st_mtime) <= \
                dt.datetime.now() - dt.timedelta(seconds=84600):
            with open(out_file_name, 'w') as out_file:
                out_file.write(response.text)
                print("Wrote to file {}".format(out_file_name))


def read_and_write_county_data(county_file_path, county_file_outpath):
    with open(county_file_path, 'r') as in_file:
        county_url = in_file.read().strip()

    response = requests.get(county_url)

    if response.status_code != 200:
        raise Exception("Oh no, non-200 status code from {}".format(source))

    bsobj = BeautifulSoup(response.content, 'html.parser')

    table_obj = bsobj.find('table')

    rows = table_obj.find_all('tr')
    table = []
    for row in rows:
        cols = row.find_all('td')
        cols = [item.text.strip() for item in cols]
        if len(cols) > 0:
            table.append(cols)
        else:
            headers = row.find_all('th')
            header = [h.text.strip() for h in headers]
            table.append(header)

    df_county = pd.DataFrame(table[1:], columns=table[0])
    df_county.to_csv(county_file_outpath)

def combine_covid_county_data():
    county_pop_data_fname = 'covid_county_population_usafacts.csv'
    county_coordinate_data_fname = 'county_coordinates.csv'
    df_pop = pd.read_csv(county_pop_data_fname)
    df_coord = pd.read_csv(county_coordinate_data_fname)

    df_coord['County'] = df_coord['County'] + ' County'
    df_all = pd.merge(df_pop, df_coord, left_on=['State', 'County Name'], right_on=['State', 'County'])
    df_all.drop(['County Name', 'Sort', 'countyFIPS'], axis=1, inplace=True)
    df_all['Population(2010)'] = df_all['Population(2010)'].str.replace(',', '').apply(int)
    df_all.to_csv('county_all.csv', index=False)

# weather_api_url = 'https://api.tomorrow.io/v4/timelines?location={},{}&fields={}&timesteps={}&units={}&apikey={}'
# row = 568

# TEMP for testing
def get_tomorrowio_temperature_average_now(latitude, longitude):
    # latitude = df_all.iloc[row]
    # longitude = df_all.iloc[row]

    fields = 'temperature'
    timesteps = '1d'
    units = 'metric'
    with open(private_key_file, 'r') as pkey_file:
        apikey = pkey_file.read().strip()

    response = requests.get(weather_api_url.format(latitude, longitude, fields, units, apikey))
    if response.status_code != 200:
        return np.nan
    df = pd.DataFrame(response.json()['data']['timelines'][0])

    df['temperature'] = df['intervals'].apply(lambda row: row['values']['temperature'])
    return df.temperature.iloc[:4].mean()


def get_n_counties_of_weather_tomorrowio(n):
    df_all = pd.read_csv('county_all.csv')
    df_all.sort_values('population', inplace=True, ascending=False)
    df_all.reset_index(drop=True, inplace=True)
    # df_all[['population', 'County', 'Latitude', 'Longitude']].head(10)

    df_meshed = df_all[['State', 'population', 'County', 'Latitude', 'Longitude']]
    df_meshed['temperature_08-09-21'] = np.nan

    for i, row in enumerate(df_meshed.iterrows()):
        if i > n:
            break
        else:
            df_meshed['temperature_08-09-21'].iloc[i] = get_tomorrowio_temperature_average_now( \
                    round(df_meshed['Latitude'].iloc[i], 3), round(float(df_meshed['Longitude'].iloc[i].replace('–', '-')), 3))

    # df_meshed.to_csv('county_historic_temperatures.csv', index=False)
    return df_meshed


# north = 41.90
# west = -87.65
# south = 41.88
# east = -87.63
def get_weather_by_county():
    df_temp = pd.read_csv('county_historic_temperatures.csv')
    df_covid = pd.read_csv('covid_confirmed_usafacts.csv')
    dates = df_covid.loc[:, '2020-09-01':'2021-08-01'].columns

    weather_api_url = 'https://ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries&startDate={}&endDate={}&dataTypes=PRCP,TMAX,TMIN&stations={}&format=json'
    start = dates[0]
    end = dates[-1]

    for i, row in df_temp.iterrows():
        if not pd.isnull(row['station_code']):
            response = requests.get(weather_api_url.format(start,end,row['station_code']))
            if response.status_code == 200:
                df_out = pd.DataFrame(response.json())
                if 'TMAX' in df_out.columns and 'TMIN' in df_out.columns:
                    df_out.to_csv(f'{row["County"]}-{row["State"]}_historic_weather.csv', index=False)
                    print(f'Wrote to {row["County"]}-{row["State"]}_historic_weather.csv')

### Station Lookups
# station_location_url = 'https://ncdc.noaa.gov/cdo-web/datatools/selectlocation'


if __name__ == '__main__':
    # read_and_update_covid_files(covid_file_path)
    get_weather_by_county()
