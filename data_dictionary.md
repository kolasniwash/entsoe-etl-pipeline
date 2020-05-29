
## Data Dictionary

#### energy_loads
- Table name: ```energy_loads```
- Type: facts table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `event_date` | `TIMESTAMP NOT NULL` | Full timestamp of when event occurred |
| `country_id` | `VARCHAR(256)` | Two letter country code  |
| `generation_type` | `VARCHAR(256)` | Type of electrical generation i.e. Solar, Coal, etc.  |
| `day_ahead_price` | `NUMERIC(8,2)` | Price in euros for day ahead settlement market |
| `demand_load` | `NUMERIC(12,2)` | Electrical energy demaned in metawatts |
| `generation_load` | `NUMERIC(12,2)` | Eletrical energy generated in megawatts |

#### installed_capacity
- Table name: ```installed_capacity```
- Type: dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `generation_type` | `VARCHAR(256) NOT NULL` | Type of electrical generation i.e. Solar, Coal, etc. |
| `station_name` | `VARCHAR(256) NOT NULL` | Name of the generation plant |
| `country_id` | `VARCHAR(256) NOT NULL` | Two letter country code |
| `installed_capacity` | `INT4` | Generation capacity in megawatts (MW)  |
| `connection_voltage` | `INT4` | Transmission connection voltage in Volts. |
| `commission_date` | `VARCHAR(256)` | Date plant started operations  |
| `control_area` | `VARCHAR(256)` | Market control area within transmission system |
| `code` | `VARCHAR(256)` | Generation station identifier code |

#### countries
- Table name: ```countries```
- Type: dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `country_id` | `VARCHAR(256)` | Two letter country code |
| `country_name` | `VARCHAR(256)` | Country full name  |
| `latitude` | `NUMERIC(18,0)` | Latitude of national capital |
| `longitude` | `NUMERIC(18,0)` | Longitude of national capital  |

#### times 
- Table name: ```times```
- Type: dimension table

| Column | Type | Description |
| ------ | ---- | ----------- |
| `event_date` | `TIMESTAMP NOT NULL` | Full timestamp of when event occurred |
| `year` | `INT2` | Year event occurred  |
| `month` | `INT2` | Month event occurred  |
| `day` | `INT2` | Day event occurred  |
| `hour` | `INT2` | Hour event occurred |
| `minute` | `INT2` | Minute event occurred  |
| `dayofweek` | `INT2` | Day of week event occurred (i.e. 1 for monday) |

