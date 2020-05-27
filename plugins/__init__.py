from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class EntsoeETLPlugins(AirflowPlugin):
    name = "entsoe_etl_plugins"
    operators = [
        operators.StageCSVToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.euro_energy_sql_queries.py
    ]
