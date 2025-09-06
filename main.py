"""World Development Indicators Connector."""
import os
os.environ['CONNECTOR_NAME'] = 'world-development-indicators'
os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from utils import validate_environment, upload_data
from assets.wdi.wdi import process_wdi

def main():
    validate_environment()
    
    # Process WDI CSV data
    (wdi_data, 
     series_metadata, 
     country_series_metadata, 
     observation_metadata, 
     series_time_metadata) = process_wdi()
    
    # Upload each dataset immediately
    upload_data(wdi_data, "world_development_indicators")
    upload_data(series_metadata, "world_development_indicators_series_metadata")
    upload_data(country_series_metadata, "world_development_indicators_country_series_metadata")
    upload_data(observation_metadata, "world_development_indicators_observation_metadata")
    upload_data(series_time_metadata, "world_development_indicators_series_year_metadata")


if __name__ == "__main__":
    main()