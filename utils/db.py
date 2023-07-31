import json
import pandas as pd

def load_electricity_data(json_filepath)->pd.DataFrame:
    """Loads the electricity data to a pandas DataFrame."""
    f = open(json_filepath)
    data = json.load(f)
    f.close()
    return(pd.DataFrame(data['response']['data']))