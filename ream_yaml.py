import yaml
from datetime import timedelta

with open("jobconfig.yaml", 'r') as sample:
    txt = yaml.safe_load(sample)
    print(txt)