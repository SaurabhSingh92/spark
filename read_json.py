import json

with open("jobconfig.json", "r") as sample:
    data = json.load(sample)
    for k, v in data['Task'].items():
        print(k)
        print(v)
