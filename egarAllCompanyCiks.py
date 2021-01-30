import requests
import json


all_companies = requests.get("https://www.sec.gov/Archives/edgar/cik-lookup-data.txt")
all_companies_content = all_companies.content.decode("latin1")
all_companies_array = all_companies_content.split("\n")
del all_companies_array[-1]
all_companies_array_rev = []
for i, item in enumerate(all_companies_array):
    if item == "":
        continue
    _name, _cik = edgar.Edgar.split_raw_string_to_cik_name(item)
    all_companies_array[i] = (_name, _cik)
    all_companies_array_rev.append((_cik, _name))
all_companies_dict = dict(all_companies_array)
all_companies_dict_rev = dict(all_companies_array_rev)

json = json.dumps(all_companies_dict)
f = open("company_dict.json", "w")
f.write(json)
f.close()