import json
import os
import random
import string


def generate_json_files(folder_path):
    os.mkdir(folder_path)
    # os.mkdir("processed/{}".format(folder_path))
    statistic = {}
    for i in range(0, 10):
        file_path = folder_path + "/" +str(i) + '.json'
        statistic['id'] = random_string(10)
        statistic['companyName'] = random_string(10)
        statistic['other'] = random_string(10)
        statistic['description'] = random_string(50)
        statistic['code'] = random_string(10)
        statistic['address'] = random_string(20)
        statistic['phone'] = random.randint(10, 1000)
        statistic['category'] = random_string(50)
        statistic['rank'] = i
        statistic_json = json.dumps(statistic)
        with open(file_path, 'w') as outfile:
            outfile.write(statistic_json)


def random_string(stringLength=10):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def generate_jsons(folder_paths):
    # os.mkdir("processed")
    for folder_path in folder_paths:
        generate_json_files(folder_path)


generate_jsons(["folder1", "folder2"])
