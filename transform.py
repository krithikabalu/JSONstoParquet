import glob
import json
import logging
import os
import multiprocessing as mp
import shutil
import uuid
from functools import reduce

import pandas
from pandas.io.json import json_normalize

log = logging.getLogger(__name__)


def transform_json_to_parquet(sources):
    pool = mp.Pool(mp.cpu_count())
    [pool.apply_async(process_chunk, args=(files, src)) for src, files in get_files_in_paths(sources)]
    pool.close()
    pool.join()


def process_chunk(relative_files, src):
    log.error("Starting")
    files = map(lambda file: os.path.join(src, file), relative_files)
    for chunked_group in chunk(list(files), 10):
        data_frames = list(map(lambda file: json_normalize(read_json(file)), chunked_group))
        merged_data_frame = reduce(join, data_frames, pandas.DataFrame())
        save_success = create_or_update(merged_data_frame, src)
        if save_success:
            delete_files(src)
    log.error("Completed")


def read_json(file):
    with open(file) as f:
        return json.load(f)


def get_files_in_paths(sources):
    return map(lambda src: (src, os.listdir(src)), sources)


def create_or_update(data_frame, src):
    if data_frame.empty:
        return False
    dest = "processed/{}".format(src)
    existing_file = get_latest_existing_file(dest)
    if existing_file and os.path.getsize(existing_file)<1000:
        append_to_parquet(existing_file, data_frame)
    else:
        dest_file = "processed/{}/{}".format(src, str(uuid.uuid4()))
        data_frame.to_parquet(dest_file)
    return True


def append_to_parquet(existing_file, data_frame):
    existing_data_frame = pandas.read_parquet(existing_file)
    result_data_frame = reduce(join, [existing_data_frame, data_frame], pandas.DataFrame())
    result_data_frame.to_parquet(existing_file)


def get_latest_existing_file(path):
    list_of_files = glob.glob(path+"/*")
    if list_of_files:
        return max(list_of_files, key=os.path.getctime)
    return None


def chunk(array, chunk_size):
    result = [array[i * chunk_size:(i + 1) * chunk_size]
              for i in range((len(array) + chunk_size - 1) // chunk_size)]
    return result


def join(data_frame, new_data_frame):
    return pandas.concat([data_frame, new_data_frame], sort=True, ignore_index=True) \
        .astype(str)


def delete_files(path):
    shutil.rmtree(path)


transform_json_to_parquet(["folder1", "folder2"])
