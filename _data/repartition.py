#!/net/ent-home/home/p/your_favorite_user/data-capture-38/bin/python
import pyarrow as pa
import pyarrow.parquet
import os

def repartition(path, partition=None, partitions=None):
    _ = pa.parquet.read_table(path) if partition is None else pa.parquet.read_table(path, filters=[('ym_state_time', 'in', partition)])
    if partitions is None:
        pyarrow.parquet.write_to_dataset(_, root_path=path, existing_data_behavior='delete_matching')
    else:
        pyarrow.parquet.write_to_dataset(_, root_path=path, partition_cols=partitions, existing_data_behavior='delete_matching')
    return None

if __name__ == "__main__":
    root = os.path.join('C:\\', 'Users', 'your_favorite_user', 'Documents', 'nagios', 'data')
    parquetPath = os.path.join(root, 'state-change-objects.parquet')
    for _root, _dir, _files in os.walk(parquetPath):
        for d in _dir:
            dPath = os.path.join(_root, d)
            if len(os.listdir(dPath)) <= 1: continue
            part = [p.split('=')[1] for p in [d, ]]
            repartition(parquetPath, partition=part, partitions=['ym_state_time', ])

