#!/home/your_favorite_user/data-capture-38/bin/python
import pyarrow as pa
import pyarrow.parquet
import os

def deduplicate(oldPath, newPath, partition=None, partitions=None):
    _0 = pa.parquet.read_table(oldPath) if partition is None else pa.parquet.read_table(oldPath, filters=[('ym_state_time', 'in', partition)])
    _1 = _0.to_pandas().drop_duplicates()
    _2 = pa.Table.from_pandas(_1)
    if partitions is None:
        pyarrow.parquet.write_to_dataset(_2, root_path=newPath, existing_data_behavior='delete_matching')
    else:
        pa.parquet.write_to_dataset(_2, root_path=newPath, partition_cols=partitions, existing_data_behavior='delete_matching')
    return None

if __name__ == "__main__":
    root = os.path.join('/data/nagios')
    parquetPath = os.path.join(root, 'state-change-objects.parquet')
    newParquetPath = os.path.join(root, 'dedup-sco.parquet')
    for _root, _dir, _files in os.walk(parquetPath):
        for d in _dir:
            dPath = os.path.join(_root, d)
            if len(os.listdir(dPath)) <= 1: continue
            part = [p.split('=')[1] for p in [d, ]]
            deduplicate(parquetPath, newParquetPath, partition=part, partitions=['ym_state_time', ])


