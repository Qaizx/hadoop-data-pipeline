# jobs/utils/hdfs.py
from typing import List, Optional


def get_fs(sc):
    uri = sc._jvm.java.net.URI.create("hdfs://namenode:8020")
    conf = sc._jsc.hadoopConfiguration()
    return sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)


def hdfs_ls_recursive(sc, path: str) -> List[str]:
    fs = get_fs(sc)
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(hadoop_path):
        return []
    files = []
    iterator = fs.listFiles(hadoop_path, True)
    while iterator.hasNext():
        files.append(iterator.next().getPath().toString())
    return files


def hdfs_touch(sc, path: str):
    fs = get_fs(sc)
    fs.create(sc._jvm.org.apache.hadoop.fs.Path(path)).close()
    print(f"   ✅ Marker created: {path}")


def extract_year_from_path(path: str) -> Optional[int]:
    import re
    m = re.search(r"year=(\d{4})", path)
    return int(m.group(1)) if m else None