# jobs/utils/hdfs.py
from typing import List, Optional

from logger import get_logger

log = get_logger(__name__)


def get_fs(sc):
    uri = sc._jvm.java.net.URI.create("hdfs://namenode:8020")
    conf = sc._jsc.hadoopConfiguration()
    return sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)


def hdfs_ls_recursive(sc, path: str) -> List[str]:
    fs = get_fs(sc)
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(hadoop_path):
        log.warning("HDFS path not found", path=path)
        return []
    files = []
    iterator = fs.listFiles(hadoop_path, True)
    while iterator.hasNext():
        files.append(iterator.next().getPath().toString())
    log.debug("HDFS listing complete", path=path, total_files=len(files))
    return files


def hdfs_touch(sc, path: str):
    fs = get_fs(sc)
    fs.create(sc._jvm.org.apache.hadoop.fs.Path(path)).close()
    log.info("Marker created", path=path)


def extract_year_from_path(path: str) -> Optional[int]:
    import re
    m = re.search(r"year=(\d{4})", path)
    if m:
        return int(m.group(1))
    log.warning("Could not extract year from path", path=path)
    return None