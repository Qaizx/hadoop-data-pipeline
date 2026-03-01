# tests/test_versioning.py
"""
ทดสอบ versioning module:
1. create_version — สร้าง snapshot + metadata
2. list_versions — เรียงจากใหม่ไปเก่า
3. cleanup_old_versions — เก็บแค่ N version ล่าสุด
4. restore_version — restore ผ่าน atomic write
"""

import json
from unittest.mock import MagicMock, patch


# ── Helpers ───────────────────────────────────────────────────────
def make_mock_sc():
    sc = MagicMock()
    sc._jvm.java.net.URI.create.return_value = MagicMock()
    sc._jsc.hadoopConfiguration.return_value = MagicMock()
    sc._jvm.org.apache.hadoop.fs.Path.side_effect = lambda p: p

    fs = MagicMock()
    fs.exists.return_value = False
    fs.rename.return_value = True
    # checksum ต้อง return string ไม่งั้น json.dumps fail
    fs.getFileChecksum.return_value.toString.return_value = "mock_checksum_abc123"
    sc._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs
    return sc, fs


def make_mock_df(row_count=100, columns=None):
    df = MagicMock()
    df.count.return_value = row_count
    df.columns = columns or ["date", "details", "year", "total_amount"]
    df.sparkSession.sparkContext = make_mock_sc()[0]
    df.write.mode.return_value.parquet = MagicMock()
    return df


def make_version_metadata(version_id: str, timestamp: str) -> dict:
    return {
        "version": version_id,
        "source_file": "finance_2024.csv",
        "year": 2024,
        "timestamp": timestamp,
        "row_count": 100,
        "checksum": "abc123",
        "columns": ["date", "details", "year"],
        "keep_versions": 5,
    }


# ── Test 1: create_version เขียน parquet + metadata ──────────────
def test_create_version_writes_parquet_and_metadata():
    sc, fs = make_mock_sc()
    df = make_mock_df()

    written_metadata = {}

    def mock_create(path):
        stream = MagicMock()
        def capture_write(data):
            written_metadata.update(json.loads(data.decode("utf-8")))
        stream.write.side_effect = capture_write
        return stream

    fs.create.side_effect = mock_create

    with patch("utils.versioning.datetime") as mock_dt:
        mock_dt.now.return_value.strftime.return_value = "v_20260301_120000"
        mock_dt.now.return_value.isoformat.return_value = "2026-03-01T12:00:00"

        from utils.versioning import create_version
        version_id = create_version(sc, df, "/raw/finance_2024.csv", year=2024)

    assert version_id == "v_20260301_120000"
    assert written_metadata["year"] == 2024
    assert written_metadata["row_count"] == 100
    assert written_metadata["source_file"] == "finance_2024.csv"


# ── Test 2: list_versions เรียงจากใหม่ไปเก่า ──────────────────────
def test_list_versions_sorted_newest_first():
    sc, fs = make_mock_sc()

    meta_v1 = make_version_metadata("v_20260101_000000", "2026-01-01T00:00:00")
    meta_v2 = make_version_metadata("v_20260201_000000", "2026-02-01T00:00:00")
    meta_v3 = make_version_metadata("v_20260301_000000", "2026-03-01T00:00:00")

    with patch("utils.hdfs.hdfs_ls_recursive") as mock_ls, \
         patch("utils.versioning._read_file") as mock_read:

        mock_ls.return_value = [
            "/versions/year=2024/v_20260101_000000/_version.json",
            "/versions/year=2024/v_20260201_000000/_version.json",
            "/versions/year=2024/v_20260301_000000/_version.json",
        ]
        mock_read.side_effect = [
            json.dumps(meta_v1),
            json.dumps(meta_v2),
            json.dumps(meta_v3),
        ]

        from utils.versioning import list_versions
        versions = list_versions(sc, year=2024)

    assert versions[0]["version"] == "v_20260301_000000"  # ใหม่สุดก่อน
    assert versions[1]["version"] == "v_20260201_000000"
    assert versions[2]["version"] == "v_20260101_000000"


# ── Test 3: cleanup เก็บแค่ N version ล่าสุด ─────────────────────
def test_cleanup_keeps_only_n_versions():
    sc, fs = make_mock_sc()
    fs.exists.return_value = True

    versions = [
        make_version_metadata(f"v_2026030{i}_000000", f"2026-03-0{i}T00:00:00")
        for i in range(1, 8)  # 7 versions
    ]
    # เรียงจากใหม่ไปเก่า
    versions_sorted = list(reversed(versions))

    with patch("utils.versioning.list_versions", return_value=versions_sorted), \
         patch("utils.versioning._hdfs_delete") as mock_delete:

        from utils.versioning import cleanup_old_versions
        cleanup_old_versions(sc, year=2024, keep=5)

    # ลบแค่ 2 อัน (7 - 5 = 2)
    assert mock_delete.call_count == 2


# ── Test 4: cleanup ไม่ลบถ้า version น้อยกว่า N ──────────────────
def test_cleanup_no_delete_when_under_limit():
    sc, _ = make_mock_sc()

    versions = [
        make_version_metadata(f"v_2026030{i}_000000", f"2026-03-0{i}T00:00:00")
        for i in range(1, 4)  # 3 versions
    ]

    with patch("utils.versioning.list_versions", return_value=versions), \
         patch("utils.versioning._hdfs_delete") as mock_delete:

        from utils.versioning import cleanup_old_versions
        cleanup_old_versions(sc, year=2024, keep=5)

    mock_delete.assert_not_called()


# ── Test 5: restore_version ใช้ atomic_write_table ────────────────
def test_restore_version_uses_atomic_write():
    sc, _ = make_mock_sc()
    spark = MagicMock()
    spark.sparkContext = sc
    spark.read.parquet.return_value = make_mock_df()

    with patch("utils.versioning.atomic_write_table") as mock_atomic:
        from utils.versioning import restore_version
        restore_version(
            spark,
            version_id="v_20260215_090000",
            year=2024,
            target_table="finance_itsc_wide",
            target_path="hdfs://namenode:8020/datalake/staging/finance-itsc_wide",
        )

    mock_atomic.assert_called_once()
    call_kwargs = mock_atomic.call_args.kwargs
    assert call_kwargs["table_name"] == "finance_itsc_wide"
    assert call_kwargs["partition_value"] == 2024


# ── Test 6: restore_version อ่าน parquet จาก path ที่ถูกต้อง ──────
def test_restore_version_reads_correct_path():
    sc, _ = make_mock_sc()
    spark = MagicMock()
    spark.sparkContext = sc
    spark.read.parquet.return_value = make_mock_df()

    with patch("utils.versioning.atomic_write_table"):
        from utils.versioning import restore_version
        restore_version(
            spark,
            version_id="v_20260215_090000",
            year=2024,
            target_table="finance_itsc_wide",
            target_path="hdfs://namenode:8020/datalake/staging/finance-itsc_wide",
        )

    expected_path = "hdfs://namenode:8020/datalake/versions/finance-itsc/year=2024/v_20260215_090000"
    spark.read.parquet.assert_called_once_with(expected_path)