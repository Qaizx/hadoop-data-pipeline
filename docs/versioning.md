# Data Versioning

ทุกครั้งที่ ETL pipeline ประมวลผล CSV สำเร็จ จะสร้าง snapshot ไว้ใน HDFS อัตโนมัติ
เก็บไว้ **5 version ล่าสุด** ต่อปี สามารถ rollback ได้ทุกเมื่อ

## HDFS Structure

```
/datalake/versions/finance-itsc/
└── year=2024/
    ├── v_20260301_120000/
    │   ├── part-00000.parquet   ← snapshot ข้อมูล
    │   └── _version.json        ← metadata
    └── v_20260215_090000/
        ├── part-00000.parquet
        └── _version.json
```

## _version.json ตัวอย่าง

```json
{
  "version": "v_20260301_120000",
  "source_file": "finance_2024.csv",
  "year": 2024,
  "timestamp": "2026-03-01T12:00:00",
  "row_count": 1500,
  "checksum": "abc123def456",
  "columns": ["date", "details", "year", "total_amount", "..."],
  "keep_versions": 5
}
```

## Common Operations

### ดู versions ทั้งหมดของปี

```python
from utils.versioning import list_versions

versions = list_versions(sc, year=2024)
for v in versions:
    print(f"{v['version']} | {v['timestamp']} | rows={v['row_count']}")

# Output:
# v_20260301_120000 | 2026-03-01T12:00:00 | rows=1500
# v_20260215_090000 | 2026-02-15T09:00:00 | rows=1480
```

### Rollback ไป version เก่า

```python
from utils.versioning import restore_version

restore_version(
    spark,
    version_id="v_20260215_090000",   # version ที่อยากกลับไป
    year=2024,
    target_table="finance_itsc_wide",
    target_path="hdfs://namenode:8020/datalake/staging/finance-itsc_wide",
)
```

> ✅ ใช้ atomic write — ข้อมูลปีอื่นไม่โดนแตะ ถ้า restore fail ข้อมูลเดิมยังอยู่ครบ

### เปลี่ยนจำนวน version ที่เก็บ

ใน `.env`:
```env
KEEP_VERSIONS=10
```

หรือระบุตอนเรียก:
```python
from utils.versioning import cleanup_old_versions
cleanup_old_versions(sc, year=2024, keep=10)
```

## Config

| Variable | Default | Description |
|----------|---------|-------------|
| `KEEP_VERSIONS` | 5 | จำนวน version ที่เก็บต่อปี |
| `VERSIONS_BASE_PATH` | `/datalake/versions/finance-itsc` | HDFS path สำหรับเก็บ versions |