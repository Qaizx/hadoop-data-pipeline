"""
Structured logger สำหรับ Finance ITSC Pipeline
ใช้ loguru — output ไป console (plain) และ file (JSON)
"""

import sys
from pathlib import Path
from loguru import logger

LOG_DIR = Path("/jobs/logs")
LOG_DIR.mkdir(exist_ok=True)


def get_logger(name: str):
    """
    สร้าง logger สำหรับแต่ละ module

    Usage:
        from logger import get_logger
        log = get_logger(__name__)
        log.info("ETL started", rows=1000, file="finance_2024.csv")
    """
    log = logger.bind(module=name)
    return log


def setup_logger(name: str = "app"):
    """
    ตั้งค่า logger ทั้งระบบ — เรียกครั้งเดียวตอน startup
    """
    logger.remove()  # ลบ default handler

    # ── Console: plain text อ่านง่าย ──────────────────────────────
    logger.add(
        sys.stdout,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{extra[module]}</cyan> | "
            "{message}"
        ),
        level="DEBUG",
        colorize=True,
    )

    # ── File: JSON สำหรับ query / filter ──────────────────────────
    logger.add(
        LOG_DIR / f"{name}.log",
        format="{time} | {level} | {extra[module]} | {message} | {extra}",
        level="INFO",
        rotation="10 MB",   # rotate ทุก 10 MB
        retention="30 days", # เก็บ 30 วัน
        compression="zip",   # compress ไฟล์เก่า
        serialize=True,      # output เป็น JSON
        encoding="utf-8",
    )

    # ── Error file: เก็บเฉพาะ ERROR ขึ้นไป ───────────────────────
    logger.add(
        LOG_DIR / f"{name}.error.log",
        level="ERROR",
        rotation="5 MB",
        retention="60 days",
        compression="zip",
        serialize=True,
        encoding="utf-8",
    )

    return get_logger(name)