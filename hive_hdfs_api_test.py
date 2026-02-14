from fastapi import FastAPI, UploadFile, File, HTTPException
import subprocess
import shutil
import os
from typing import List
import requests

NAMENODE = "http://localhost:9870"  # webhdfs port
app = FastAPI(title="HDFS API")

@app.get("/hdfs/list")
def list_hdfs(path: str = "/data"):
    url = f"{NAMENODE}/webhdfs/v1{path}"
    r = requests.get(url, params={
        "op": "LISTSTATUS",
        "user.name": "root"
    })
    return r.json()
