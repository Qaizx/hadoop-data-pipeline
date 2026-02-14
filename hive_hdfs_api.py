from fastapi import FastAPI, UploadFile, File, HTTPException
import subprocess
import os
import shutil
import uuid

app = FastAPI(title="Hive + HDFS API")

CONTAINER = "hive-server"
TMP_DIR = "./tmp"

os.makedirs(TMP_DIR, exist_ok=True)


def run_hdfs_cmd(args: list[str]) -> str:
    """
    run hdfs dfs command inside hive-server container
    """
    cmd = ["docker", "exec", CONTAINER, "hdfs", "dfs"] + args

    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())

    return result.stdout.strip()


# -------------------------
# 1️⃣ List files
# -------------------------
@app.get("/hdfs/list")
def list_hdfs(path: str = "/data"):
    try:
        output = run_hdfs_cmd(["-ls", path])
        return {"path": path, "raw": output}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------
# 2️⃣ Upload file
# -------------------------
@app.post("/hdfs/upload")
def upload_to_hdfs(
    file: UploadFile = File(...),
    hdfs_path: str = "/data"
):
    tmp_name = f"{uuid.uuid4()}_{file.filename}"
    local_path = os.path.join(TMP_DIR, tmp_name)

    try:
        # save temp file
        with open(local_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        # copy into container
        subprocess.run(
            ["docker", "cp", local_path, f"{CONTAINER}:/tmp/{tmp_name}"],
            check=True
        )

        # put to HDFS
        run_hdfs_cmd(["-put", "-f", f"/tmp/{tmp_name}", hdfs_path])

        return {
            "message": "Upload successful",
            "hdfs_path": f"{hdfs_path}/{file.filename}"
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        if os.path.exists(local_path):
            os.remove(local_path)


# -------------------------
# 3️⃣ Delete file
# -------------------------
@app.delete("/hdfs/delete")
def delete_hdfs(path: str):
    try:
        run_hdfs_cmd(["-rm", "-f", path])
        return {"message": "Deleted", "path": path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
