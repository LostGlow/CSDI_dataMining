import tarfile
import zipfile
import sys
import os
import wget
import requests
import pandas as pd
import pickle
import shutil
import io

os.makedirs("data/", exist_ok=True)
if sys.argv[1] == "physio":
    url = "https://physionet.org/files/challenge-2012/1.0.0/set-a.tar.gz?download"
    wget.download(url, out="data")
    with tarfile.open("data/set-a.tar.gz", "r:gz") as t:
        t.extractall(path="data/physio")

elif sys.argv[1] == "pm25":
    # url = "https://www.microsoft.com/en-us/research/wp-content/uploads/2016/06/STMVL-Release.zip"
    # urlData = requests.get(url).content
    filename = "data/STMVL Release.zip"
    # with open(filename, mode="wb") as f:
        # f.write(urlData)
    with zipfile.ZipFile(filename) as z:
        z.extractall("data/pm25")
        
    def create_normalizer_pm25():
        df = pd.read_csv(
            "./data/pm25/Code/STMVL/SampleData/pm25_ground.txt",
            index_col="datetime",
            parse_dates=True,
        )
        test_month = [3, 6, 9, 12]
        for i in test_month:
            df = df[df.index.month != i]
        mean = df.describe().loc["mean"].values
        std = df.describe().loc["std"].values
        path = "./data/pm25/pm25_meanstd.pk"
        with open(path, "wb") as f:
            pickle.dump([mean, std], f)
    create_normalizer_pm25()

elif sys.argv[1] == "gps_i80":
    base_dir = "data/gps_i80"
    os.makedirs(base_dir, exist_ok=True)

    filename = "data/I-80 Emeryville CA Data.zip"
    if not os.path.exists(filename):
        raise FileNotFoundError(f"{filename} not found. Please download the dataset and place the zip at this path.")

    targets = {
        "trajectories-0400-0415.csv": "trajectories_0400_0415.csv",
        "trajectories-0500-0515.csv": "trajectories_0500_0515.csv",
        "trajectories-0515-0530.csv": "trajectories_0515_0530.csv",
    }

    found_any = False

    # Open the top-level zip and look for the inner vehicle trajectory zips
    with zipfile.ZipFile(filename) as z:
        vehicle_zip_members = [m for m in z.namelist() if m.lower().endswith('.zip') and 'vehicle' in m.lower()]
        for member in vehicle_zip_members:
            try:
                data_bytes = z.read(member)
            except KeyError:
                continue
            
            with zipfile.ZipFile(io.BytesIO(data_bytes)) as inner:
                inner_names = inner.namelist()
                for src_name, out_name in targets.items():
                    out_path = os.path.join(base_dir, out_name)
                    if os.path.exists(out_path):
                        continue
                    matches = [n for n in inner_names if n.lower().endswith(src_name)]
                    if matches:
                        with inner.open(matches[0]) as fsrc, open(out_path, 'wb') as fdst:
                            shutil.copyfileobj(fsrc, fdst)
                        print(f"Extracted {matches[0]} tp {out_path}")
                        found_any = True

        if not found_any:
            top_names = z.namelist()
            for src_name, out_name in targets.items():
                out_path = os.path.join(base_dir, out_name)
                if os.path.exists(out_path):
                    continue
                matches = [n for n in top_names if n.lower().endswith(src_name)]
                if matches:
                    with z.open(matches[0]) as fsrc, open(out_path, 'wb') as fdst:
                        shutil.copyfileobj(fsrc, fdst)
                    print(f"Extracted {matches[0]} to {out_path}")
                    found_any = True

        if not found_any:
            print("Warning: target csvs not found inside the provided zip. Pls check the archive contents.")

    # Remove other zip files in the target directory (if nay left)
    for fname in os.listdir(base_dir):
        if fname.lower().endswith('.zip'):
            try:
                os.remove(os.path.join(base_dir, fname))
            except Exception:
                pass

    # # Verification
    # for out_name in targets.values():
    #     out_path = os.path.join(base_dir, out_name)
    #     if os.path.exists(out_path):
    #         try:
    #             # small sample with pandas for preview
    #             sample = pd.read_csv(out_path, nrows=3)
    #         except Exception as e:
    #             print(f"Could not read sample from {out_path}: {e}")
    #             sample = None

    #         try:
    #             with open(out_path, 'rb') as fh:
    #                 line_count = sum(1 for _ in fh)
    #             rows = max(0, line_count - 1)
    #         except Exception as e:
    #             print(f"Could not count rows for {out_path}: {e}")
    #             rows = 'unknown'

    #         print(f"File: {out_path}  Rows: {rows}")
    #         if sample is not None:
    #             print(sample.to_string(index=False))
    #     else:
    #         print(f"Missing expected CSV: {out_path}")
    