from datetime import datetime, timezone
import time
import os
import sys
import random

print()
NEW_VENV_NAME = "venv_app01" 
NEW_VENV_PATH = f"/home/docker/{NEW_VENV_NAME}"

if __name__ == "__main__":
    time.sleep(start_timeout)
    if len(sys.argv) > 1:
        proc_name = sys.argv[1]
        #  print(f"[{proc_name}]:\tFile saved as: {os.path.basename(filename)} | workdir [{os.path.join(NEW_VENV_PATH, proc_name)}]")
    else:
        proc_name = 'NULL'
        print("No argument provided.")
    try:
        start_timeout = int(proc_name[-1])
    except Exception as e:
        start_timeout = random.randrange(start=0, stop=6, step=2)
        print(f"Input argument should ends with integer, ex. app_01")
    time.sleep(start_timeout)
    current_time = datetime.now(timezone.utc).strftime('%Y_%m_%d_%H-%M-%S')    
    filename = os.path.join(NEW_VENV_PATH, 'temp', f"{current_time}_{proc_name}.txt")
    with open(filename, 'w') as file:
        file.write(f"UTC Time: {current_time}\n")
    sleep_timeout = int(random.randrange(start=5, stop=15, step=5))
    print(f"[{proc_name}]:\tFile saved as: {os.path.basename(filename)} | workdir [{os.path.join(NEW_VENV_PATH, proc_name)}]\nsleep for [{sleep_timeout}] sec.")
    time.sleep(sleep_timeout)
