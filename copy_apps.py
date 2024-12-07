import os
import json

workdir = os.path.dirname(os.path.realpath(__file__))

def copy_files(src_path, dest_path):
  os.makedirs(os.path.join(workdir, 'apps'), exist_ok=True)
  os.makedirs(dest_path, exist_ok=True)
  for file_name in os.listdir(src_path):
    src_file = os.path.join(src_path, file_name)
    if os.path.isfile(src_file) and (file_name.endswith(".py") or file_name.endswith(".yml")):
      dest_file = os.path.join(dest_path, file_name)
      with open(src_file, 'rb') as src, open(dest_file, 'wb') as dest:
        dest.write(src.read())  # Copy file content
      print(f"Copied: {src_file} to {dest_file}")
  print("All matching files copied.")

fpath = os.path.join(workdir, 'configs', 'vendors_json.txt')
with open(fpath, 'r') as fp:
  vendors_json = json.load(fp)
vendors_conf = os.path.join(workdir, 'apps', 'vendors_json.txt')
os.system(f'copy "{fpath}" "{vendors_conf}"')

for i, vendor_json in enumerate(vendors_json):
  if i > 0:
    dest_path = os.path.join(workdir, 'apps', vendor_json['alias'])
    if len(vendor_json['main_py']) > 0:
      app_path, app_main_py = vendor_json['main_py'].split('/')[1], vendor_json['main_py'].split('/')[2]
      src_path = os.path.join(workdir, app_path)
      main_path = os.path.join(workdir, app_path, app_main_py)
      copy_files(src_path, dest_path)
      long_main_name = os.path.join(dest_path, vendor_json['main_py'].split('/')[2])
      main_path = os.path.join(dest_path, 'main.py')
      if os.path.exists(long_main_name) and not os.path.exists(main_path):
        os.rename(long_main_name, main_path)
      vend_conf_path = os.path.join(dest_path, f"data_{vendor_json['vendor_code']}_json.txt")
      with open(fpath, 'w') as fp:
        vendors_json = json.dump(vendor_json, fp, indent=2)