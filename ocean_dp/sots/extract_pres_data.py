
import os
import sys

from pathlib import Path


def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent


root = get_project_root()
print('root', root)
sys.path.extend([str(root.absolute())])


import ocean_dp.file_name.find_file_with

path = sys.argv[1]
print ('file path : ', path)

pulse_files = ocean_dp.file_name.find_file_with.find_files_pattern(os.path.join(path, "IMOS*FV01*.nc"))

pres_files = ocean_dp.file_name.find_file_with.find_variable(pulse_files, 'PRES')

print(pres_files)
