
import os
import sys

from pathlib import Path


def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent


root = get_project_root()
print('root', root)
sys.path.extend([str(root.absolute())])


import ocean_dp.file_name.find_file_with
import ocean_dp.processing.add_density

path = sys.argv[1]
print ('file path : ', path)

pulse_files = ocean_dp.file_name.find_file_with.find_files_pattern(os.path.join(path, "IMOS*FV01*.nc"))

psal_files = ocean_dp.file_name.find_file_with.find_variable(pulse_files, 'PSAL')

#print(psal_files)

for f in psal_files:
    print('processing', f)
    ocean_dp.processing.add_density.add_density(f)
