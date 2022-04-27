import os
import shutil
import sys

from netCDF4 import Dataset, date2num, num2date

from datetime import datetime, timedelta
import matplotlib.pyplot as plt

import numpy as np
from scipy.interpolate import interp1d
import statsmodels.api as sm

np.set_printoptions(linewidth=256)


def make_depth(files):

    ds = Dataset(files[0], 'a')
    ds.set_auto_mask(False)

    n_depths_var = ds.variables['DEPTH_PRES']

    pres_var = ds.variables['PRES']

    pres = pres_var[:]

    n_depths = n_depths_var[:]
    n_depths[n_depths > 10000] = np.NaN

    print('PRES shape', pres_var.shape)

    # ok its ugly, interpolate pressure from another sensor where there is missing data from nearest sensor
    p_any_nan = np.any(np.isnan(pres), axis=1)
    p_all_nan = np.all(np.isnan(pres), axis=1)

    print("pressures (all nan) where there are pressure obs", n_depths[~p_all_nan])
    print("pressures (any nan) need filling", n_depths[p_any_nan & ~p_all_nan])

    for needs_filling_i in range(n_depths.shape[0]):
        dist = np.empty_like(n_depths) + np.nan

        print(needs_filling_i, p_any_nan[needs_filling_i], ~p_all_nan[needs_filling_i])
        if p_any_nan[needs_filling_i] & ~p_all_nan[needs_filling_i] & ~np.isnan(n_depths[needs_filling_i]):
            print(n_depths[needs_filling_i])
            # now we need to find a pressure which has a record where we have data
            have_data = ~np.isnan(pres[needs_filling_i, :])
            #print('have_data', have_data)
            p_all_not_nan = np.all(~np.isnan(pres[:, have_data]), axis=1)
            #print('p_all_not_nan', p_all_not_nan)
            # find nearest sensor with data where we have data
            min_dist = 10000
            source_idx = -1
            for source_candidate_i in range(n_depths.shape[0]):
                if p_all_not_nan[source_candidate_i] and (needs_filling_i != source_candidate_i):
                    print('candidate for fitting', n_depths[source_candidate_i], 'to', n_depths[needs_filling_i])
                    dist[source_candidate_i] = abs(n_depths[needs_filling_i] - n_depths[source_candidate_i])
                    if min_dist > dist[source_candidate_i]:
                        min_dist = dist[source_candidate_i]
                        source_idx = source_candidate_i
            print('distance', dist)
            print('min', min_dist, source_idx)
            print('source presssure', pres[source_idx, have_data])
            print('source fit presssure', pres[needs_filling_i, have_data])
            A = np.vstack([pres[source_idx, have_data], np.ones(len(pres[source_idx, have_data]))]).T
            print('A', A.shape)
            m, c = np.linalg.lstsq(A, pres[needs_filling_i, have_data], rcond=None)[0]
            print('fitting', m, c)
            B = np.vstack([pres[source_idx, ~have_data], np.ones(len(pres[source_idx, ~have_data]))]).T
            print('B', B.shape)
            p_fill = B[:, 0]*m + c
            print('p_fill', p_fill)
            pres[needs_filling_i, ~have_data] = p_fill
            #p_fill = np.interp(pres[needs_filling_i, have_data], pres[source_candidate_i, have_data], )

    # add a point a 0 and 5000 dbar depth for when no pressure at top of bottom
    p_nd = np.concatenate(([0], n_depths, [5000]))
    pres_fill = np.insert(pres, 0, 0, axis=0) # add 0 to first
    pres_fill = np.insert(pres_fill, pres_fill.shape[0], 5000, axis=0) # add 5000 to end

    print('pres shape', pres.shape, pres_fill.shape)

    print("extended nominal depths", p_nd)

    nominal_depth_var = ds.variables['DEPTH_FILE_NAME']
    nominal_depth = nominal_depth_var[:]

    # create a depth array, from filled data and each nominal depth
    depth = np.empty([nominal_depth.shape[0], pres_var.shape[1]])
    print('depth shape', depth.shape)

    # for each timestep, fill the depth record from the pressure observations
    for needs_filling_i in range(pres_var.shape[1]):
        p = pres_fill[:, needs_filling_i]

        # should fill pressure first, with pressure record fitted
        pres_msk = ~np.isnan(p)

        #print('shape', pres_msk.shape, pres_touse.shape)
        depth[:, needs_filling_i] = np.interp(nominal_depth, p_nd[pres_msk], p[pres_msk])

    if 'PRES_ALL' not in ds.variables:
        depth_out_var = ds.createVariable("PRES_ALL", 'f4', ['INSTANCE_FILE_NAME', 'TIME'], fill_value=np.NaN, zlib=True)
    else:
        depth_out_var = ds.variables['PRES_ALL']

    depth_out_var[:] = depth
    depth_out_var.units = pres_var.units

    # update the history attribute
    try:
        hist = ds.history + "\n"
    except AttributeError:
        hist = ""

    ds.setncattr('history', hist + datetime.utcnow().strftime("%Y-%m-%d") + " : added filled depth record for each instrument measurement")

    ds.close()


if __name__ == "__main__":

    make_depth(sys.argv[1:])
