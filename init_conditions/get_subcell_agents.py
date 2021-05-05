#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
import os
import math

import numpy as np
import pandas as pd
import quilt3
from matplotlib import pyplot as plt

from lkaccess import LabKey
import lkaccess.contexts
from aicsimageio import AICSImage
import imageio
from hexalattice.hexalattice import *

def get_labkey_cell_data():
    '''
    download cell data from labkey if not already saved locally
    '''
    cell_data_path = Path("labkey_cells.pkl")
    if not cell_data_path.is_file():
        # Create our accessor class
        lk = LabKey(server_context=lkaccess.contexts.PROD)
        # Grab all production data
        cells = lk.dataset.get_pipeline_4_production_cells()
        # Put it into a dataframe
        result = pd.DataFrame(cells)
        result.to_pickle("labkey_cells.pkl")
        with open("cells.csv", "w") as text_file:
            text_file.write(result.to_csv())
        print("downloaded labkey data and saved as labkey_cells.pkl")
    else:
        result = pd.read_pickle("labkey_cells.pkl")
        print("loaded labkey data from local labkey_cells.pkl file")
    return result

def get_max_cells_fov_ids():
    '''
    group labkey cell data by FOVId and count number of cells per FOV
    '''
    labkey_cell_data = get_labkey_cell_data()
    seg_cell_counts = labkey_cell_data.groupby(["FOVId"]).size().sort_values(ascending=False)
    seg_cell_counts_csv = seg_cell_counts.to_csv()
    max_cells_fov_ids = []
    for i in range(10):
        max_cells_fov_ids.append(np.int64(seg_cell_counts.axes[0][i]))
    print(f"FOVs with most segmented cells: ids={max_cells_fov_ids}")
    with open("segmented_cell_counts.csv", "w") as text_file:
        text_file.write(seg_cell_counts_csv)
    print("saved count of segmented cells per FOV to segmented_cell_counts.csv")
    return max_cells_fov_ids

def download_max_cells_fov_images():
    '''
    download data for FOVs with most segmented cells and save XY projection as png
    '''
    if not Path("max_cells_fovs_metadata.csv").is_file():
        max_cells_fov_ids = get_max_cells_fov_ids()
        pkg = quilt3.Package.browse("aics/hipsc_single_cell_image_dataset", "s3://allencell")
        pkg_metadata = pkg["metadata.csv"]()
        max_cells_fovs = pkg_metadata.loc[pkg_metadata["FOVId"].isin(max_cells_fov_ids)].sort_values(by=["FOVId"])
        file_paths = np.unique(max_cells_fovs.loc[:,"fov_seg_path"].values)
        with open("max_cells_fovs_metadata.csv", "w") as text_file:
            text_file.write(max_cells_fovs.to_csv())
        print("saved metadata for cells in FOVs with most segmented cells to max_cells_fovs_metadata.csv")
        if not Path("aics_images").is_dir():
            os.mkdir("aics_images")
        for file_path in file_paths:
            file_name = Path(file_path).name
            endname_ind = file_name.find(".")
            file_name_noext = file_name[:endname_ind]
            filedir = f"aics_images/{file_name_noext}"
            if not Path(filedir).is_dir():
                os.mkdir(filedir)
                if not Path(f"{filedir}/{file_name}").is_file():
                    # download data
                    pkg["fov_seg_path"][file_name].fetch(Path(f"{filedir}/{file_name}"))
                img = AICSImage(f"{filedir}/{file_name}")
                for z in range(img.shape[3]):
                    if not Path(f"{filedir}/{file_name_noext}.png").is_file():
                        # save XY projection of segmented FOV as png    
                        imageio.imwrite(f"{filedir}/{file_name_noext}_z{z}.png", img.get_image_data("XYZ", S=0, T=0, C=1)[:,:,z])
                
def sample_hex_grid(resolution = 10):

    if not Path(f"hex_seeds").is_dir():
        os.mkdir("hex_seeds")

    # loop through all images, and sample desired zlices on hexgrid
    for file_dir in os.listdir("aics_images/"):

        # Create subdirectory for hex seed data for this image file
        if not Path(f"hex_seeds/{file_dir}").is_dir():
            os.mkdir(f"hex_seeds/{file_dir}")

        # Get list of z values to sample
        zmax = len(os.listdir(f"aics_images/{file_dir}")) - 2 # Subtract 2: 1 for 0-indexing and 1 for .tiff file
        zlist = np.arange(0, zmax, resolution)

        # Get list of x and y values to sample on hexgrid using a sample 2D image slice
        sample_filename = os.listdir(f"aics_images/{file_dir}")[0]
        fov_image_slice = imageio.imread(f"aics_images/{file_dir}/{sample_filename}")
        hex_centers, _ = create_hex_grid(
                nx=round(fov_image_slice.shape[0] / resolution),
                ny=round(fov_image_slice.shape[1] / resolution),
                min_diam=resolution,
                align_to_origin=False,
                do_plot=False,
            )

        # Initialize lists of coordinates and values to fill
        x_array = []
        y_array = []
        z_array = []
        values = []

        # Loop through files and sample 2D hexgrid on each z slice
        for file_path in os.listdir(f"aics_images/{file_dir}"):

            # Only use the png files which are the zslice images
            if "png" in file_path:

                # Get z value
                zstart = file_path.rfind("z")+1
                zend = file_path.rfind(".")
                z = int(file_path[zstart:zend])

                # check if this file is for a zslice in our list of z's to sample
                if z in zlist:

                    # Shift 2D hexgrid for every other z slice
                    ind = np.where(zlist == z)[0][0]
                    if ind % 2 == 0:
                        xlist = [int(round(hex_center[0])) for hex_center in hex_centers[:]]
                        ylist = [int(round(hex_center[1])) for hex_center in hex_centers[:]]

                    else:
                        xlist = [int(round(hex_center[0] + (resolution/2))) for hex_center in hex_centers[:]]
                        ylist = [int(round(hex_center[1] + (resolution/2))) for hex_center in hex_centers[:]]

                    # load image file
                    file_name = Path(file_path).name
                    fov_image_slice = imageio.imread(f"aics_images/{file_dir}/{file_name}")

                    # cycle through all x and y coordinates to sample
                    for x, y in zip(xlist, ylist):
                        if  y < fov_image_slice.shape[1] and x < fov_image_slice.shape[0]:
                            v = fov_image_slice[x][y]
                            if v > 0:
                                x_array.append(x)
                                y_array.append(y)
                                z_array.append(z)
                                values.append(v)

        # Compile coordinates and values into a dataframe and save as csv
        data = pd.DataFrame({"x": x_array, "y": y_array, "z": z_array, "id": values})
        data.to_csv(f"hex_seeds/{file_dir}/hex_id_data_{file_dir}.csv", index=False)

        if not Path(f"hex_seeds/{file_dir}/scatter_by_z").is_dir():
            os.mkdir(f"hex_seeds/{file_dir}/scatter_by_z")
        for z in zlist:
            if sum(data["z"] == z) > 0:
                data_z = data.loc[data["z"] == z]
                plt.clf()
                plt.scatter(data_z["x"], data_z["y"], c=data_z["id"], cmap="jet", s=5)
                plt.axis("equal")
                plt.savefig(f"hex_seeds/{file_dir}/scatter_by_z/z{z}_hex_ids_{file_name}",bbox_inches="tight")

def main():
    '''
    Save images of the fields of view with the most segmented cells
    '''
    download_max_cells_fov_images()
    sample_hex_grid()

if __name__ == '__main__':
    main()