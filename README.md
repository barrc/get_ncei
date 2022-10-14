# get_ncei

# Getting updated precipitation data

1. Change `CURRENT_END_YEAR` in common.py

2. Change `DATA_BASE_DIR` in common.py
   - Combined, filled, processed, and raw data for both original and 2021 were uploaded to Michelle Simon's OneDrive
   - Combined data for original update can also be found in swcalculator_home\data

3. Change original_file_name in yearly_update.py in function `combine_old_new` (see note)

4. Use the script yearly_update.py to get new data. You may not want or be able to run the whole script through. Sometimes NCEI servers are down or responses from web services change. It has a few parts:
    - `make_directories` will make directories to store the data for the new year. If the directories are already made, it will not make new ones.

    - `make_updated_coops` will download the latest station inventory file from NCEI and determine which COOP stations to obtain data for.

    - `update_coop_data` will download the precipitation data for the COOP stations obtained above, process it, and fill it with NLDAS/GLDAS, and combine it with the previous COOP data (see the folder `CURRENT_END_YEAR`_combined_data). There is also a hint here about how to restart the script from where you left off if the COOP server is unavailable at some point.

    - `update_isd_data` will do the same as above for the ISD stations.

    - `process` will create the new D4EMLite file to include in the SWC with name `CURRENT_END_YEAR`_D4EM_PREC_updated.txt

# Also in this directory

- The file post_process_evap.py can be used to create the D4EM_PMET_updated file
    - Update the end year and period of record (marked with #TODO)