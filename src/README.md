
# Plan


1.	Work applying to both datasets

    a.	Decide time cutoffs (need to do very soon)
    
      - What is our cutoff for stations that are not currently reporting data?  For example if a station stopped reporting in 2014 should we get 2006 – 2014?  Does it matter if that station was previously in BASINS?
      
      -	What is our cutoff for stations that started reporting recently?  For example if a station started in 2018 are we using that station?
      
    b.	For including in the SWC, write data files through 12/31/2019 or present?  Complete years work better for calculating statistics. This will be a variable in a script so we can change our minds later.  
    
    c.	Decide how to fill missing data 
    
      -	Research Glenn’s methods
      
      -	Decide which to use and if it depends on station origin (ISD vs COOP)
      
      -	Write code to fill datasets
      
    d.	Decide how to include in SWC web app
    
      - Append to current .dat files?
      
      -	If so, write script to update .txt file used in web app to contain any new stations if new stations exist
      
      -	If so, write script to update .txt file used in web app to reflect new end dates
      
      -	Front end changes?

2.	COOP data

    a.	Determine which stations to use
    
      - Look at station inventory file: HPD_v02r02_stationinv_C20200715.csv
      
      -	Based on time cutoffs, write script to parse station inventory file to determine stations of interest
      
      -	Output of script should be new .csv with station inventory file information and some additional columns like Start_Date, End_Date, Use
      
      -	Determine which stations are in SWC now
      
      - In SWC if state + last five digits of station name = BASINS filename 
      
    b.	Complete initial proof of concept for one station
    
    - Write script to download data using https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/access/ and station name
    
    -	Write script to parse to SWMM-formatted .dat file
    
    c. Get all data
    
    - Get folder on shared drive or other location to store both raw and formatted

    - Adopt script from 2(b)(i) to download all stations of interest

    - Fill data based on decision from 1(c)

    - Parse all data to SWMM-formatted .dat files

3.	ISD data
  
    a.	Determine which stations to use (more complicated than COOP because of current unknowns)

    - Write script to process this file https://www1.ncdc.noaa.gov/pub/data/noaa/isd-history.txt to get stations in U.S. with appropriate start/end dates
    
    - Figure out which stations are in SWC now

    - Is there a way to correlate ISD filenames with BASINS filenames?

    b.	Complete initial proof of concept for one station

    - Write script to download data using v1 API

    - Write script to parse to SWMM-formatted .dat file
  
    c.	Get all data

    - Download all stations of interest

    - Fill data

    - Parse all data to SWMM-formatted .dat files
