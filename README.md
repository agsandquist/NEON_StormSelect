# NEON_StormSelect
This repository includes functions that define and filter storm events from NEON precipitation and throughfall data, and calculate estimated interception loss for each storm. 

## get_new_storms.py
Run the get_new_storms.py file on newly downloaded and unzipped NEON precipitation and throughfall data. Data can be downloaded from here: 
https://data.neonscience.org/data-products/DP1.00006.001

You will need to edit the names of the file directories as desired at the end of the python file. The functions should not need to be edited otherwise. This will create
several output files, the final of which is defined storm events with start date and time, storm duration (minutes), total precip amount (mm), total throughfall amount 
(mm) for each collector and median of all collectors, estimated percent IL (using the median throughfall value), and estimated amount interception loss (mm). 

## stormselection.py
Run the stormselection.py file to filter the defined storms from the get_new_storms.py file based on several quality control metrics. You will need NEON air temperature
data, downloaded and unzipped. You can download that data here: 
https://data.neonscience.org/data-products/DP1.00002.001

You will need to update the file directory names at the end of the python file to match your file structure. You can also change the parameters near the beginning of the
storm_select() function definintion to control how you want to filter out storms. Variables a, b, c, d, e, f, g, p, and q in lines 71-79 can be set to 0 or 1 to turn the 
filter on or off, respectively. The variables highest, excess, metric, small, med, large, temp_thresh, and perc_thresh can also be edited to change the limits of 
their associated filtering functions. For example, temp_thresh could be changed from 0.0 to 2.0 to change the limiting temperature (degrees Celcius) for 
storms to be removed for assumed snowfall. 

Future python files will be added to run regression and plotting functions on the selected storm files. 
