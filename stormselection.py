import os
import glob
import pandas as pd
import shutil

# This function flattens the file structure of the downloaded files from NEON.
def flatten(destination):
    all_files = []
    first_loop_pass = True
    for root, _dirs, files in os.walk(destination):
        if first_loop_pass:
            first_loop_pass = False
            continue
        for filename in files:
            all_files.append(os.path.join(root, filename))
    for filename in all_files:
        shutil.move(filename, destination)
    print("Files flattened")

# This function filters for only the files you want to read from the downloaded package.
def filter(source, destination, filter_string):
    files = os.listdir(source)
    for file in files:
        if filter_string in file:
            file_name = os.path.join(source, file)
            shutil.move(file_name, destination)
    print("Files filtered")

#This function combines all air temp monthly data into one file per site
# Inputs: Sites = list of sites to run the function on, path = path to the folder where
# the monthly air temp data is located, output_folder = path to the folder where you want
# the output combined Air temp file per site to be stored.
def combine_air(Sites, path, output_folder):
    for site in Sites:
        combinedA_df = pd.DataFrame()
        all_filenames_air = [i for i in glob.glob(path + 'NEON.D*.'+site+'.*.020.030.SAAT_30min.*.csv')]
        if (len(all_filenames_air) == 0):
            print('No air temp files for', site)
        else:
            #combine all files in the list
            combined_csv_air = pd.concat([pd.read_csv(f) for f in all_filenames_air])
            #export to csv
            combinedA_df['startDateTime'] = combined_csv_air['startDateTime']
            combinedA_df['endDateTime'] = combined_csv_air['endDateTime']
            combinedA_df['air_temp'] = combined_csv_air['tempSingleMean']

        combinedA_df.to_csv(output_folder+'Combined_airTemp_'+site+'.csv', index=False, encoding='utf-8-sig')
    print('All air temp files combined.')

#This function selects storms from the defined data based on defined criteria.
# Inputs: Sites = list of sites to run the function on, input_air = path to the folder
# with combined Air temp data per site, input_storm = path to folder with defined storm
# data per site (from get_new_storms.py), output_folder = path to folder where you want
# the output selected storm data to be stored.
def storm_select(Sites, input_air, input_storm, output_folder):
    for site in Sites:
        # read in storms and air temp files
        air_temp = pd.read_csv(input_air+'Combined_airTemp_'+site+'.csv')
        air_temp['startDateTime'] = pd.to_datetime(air_temp['startDateTime'])
        storm = pd.read_csv(input_storm+'Output_'+site+'.csv')
        storm['startDateTime'] = pd.to_datetime(storm['startDateTime'])

        # match air temp to start date of storm, add air temp column
        storms = storm.merge(air_temp, on = 'startDateTime')

        # set all storms to be 'selected', or 1
        storms['select'] = 0

        # Set the following variables to 1 if you want the filter turned on,
        # or to zero if you want the associated filter turned off.
        a = 1 # Removes storms with suspected snow. Set temp_thresh variable for the limit)
        b = 1 # Removes storms where not all TF collectors are working in the next storm.
        c = 1 # Removes storms where not all TF collectors are working.
        d = 0 # Removes storms if a TF collector has large difference from average of other TF collectors. See perc_thresh variable
        e = 1 # Removes storms where more than 1 TF collector exceeds % of SecPrecip, see highest variable
        f = 0 # Removes storms that have too wide range in TF values. Similar to d, but calculated based on percent IL for each TF collector. See diff_thresh and small, med, and large variables.
        g = 1 # Removes storms that have too wide range of TF values (continuous function with storm size, see metric variable).
        p = 1 # Removes storms in which not all TF collectors were working in the previous storm.
        q = 1 # Removes storms with more than 2 zero TF readings

        # The parameters below affect the filtering functions. They can be adjusted as desired.
        highest = 1
        excess = 2
        metric = 120
        small = 300
        med = 200
        large = 100

        #sites = ['BLAN', 'BART', 'DEJU', 'DELA', 'DSNY', 'GRSM', 'GUAN', 'JERC', 'KONZ', 'LENO', 'MLBS', 'ORNL', 'OSBS', 'SERC', 'STEI', 'TALL', 'TEAK', 'UKFS', 'UNDE', 'WREF', 'YELL'] #Tree

        # set threshholds for evaluating storms
        temp_thresh = 0.0 #degrees Celcius
        perc_thresh = .5
        TF_thresh = 2

        for i in range(len(storms)):
            # remove storms with snow
            temp = storms.at[i, 'air_temp']
            if a == 1:
                if temp != 'nan':
                    if temp < temp_thresh:
                        storms.at[i, 'select'] = 1

            #reset TF check values to 1 (non-zero)
            TF1 = 1
            TF2 = 1
            TF3 = 1
            TF4 = 1
            TF5 = 1

            #check for zero TF
            if storms.at[i, 'TF1'] == 0:
                TF1 = 0
            if storms.at[i, 'TF2'] == 0:
                TF2 = 0
            if storms.at[i, 'TF3'] == 0:
                TF3 = 0
            if storms.at[i, 'TF4'] == 0:
                TF4 = 0
            if storms.at[i, 'TF5'] == 0:
                TF5 = 0

            #if all TF collectors are non-zero
            if TF1 == 1 and TF2 == 1 and TF3 == 1 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF1 == 0 or TF2 == 0 or TF3 == 0 or TF4 == 0 or TF5 == 0:
                        storms.at[i, 'select'] = 2
                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i-1
                        if storms.at[m, 'TF1'] == 0 or storms.at[m,'TF2'] == 0 or storms.at[m,'TF3'] == 0 or storms.at[m,'TF4'] == 0 or storms.at[m,'TF5'] == 0:
                            storms.at[i,'select'] = 3
                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i+1
                        if storms.at[j,'TF1'] == 0 or storms.at[j,'TF2'] == 0 or storms.at[j,'TF3'] == 0 or storms.at[j,'TF4'] == 0 or storms.at[j,'TF5'] == 0:
                            storms.at[i,'select'] = 4
                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i,'TF1'] < perc_thresh*((storms.at[i,'TF2']+storms.at[i,'TF3']+storms.at[i,'TF4']+storms.at[i,'TF5'])/4):
                            storms.at[i,'select'] = 8
                        if storms.at[i,'TF2'] < perc_thresh*((storms.at[i,'TF1']+storms.at[i,'TF3']+storms.at[i,'TF4']+storms.at[i,'TF5'])/4):
                            storms.at[i,'select'] = 8
                        if storms.at[i,'TF3'] < perc_thresh*((storms.at[i,'TF1']+storms.at[i,'TF2']+storms.at[i,'TF4']+storms.at[i,'TF5'])/4):
                            storms.at[i,'select'] = 8
                        if storms.at[i,'TF4'] < perc_thresh*((storms.at[i,'TF1']+storms.at[i,'TF2']+storms.at[i,'TF3']+storms.at[i,'TF5'])/4):
                            storms.at[i,'select'] = 8
                        if storms.at[i,'TF5'] < perc_thresh*((storms.at[i,'TF1']+storms.at[i,'TF2']+storms.at[i,'TF3']+storms.at[i,'TF4'])/4):
                            storms.at[i,'select'] = 8

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffa = abs(((IL1 - IL2) / ((IL1 + IL2) / 2)) * 100)
                percDiffb = abs(((IL1 - IL3) / ((IL1 + IL3) / 2)) * 100)
                percDiffc = abs(((IL1 - IL4) / ((IL1 + IL4) / 2)) * 100)
                percDiffd = abs(((IL1 - IL5) / ((IL1 + IL5) / 2)) * 100)
                percDiffe = abs(((IL2 - IL3) / ((IL2 + IL3) / 2)) * 100)
                percDifff = abs(((IL2 - IL4) / ((IL2 + IL4) / 2)) * 100)
                percDiffg = abs(((IL2 - IL5) / ((IL2 + IL5) / 2)) * 100)
                percDiffh = abs(((IL3 - IL4) / ((IL3 + IL4) / 2)) * 100)
                percDiffi = abs(((IL3 - IL5) / ((IL3 + IL5) / 2)) * 100)
                percDiffj = abs(((IL4 - IL5) / ((IL4 + IL4) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffa > diff_thresh or percDiffb > diff_thresh or percDiffc > diff_thresh or percDiffd > diff_thresh or percDiffe > diff_thresh or percDifff > diff_thresh or percDiffg > diff_thresh or percDiffh > diff_thresh or percDiffi > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF5 is zero
            if TF1 == 1 and TF2 == 1 and TF3 ==1 and TF4 == 1 and TF5 == 0:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF1 == 0 or TF2 == 0 or TF3 == 0 or TF4 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF1'] == 0 or storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF1'] == 0 or storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF3'] + storms.at[i, 'TF4'])/ 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF3'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2'] + storms.at[i, 'TF3']) / 3):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']

                # Calculate the percent difference between IL at each TF collector
                percDiffa = abs(((IL1 - IL2) / ((IL1 + IL2) / 2)) * 100)
                percDiffb = abs(((IL1 - IL3) / ((IL1 + IL3) / 2)) * 100)
                percDiffc = abs(((IL1 - IL4) / ((IL1 + IL4) / 2)) * 100)
                percDiffe = abs(((IL2 - IL3) / ((IL2 + IL3) / 2)) * 100)
                percDifff = abs(((IL2 - IL4) / ((IL2 + IL4) / 2)) * 100)
                percDiffh = abs(((IL3 - IL4) / ((IL3 + IL4) / 2)) * 100)


                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffa > diff_thresh or percDiffb > diff_thresh or percDiffc > diff_thresh or percDiffe > diff_thresh or percDifff > diff_thresh or percDiffh > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF4 is zero
            if TF1 == 1 and TF2 == 1 and TF3 ==1 and TF4 == 0 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF1 == 0 or TF2 == 0 or TF3 == 0 or TF5 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p ==1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF1'] == 0 or storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF5'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF1'] == 0 or storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF5'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF3'] + storms.at[i, 'TF5'])/ 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF3'] + storms.at[i, 'TF5']) / 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2'] + storms.at[i, 'TF5']) / 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2'] + storms.at[i, 'TF3']) / 3):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffa = abs(((IL1 - IL2) / ((IL1 + IL2) / 2)) * 100)
                percDiffb = abs(((IL1 - IL3) / ((IL1 + IL3) / 2)) * 100)
                percDiffd = abs(((IL1 - IL5) / ((IL1 + IL5) / 2)) * 100)
                percDiffe = abs(((IL2 - IL3) / ((IL2 + IL3) / 2)) * 100)
                percDiffg = abs(((IL2 - IL5) / ((IL2 + IL5) / 2)) * 100)
                percDiffi = abs(((IL3 - IL5) / ((IL3 + IL5) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffa > diff_thresh or percDiffb > diff_thresh or percDiffd > diff_thresh or percDiffe > diff_thresh or percDiffg > diff_thresh or percDiffi > diff_thresh:
                        storms.at[i, 'select'] = 7


            #if TF3 is zero
            if TF1 == 1 and TF2 == 1 and TF3 == 0 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF1 == 0 or TF2 == 0 or TF5 == 0 or TF4 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF1'] == 0 or storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF1'] == 0 or storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF5'] + storms.at[i, 'TF4'])/ 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF5'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2'] + storms.at[i, 'TF5']) / 3):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffa = abs(((IL1 - IL2) / ((IL1 + IL2) / 2)) * 100)
                percDiffc = abs(((IL1 - IL4) / ((IL1 + IL4) / 2)) * 100)
                percDiffd = abs(((IL1 - IL5) / ((IL1 + IL5) / 2)) * 100)
                percDifff = abs(((IL2 - IL4) / ((IL2 + IL4) / 2)) * 100)
                percDiffg = abs(((IL2 - IL5) / ((IL2 + IL5) / 2)) * 100)
                percDiffj = abs(((IL4 - IL5) / ((IL4 + IL4) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffa > diff_thresh or percDiffc > diff_thresh or percDiffd > diff_thresh or percDifff > diff_thresh or percDiffg > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF2 is zero
            if TF1 == 1 and TF2 == 0 and TF3 ==1 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF1 == 0 or TF5 == 0 or TF3 == 0 or TF4 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF1'] == 0 or storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF1'] == 0 or storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF5'] + storms.at[i, 'TF3'] + storms.at[i, 'TF4'])/ 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF3'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF5'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF5'] + storms.at[i, 'TF3']) / 3):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffb = abs(((IL1 - IL3) / ((IL1 + IL3) / 2)) * 100)
                percDiffc = abs(((IL1 - IL4) / ((IL1 + IL4) / 2)) * 100)
                percDiffd = abs(((IL1 - IL5) / ((IL1 + IL5) / 2)) * 100)
                percDiffh = abs(((IL3 - IL4) / ((IL3 + IL4) / 2)) * 100)
                percDiffi = abs(((IL3 - IL5) / ((IL3 + IL5) / 2)) * 100)
                percDiffj = abs(((IL4 - IL5) / ((IL4 + IL4) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip']*metric
                    if percDiffb > diff_thresh or percDiffc > diff_thresh or percDiffd > diff_thresh or percDiffh > diff_thresh or percDiffi > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF1 is zero
            if TF1 == 0 and TF2 == 1 and TF3 ==1 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF2 == 0 or TF3 == 0 or TF4 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF3'] + storms.at[i, 'TF4'])/ 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF5'] + storms.at[i, 'TF3'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF5'] + storms.at[i, 'TF2'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF5'] + storms.at[i, 'TF2'] + storms.at[i, 'TF3']) / 3):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffe = abs(((IL2 - IL3) / ((IL2 + IL3) / 2)) * 100)
                percDifff = abs(((IL2 - IL4) / ((IL2 + IL4) / 2)) * 100)
                percDiffg = abs(((IL2 - IL5) / ((IL2 + IL5) / 2)) * 100)
                percDiffh = abs(((IL3 - IL4) / ((IL3 + IL4) / 2)) * 100)
                percDiffi = abs(((IL3 - IL5) / ((IL3 + IL5) / 2)) * 100)
                percDiffj = abs(((IL4 - IL5) / ((IL4 + IL4) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffe > diff_thresh or percDifff > diff_thresh or percDiffg > diff_thresh or percDiffh > diff_thresh or percDiffi > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF1 and TF2 are zero
            if TF1 == 0 and TF2 == 0 and TF3 ==1 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF3 == 0 or TF4 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF4'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF4']) / 2):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffh = abs(((IL3 - IL4) / ((IL3 + IL4) / 2)) * 100)
                percDiffi = abs(((IL3 - IL5) / ((IL3 + IL5) / 2)) * 100)
                percDiffj = abs(((IL4 - IL5) / ((IL4 + IL4) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffh > diff_thresh or percDiffi > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF1 and TF3 are zero
            if TF1 == 0 and TF2 == 1 and TF3 ==0 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF2 == 0 or TF4 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF4'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF4']) / 2):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDifff = abs(((IL2 - IL4) / ((IL2 + IL4) / 2)) * 100)
                percDiffg = abs(((IL2 - IL5) / ((IL2 + IL5) / 2)) * 100)
                percDiffj = abs(((IL4 - IL5) / ((IL4 + IL4) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDifff > diff_thresh or percDiffg > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF1 and TF4 are zero
            if TF1 == 0 and TF2 == 1 and TF3 ==1 and TF4 == 0 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF3 == 0 or TF2 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF2'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF2'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffe = abs(((IL2 - IL3) / ((IL2 + IL3) / 2)) * 100)
                percDiffg = abs(((IL2 - IL5) / ((IL2 + IL5) / 2)) * 100)
                percDiffi = abs(((IL3 - IL5) / ((IL3 + IL5) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffe > diff_thresh or percDiffg > diff_thresh or percDiffi > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF1 and TF5 are zero
            if TF1 == 0 and TF2 == 1 and TF3 ==1 and TF4 == 1 and TF5 == 0:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF2 == 0 or TF3 == 0 or TF4 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF4'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF4']) / 2):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']

                # Calculate the percent difference between IL at each TF collector
                percDiffe = abs(((IL2 - IL3) / ((IL2 + IL3) / 2)) * 100)
                percDifff = abs(((IL2 - IL4) / ((IL2 + IL4) / 2)) * 100)
                percDiffh = abs(((IL3 - IL4) / ((IL3 + IL4) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffe > diff_thresh or percDifff > diff_thresh or percDiffh > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF2 and TF3 are zero
            if TF1 == 1 and TF2 == 0 and TF3 ==0 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF1 == 0 or TF4 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF1'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF1'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF4'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF4']) / 2):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffc = abs(((IL1 - IL4) / ((IL1 + IL4) / 2)) * 100)
                percDiffd = abs(((IL1 - IL5) / ((IL1 + IL5) / 2)) * 100)
                percDiffj = abs(((IL4 - IL5) / ((IL4 + IL4) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffc > diff_thresh or percDiffd > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF2 and TF4 are zero
            if TF1 == 1 and TF2 == 0 and TF3 ==1 and TF4 == 0 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF3 == 0 or TF1 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF1'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF1'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF1']) / 2):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffb = abs(((IL1 - IL3) / ((IL1 + IL3) / 2)) * 100)
                percDiffd = abs(((IL1 - IL5) / ((IL1 + IL5) / 2)) * 100)
                percDiffi = abs(((IL3 - IL5) / ((IL3 + IL5) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffb > diff_thresh or percDiffd > diff_thresh or percDiffi > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF2 and TF5 are zero
            if TF1 == 1 and TF2 == 0 and TF3 ==1 and TF4 == 1 and TF5 == 0:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF4 == 0 or TF3 == 0 or TF1 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF4'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF1'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF4'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF1'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF4']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF4']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF1']) / 2):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']

                # Calculate the percent difference between IL at each TF collector
                percDiffb = abs(((IL1 - IL3) / ((IL1 + IL3) / 2)) * 100)
                percDiffc = abs(((IL1 - IL4) / ((IL1 + IL4) / 2)) * 100)
                percDiffh = abs(((IL3 - IL4) / ((IL3 + IL4) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffb > diff_thresh or percDiffc > diff_thresh or percDiffh > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF3 and TF4 are zero
            if TF1 == 1 and TF2 == 1 and TF3 ==0 and TF4 == 0 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF2 == 0 or TF1 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF1'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF1'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF1']) / 2):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffa = abs(((IL1 - IL2) / ((IL1 + IL2) / 2)) * 100)
                percDiffd = abs(((IL1 - IL5) / ((IL1 + IL5) / 2)) * 100)
                percDiffg = abs(((IL2 - IL5) / ((IL2 + IL5) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffa > diff_thresh or percDiffd > diff_thresh or percDiffg > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF3 and TF5 are zero
            if TF1 == 1 and TF2 == 1 and TF3 ==0 and TF4 == 1 and TF5 == 0:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF2 == 0 or TF4 == 0 or TF1 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF4'] == 0 or storms.at[m, 'TF1'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF4'] == 0 or storms.at[j, 'TF1'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF4'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF4'] + storms.at[i, 'TF1']) / 2):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']

                # Calculate the percent difference between IL at each TF collector
                percDiffa = abs(((IL1 - IL2) / ((IL1 + IL2) / 2)) * 100)
                percDiffc = abs(((IL1 - IL4) / ((IL1 + IL4) / 2)) * 100)
                percDifff = abs(((IL2 - IL4) / ((IL2 + IL4) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip'] * metric
                    if percDiffa > diff_thresh or percDiffc > diff_thresh or percDifff > diff_thresh:
                        storms.at[i, 'select'] = 7

            #if TF5 and TF4 are zero
            if TF1 == 1 and TF2 == 1 and TF3 ==1 and TF4 == 0 and TF5 == 0:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF2 == 0 or TF3 == 0 or TF1 == 0:
                        storms.at[i, 'select'] = 2

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF1'] == 0:
                            storms.at[i, 'select'] = 3

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF1'] == 0:
                            storms.at[i, 'select'] = 4

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select'] = 8
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF1']) / 2):
                            storms.at[i, 'select'] = 8

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']

                # Calculate the percent difference between IL at each TF collector
                percDiffa = abs(((IL1 - IL2) / ((IL1 + IL2) / 2)) * 100)
                percDiffb = abs(((IL1 - IL3) / ((IL1 + IL3) / 2)) * 100)
                percDiffe = abs(((IL2 - IL3) / ((IL2 + IL3) / 2)) * 100)

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large
                    if percDiffa > diff_thresh or percDiffb > diff_thresh or percDiffe > diff_thresh:
                        storms.at[i, 'select'] = 7

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values based on linear function of storm size (adjustable threshhold with metric variable)
                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip']*metric
                    print('diff_thresh:', diff_thresh)
                    if percDiffa > diff_thresh or percDiffb > diff_thresh or percDiffe > diff_thresh:
                        storms.at[i, 'select'] = 1

                # if h == 1:
                #     diff_thresh = (storms.at[i, 'SecPrecip']-(storms.at[i,'TF'*metric
                #     if percDiffa > diff_thresh or percDiffb > diff_thresh or percDiffe > diff_thresh:
                #         storms.at[i, 'select'] = 1

            #remove storms with more than 2 zero TF readings
            if q == 1:
                if (TF1 + TF2 + TF3 + TF4 + TF5) < 3:
                    storms.at[i, 'select'] = 9

            # Remove storms where more than 1 TF collector exceeds highest% of SecPrecip
            if e == 1:
                measure = 0
                if storms.at[i, 'SecPrecip'] < (highest*storms.at[i,'TF1']):
                    measure = measure + 1
                if storms.at[i, 'SecPrecip'] < (highest*storms.at[i,'TF2']):
                    measure = measure + 1
                if storms.at[i, 'SecPrecip'] < (highest*storms.at[i,'TF3']):
                    measure = measure + 1
                if storms.at[i, 'SecPrecip'] < (highest*storms.at[i,'TF4']):
                    measure = measure + 1
                if storms.at[i, 'SecPrecip'] < (highest*storms.at[i,'TF5']):
                    measure = measure + 1
                if measure > excess:
                    storms.at[i, 'select'] = 5
                    #print('i:', i, 'SecPrecip', storms.at[i, 'SecPrecip'], 'TF1:', storms.at[i, 'TF1'])

        #write storms with 'select' column to new csv.
        storms.to_csv(output_folder+'all_storms_'+site+'_filter_index.csv')

        #remove filtered storms
        selected = storms
        selected.drop(selected.index[selected['select']!=0], inplace=True)
        selected.to_csv(output_folder+'Selected_storms_'+site+'.csv')

    print('Storm selection complete.')

main_dir = 'C:/Users/Abigail Sandquist/Box/IL/IL_Project/'

#flatten files
destination = main_dir+'NEON_Downloads/NEON_Temp/NEON_temp-air-single/'
flatten(destination)

#filter for the desired 30-min sensor 2 temp file
source = destination
destination_filter = main_dir+'NEON_Downloads/NEON_Temp/'
filter_string = '020.030.SAAT_30min.2022'
filter(source, destination_filter, filter_string)

Sites = ['DSNY']
# Sites = ['SCBI', 'SERC', 'DSNY', 'JERC', 'OSBS', 'GUAN', 'STEI', 'TREE', 'UNDE', 'KONZ',
#          'UKFS', 'GRSM', 'MLBS', 'ORNL', 'DELA', 'LENO', 'TALL', 'RMNP', 'CLBJ', 'YELL',
#          'SRER', 'ABBY', 'WREF', 'SJER', 'SOAP', 'TEAK', 'BONA', 'JORN', 'DEJU']

#combine air temp files for each site
path = destination_filter
output_folder = main_dir+'Combined/Temp/'
combine_air(Sites, path, output_folder)

#Filter to get selected storms for each site
input_air = output_folder
input_storm = main_dir+'Staging/'
output_folder_selected_storms = main_dir+'Selected_Storms/'
storm_select(Sites, input_air, input_storm, output_folder_selected_storms)