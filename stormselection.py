import os
import glob
import pandas as pd
import shutil
import math
import matplotlib.pyplot as plt
from pathlib import Path

# This function flattens the file structure of the downloaded files from NEON.
def flatten(destination):
    all_files = []
    first_loop_pass = True
    for root, _dirs, files in os.walk(destination):
        if first_loop_pass:
            first_loop_pass = False
            continue
        for filename in files:
            if not 'readme' in filename and not 'JERC' in filename:
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

        path_check = Path(output_folder+'Combined_airTemp_'+site+'.csv')
        if path_check.is_file() == True:
            ha = False
        else:
            ha = True
        #ha = True
        print('header for combined air temp file', ha)
        combinedA_df.to_csv(output_folder+'Combined_airTemp_'+site+'.csv', index=False, header = ha, mode='a', encoding='utf-8-sig')
    print('All air temp files combined.')

#This function selects storms from the defined data based on defined criteria.
# Inputs: Sites = list of sites to run the function on, input_air = path to the folder
# with combined Air temp data per site, input_storm = path to folder with defined storm
# data per site (from get_new_storms.py), output_folder = path to folder where you want
# the output selected storm data to be stored.
def storm_select(Sites, input_air, input_storm, output_folder):
    for site in Sites:
        print('Site:',site)
        # read in storms and air temp files
        air_temp = pd.read_csv(input_air+'Combined_airTemp_'+site+'.csv')
        air_temp['startDateTime'] = pd.to_datetime(air_temp['startDateTime'])
        storm = pd.read_csv(input_storm+'Output_'+site+'.csv')
        #print('storm', storm)
        storm['startDateTime'] = pd.to_datetime(storm['startDateTime'], errors = 'coerce')


        # match air temp to start date of storm, add air temp column
        storms = storm.merge(air_temp, on = 'startDateTime')
        #print('storms_', storms.head())
        storms['SecPrecip'] = pd.to_numeric(storms['SecPrecip'])
        storms['PriPrecip'] = pd.to_numeric(storms['PriPrecip'])
        storms['TF1'] = pd.to_numeric(storms['TF1'])
        storms['TF2'] = pd.to_numeric(storms['TF2'])
        storms['TF3'] = pd.to_numeric(storms['TF3'])
        storms['TF4'] = pd.to_numeric(storms['TF4'])
        storms['TF5'] = pd.to_numeric(storms['TF5'])

        #replace SecPrecip column with PriPrecip if site has no SecPrecip values
        if storms['SecPrecip'].sum() == 0:
            storms['SecPrecip'] = storms['PriPrecip']
            print('PriPrecip copied to SecPrecip for ', site)

        # set all storms to be 'selected', or 1
        storms['select_a'] = 0
        storms['select_b'] = 0
        storms['select_c'] = 0
        storms['select_d'] = 0
        storms['select_e'] = 0
        storms['select_fg'] = 0
        storms['select_h'] = 0
        storms['select_p'] = 0
        storms['select_q'] = 0
        storms['select'] = 1
        #
        # for i in storms:
        #     storms[i,'SecPrecip'] = float(storms[i,'SecPrecip'])
        #     storms[i,'TF1'] = float(storms[i,'TF1'])

        #print('storms_2', storms.head())

        # Set the following variables to 1 if you want the filter turned on,
        # or to zero if you want the associated filter turned off.
        a = 1 # Removes storms with suspected snow. Set temp_thresh variable for the limit)
        b = 1 # Removes storms where not all TF collectors are working in the next storm.
        c = 1 # Removes storms where not all TF collectors are working.
        d = 0 # Removes storms if a TF collector has large difference from average of other TF collectors. See perc_thresh variable
        e = 1 # Removes storms where more than 1 TF collector exceeds % of SecPrecip, see highest variable
        f = 0 # Removes storms that have too wide range in TF values. Similar to d, but calculated based on percent IL for each TF collector. See diff_thresh and small, med, and large variables.
        g = 0 # Removes storms that have too wide range of TF values (continuous function with storm size, see metric variable).
        h = 1 # Removes storms if there is a large difference between TF collectors (controlled by storm size and logaritmic decay function. See variables A, B, and C)
        p = 1 # Removes storms in which not all TF collectors were working in the previous storm.
        q = 1 # Removes storms with more than 2 zero TF readings

        # Variables A and B are used to calculate logarithmic decay function to define threshold for percent difference in TF
        # for filter h. diff_thresh = A*e^(-B*storm size)+C
        A = 1000
        B = 0.25
        C = 68

        # The parameters below affect the filtering functions. They can be adjusted as desired.
        highest = 1.05
        excess = 2
        metric = 120
        small = 300
        med = 200
        large = 100

        # set threshholds for evaluating storms
        temp_thresh = 0.0 #degrees Celcius
        perc_thresh = .5

        for i in range(1,len(storms)):
            # remove storms with snow
            temp = storms.at[i, 'air_temp']
            if a == 1:
                if temp != 'nan':
                    if temp < temp_thresh:
                        storms.at[i, 'select_a'] = 1

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
                        storms.at[i, 'select_c'] = 1
                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i-1
                        if storms.at[m, 'TF1'] == 0 or storms.at[m,'TF2'] == 0 or storms.at[m,'TF3'] == 0 or storms.at[m,'TF4'] == 0 or storms.at[m,'TF5'] == 0:
                            storms.at[i,'select_p'] = 1
                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i+1
                        if storms.at[j,'TF1'] == 0 or storms.at[j,'TF2'] == 0 or storms.at[j,'TF3'] == 0 or storms.at[j,'TF4'] == 0 or storms.at[j,'TF5'] == 0:
                            storms.at[i,'select_b'] = 1
                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i,'TF1'] < perc_thresh*((storms.at[i,'TF2']+storms.at[i,'TF3']+storms.at[i,'TF4']+storms.at[i,'TF5'])/4):
                            storms.at[i,'select_d'] = 1
                        if storms.at[i,'TF2'] < perc_thresh*((storms.at[i,'TF1']+storms.at[i,'TF3']+storms.at[i,'TF4']+storms.at[i,'TF5'])/4):
                            storms.at[i,'select_d'] = 1
                        if storms.at[i,'TF3'] < perc_thresh*((storms.at[i,'TF1']+storms.at[i,'TF2']+storms.at[i,'TF4']+storms.at[i,'TF5'])/4):
                            storms.at[i,'select_d'] = 1
                        if storms.at[i,'TF4'] < perc_thresh*((storms.at[i,'TF1']+storms.at[i,'TF2']+storms.at[i,'TF3']+storms.at[i,'TF5'])/4):
                            storms.at[i,'select_d'] = 1
                        if storms.at[i,'TF5'] < perc_thresh*((storms.at[i,'TF1']+storms.at[i,'TF2']+storms.at[i,'TF3']+storms.at[i,'TF4'])/4):
                            storms.at[i,'select_d'] = 1

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

                tf1 = storms.at[i, 'TF1']
                tf2 = storms.at[i, 'TF2']
                tf3 = storms.at[i, 'TF3']
                tf4 = storms.at[i, 'TF4']
                tf5 = storms.at[i, 'TF5']

                TFdiffa = abs(((tf1 - tf2) / ((tf1 + tf2) / 2)) * 100)
                TFdiffb = abs(((tf1 - tf3) / ((tf1 + tf3) / 2)) * 100)
                TFdiffc = abs(((tf1 - tf4) / ((tf1 + tf4) / 2)) * 100)
                TFdiffd = abs(((tf1 - tf5) / ((tf1 + tf5) / 2)) * 100)
                TFdiffe = abs(((tf2 - tf3) / ((tf2 + tf3) / 2)) * 100)
                TFdifff = abs(((tf2 - tf4) / ((tf2 + tf4) / 2)) * 100)
                TFdiffg = abs(((tf2 - tf5) / ((tf2 + tf5) / 2)) * 100)
                TFdiffh = abs(((tf3 - tf4) / ((tf3 + tf4) / 2)) * 100)
                TFdiffi = abs(((tf3 - tf5) / ((tf3 + tf5) / 2)) * 100)
                TFdiffj = abs(((tf4 - tf5) / ((tf4 + tf5) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffa > diff_thresh or percDiffb > diff_thresh or percDiffc > diff_thresh or percDiffd > diff_thresh or percDiffe > diff_thresh or percDifff > diff_thresh or percDiffg > diff_thresh or percDiffh > diff_thresh or percDiffi > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffa > diff_thresh or TFdiffb > diff_thresh or TFdiffc > diff_thresh or TFdiffd > diff_thresh or TFdiffe > diff_thresh or TFdifff > diff_thresh or TFdiffg > diff_thresh or TFdiffh > diff_thresh or TFdiffi > diff_thresh or TFdiffj > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF5 is zero
            if TF1 == 1 and TF2 == 1 and TF3 ==1 and TF4 == 1 and TF5 == 0:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF1 == 0 or TF2 == 0 or TF3 == 0 or TF4 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF1'] == 0 or storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF1'] == 0 or storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF3'] + storms.at[i, 'TF4'])/ 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF3'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2'] + storms.at[i, 'TF3']) / 3):
                            storms.at[i, 'select_d'] = 1

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

                tf1 = storms.at[i, 'TF1']
                tf2 = storms.at[i, 'TF2']
                tf3 = storms.at[i, 'TF3']
                tf4 = storms.at[i, 'TF4']

                TFdiffa = abs(((tf1 - tf2) / ((tf1 + tf2) / 2)) * 100)
                TFdiffb = abs(((tf1 - tf3) / ((tf1 + tf3) / 2)) * 100)
                TFdiffc = abs(((tf1 - tf4) / ((tf1 + tf4) / 2)) * 100)
                TFdiffe = abs(((tf2 - tf3) / ((tf2 + tf3) / 2)) * 100)
                TFdifff = abs(((tf2 - tf4) / ((tf2 + tf4) / 2)) * 100)
                TFdiffh = abs(((tf3 - tf4) / ((tf3 + tf4) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffa > diff_thresh or percDiffb > diff_thresh or percDiffc > diff_thresh or percDiffe > diff_thresh or percDifff > diff_thresh or percDiffh > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffa > diff_thresh or TFdiffb > diff_thresh or TFdiffc > diff_thresh or TFdiffe > diff_thresh or TFdifff > diff_thresh or TFdiffh > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF4 is zero
            if TF1 == 1 and TF2 == 1 and TF3 ==1 and TF4 == 0 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF1 == 0 or TF2 == 0 or TF3 == 0 or TF5 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p ==1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF1'] == 0 or storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF5'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF1'] == 0 or storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF5'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF3'] + storms.at[i, 'TF5'])/ 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF3'] + storms.at[i, 'TF5']) / 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2'] + storms.at[i, 'TF5']) / 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2'] + storms.at[i, 'TF3']) / 3):
                            storms.at[i, 'select_d'] = 1

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

                tf1 = storms.at[i, 'TF1']
                tf2 = storms.at[i, 'TF2']
                tf3 = storms.at[i, 'TF3']
                tf5 = storms.at[i, 'TF5']

                TFdiffa = abs(((tf1 - tf2) / ((tf1 + tf2) / 2)) * 100)
                TFdiffb = abs(((tf1 - tf3) / ((tf1 + tf3) / 2)) * 100)
                TFdiffd = abs(((tf1 - tf5) / ((tf1 + tf5) / 2)) * 100)
                TFdiffe = abs(((tf2 - tf3) / ((tf2 + tf3) / 2)) * 100)
                TFdiffg = abs(((tf2 - tf5) / ((tf2 + tf5) / 2)) * 100)
                TFdiffi = abs(((tf3 - tf5) / ((tf3 + tf5) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffa > diff_thresh or percDiffb > diff_thresh or percDiffd > diff_thresh or percDiffe > diff_thresh or percDiffg > diff_thresh or percDiffi > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffa > diff_thresh or TFdiffb > diff_thresh or TFdiffd > diff_thresh or TFdiffe > diff_thresh or TFdiffg > diff_thresh or TFdiffi > diff_thresh:
                        storms.at[i, 'select_h'] = 1


            #if TF3 is zero
            if TF1 == 1 and TF2 == 1 and TF3 == 0 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF1 == 0 or TF2 == 0 or TF5 == 0 or TF4 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF1'] == 0 or storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF1'] == 0 or storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF5'] + storms.at[i, 'TF4'])/ 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF5'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2'] + storms.at[i, 'TF5']) / 3):
                            storms.at[i, 'select_d'] = 1

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

                tf1 = storms.at[i, 'TF1']
                tf2 = storms.at[i, 'TF2']
                tf4 = storms.at[i, 'TF4']
                tf5 = storms.at[i, 'TF5']

                TFdiffa = abs(((tf1 - tf2) / ((tf1 + tf2) / 2)) * 100)
                TFdiffc = abs(((tf1 - tf4) / ((tf1 + tf4) / 2)) * 100)
                TFdiffd = abs(((tf1 - tf5) / ((tf1 + tf5) / 2)) * 100)
                TFdifff = abs(((tf2 - tf4) / ((tf2 + tf4) / 2)) * 100)
                TFdiffg = abs(((tf2 - tf5) / ((tf2 + tf5) / 2)) * 100)
                TFdiffj = abs(((tf4 - tf5) / ((tf4 + tf5) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffa > diff_thresh or percDiffc > diff_thresh or percDiffd > diff_thresh or percDifff > diff_thresh or percDiffg > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffa > diff_thresh or TFdiffc > diff_thresh or TFdiffd > diff_thresh or TFdifff > diff_thresh or TFdiffg > diff_thresh or TFdiffj > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF2 is zero
            if TF1 == 1 and TF2 == 0 and TF3 ==1 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF1 == 0 or TF5 == 0 or TF3 == 0 or TF4 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF1'] == 0 or storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF1'] == 0 or storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF5'] + storms.at[i, 'TF3'] + storms.at[i, 'TF4'])/ 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF3'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF5'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF5'] + storms.at[i, 'TF3']) / 3):
                            storms.at[i, 'select_d'] = 1

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

                tf1 = storms.at[i, 'TF1']
                tf3 = storms.at[i, 'TF3']
                tf4 = storms.at[i, 'TF4']
                tf5 = storms.at[i, 'TF5']

                TFdiffb = abs(((tf1 - tf3) / ((tf1 + tf3) / 2)) * 100)
                TFdiffc = abs(((tf1 - tf4) / ((tf1 + tf4) / 2)) * 100)
                TFdiffd = abs(((tf1 - tf5) / ((tf1 + tf5) / 2)) * 100)
                TFdiffh = abs(((tf3 - tf4) / ((tf3 + tf4) / 2)) * 100)
                TFdiffi = abs(((tf3 - tf5) / ((tf3 + tf5) / 2)) * 100)
                TFdiffj = abs(((tf4 - tf5) / ((tf4 + tf5) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffb > diff_thresh or percDiffc > diff_thresh or percDiffd > diff_thresh or percDiffh > diff_thresh or percDiffi > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffb > diff_thresh or TFdiffc > diff_thresh or TFdiffd > diff_thresh or TFdiffh > diff_thresh or TFdiffi > diff_thresh or TFdiffj > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF1 is zero
            if TF1 == 0 and TF2 == 1 and TF3 ==1 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF2 == 0 or TF3 == 0 or TF4 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF3'] + storms.at[i, 'TF4'])/ 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF5'] + storms.at[i, 'TF3'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF5'] + storms.at[i, 'TF2'] + storms.at[i, 'TF4']) / 3):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF5'] + storms.at[i, 'TF2'] + storms.at[i, 'TF3']) / 3):
                            storms.at[i, 'select_d'] = 1

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

                tf2 = storms.at[i, 'TF2']
                tf3 = storms.at[i, 'TF3']
                tf4 = storms.at[i, 'TF4']
                tf5 = storms.at[i, 'TF5']

                TFdiffe = abs(((tf2 - tf3) / ((tf2 + tf3) / 2)) * 100)
                TFdifff = abs(((tf2 - tf4) / ((tf2 + tf4) / 2)) * 100)
                TFdiffg = abs(((tf2 - tf5) / ((tf2 + tf5) / 2)) * 100)
                TFdiffh = abs(((tf3 - tf4) / ((tf3 + tf4) / 2)) * 100)
                TFdiffi = abs(((tf3 - tf5) / ((tf3 + tf5) / 2)) * 100)
                TFdiffj = abs(((tf4 - tf5) / ((tf4 + tf5) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffe > diff_thresh or percDifff > diff_thresh or percDiffg > diff_thresh or percDiffh > diff_thresh or percDiffi > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffe > diff_thresh or TFdifff > diff_thresh or TFdiffg > diff_thresh or TFdiffh > diff_thresh or TFdiffi > diff_thresh or TFdiffj > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF1 and TF2 are zero
            if TF1 == 0 and TF2 == 0 and TF3 ==1 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF3 == 0 or TF4 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF4'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF4']) / 2):
                            storms.at[i, 'select_d'] = 1

                # Calculate IL amount for each TF collector
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffh = abs(((IL3 - IL4) / ((IL3 + IL4) / 2)) * 100)
                percDiffi = abs(((IL3 - IL5) / ((IL3 + IL5) / 2)) * 100)
                percDiffj = abs(((IL4 - IL5) / ((IL4 + IL4) / 2)) * 100)

                tf3 = storms.at[i, 'TF3']
                tf4 = storms.at[i, 'TF4']
                tf5 = storms.at[i, 'TF5']

                TFdiffh = abs(((tf3 - tf4) / ((tf3 + tf4) / 2)) * 100)
                TFdiffi = abs(((tf3 - tf5) / ((tf3 + tf5) / 2)) * 100)
                TFdiffj = abs(((tf4 - tf5) / ((tf4 + tf5) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffh > diff_thresh or percDiffi > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffh > diff_thresh or TFdiffi > diff_thresh or TFdiffj > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF1 and TF3 are zero
            if TF1 == 0 and TF2 == 1 and TF3 ==0 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF2 == 0 or TF4 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF4'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF4']) / 2):
                            storms.at[i, 'select_d'] = 1

                # Calculate IL amount for each TF collector
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDifff = abs(((IL2 - IL4) / ((IL2 + IL4) / 2)) * 100)
                percDiffg = abs(((IL2 - IL5) / ((IL2 + IL5) / 2)) * 100)
                percDiffj = abs(((IL4 - IL5) / ((IL4 + IL4) / 2)) * 100)

                tf2 = storms.at[i, 'TF2']
                tf4 = storms.at[i, 'TF4']
                tf5 = storms.at[i, 'TF5']

                TFdifff = abs(((tf2 - tf4) / ((tf2 + tf4) / 2)) * 100)
                TFdiffg = abs(((tf2 - tf5) / ((tf2 + tf5) / 2)) * 100)
                TFdiffj = abs(((tf4 - tf5) / ((tf4 + tf5) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDifff > diff_thresh or percDiffg > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdifff > diff_thresh or TFdiffg > diff_thresh or TFdiffj > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF1 and TF4 are zero
            if TF1 == 0 and TF2 == 1 and TF3 ==1 and TF4 == 0 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF3 == 0 or TF2 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF2'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF2'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select_d'] = 1

                # Calculate IL amount for each TF collector
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffe = abs(((IL2 - IL3) / ((IL2 + IL3) / 2)) * 100)
                percDiffg = abs(((IL2 - IL5) / ((IL2 + IL5) / 2)) * 100)
                percDiffi = abs(((IL3 - IL5) / ((IL3 + IL5) / 2)) * 100)

                tf2 = storms.at[i, 'TF2']
                tf3 = storms.at[i, 'TF3']
                tf5 = storms.at[i, 'TF5']

                TFdiffe = abs(((tf2 - tf3) / ((tf2 + tf3) / 2)) * 100)
                TFdiffg = abs(((tf2 - tf5) / ((tf2 + tf5) / 2)) * 100)
                TFdiffi = abs(((tf3 - tf5) / ((tf3 + tf5) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffe > diff_thresh or percDiffg > diff_thresh or percDiffi > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffe > diff_thresh or TFdiffg > diff_thresh or TFdiffi > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF1 and TF5 are zero
            if TF1 == 0 and TF2 == 1 and TF3 ==1 and TF4 == 1 and TF5 == 0:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF2 == 0 or TF3 == 0 or TF4 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF4'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF4']) / 2):
                            storms.at[i, 'select_d'] = 1

                # Calculate IL amount for each TF collector
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']

                # Calculate the percent difference between IL at each TF collector
                percDiffe = abs(((IL2 - IL3) / ((IL2 + IL3) / 2)) * 100)
                percDifff = abs(((IL2 - IL4) / ((IL2 + IL4) / 2)) * 100)
                percDiffh = abs(((IL3 - IL4) / ((IL3 + IL4) / 2)) * 100)

                tf2 = storms.at[i, 'TF2']
                tf3 = storms.at[i, 'TF3']
                tf4 = storms.at[i, 'TF4']

                TFdiffe = abs(((tf2 - tf3) / ((tf2 + tf3) / 2)) * 100)
                TFdifff = abs(((tf2 - tf4) / ((tf2 + tf4) / 2)) * 100)
                TFdiffh = abs(((tf3 - tf4) / ((tf3 + tf4) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffe > diff_thresh or percDifff > diff_thresh or percDiffh > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffe > diff_thresh or TFdifff > diff_thresh or TFdiffh > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF2 and TF3 are zero
            if TF1 == 1 and TF2 == 0 and TF3 ==0 and TF4 == 1 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF1 == 0 or TF4 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF1'] == 0 or storms.at[m, 'TF4'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF1'] == 0 or storms.at[j, 'TF4'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF4'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF4']) / 2):
                            storms.at[i, 'select_d'] = 1

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffc = abs(((IL1 - IL4) / ((IL1 + IL4) / 2)) * 100)
                percDiffd = abs(((IL1 - IL5) / ((IL1 + IL5) / 2)) * 100)
                percDiffj = abs(((IL4 - IL5) / ((IL4 + IL4) / 2)) * 100)

                tf1 = storms.at[i, 'TF1']
                tf4 = storms.at[i, 'TF4']
                tf5 = storms.at[i, 'TF5']

                TFdiffc = abs(((tf1 - tf4) / ((tf1 + tf4) / 2)) * 100)
                TFdiffd = abs(((tf1 - tf5) / ((tf1 + tf5) / 2)) * 100)
                TFdiffj = abs(((tf4 - tf5) / ((tf4 + tf5) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffc > diff_thresh or percDiffd > diff_thresh or percDiffj > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffc > diff_thresh or TFdiffd > diff_thresh or TFdiffj > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF2 and TF4 are zero
            if TF1 == 1 and TF2 == 0 and TF3 ==1 and TF4 == 0 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF3 == 0 or TF1 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF1'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF1'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF1']) / 2):
                            storms.at[i, 'select_d'] = 1

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffb = abs(((IL1 - IL3) / ((IL1 + IL3) / 2)) * 100)
                percDiffd = abs(((IL1 - IL5) / ((IL1 + IL5) / 2)) * 100)
                percDiffi = abs(((IL3 - IL5) / ((IL3 + IL5) / 2)) * 100)

                tf1 = storms.at[i, 'TF1']
                tf3 = storms.at[i, 'TF3']
                tf5 = storms.at[i, 'TF5']

                TFdiffb = abs(((tf1 - tf3) / ((tf1 + tf3) / 2)) * 100)
                TFdiffd = abs(((tf1 - tf5) / ((tf1 + tf5) / 2)) * 100)
                TFdiffi = abs(((tf3 - tf5) / ((tf3 + tf5) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffb > diff_thresh or percDiffd > diff_thresh or percDiffi > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffb > diff_thresh or TFdiffd > diff_thresh or TFdiffi > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF2 and TF5 are zero
            if TF1 == 1 and TF2 == 0 and TF3 ==1 and TF4 == 1 and TF5 == 0:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF4 == 0 or TF3 == 0 or TF1 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF4'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF1'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF4'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF1'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF4']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF4']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF1']) / 2):
                            storms.at[i, 'select_d'] = 1

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']

                # Calculate the percent difference between IL at each TF collector
                percDiffb = abs(((IL1 - IL3) / ((IL1 + IL3) / 2)) * 100)
                percDiffc = abs(((IL1 - IL4) / ((IL1 + IL4) / 2)) * 100)
                percDiffh = abs(((IL3 - IL4) / ((IL3 + IL4) / 2)) * 100)

                tf1 = storms.at[i, 'TF1']
                tf3 = storms.at[i, 'TF3']
                tf4 = storms.at[i, 'TF4']

                TFdiffb = abs(((tf1 - tf3) / ((tf1 + tf3) / 2)) * 100)
                TFdiffc = abs(((tf1 - tf4) / ((tf1 + tf4) / 2)) * 100)
                TFdiffh = abs(((tf3 - tf4) / ((tf3 + tf4) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffb > diff_thresh or percDiffc > diff_thresh or percDiffh > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffb > diff_thresh or TFdiffc > diff_thresh or TFdiffh > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF3 and TF4 are zero
            if TF1 == 1 and TF2 == 1 and TF3 ==0 and TF4 == 0 and TF5 == 1:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF5 == 0 or TF2 == 0 or TF1 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF5'] == 0 or storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF1'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF5'] == 0 or storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF1'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF5']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF5'] < perc_thresh * (
                                (storms.at[i, 'TF2'] + storms.at[i, 'TF1']) / 2):
                            storms.at[i, 'select_d'] = 1

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL5 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF5']

                # Calculate the percent difference between IL at each TF collector
                percDiffa = abs(((IL1 - IL2) / ((IL1 + IL2) / 2)) * 100)
                percDiffd = abs(((IL1 - IL5) / ((IL1 + IL5) / 2)) * 100)
                percDiffg = abs(((IL2 - IL5) / ((IL2 + IL5) / 2)) * 100)

                tf1 = storms.at[i, 'TF1']
                tf2 = storms.at[i, 'TF2']
                tf5 = storms.at[i, 'TF5']

                TFdiffa = abs(((tf1 - tf2) / ((tf1 + tf2) / 2)) * 100)
                TFdiffd = abs(((tf1 - tf5) / ((tf1 + tf5) / 2)) * 100)
                TFdiffg = abs(((tf2 - tf5) / ((tf2 + tf5) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffa > diff_thresh or percDiffd > diff_thresh or percDiffg > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffa > diff_thresh or TFdiffd > diff_thresh or TFdiffg > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF3 and TF5 are zero
            if TF1 == 1 and TF2 == 1 and TF3 ==0 and TF4 == 1 and TF5 == 0:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF2 == 0 or TF4 == 0 or TF1 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF4'] == 0 or storms.at[m, 'TF1'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF4'] == 0 or storms.at[j, 'TF1'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF4'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF4'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF4'] + storms.at[i, 'TF1']) / 2):
                            storms.at[i, 'select_d'] = 1

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL4 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF4']

                # Calculate the percent difference between IL at each TF collector
                percDiffa = abs(((IL1 - IL2) / ((IL1 + IL2) / 2)) * 100)
                percDiffc = abs(((IL1 - IL4) / ((IL1 + IL4) / 2)) * 100)
                percDifff = abs(((IL2 - IL4) / ((IL2 + IL4) / 2)) * 100)

                tf1 = storms.at[i, 'TF1']
                tf2 = storms.at[i, 'TF2']
                tf4 = storms.at[i, 'TF4']

                TFdiffa = abs(((tf1 - tf2) / ((tf1 + tf2) / 2)) * 100)
                TFdiffc = abs(((tf1 - tf4) / ((tf1 + tf4) / 2)) * 100)
                TFdifff = abs(((tf2 - tf4) / ((tf2 + tf4) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

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

                if f == 1 or g == 1:
                    if percDiffa > diff_thresh or percDiffc > diff_thresh or percDifff > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffa > diff_thresh or TFdiffc > diff_thresh or TFdifff > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #if TF5 and TF4 are zero
            if TF1 == 1 and TF2 == 1 and TF3 ==1 and TF4 == 0 and TF5 == 0:
                # remove storms where not all TF collectors are working
                if c == 1:
                    if TF2 == 0 or TF3 == 0 or TF1 == 0:
                        storms.at[i, 'select_c'] = 1

                # remove storms if not all TF collectors were working in the previous storm
                if p == 1:
                    if i > 1:
                        m = i - 1
                        if storms.at[m, 'TF2'] == 0 or storms.at[m, 'TF3'] == 0 or storms.at[m, 'TF1'] == 0:
                            storms.at[i, 'select_p'] = 1

                # remove storms if not all TF collectors are working in the next storm
                if b == 1:
                    if i < (len(storms)-1):
                        j = i + 1
                        if storms.at[j, 'TF2'] == 0 or storms.at[j, 'TF3'] == 0 or storms.at[j, 'TF1'] == 0:
                            storms.at[i, 'select_b'] = 1

                # remove storms if a TF collector has large difference from average of other TF collectors
                if d == 1:
                    if i > 1:
                        if storms.at[i, 'TF3'] < perc_thresh * (
                                (storms.at[i, 'TF1'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF1'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF2']) / 2):
                            storms.at[i, 'select_d'] = 1
                        if storms.at[i, 'TF2'] < perc_thresh * (
                                (storms.at[i, 'TF3'] + storms.at[i, 'TF1']) / 2):
                            storms.at[i, 'select_d'] = 1

                # Calculate IL amount for each TF collector
                IL1 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF1']
                IL2 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF2']
                IL3 = storms.at[i, 'SecPrecip'] - storms.at[i, 'TF3']

                # Calculate the percent difference between IL at each TF collector
                percDiffa = abs(((IL1 - IL2) / ((IL1 + IL2) / 2)) * 100)
                percDiffb = abs(((IL1 - IL3) / ((IL1 + IL3) / 2)) * 100)
                percDiffe = abs(((IL2 - IL3) / ((IL2 + IL3) / 2)) * 100)

                tf1 = storms.at[i, 'TF1']
                tf2 = storms.at[i, 'TF2']
                tf3 = storms.at[i, 'TF3']

                TFdiffa = abs(((tf1 - tf2) / ((tf1 + tf2) / 2)) * 100)
                TFdiffb = abs(((tf1 - tf3) / ((tf1 + tf3) / 2)) * 100)
                TFdiffe = abs(((tf2 - tf3) / ((tf2 + tf3) / 2)) * 100)

                diff_thresh = storms.at[i, 'SecPrecip'] * metric

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values (adjustable threshhold with diff_thresh variable)
                if f == 1:
                    if storms.at[i, 'SecPrecip'] < 5:
                        diff_thresh = small
                    elif storms.at[i, 'SecPrecip'] < 10:
                        diff_thresh = med
                    elif storms.at[i, 'SecPrecip'] > 10:
                        diff_thresh = large

                # Remove storms that have too large variation in perc difference, meaning wide range in TF values based on linear function of storm size (adjustable threshhold with metric variable)
                if g == 1:
                    diff_thresh = storms.at[i, 'SecPrecip']*metric
                    #print('diff_thresh:', diff_thresh)

                if f == 1 or g == 1:
                    if percDiffa > diff_thresh or percDiffb > diff_thresh or percDiffe > diff_thresh:
                        storms.at[i, 'select_fg'] = 1

                if h == 1:
                    x = storms.at[i, 'SecPrecip']
                    diff_thresh = A*math.exp(-x*B)+C
                    if TFdiffa > diff_thresh or TFdiffb > diff_thresh or TFdiffe > diff_thresh:
                        storms.at[i, 'select_h'] = 1

            #remove storms with more than 2 zero TF readings
            if q == 1:
                if (TF1 + TF2 + TF3 + TF4 + TF5) < 3:
                    storms.at[i, 'select_q'] = 1

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
                    storms.at[i, 'select_e'] = 1
                    #print('i:', i, 'SecPrecip', storms.at[i, 'SecPrecip'], 'TF1:', storms.at[i, 'TF1'])

            #print('select_a',storms.at[i,'select_a'])

            if storms.at[i, 'select_a'] != 1 and storms.at[i, 'select_b'] == 0 and storms.at[i, 'select_c'] == 0 and storms.at[i, 'select_d'] == 0 and storms.at[i, 'select_e'] == 0 and storms.at[i, 'select_fg'] == 0 and storms.at[i, 'select_h'] == 0 and storms.at[i, 'select_p'] == 0 and storms.at[i, 'select_q'] == 0:
                storms.at[i, 'select'] = 0

        path_check = Path(output_folder+'all_storms_'+site+'_filter_index.csv')
        if path_check.is_file() == True:
            ha = False
        else:
            ha = True
        ha = True
        #write storms with 'select' column to new csv.
        storms.to_csv(output_folder+'all_storms_'+site+'_filter_index.csv', header = ha, mode='w')

        #print('storms_3', storms.head())

        #remove filtered storms
        selected = storms.copy(deep=True)

        path_check = Path(output_folder+'Selected_storms_'+site+'.csv')
        if path_check.is_file() == True:
            ha = False
        else:
            ha = True
        ha = True
        selected.drop(selected.index[selected['select']!= 0], inplace=True)
        selected.to_csv(output_folder+'Selected_storms_'+site+'.csv', header = ha, mode='w')

        #print('all storms', storms.head())
        #print('selected storms_2', selected.head())

    print('Storm selection complete.')
    return(storms, selected)

def plot_selected_storms(site, storms, output_folder,date):
    from matplotlib.ticker import FormatStrFormatter
    selected_a = storms.copy()
    selected_a.drop(selected_a.index[selected_a['select_a']==0], inplace = True)
    selected_b = storms.copy()
    selected_b.drop(selected_b.index[selected_b['select_b']==0], inplace = True)
    selected_c = storms.copy()
    selected_c.drop(selected_c.index[selected_c['select_c']==0], inplace = True)
    selected_d = storms.copy()
    selected_d.drop(selected_d.index[selected_d['select_d']==0], inplace = True)
    selected_e = storms.copy()
    selected_e.drop(selected_e.index[selected_e['select_e']==0], inplace = True)
    selected_fg = storms.copy()
    selected_fg.drop(selected_fg.index[selected_fg['select_fg']==0], inplace = True)
    selected_h = storms.copy()
    selected_h.drop(selected_h.index[selected_h['select_h']==0], inplace = True)
    selected_p = storms.copy()
    selected_p.drop(selected_p.index[selected_p['select_p']==0], inplace = True)
    selected_q = storms.copy()
    selected_q.drop(selected_q.index[selected_q['select_q']==0], inplace = True)
    selected_all = storms.copy()
    selected_all.drop(selected_all.index[selected_all['select']!=0], inplace = True)

    fig = plt.figure(num=1, figsize=(20, 12))
    gs = fig.add_gridspec(2,2)
    ax = fig.add_subplot(gs[0,0])
    ax2 = fig.add_subplot(gs[0,1], sharex = ax, sharey = ax)
    ax.set_title(None)
    ax2.set_title(None)
    ax.scatter(storms['SecPrecip'], storms['IL_perc'], s=120, c="black", marker='s', label='All Storms')
    ax.scatter(selected_a['SecPrecip'], selected_a['IL_perc'], s=100, c="white", marker='s', edgecolors='black',
                linewidths=.3, label='Suspected snowfall')
    ax.scatter(selected_b['SecPrecip'], selected_b['IL_perc'], s=65, c="lime", marker='<', edgecolors='black',
                linewidths=.3, label='Not all TF collectors working in next storm')
    ax.scatter(selected_c['SecPrecip'], selected_c['IL_perc'], s=60, c="orange", marker='v', edgecolors='black',
                linewidths=.3, label='Not all TF collectors working in current storm')
    ax.scatter(selected_p['SecPrecip'], selected_p['IL_perc'], s=55, c="magenta", marker='>', edgecolors='black',
                linewidths=.3, label='Not all TF collectors working in previous storm')
    ax.scatter(selected_d['SecPrecip'], selected_d['IL_perc'], s=50, c="cyan",  marker='^', edgecolors='black',
                linewidths=.3)  # label = 'Storms removed because of large variance across TF collectors')
    ax.scatter(selected_fg['SecPrecip'], selected_fg['IL_perc'], s=50, c="cyan", marker='^', edgecolors='black',
                linewidths=.3)  # , label = 'Storms removed because of large variance across TF collectors')
    ax.scatter(selected_h['SecPrecip'], selected_h['IL_perc'], s=50, c="cyan", marker='^',edgecolors='black',
                linewidths=.3, label='Too large variance across TF collectors')
    ax.scatter(selected_e['SecPrecip'], selected_e['IL_perc'], s=40, c="orange", marker='p', edgecolors='black',
                linewidths=.3, label='More than 1 TF collector exceeds precip amount')
    ax.scatter(selected_q['SecPrecip'], selected_q['IL_perc'], s=40, c="yellow", marker='d', edgecolors='black',
                linewidths=.3, label='More than 2 TF collectors with 0 values')  # , edgecolors='black', linewidths=.1)
    ax.scatter(selected_all['SecPrecip'], selected_all['IL_perc'], s=10, c="red", marker='o', label='Remaining storms')

    ax.set_ylim([-10,110])
    ax.set_xlabel('Storm Size (mm)')
    ax.set_ylabel('Percent Interception Loss')
    ax.set_title('All storms and filters at '+site)
    #ax.legend(bbox_to_anchor=(0.2,-.3))
    ax.legend(loc='lower center', bbox_to_anchor=(.5, -.6))

   #ax2 = fig.add_subplot(2,2,(2,1))
    ax2.scatter(selected_all['SecPrecip'], selected_all['IL_perc'], s=10, c="red", marker='o')
    ax2.set_xlabel('Storm Size (mm)')
    ax2.set_ylabel('Percent Interception Loss')
    ax2.set_title('Selected Storms at '+site)

    ax.yaxis.set_major_formatter(FormatStrFormatter('%.0f'))
    ytick_loc = [0, 20, 40, 60, 80, 100]
    ax.set_yticks(ytick_loc)

    plt.savefig(output_folder+"Selected_storms_"+site+'_'+date+".png")

    plt.show()


main_dir = 'C:/Users/Abigail Sandquist/Box/IL/IL_Project/'
#flatten files
destination = main_dir+'NEON_Downloads/NEON_Temp/NEON_temp-air-single/'
flatten(destination)

#filter for the desired 30-min sensor 2 temp file
source = destination
destination_filter = main_dir+'NEON_Downloads/NEON_Temp/'
filter_string = '020.030.SAAT_30min'
filter(source, destination_filter, filter_string)

Sites = ['ABBY','BART','BLAN', 'DEJU', 'DELA', 'DSNY', 'GRSM', 'GUAN', 'JERC', 'KONZ','LENO', 'MLBS', 'ORNL', 'OSBS','SERC', 'STEI','TEAK','TREE','UKFS','UNDE','WREF','YELL']
date = 'jan23'
#combine air temp files for each site
path = destination_filter
output_folder = main_dir+'Combined/Temp/'
combine_air(Sites, path, output_folder)

#Filter to get selected storms for each site
input_air = output_folder
input_storm = main_dir+'Staging/'
output_folder_selected_storms = main_dir+'Selected_Storms/'
storms, selected = storm_select(Sites, input_air, input_storm, output_folder_selected_storms)


