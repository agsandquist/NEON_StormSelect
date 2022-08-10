import os
import numpy as np
import glob
import pandas as pd
import shutil

## This program includes functions to take raw downloaded NEON precipitation and
## throughfall data and outputs a file with defined storms for each site in a given
## list. The functions also calculate duration and interception loss for each storm.

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

# This function moves only the 30 minute files from the flattened NEON folder
# into the resource folder. Filter string should be '30min'.
def filter(source, destination, filter_string):
    files = os.listdir(source)
    for file in files:
        if filter_string in file:
            file_name = os.path.join(source, file)
            shutil.move(file_name, destination)
    print("Files filtered")

# This function concatenates all monthly precip files per site into one file.
def concatPrecip(Sites, dir, output_folder):
    os.chdir(dir)
    for site in Sites:
        secP = 0
        priP = 0

        all_filenames_pri = [i for i in glob.glob('NEON.*.'+site+'*.PRIPRE_30min.*.csv')]
        if (len(all_filenames_pri) == 0):
                print('No priPrecip for', site)
                priP = 1
        else:
                combined_pri = pd.concat([pd.read_csv(f, engine='python') for f in all_filenames_pri])

        all_filenames_sec = [i for i in glob.glob('NEON.*.'+site+'*.SECPRE_30min.*.csv')]
        if (len(all_filenames_sec) == 0):
            print('No secPrecip for', site)
            secP = 1
        else:
            combined_sec = pd.concat([pd.read_csv(f, engine='python') for f in all_filenames_sec])

        # Combine secondary and primary precip concatenated files into one csv.
          # If there are no data for sec or pri precip, fill in the column with zeros.
          # If there are no sec or pri precip data, print a statement indicating no precip data at that site.
        if priP == 0 and secP == 0:
            df_comb = pd.merge(combined_pri, combined_sec, on='startDateTime')
            df_comb.to_csv(output_folder+'Combined_Allprecip_' + site + '.csv',
                           index=False,
                           encoding='utf-8-sig')
        elif priP == 0 and secP == 1:
            df_comb = combined_pri
            df_comb['secPrecipBulk'] = 0
            df_comb.to_csv(output_folder+'Combined_Allprecip_' + site + '.csv',
                           index=False,
                           encoding='utf-8-sig')
        elif secP == 0 and priP == 1:
            df_comb = combined_sec
            df_comb['priPrecipBulk'] = 0
            df_comb.to_csv(output_folder+'Combined_Allprecip_' + site + '.csv',
                           index=False,
                           encoding='utf-8-sig')
        elif priP == 1 and secP == 1:
            print('NO PRECIP DATA FOR', site)


# This function concatenates all the TF sensor values for one TF sensor
# at a time (TF_sensor can be 1-5).
def concatTF(Sites, dir, output_folder, TF_sensor):
    os.chdir(dir)
    for site in Sites:
        all_filenames_tf = [i for i in glob.glob('NEON.*.' + site + '*' + TF_sensor + '.000.030.THRPRE_30min.*.csv')]
        #TF_sensor = '1'
        if (len(all_filenames_tf) == 0):
            print('No TF' +TF_sensor+ ' for', site)
        else:
            combined_csv_tf = pd.concat([pd.read_csv(f, engine='python') for f in all_filenames_tf])
            # export to csv
            combined_csv_tf.to_csv(output_folder + 'TF'+ TF_sensor +'_' + site + '.csv', index=False, encoding='utf-8-sig')

# This function combines each of the concatenated TF sensor files into one csv.
    # Inputs: Site = list of sites, dir = directory to concatenated TF files,
    # output_fodler = directory where you want to store output combined csv files
def combineTF(Sites, dir, output_folder):
    for site in Sites:
        no1 = 0
        no2 = 0
        no3 = 0
        no4 = 0
        combinedTF_df = pd.DataFrame()
        all_filenames_tf1 = [i for i in
                             glob.glob(dir+'TF1_' + site + '.csv')]
        if (len(all_filenames_tf1) == 0):
            print('No TF1 for', site)
            no1 = 1
        else:
            # read in concatenated csv
            combined_csv_tf1 = pd.read_csv(dir+'TF1_' + site + '.csv')
            # set column of combined dataframe to values from concatenated TF1 file
            combinedTF_df['startDateTime'] = combined_csv_tf1['startDateTime']
            combinedTF_df['endDateTime'] = combined_csv_tf1['endDateTime']
            combinedTF_df['TF1'] = combined_csv_tf1['TFPrecipBulk']

        all_filenames_tf2 = [i for i in
                             glob.glob(dir+'TF2_' + site + '.csv')]
        if (len(all_filenames_tf2) == 0):
            print('No TF2 for', site)
            no2 = 1
            if no1 == 0:
                combinedTF_df['TF2'] = 0
        elif no1 == 0 and not (len(all_filenames_tf2) == 0):
            # read in concatenated csv
            combined_csv_tf2 = pd.read_csv(dir+'TF2_' + site + '.csv')
            # set column of combined dataframe to values from concatenated TF2 file
            combinedTF_df['TF2'] = combined_csv_tf2['TFPrecipBulk']
        elif no1 == 1 and not (len(all_filenames_tf2) == 0):
            # read in concatenated csv
            combined_csv_tf2 = pd.read_csv(dir + 'TF2_' + site + '.csv')
            # set columns of combined dataframe to values from concatenated TF2 file
            combinedTF_df['startDateTime'] = combined_csv_tf2['startDateTime']
            combinedTF_df['endDateTime'] = combined_csv_tf2['endDateTime']
            combinedTF_df['TF2'] = combined_csv_tf2['TFPrecipBulk']

        all_filenames_tf3 = [i for i in
                             glob.glob(dir+'TF3_' + site + '.csv')]
        if (len(all_filenames_tf3) == 0):
            print('No TF3 for', site)
            no3 = 1
            if no1 == 0 or no2 == 0:
                combinedTF_df['TF3'] = 0
        elif no1 == 0 or no2 == 0 and not (len(all_filenames_tf3) == 0):
            # read in concatenated csv
            combined_csv_tf3 = pd.read_csv(dir+'TF3_' + site + '.csv')
            # set column of combined dataframe to values from concatenated TF3 file
            combinedTF_df['TF3'] = combined_csv_tf3['TFPrecipBulk']
        elif no1 == 1 and no2 == 1 and not (len(all_filenames_tf3) == 0):
            # read in concatenated csv
            combined_csv_tf3 = pd.read_csv(dir + 'TF3_' + site + '.csv')
            # set column of combined dataframe to values from concatenated TF3 file
            combinedTF_df['startDateTime'] = combined_csv_tf3['startDateTime']
            combinedTF_df['endDateTime'] = combined_csv_tf3['endDateTime']
            combinedTF_df['TF3'] = combined_csv_tf3['TFPrecipBulk']

        all_filenames_tf4 = [i for i in
                             glob.glob(dir+'TF4_' + site + '.csv')]
        if (len(all_filenames_tf4) == 0):
            print('No TF4 for', site)
            no4 = 1
            if no1 == 0 or no2 == 0 or no3 == 0:
                combinedTF_df['TF4'] = 0
        elif no1 == 0 or no2 == 0 or no3 == 0 and not (len(all_filenames_tf4) == 0):
            # read in concatenated csv
            combined_csv_tf4 = pd.read_csv(dir+'TF4_' + site + '.csv')
            # set column of combined dataframe to values from concatenated TF4 file
            combinedTF_df['TF4'] = combined_csv_tf4['TFPrecipBulk']
        elif no1 == 1 and no2 == 1 and no3 == 1 and not (len(all_filenames_tf4) == 0):
            # read in concatenated csv
            combined_csv_tf4 = pd.read_csv(dir+'TF4_' + site + '.csv')
            # set column of combined dataframe to values from concatenated TF4 file
            combinedTF_df['startDateTime'] = combined_csv_tf4['startDateTime']
            combinedTF_df['endDateTime'] = combined_csv_tf4['endDateTime']
            combinedTF_df['TF4'] = combined_csv_tf4['TFPrecipBulk']

        all_filenames_tf5 = [i for i in
                             glob.glob(dir+'TF5_' + site + '.csv')]
        if (len(all_filenames_tf5) == 0):
            print('No TF5 for', site)
            if no1 == 0 or no2 == 0 or no3 == 0 or no4 == 0:
                combinedTF_df['TF5'] = 0
        elif no1 == 0 or no2 == 0 or no3 == 0 or no4 == 0 and not (len(all_filenames_tf5) == 0):
            # read in concatenated csv
            combined_csv_tf5 = pd.read_csv(dir+'TF5_' + site + '.csv')
            # set column of combined dataframe to values from concatenated TF5 file
            combinedTF_df['TF5'] = combined_csv_tf5['TFPrecipBulk']
        elif no1 == 1 and no2 == 1 and no3 == 1 and no4 == 1 and not (len(all_filenames_tf5) == 0):
            # read in concatenated csv
            combined_csv_tf5 = pd.read_csv(dir+'TF5_' + site + '.csv')
            # set column of combined dataframe to values from concatenated TF5 file
            combinedTF_df['startDateTime'] = combined_csv_tf5['startDateTime']
            combinedTF_df['endDateTime'] = combined_csv_tf5['endDateTime']
            combinedTF_df['TF5'] = combined_csv_tf5['TFPrecipBulk']

        # write dataframe with all concatenated TF sensor data to one csv
        combinedTF_df.to_csv(output_folder+'Combined_allTF_' + site + '.csv', index=False, encoding='utf-8-sig')

# The below functions will define storm events from the TF and precip data.
    # Inputs: a = the dataframe column that contains the precip data.
def zero_runs(a):
    # Create an array that is 1 where a is 0, and pad each end with an extra 0.
    iszero = np.concatenate(([0], np.not_equal(a, 0).view(np.int8), [0]))
    absdiff = np.abs(np.diff(iszero))
    ranges = np.where(absdiff == 1)[0].reshape(-1, 2)
    print('ranges', ranges)
    return ranges

    # Inputs: zero_trail = the ouput of the zero_runs function.
    # storm_len = minimum length of the storm (in 30 min increments)
    # storm_gap = mininum dry period between storms (in 30 min increments)
def storm_event(zero_trail, storm_len, storm_gap):
    diffin = np.array([zero_trail[i + 1][0] - zero_trail[i][1] for i in range(len(zero_trail) - 1)] + [-1])
    empty_array = np.empty((0, 2), int)
    if len(zero_trail) == 0:
        return empty_array
    start, end = zero_trail[0]
    for i, j in zip(zero_trail, diffin):
        end = i[1]
        if j > storm_gap:
            if end - start > storm_len:
                empty_array = np.append(empty_array, np.array([[start, end]]), axis=0)
            start = end + j
        elif j == -1:
            if i[1] - i[0] > storm_len:
                empty_array = np.append(empty_array, np.array([[i[0], i[1]]]), axis=0)
    return empty_array

    # Inputs: df = dataframe with precip and TF data, prec1 = name of column with priPrecip data,
    # prec2 = name of column with SecPrecip data, tf1 through tf 5 = name of columns with tf data,
    # gap = minimum gap (in 30 min chunks) between storms
def agg_prec(df, prec1, prec2, tf1, tf2, tf3, tf4, tf5, medTF, gap):
    df['startDateTime'] = pd.to_datetime(df['startDateTime'])
    if df[prec2].sum() == 0.0:
        print('secPreip is zero for ', site)
        prep = prec1 #df[prec1].to_numpy() # **defining storms based on PriPrecip measurement only if SecPrecip = 0
    else:
        prep = prec2 #df[prec2].to_numpy()
    zero_trail = zero_runs(df[prep])
    df_res = pd.DataFrame(columns=['startDateTime', 'duration', 'duration2', 'p1', 'p2', 'tf1', 'tf2', 'tf3', 'tf4', 'tf5', 'medTF', 'tf1post', 'tf2post', 'tf3post', 'tf4post', 'tf5post', 'IL_perc', 'IL_mm'])
    storms = storm_event(zero_trail, 0, 6)
    count = 1
    counter = 0
    q = 1
    for i, j in storms:
        if count < len(storms):
            k = storms[count, 0]
        else:
            k = j
        m = k - gap
        if not df[prep][m:k-1].sum() > 0 and not df[tf1][m:k-1].sum() > 0.0 and not df[tf2][m:k-1].sum() > 0 and not df[tf3][m:k-1].sum() > 0 and not df[tf4][m:k-1].sum() > 0 and not df[tf5][m:k-1].sum() > 0:
            q = 1
            counter = counter + 1

            # combine storms if there is precip or TF in the gap.
        else:
            if count < len(storms):
                p = storms[count-q, 0]  # set j value for next storm
                q = q + 1
            else:
                p = i
            if count < (len(storms)):
                storms[count, 0] = p
                k = storms[count, 0]
        count = count + 1

    r = 0
    for y in range(len(storms)):
        r = r + 1
        if r < len(storms):
            if storms[r - 1, 0] == storms[r, 0]:
                g = 0
                g = np.count_nonzero(storms == storms[r - 1, 0])
                a = r-1
                b = ((r-1)+g-1)
                storms = np.delete(storms, obj=slice(a,b), axis=0) # remove lines above last line with same start time

    step = 1
    for i, j in storms:
        if step < len(storms):
            k = storms[step, 0]
        else:
            k = j
        for m in range(k): # define g as row after precip stops in storm
            if df[prep][m:k].sum() == 0:
                g = m
            else:
                g = k

        prep1 = df[prec1][i:k].sum()
        prep2 = df[prec2][i:k].sum()
        prep_storm = df[prep][i:k].sum()
        thr1 = df[tf1][i:k].sum()
        thr2 = df[tf2][i:k].sum()
        thr3 = df[tf3][i:k].sum()
        thr4 = df[tf4][i:k].sum()
        thr5 = df[tf5][i:k].sum()
        med_TF = df[medTF][i:k].sum()
        thr1post = df[tf1][g:k].sum()
        thr2post = df[tf2][g:k].sum()
        thr3post = df[tf3][g:k].sum()
        thr4post = df[tf4][g:k].sum()
        thr5post = df[tf5][g:k].sum()
        # Calculate Interception loss percent and amount
        if prep_storm == 0:
            itcp_loss_perc = 0
            itcp_mm = 0
        else:
            itcp_loss_perc = ((prep_storm - med_TF) / prep_storm) * 100
            itcp_mm = prep_storm - med_TF

        df_res = df_res.append(
              {'startDateTime': df['startDateTime'][i],
              'decimaltime': df['startDateTime'][i],
              'endDateTime' : df['startDateTime'][k-6],
              'duration': pd.Timedelta(df['startDateTime'][j-1] - df['startDateTime'][i]).seconds / 60.0,
              'duration2': pd.Timedelta(df['startDateTime'][k-6] - df['startDateTime'][i]).seconds / 60.0,
              'p1': prep1,
              'p2': prep2,
              'tf1': thr1,
              'tf2': thr2,
              'tf3': thr3,
              'tf4': thr4,
              'tf5': thr5,
              'medTF': med_TF,
              'tf1post': thr1post,
              'tf2post': thr2post,
              'tf3post': thr3post,
              'tf4post': thr4post,
              'tf5post': thr5post, #}, ignore_index=True)
              'IL_perc': itcp_loss_perc,
              'IL_mm': itcp_mm}, ignore_index=True)
        step = step+1

    print('storms_final:', storms)
    return df_res

# This function creates a file with defined storms, associated precip amount, tf amount,
# storm duration, IL, and vegetation structure data for a site
    # Inputs: precip_path = path to the combined precip data file,
    # thrfall_path = path to the combined throughfall file, site = NEON site
def staging(precip_path, thrfall_path, site, output_path):
    # biomass_df = pd.read_csv('static/Biomass.csv')
    # lai_df = pd.read_csv('static/LAI-500m-8d-MCD15A2H-006-results.csv')
    # veg_df = pd.read_csv('static/site_veg.csv')

    prec_df = pd.read_csv(precip_path)
    thrfall_df = pd.read_csv(thrfall_path)

    # site_biomass = biomass_df.loc[(biomass_df.Site == site)]
    # site_lai = lai_df.loc[lai_df.Category == site]
    # site_veg = veg_df.loc[veg_df.Site == site]

    prec_tf_df = pd.merge(prec_df, thrfall_df, on="startDateTime")

    # The if statements below calculate a median TF based on non-zero TF columns
    if (prec_tf_df['TF1'].any() > 0) & (prec_tf_df['TF2'].any() > 0) & (prec_tf_df['TF3'].any() > 0) & (prec_tf_df['TF4'].any() > 0) & \
            (prec_tf_df['TF5'].any() > 0):
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF2', 'TF3', 'TF4', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() == 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF2', 'TF3', 'TF4']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() == 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF2', 'TF3', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() == 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF2', 'TF4', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() == 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF3', 'TF4', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() == 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF2', 'TF3', 'TF4', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() == 0 & \
            prec_tf_df['TF5'].any() == 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF2', 'TF3']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() == 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() == 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF2', 'TF4']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() == 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() == 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF3', 'TF4']].median(axis=1)
    elif prec_tf_df['TF1'].any() == 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() == 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF2', 'TF3', 'TF4']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() == 0 & prec_tf_df['TF4'].any() == 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF2', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() == 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() == 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF3', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() == 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() == 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF2', 'TF3', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() == 0 & prec_tf_df['TF3'].any() == 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF4', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() == 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() == 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF2', 'TF4', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() == 0 & prec_tf_df['TF2'].any() == 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF3', 'TF4', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() == 0 & prec_tf_df['TF2'].any() == 0 & prec_tf_df['TF3'].any() == 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF4', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() == 0 & prec_tf_df['TF2'].any() == 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() == 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF3', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() == 0 & prec_tf_df['TF2'].any() == 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() == 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF3', 'TF4']].median(axis=1)
    elif prec_tf_df['TF1'].any() == 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() == 0 & prec_tf_df['TF4'].any() == 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF2', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() == 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() == 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() == 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF2', 'TF4']].median(axis=1)
    elif prec_tf_df['TF1'].any() == 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() == 0 & \
            prec_tf_df['TF5'].any() == 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF2', 'TF3']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() == 0 & prec_tf_df['TF3'].any() > 0 & prec_tf_df['TF4'].any() == 0 & \
            prec_tf_df['TF5'].any() == 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF3']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() == 0 & prec_tf_df['TF3'].any() == 0 & prec_tf_df['TF4'].any() == 0 & \
            prec_tf_df['TF5'].any() > 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF5']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() == 0 & prec_tf_df['TF3'].any() == 0 & prec_tf_df['TF4'].any() > 0 & \
            prec_tf_df['TF5'].any() == 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF4']].median(axis=1)
    elif prec_tf_df['TF1'].any() > 0 & prec_tf_df['TF2'].any() > 0 & prec_tf_df['TF3'].any() == 0 & prec_tf_df['TF4'].any() == 0 & \
            prec_tf_df['TF5'].any() == 0:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF2']].median(axis=1)
    else:
        prec_tf_df['medTF'] = prec_tf_df[['TF1', 'TF2', 'TF3', 'TF4', 'TF5']].median(axis=1)

    interception = agg_prec(prec_tf_df, 'priPrecipBulk', 'secPrecipBulk', 'TF1', 'TF2', 'TF3', 'TF4', 'TF5', 'medTF', 12)
    interception['Site'] = site

    if len(interception) != 0:
        #interception = pd.merge(interception, site_biomass, on="Site")
        #interception = pd.merge(interception, site_veg, on="Site")

        interception["Date"] = interception["startDateTime"].dt.date
        # interception["ldiFromDate"] = interception["Date"] - pd.Timedelta("3 day")
        # interception["ldiToDate"] = interception["Date"] + pd.Timedelta("4 day")

        # # A.Date,A.ldiFromDate,A.ldiToDate,B.Date,
        # sqlcode = '''
        #     select B.MCD15A2H_006_Lai_500m
        #     from interception A
        #     left outer join site_lai B on A.Site=B.Category
        #     where A.ldiFromDate <= B.Date and A.ldiToDate >= B.Date
        #     '''
        #
        # Lai_500m = ps.sqldf(sqlcode, locals())
        # interception["Lai_500m"] = Lai_500m

        interception_loss_df = interception[
            ["Date", "startDateTime", "duration", "p1", "p2", 'tf1', "tf2", 'tf3', 'tf4', 'tf5', 'medTF', 'tf1post', 'tf2post', 'tf3post', 'tf4post', 'tf5post', "IL_perc", "IL_mm", "Site"]] #"EF", "GH", "SH", "DF", "MF", "PH", "WW", "Lai_500m", "MCH", "Biomass",
        # Reformat column names to match those used by stormselection.py
        interception_loss_df.rename(columns={'p1': 'PriPrecip', 'p2': 'SecPrecip', 'tf1':'TF1', 'tf2':'TF2', 'tf3':'TF3', 'tf4':'TF4','tf5':'TF5'}, inplace=True)
        interception_loss_df.to_csv(output_path+'Output_'+site+'.csv', mode='a', header = True, index=False)



if __name__ == "__main__":
    main_dir = 'C:/Users/Abigail Sandquist/Box/IL/IL_Project/'

    # Define the path to newly downloaded and unzipped NEON data
    folder_flatten = main_dir+'NEON_Downloads/NEON_precip/NEON_precipitation/'

    # Flatten nested files in destination_flatten folder
    flattened = flatten(folder_flatten)

    # Filter for only 30 min precip and TF files from flattened folder, move to new destination folder
    source_filter = folder_flatten
    destination_filter = main_dir+'NEON_Downloads/NEON_precip/'
    filter_string = '30min'
    sort = filter(source_filter, destination_filter, filter_string)

    # Define Sites on which to combine files and define storms
    Sites = ['DSNY']

        # ['BLAN', 'SCBI', 'SERC', 'DSNY', 'JERC', 'OSBS', 'GUAN', 'STEI', 'TREE', 'UNDE', 'KONZ',
        #      'UKFS', 'GRSM', 'MLBS', 'ORNL', 'DELA', 'LENO', 'TALL', 'RMNP', 'CLBJ', 'YELL', 'SRER', 'ABBY',
        #      'WREF', 'SJER', 'SOAP', 'TEAK', 'BONA', 'JORN', 'DEJU']

    # Combine monthly precip files into one file per site
    dir_precip = destination_filter
    output_folder_precip = main_dir+'Combined/Precip/'
    concatP = concatPrecip(Sites, dir_precip, output_folder_precip)
    print('All precip data combined')

    # Combine monthly throughfall files into one file per site
    dir_TF = destination_filter
    output_folder_TFconcat = main_dir+'Combined/TF/Concat/'
    for sensor in range(1, 6):
        sensor_str = str(sensor)
        concatTF(Sites, dir_TF, output_folder_TFconcat, sensor_str)
    print('All TF sensors concatenated.')
    dir_TFcombine = output_folder_TFconcat
    output_folder_TFcombine = main_dir+'Combined/TF/'
    combTF = combineTF(Sites, dir_TFcombine, output_folder_TFcombine)
    print('All TF data combined')

    # Define storms from combined data files
    for site in Sites:
        # Read in combined TF and precip files
        precip_path = glob.glob(output_folder_precip + 'Combined_Allprecip_' + site + '.csv')
        thrfall_path = glob.glob(output_folder_TFcombine+'Combined_allTF_' + site + '.csv')
        output_path_staging = main_dir+'Staging/'

        if len(precip_path) == 0 or len(thrfall_path) == 0:
            if len(precip_path) == 0:
                print('Combined Precip Data file does not exist for ', site)
            if len(thrfall_path) == 0:
                print("Combined TF file does not exist for", site)
        else:
            staging(precip_path[0], thrfall_path[0], site, output_path_staging) #ouput csv with new storms
