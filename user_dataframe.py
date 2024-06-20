# add 1 second if start timestampt equals end timestamp
def add_s(st, end):
    
    if st == end:
        end += pd.Timedelta(seconds=1)
    return end
# if end timestamp equals '1970-01-01 00:00:00' set end to start timestamp
def clean_endv(st, end):
    if end == pd.Timestamp('1970-01-01 00:00:00'):
        end = st
    return end
# get all epoch entries for one user
def get_epoch(user_ids):
     
    ft = tuple(user_ids)    
    us_data = query_ch_df(
        """SELECT * FROM rocs.vital_data_epoch WHERE vital_data_epoch.customer IN {}""".format(ft)   )
    
    us_data = pd.merge(us_data, value_types, on='type')
    us_data = us_data.drop(columns=['type'])
    us_data = us_data.rename(columns={"code": "type"})
    us_data.startTimestamp = us_data.startTimestamp//1000
    us_data.endTimestamp = us_data.endTimestamp//1000
    us_data.startTimestamp = us_data.startTimestamp.apply(lambda x: datetime.datetime.fromtimestamp(x))
    us_data.endTimestamp = us_data.endTimestamp.apply(lambda x: datetime.datetime.fromtimestamp(x))
    us_data.endTimestamp  = us_data.apply(lambda x: clean_endv(x.startTimestamp, x.endTimestamp),axis=1)
    us_data.endTimestamp  = us_data.apply(lambda x: add_s(x.startTimestamp, x.endTimestamp),axis=1)
    us_data = us_data.rename(columns={"startTimestamp": "start", "endTimestamp": "end"})
    us_data = us_data.rename(columns={"customer": "id"})
    
    return us_data

# get age and sex of user
def get_as(user_ids):
    
    if isinstance(user_ids, int) or isinstance(user_ids, np.int64):
        formatter = f'({user_ids})'
    elif len(user_ids) == 1:
        formatter = f'({user_ids[0]})'
    else:
        formatter = tuple(user_ids) 
 
    query = f"""
    SELECT 
        user_id, salutation, birth_date, weight, height, creation_timestamp
    FROM 
        rocs.datenspende.users
    WHERE 
        users.user_id IN {formatter} 
    """ 

    ags = query_pg_df(query)
    ags.creation_timestamp = pd.to_datetime(ags['creation_timestamp'],unit='ms') 
    ags.creation_timestamp = ags.creation_timestamp.dt.date
    ags['age'] = np.floor((2023 + 1 / 12) - ags['birth_date'] + 2.5)
    
    qu = f"""
    select    
        a.user_id,
        a.created_at,
        a.question,
        a.element        
    from 
        rocs.datenspende.answers a
    where 
        a.user_id IN {formatter}
    AND
        a.question = 127    
    """
    sxs = query_pg_df(qu)
    sxs.created_at = pd.to_datetime(sxs['created_at'],unit='ms')
    sxs.created_at = sxs.created_at.dt.date
    
    
    if len(sxs) > 0:
        sex = 'female' if sxs['element'].values[0] == 773 else 'male'
    else: 
        sex = 'nd'
    if len(ags) > 0:
        age = ags['age'].values[0]
    else:
        age = 'nd'
    
    return sex, age
# define phases of infection (pre-, acute-, sub-acute-, and post-phase)
def phases(week):
    if week < 0:
        ph = 0
    elif (week >= 0 and week <= 4):
        ph = 1
    elif (week >= 5 and week <= 12):
        ph = 2
    elif week > 12:
        ph = 3
    return ph
# resample into 60 second intervals and create dataframe for analysis
def resample(us_data, bin_size_in_min):
           
    if len(us_data.index) > 50:
        
        user_data = us_data.copy()
        user_data = user_data[['id', 'doubleValue', 'longValue', 'booleanValue', 'start', 'end', 'source', 'type']]
        user_data.rename(
            columns={"longValue": "hr", "doubleValue": "steps", "booleanValue": "sleep"}, inplace=True
        )
        user_data["duration"] = (user_data.end - user_data.start) / pd.Timedelta(
            "1 sec"
        )
        user_data.reset_index(drop=True, inplace=True)
        
 
        df = user_data.copy()
        add_values = df[(df.duration > 60)]

        new_values = []
        for idx, row in add_values.iterrows():
            for i in np.arange(0, row.duration, 60):
                end_time = min(
                    row.end,
                    row.start
                    + pd.Timedelta("%d sec" % i)
                    + pd.Timedelta("%d sec" % 60),
                )
                new_duration = (
                    end_time - (row.start + pd.Timedelta("%d sec" % i))
                ) / pd.Timedelta("1 sec")
                new_values.append([
                    
                    row.id,
                    (row.steps / (row.duration / new_duration)),
                    row.hr,
                    row.sleep,
                    row.start + pd.Timedelta("%d sec" % i),
                    end_time,
                    row.source,
                    row.type,
                    new_duration,
                ])
                
        df = df[df.duration <= 60].append(pd.DataFrame(data=new_values, columns=user_data.columns))
        df = df.sort_values(by='start')
        df = df.groupby(['start','type']).mean().reset_index()
        
        if 'hr' not in df.columns:
            df['hr'] = np.nan
        if 'sleep' not in df.columns:
            df['sleep'] = np.nan
        
        heartrate_bin = (
            df[df.type == "HeartRate"][["start", "hr"]]
            .set_index("start")
            .resample("%d Min" % bin_size_in_min)
            .mean()
            .reset_index()
        ).dropna(subset=["hr"])
        heartrate_bin["source"] = df.source.unique()[0]
        heartrate_bin["id"] = df.id.unique()[0]
      
        restheartrate_bin = (
            df[df.type == "HeartRateRestingHourly"][["start",  "hr"]]
            .set_index("start")
            .resample("%d Min" % bin_size_in_min)
            .mean()
            .reset_index()
        ).dropna(subset=["hr"])
       
        restheartrate_bin = restheartrate_bin.rename(columns={"hr": "rhr"})
  
        sleep_bin = (
            df[df.type == "SleepStateBinary"][["start",  "sleep"]]
            .set_index("start")
            .resample("%d Min" % bin_size_in_min)
            .mean()
            .reset_index()
        ).dropna(subset=["sleep"])

        steps_bin = (
            df[df.type == "Steps"][["start",  "steps"]]
            .set_index("start")
            .resample("%d Min" % bin_size_in_min)
            .sum()
            .reset_index()
        ).dropna(subset=["steps"])     

        data_frames = [heartrate_bin, restheartrate_bin, sleep_bin, steps_bin]
        df_lc = reduce(lambda  left,right: pd.merge(left,right,on=['start'],
                                                    how='outer'), data_frames)
        return df_lc
    return 