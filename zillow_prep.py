def nulls_by_col(df):
    '''
    This function takes in a dataframe of observations and attributes and returns a dataframe where each row is an attribute name, 
    the first column is the number of rows with missing values for that attribute, and the second column is percent of total rows 
    that have missing values for that attribute. 
    '''
    
    num_missing = df.isnull().sum()
    rows = df.shape[0]
    pct_missing = num_missing / rows
    cols_missing = pd.DataFrame({'number_missing_rows': num_missing, 'percent_rows_missing': pct_missing})
    return cols_missing



def cols_missing(df):
    '''
    This function takes in a dataframe and returns a dataframe with 3 columns: the number of columns missing, 
    percent of columns missing, and number of rows with n columns missing. 
    '''
    
    df2 = pd.DataFrame(df.isnull().sum(axis =1), columns = ['num_cols_missing']).reset_index()\
    .groupby('num_cols_missing').count().reset_index().\
    rename(columns = {'index': 'num_rows' })
    df2['pct_cols_missing'] = df2.num_cols_missing/df.shape[1]
    return df2




def handle_missing_values(df, prop_required_column = .5, prop_required_row = .70):
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df