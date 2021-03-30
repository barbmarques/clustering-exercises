#basic imports
import pandas as pd
import numpy as np
import env

import env

# connection function for accessing mysql 
def get_connection(db, user=env.user, host=env.host, password=env.password):
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def acquire(df):
    '''
    This function connects to Codeup's SQL Server using given parameters in the user's
    env file.  It then uses a SQL query to acquire all data from Zillow's database that has a transaction data in 2017 and
    has longitude and latitude information on the property.
    
    It returns all the data in a single dataframe called df.
    '''
    
    def get_connection(db, user=env.user, host=env.host, password=env.password):
         return f'mysql+pymysql://{user}:{password}@{host}/{db}'
    query = '''
           SELECT prop.*, 
                   pred.logerror, 
                   pred.transactiondate, 
                   air.airconditioningdesc, 
                   arch.architecturalstyledesc, 
                   build.buildingclassdesc, 
                   heat.heatingorsystemdesc, 
                   landuse.propertylandusedesc, 
                   story.storydesc, 
                   construct.typeconstructiondesc 

            FROM   properties_2017 prop  
                   INNER JOIN (SELECT parcelid,logerror, Max(transactiondate) transactiondate 
                                FROM   predictions_2017 
                                GROUP  BY parcelid, logerror) pred
                                USING (parcelid) 
                   LEFT JOIN airconditioningtype air USING (airconditioningtypeid) 
                   LEFT JOIN architecturalstyletype arch USING (architecturalstyletypeid) 
                   LEFT JOIN buildingclasstype build USING (buildingclasstypeid) 
                   LEFT JOIN heatingorsystemtype heat USING (heatingorsystemtypeid) 
                   LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid) 
                   LEFT JOIN storytype story USING (storytypeid) 
                   LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid) 
            WHERE  prop.latitude IS NOT NULL 
                   AND prop.longitude IS NOT NULL
            '''

    df = pd.read_sql(query, get_connection('zillow'))
    
    return df

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def handle_missing_values(df, prop_required_column = .5, prop_required_row = .70):
    '''
    This function takes in: a dataframe, the proportion (0-1) of rows (for each column) with non-missing values required to keep 
    the column, and the proportion (0-1) of columns/variables with non-missing values required to keep each row.  
    
    It returns the dataframe with the columns and rows dropped as indicated. 
    '''
    threshold = int(round(prop_required_column*len(df.index),0))
    df.dropna(axis=1, thresh=threshold, inplace=True)
    threshold = int(round(prop_required_row*len(df.columns),0))
    df.dropna(axis=0, thresh=threshold, inplace=True)
    return df

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Final wrangle_zillow Function:

def wrangle_zillow():
    
    '''
    This function acquires the Zillow data from Codeup's database on the MySQL server.  
    
    It then prepares the data by removing columns and rows that are missing more than 50% of the 
    data, restricts the dataframe to include only single unit properties, with at least one
    bedroom and bathroom and at least 500 square feet, adds a column to indicate county (based on 
    fips), drops any unnecessary columns, adjusts for outliers in taxvaluedollarcnt and
    calculatedfinishedsquarefeet, fills missing values in buildinglotsize and buildingquality with 
    median values, and renames columns to user-friendly titles.
    '''
    df = acquire('zillow')
    
    # Restrict df to only properties that meet single unit use criteria
    single_use = [261, 262, 263, 264, 266, 268, 273, 276, 279]
    df = df[df.propertylandusetypeid.isin(single_use)]
    
    # Restrict df to only those properties with at least 1 bath & bed and 500 sqft area
    df = df[(df.bedroomcnt > 0) & (df.bathroomcnt > 0) & ((df.unitcnt<=1)|df.unitcnt.isnull())\
            & (df.calculatedfinishedsquarefeet>=500)]

    # Handle missing values i.e. drop columns and rows based on a threshold
    df = handle_missing_values(df)
    
    # Add column for counties
    df['county'] = np.where(df.fips == 6037, 'Los_Angeles',
                           np.where(df.fips == 6059, 'Orange', 
                                   'Ventura'))    
    # drop columns not needed
    df = df.drop(columns = ['id','calculatedbathnbr', 'finishedsquarefeet12', 'fullbathcnt', 'heatingorsystemtypeid'
       ,'propertycountylandusecode', 'propertylandusetypeid','propertyzoningdesc', 
        'censustractandblock', 'propertylandusedesc'])

    # replace nulls in unitcnt with 1
    df.unitcnt.fillna(1, inplace = True)
    
    # assume that since this is Southern CA, null means 'None' for heating system
    df.heatingorsystemdesc.fillna('None', inplace = True)
    
    # replace nulls with median values for select columns
    df.lotsizesquarefeet.fillna(7313, inplace = True)
    df.buildingqualitytypeid.fillna(6.0, inplace = True)

    # Columns to look for outliers
    df = df[df.taxvaluedollarcnt < 5_000_000]
    df[df.calculatedfinishedsquarefeet < 8000]
    
    # Just to be sure we caught all nulls, drop them here
    df = df.dropna()
    
    #rename columns:
    df.rename(columns={'taxvaluedollarcounty':'tax_value', 'bedroomcnt':'bedrooms', 'bathroomcnt':'bathrooms', 'calculatedfinishedsquarefeet':
                      'square_feet', 'lotsizesquarefeet':'lot_size', 'buildingqualitytypeid':'buildingquality'}, inplace=True)
    
    return df

