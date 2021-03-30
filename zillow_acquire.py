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

