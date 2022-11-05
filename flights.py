
import pandas as pd
import math
class Scraper:
    def __init__(self):
        
        self.df_1987 = pd.read_csv('1987.csv')
        print(self.df_1987.head())
        self.df_1988 = pd.read_csv('1988.csv')
        print(self.df_1988.head())
        self.df_1989 = pd.read_csv('1989.csv')
        print(self.df_1989.head())
        self.df_1990 = pd.read_csv('1990.csv')
        print(self.df_1990.head())

    def data_null(self):
        self.df_NULL_1987 = self.df_1987.isnull().sum().sum()
        print(f'null values in 1987 dataset is: ', self.df_NULL_1987)
        self.df_NULL_1988 = self.df_1988.isnull().sum().sum()
        print(f'null values in 1988 dataset is: ', self.df_NULL_1988)
        self.df_NULL_1989 = self.df_1989.isnull().sum().sum()
        print(f'null values in 1989 dataset is: ', self.df_NULL_1989)
        self.df_NULL_1990 = self.df_1990.isnull().sum().sum()
        print(f'null values in 1990 dataset is: ' ,self.df_NULL_1990)
        
    def data_cleaning(self):
        self.df_1987_isna = self.df_1987.columns[self.df_1987.isna().any()]
        self.df_1988_isna = self.df_1988.columns[self.df_1988.isna().any()]
        self.df_1989_isna = self.df_1989.columns[self.df_1989.isna().any()]
        self.df_1990_isna = self.df_1990.columns[self.df_1990.isna().any()]
        print(len(self.df_1987_isna), self.df_1987_isna)
        print(len(self.df_1988_isna),self.df_1988_isna)
        print(len(self.df_1989_isna),self.df_1989_isna)
        print(len(self.df_1990_isna),self.df_1990_isna)
        self.df_1987['Distance'] = self.df_1987['Distance'].fillna(0)
        
        # self.df_1987['Distance'] = 
        # self.df_1987['Distance'] = math.ceil(self.df_1987['Distance'])
        self.df_1987['ArrDelay'] = self.df_1987['ArrDelay'].fillna(0)
        self.df_1987['DepDelay'] = self.df_1987['DepDelay'].fillna(0)
      
        
        self.df_1987_drop = ['DepTime', 'ArrTime', 'TailNum', 'ActualElapsedTime', 'AirTime',
        'TaxiIn', 'TaxiOut',
       'CancellationCode', 'CarrierDelay', 'WeatherDelay', 'NASDelay',
       'SecurityDelay', 'LateAircraftDelay'] 
        self.df_1987_new = self.df_1987.drop(self.df_1987_drop, axis = 1)
        self.df_1987_new= pd.DataFrame(self.df_1987_new, index=None)

        print(self.df_1987_new.columns[self.df_1987_new.isna().any()])
        print(self.df_1987_new.isnull().sum().sum())
        self.df_1988['Distance'] = self.df_1988['Distance'].fillna(0)
        # self.df_1988['Distance'] = math.ceil(self.df_1988['Distance'])
        self.df_1988['ArrDelay'] = self.df_1988['ArrDelay'].fillna(0)
        self.df_1988['DepDelay'] = self.df_1988['DepDelay'].fillna(0)
       
        self.df_1988_drop = ['DepTime', 'ArrTime', 'TailNum', 'ActualElapsedTime', 'AirTime',
        'TaxiIn', 'TaxiOut',
       'CancellationCode', 'CarrierDelay', 'WeatherDelay', 'NASDelay',
       'SecurityDelay', 'LateAircraftDelay']
        self.df_1988_new = self.df_1988.drop(self.df_1988_drop, axis = 1)
        self.df_1988_new= pd.DataFrame(self.df_1988_new, index=None)

        print(self.df_1988_new.columns[self.df_1988_new.isna().any()])
        print(self.df_1988_new.isnull().sum().sum())
        self.df_1989['Distance'] = self.df_1989['Distance'].fillna(0)
        # self.df_1989['Distance'] = math.ceil(self.df_1989['Distance'])
        self.df_1989['ArrDelay'] = self.df_1989['ArrDelay'].fillna(0)
        self.df_1989['DepDelay'] = self.df_1989['DepDelay'].fillna(0)
       
        self.df_1989_drop = ['DepTime', 'ArrTime', 'TailNum', 'ActualElapsedTime', 'AirTime',
        'TaxiIn', 'TaxiOut',
       'CancellationCode', 'CarrierDelay', 'WeatherDelay', 'NASDelay',
       'SecurityDelay', 'LateAircraftDelay']
        self.df_1989_new = self.df_1989.drop(self.df_1989_drop, axis = 1)
        self.df_1989_new= pd.DataFrame(self.df_1989_new, index=None)

        print(self.df_1989_new.columns[self.df_1989_new.isna().any()])
        print(self.df_1989_new.isnull().sum().sum())
        
        self.df_1990['Distance'] = self.df_1990['Distance'].fillna(0)
        # print(f'Distance data type is :', type(self.df_1990['Distance']))
        # self.df_1990['Distance'] = math.ceil(self.df_1990['Distance'])
        self.df_1990['ArrDelay'] = self.df_1990['ArrDelay'].fillna(0)
        self.df_1990['DepDelay'] = self.df_1990['DepDelay'].fillna(0)
     
        self.df_1990_drop = ['DepTime', 'ArrTime', 'TailNum', 'ActualElapsedTime', 'AirTime',
        'TaxiIn', 'TaxiOut',
       'CancellationCode', 'CarrierDelay', 'WeatherDelay', 'NASDelay',
       'SecurityDelay', 'LateAircraftDelay']
        self.df_1990_new = self.df_1990.drop(self.df_1990_drop, axis = 1)
        self.df_1990_new= pd.DataFrame(self.df_1990_new, index=None)
        print(self.df_1990_new.columns[self.df_1990_new.isna().any()])
        print(self.df_1987_new.isnull().sum().sum())
        
        
        self.frames = [self.df_1987_new, self.df_1988_new, self.df_1989_new, self.df_1990_new]
        self.master_file = pd.concat(self.frames)
        print(self.master_file)
        print(len(self.master_file))
        self.master_file.to_csv(r'/Users/prabhuswamikallur/Desktop/Data_Migration_to_Tableau/combined_data.csv', header=True, index=False)
        
        
    
        
        
        
    
    def scraper_main(self):
        self.data_null()
        self.data_cleaning()
        
if __name__== "__main__":
    DMT = Scraper()
    DMT.scraper_main()


 

        