import pandas as pd

months_fr_to_en = {
    'janv': 'Jan',
    'févr': 'Feb',
    'mars': 'Mar',
    'avr': 'Apr',
    'mai': 'May',
    'juin': 'Jun',
    'juil': 'Jul',
    'août': 'Aug',
    'sept': 'Sep',
    'oct': 'Oct',
    'nov': 'Nov',
    'déc': 'Dec'
}

def convert_month_in_french_to_english(date_str):
    for fr, en in months_fr_to_en.items():
        if fr in date_str:
            return date_str.replace(fr, en)
    return date_str

df_hospital_utilization = pd.read_csv(
    '/Users/benabbas/Desktop/projet_data_integration/Data/Raw/hospital-utilization-trends.csv',
    delimiter=';'
)
df_in_hospital_mortality_diagnosis = pd.read_csv(
    '/Users/benabbas/Desktop/projet_data_integration/Data/Raw/in-hospital-mortality-trends-by-diagnosis-type.csv',
    delimiter=','
)
df_in_hospital_mortality_health = pd.read_csv(
    '/Users/benabbas/Desktop/projet_data_integration/Data/Raw/in-hospital-mortality-trends-by-health-category.csv',
    delimiter=','
)

for df in [df_hospital_utilization, df_in_hospital_mortality_diagnosis, df_in_hospital_mortality_health]:
    if 'Date' in df.columns:
        df['Date'] = df['Date'].apply(convert_month_in_french_to_english)
        
        if df['Date'].str.contains('/').any(): 
            df['Date'] = pd.to_datetime(df['Date'], format='%m/%Y', errors='coerce')
        else:  
            df['Date'] = pd.to_datetime(df['Date'], format='%b-%y', errors='coerce')
    
    df['Setting'] = df['Setting'].str.lower().str.strip().astype('category')
    if 'Category' in df.columns:
        df['Category'] = df['Category'].str.lower().str.strip().astype('category')
    
    df['Count'] = pd.to_numeric(df['Count'], errors='coerce').fillna(0)


df_hospital_utilization['System'] = df_hospital_utilization['System'].str.strip().astype('category')

most_frequent_value = df_hospital_utilization['System'].mode()[0]
df_hospital_utilization['System'].fillna(most_frequent_value, inplace=True)

df_hospital_utilization['Facility Name'] = df_hospital_utilization['Facility Name'].str.strip().astype('category')

df_in_hospital_mortality_diagnosis['Diagnosis'] = df_in_hospital_mortality_diagnosis['Diagnosis'].str.strip().astype('category')

df_in_hospital_mortality_diagnosis.drop(columns=['Month', 'Year'], inplace=True)

print(df_hospital_utilization.info())
print(df_in_hospital_mortality_diagnosis.info())
print(df_in_hospital_mortality_health.info())

df_hospital_utilization.to_csv('/Users/benabbas/Desktop/projet_data_integration/Data/Cleaned/hospital-utilization-trends-cleaned.csv', index=False)
df_in_hospital_mortality_diagnosis.to_csv('/Users/benabbas/Desktop/projet_data_integration/Data/Cleaned/in-hospital-mortality-trends-by-diagnosis-type-cleaned.csv', index=False)
df_in_hospital_mortality_health.to_csv('/Users/benabbas/Desktop/projet_data_integration/Data/Cleaned/in-hospital-mortality-trends-by-health-category-cleaned.csv', index=False)

print("Fichiers nettoyés enregistrés dans data/cleaned.")
