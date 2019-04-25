import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans

# Import the data for pre-processing
census_data_df = pd.read_csv('census_cleaned_geoid.csv')
donor_data_df = pd.read_csv('muesuem_geoid.csv')

# convert donor data geoid to an INT and change all Errors to zero
donor_data_df['GEOID'] = pd.to_numeric(donor_data_df['GEOID'], errors='coerce').fillna(0).astype(int)

# New column of y values in the census data 1 = donor present (true)
census_data_df['Donors_Present'] = census_data_df.GEOID.isin(donor_data_df.GEOID).astype(int)

# New column of y values in the census data for if there are more than 5 donors present
counts = donor_data_df['GEOID'].value_counts()
counts_df = counts.to_frame()
counts_df.columns = ['Count_of_donors']
counts_df['GEOID'] = counts_df.index

model_df = pd.merge(census_data_df, counts_df, how='left', on=['GEOID', 'GEOID'])
model_df['Count_of_donors'] = model_df['Count_of_donors'].fillna(0).astype(int)

# Get your X and Y values and scale
census_data = census_data_df.values
donor_data = donor_data_df.values

X = census_data[:, 0:5]
Y = census_data[:, 6]

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)  # Scale X values

# Kmeans
kmeans = KMeans(n_clusters=4, n_init=100, random_state=100)
kmeans.fit(X)

# Predict
labels = kmeans.predict(X)

# Evaluate
model_df['Label'] = labels

# Export for mapping
model_df.to_csv('Kmeans.csv', index=False)
