working with raw Airbnb listing data ingested from multiple sources. Each listing contains property details and a nested list of amenities. The goal is to clean, normalize, and store this data for downstream analysis.

The data is available as a JSON file (listings.json) in the Bronze layer, which now needs to be transformed into a clean Silver Delta Table.

🎯 Task
Read the input data from listings.json using Spark.
Explode the amenities array so that each row contains a single amenity.
Normalize boolean-like columns (e.g., "true", "false", "yes", "no") into proper boolean (True / False) Spark data types.
Rename or select only the relevant columns for downstream use.
Save the cleaned DataFrame in Delta format to the Silver layer path:
/Volumes/practice_db_catalog/airbnb/data_volume/airbnb/silver/listings_cleaned
