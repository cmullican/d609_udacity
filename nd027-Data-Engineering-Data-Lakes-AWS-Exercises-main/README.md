# Notes

My result counts differed, but I believe the data has changed from what was used to create the rubric.
I also had to use "keep schema" rather than  "update schema" in several places, or I got unexpected and unusable results.

## customer_landing
I have 999 rows from  the import, which affects all downstream tables.

## step_trainer_trusted
I joined using accelerometer_trusted.user and customer_curated_email because attempting to use the serialNumber fields gave a mere 36 rows, while the number from this field wasmore realstic.
