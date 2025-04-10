# Notes

My result counts differed, but I believe the data has changed from what was used to create the rubric.
I also had to use "keep schema" rather than  "update schema" in several places, or I got unexpected and unusable results.

## customer_landing
I have 999 rows from  the import, which affects all downstream tables.

## step_trainer_trusted
I joined using accelerometer_trusted.user and customer_curated_email because attempting to use the serialNumber fields gave a mere 36 rows, while the number from this field wasmore realstic.

## machine_learning_curated
I edited this script to resolve datatype issues using resolveChoice, rather thanusing the visual editor.

While my counts do not match the rubric exactly, they are close, and I have not found a solution that brings the exactly in line. I have used enough of the budget that I am submitting as-is, so I will have budget remaining, shouldrework be needed.
