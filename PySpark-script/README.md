# Data Extraction Script
PySpark script to extract data subset from NYPD Arrests Data (Historic) based on year from 2016 to 2020.

## Requirement
_Must be run on PEEL HPC.

## Usage
_ ./run.sh year: The year argument except any value between 2016 to 2020.\
_ ./run.sh all: By typing all into argument, the script will execute spark from year 2016 to 2020.

## Storage
_All generated data are store in folder sub-dataset.

## License
[MIT](https://choosealicense.com/licenses/mit/)
