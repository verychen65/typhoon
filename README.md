# Typhoon CSV Data

Auto-synced cyclone forecast data from Google DeepMind WeatherLab.

## Models
- FNV3 (ensemble + ensemble_mean)
- GENC (ensemble + ensemble_mean)

## Update Schedule
- UTC 00:30, 06:30, 12:30, 18:30 (4 times daily)
- Retention: 7 days

## Data Source
https://deepmind.google.com/science/weatherlab/

## File Naming
`{MODEL}_{TYPE}_{YYYY_MM_DDTHH_00}_paired.csv`
