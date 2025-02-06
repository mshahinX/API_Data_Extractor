# API Data Extractor

## Overview
A Python utility for extracting and processing MSISDN (Mobile Station International Subscriber Directory Number) data from various file formats.

## Features
- Read MSISDNs from CSV and Excel files
- Make API requests for each MSISDN
- Extract specified nested JSON data
- Configurable retry and error handling
- Flexible output options


## Usage
```python
from msisdn_extractor import MSISDNRequestExtractor

extractor = MSISDNRequestExtractor("https://example.com/api")
extractor.process_all_msisdns(
    input_file="input.csv", 
    msisdn_column="msisdn",
    keys_to_extract=["individualId", "accounts.accountInternalId"]
)
```

## Requirements
- Python 3.8+
- requests
- pandas
- openpyxl


