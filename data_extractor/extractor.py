import requests
import pandas as pd
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
from urllib.parse import urlencode, urlparse
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MSISDNRequestExtractor:
    def __init__(self, base_url: str, headers: Dict[str, str] = None):
        """
        Initialize the extractor with base URL and optional headers.
        """
        self.base_url = self._validate_url(base_url)
        default_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'PostmanRuntime/7.28.4',
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        if headers:
            default_headers.update(headers)
        self.headers = default_headers
        
        self.session = self._configure_session()
        
    def _validate_url(self, url: str) -> str:
        """
        Validate and clean the URL.
        """
        url = url.rstrip('&?')
        parsed = urlparse(url)
        if not all([parsed.scheme, parsed.netloc]):
            raise ValueError("Invalid URL format. Must include scheme (http/https) and domain.")
        if not url.startswith(('http://', 'https://')):
            raise ValueError("URL must start with 'http://' or 'https://'")
        return url
        
    def _configure_session(self) -> requests.Session:
        session = requests.Session()
        retry_strategy = urllib3.Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "HEAD", "OPTIONS"]
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def read_msisdns(self, input_file: str, msisdn_column: str) -> List[str]:
        try:
            if not input_file or not msisdn_column:
                raise ValueError("Input file path and MSISDN column name are required")
                
            if input_file.lower().endswith('.csv'):
                df = pd.read_csv(input_file)
            elif input_file.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(input_file, engine='openpyxl')
            else:
                raise ValueError("Unsupported file format. Use CSV or Excel files.")
                
            if msisdn_column not in df.columns:
                raise ValueError(f"Column '{msisdn_column}' not found in the file")
                
            msisdns = df[msisdn_column].astype(str).str.strip()
            valid_msisdns = [m for m in msisdns if m.isdigit()]
            if not valid_msisdns:
                raise ValueError("No valid MSISDNs found in the file")
            return valid_msisdns
        except Exception as e:
            print(f"Error reading file: {e}")
            raise

    def make_request(self, msisdn: str) -> Optional[Dict[str, Any]]:
        if not msisdn or not msisdn.isdigit():
            print(f"Invalid MSISDN format: {msisdn}")
            return None
        try:
            params = {
                'is-personal-details-embedded': 'true',
                'msisdn': msisdn,

            }
            url = f"{self.base_url}?{urlencode(params)}"
            print(f"\nRequesting URL: {url}")
            
            headers = self.headers.copy()
            headers['Msisdn'] = msisdn  
            
            print("Request Headers:", headers)
            response = self.session.get(
                url,
                headers=headers,
                verify=False,  
                timeout=30
            )
            print("Response Status Code:", response.status_code)
            print("Response Content:", response.text)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP Error for MSISDN {msisdn}: {e}")
            if e.response.status_code == 400:
                print("Check URL format and MSISDN validity")
            return None
        except requests.exceptions.ConnectionError as e:
            print(f"Connection Error for MSISDN {msisdn}: {e}")
            return None
        except requests.exceptions.Timeout as e:
            print(f"Timeout Error for MSISDN {msisdn}: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"Request Error for MSISDN {msisdn}: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON Decode Error for MSISDN {msisdn}: {e}")
            return None
    def extract_data(self, json_data: Optional[Dict[str, Any]], keys: List[str], msisdn: str) -> Dict[str, Any]:

        def get_nested_value(obj: Dict[str, Any], key_path: str) -> Any:
            try:
                parts = key_path.split('.')
                current = obj
                for part in parts:
                    if isinstance(current, list): 
                        if part == 'accountInternalId':  
                            current = current[0].get(part, None)
                        else:
                            return None
                    elif isinstance(current, dict):
                        current = current.get(part)
                    else:
                        return None
                return current
            except Exception:
                return None

        result = {'msisdn': msisdn}
        if json_data:
            for key in keys:
                result[key] = get_nested_value(json_data, key)
        else:
            for key in keys:
                result[key] = None
        return result



    def process_all_msisdns(self, input_file: str, msisdn_column: str,
                            keys_to_extract: List[str], output_file: str = None):

        try:
            msisdns = self.read_msisdns(input_file, msisdn_column)
            results = []
            total = len(msisdns)
            print(f"\nTotal MSISDNs to process: {total}")
            for idx, msisdn in enumerate(msisdns, 1):
                print(f"\nProcessing MSISDN {idx}/{total}: {msisdn}")
                json_data = self.make_request(msisdn)
                extracted_data = self.extract_data(json_data, keys_to_extract, msisdn)
                results.append(extracted_data)

            if not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = f"extracted_data_{timestamp}.csv"

            df = pd.DataFrame(results)
            if output_file.lower().endswith('.csv'):
                df.to_csv(output_file, index=False)
            elif output_file.lower().endswith(('.xlsx', '.xls')):
                df.to_excel(output_file, index=False)
            else:
                output_file = f"{output_file}.csv"
                df.to_csv(output_file, index=False)
            print(f"\nData saved to {output_file}")
            print(f"Processed {len(results)} MSISDNs")
        except Exception as e:
            print(f"Error processing MSISDNs: {e}")
            raise

def main():
    base_url = "http://account-service-amdocs-qa-tests.k8s-stage.azercell.com/api/v1/contacts/search"
    input_file = "C:\\Users\\smammadzada\\Desktop\\test.csv"  
    msisdn_column = "msisdn" 
    keys_to_extract = ["individualId", "accounts.accountInternalId", "document.pin"]


    try:
        extractor = MSISDNRequestExtractor(base_url)
        extractor.process_all_msisdns(
            input_file=input_file,
            msisdn_column=msisdn_column,
            keys_to_extract=keys_to_extract,
            output_file="C:\\Users\\smammadzada\\Desktop\\result_lwt.csv" 
        )
    except Exception as e:
        print(f"Error in main: {e}")

if __name__ == "__main__":
    main()
