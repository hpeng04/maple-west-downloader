import pandas as pd
import re
import os
from datetime import datetime
from rules import check_missing_rows, check_total_energy
from channels import channels
from log import Log
from color import color
from urllib.request import urlopen
from bs4 import BeautifulSoup
from alert import send_email
from dateutil.relativedelta import relativedelta
import requests
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import io

google_paths = {
    77: '1Z1ETfHlgitFOBx8SWqYiotkezFwL2ahb',
    78: '1oOKr0Kt2Jg2j_tqzv6sdRF4bEuNofxy2',
    79: '1NUruYAn2kafZTrUOFeMndmKHCyXnBVFM',
    80: '1Idx4pE-vBeRzFQXoBpiwRgfHbh-CZ5Go',
    81: '1Y17UL5XGQPlFf361V-9FLhP-JhAuFzaI',
    82: '1ud6EOvBFkXGPR8McKDEJR6Dx3y_NssiF',
    83: '1O08BMUVd4CnNsYYoH5l1mRYGW97AXsA5',
    84: '1UHTnt1gOF28LoF7QnS-JcQ69q20pdeDc',
    85: '1j2tTzGza7hDmwGukHo1HQFTYlMGg1qmo',
    86: '1pmiaLqn_M3y3Ebn3tT9LnHBvRbgHOsA6',
    87: '1JPCc3CIt1R5pW61DHtC24wVslnWC8QhN',
    2804: '1X8M-Ec0y-vl3CrJ9zZxuMUMYAVlMF7fJ',
    2806: '1Fh7r38--RT9_J1-e4XJnzLdQkXjD0zO5',
    2808: '127cFRH_0pa1eZN67dYp4H63dM4hwX_43',
    2810: '1euo9FBA2qem2oaa_ze0bxudcA9ZSOSVc',
    2812: '1KH_x8Sb7ix-KdBUJYvsGZDOB5Ec3oaei',
    2814: '125PAJ1RCLN4IDGVUeyh-lmvXQEhY6vMH',
    2816: '1uCERJeoSE2Oexa59g3Nv7SZNrzQZfHp0',
    2818: '17dVMIaaF0kzZofbUA4kJAIeYbVhrlNHt'
}

def is_float(value):
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False

class Unit:
    block_1 = [2804, 2806, 2808, 2810, 2812, 2814, 2816, 2818]
    block_3 = [77, 78, 79, 80, 81, 82, 83, 84, 85, 86]
    stack_units = [2818, 2820, 87, 77, 86, 78]
    units = block_1 + block_3
    datatype = ""

    yesterday = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
    last_month = (datetime.today() - relativedelta(months=1)).strftime('%Y-%m')

    def __init__(self, unit_no: int, block: int, ip_address: str, port: str, serial:str, channels:dict, data:pd.DataFrame = None):
        self.unit_no = unit_no
        self.block = block # Deprecated
        self.data = data
        self.ip_address = ip_address
        self.port = port
        self.serial = serial
        self.channels = channels
        self.warnings = []
        self.errors = []
                
    def __str__(self):
        return f"Unit {self.unit_no}"
    
    def __repr__(self):
        return f"Unit {self.unit_no}, Block {self.block}, {self.ip_address}:{self.port}/{self.serial}\n"
    
    def __eq__(self, other):
        return self.unit_no == other.unit_no
    
    def __lt__(self, other):
        return self.unit_no < other.unit_no
    
    def __hash__(self):
        return hash(self.unit_no)

    def _fix_order(self, df:pd.DataFrame):
        '''
        Sort the data in ascending order of time if not already sorted

        param: df: pd.DataFrame: data to be sorted
        return: pd.DataFrame: sorted data
        '''
        if df is None or df.empty or len(df) < 2:
            return df
            
        try:
            # Convert first column to datetime for proper comparison
            timestamps = pd.to_datetime(df.iloc[:, 0])
            if timestamps.iloc[0] > timestamps.iloc[1]:
                return df.iloc[::-1].reset_index(drop=True)
            return df
        except (ValueError, pd.errors.ParserError) as e:
            print(f"{color.RED}Error parsing timestamps: {str(e)}{color.END}")
            Log.write(f"Unit {self.unit_no}: Error parsing timestamps: {str(e)}")
            self.errors.append(f"Unit {self.unit_no}: Error parsing timestamps: {str(e)}")
            return df

    def _natural_sort_key(self, s):
        return [int(text) if text.isdigit() else text.lower() for text in re.split('(\\d+)', s)]
    
    def _download(self, url:str):
        '''
        Download data from the given url

        param: url: str: url to download data from
        '''
        Log.write(f"Downloading Unit {self.unit_no}: {self.ip_address}:{self.port}")
        print(f"Downloading Unit {self.unit_no}: {self.ip_address}:{self.port}")
        date = url.split('/')[-1]
        try:
            response = pd.read_csv(url, header=0, on_bad_lines='skip')
            if response.empty:
                raise ValueError("Downloaded data is empty")
            self.data = self._fix_order(response)
            print(f"Downloaded data for Unit {self.unit_no}")
        except (pd.errors.EmptyDataError, ValueError) as e:
            Log.write(f"Unit {self.unit_no}: Empty data from {url}\n\n")
            print(f"{color.RED}Unit {self.unit_no}: Empty data from {url}{color.END}")
            self.data = None
            self.errors.append(f"Unit {self.unit_no}: Empty data from {url}")
            # Extract date from URL for failed downloads log
            Log.record_failed_downloads(self.unit_no, date, url)
        except Exception as e:
            if self.unit_no in google_paths:
                try:
                    
                    drive_folder_id = google_paths[self.unit_no]
                    # Format the expected file name pattern in Google Drive
                    filename = f'{self.serial}_{date}.csv'

                    print(f"{color.RED}Unit {self.unit_no}: Failed to download data from {url}: {str(e)}{color.END}")
                    Log.write(f"Attempting to download {filename} from Google Drive for Unit {self.unit_no}")
                    print(f"Attempting to download {filename} from Google Drive for Unit {self.unit_no}")
                    
                    # Set up credentials
                    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
                    SERVICE_ACCOUNT_FILE = 'service_account.json'  # Path to your service account key file
                    
                    credentials = service_account.Credentials.from_service_account_file(
                        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
                    
                    # Build the Drive API client
                    service = build('drive', 'v3', credentials=credentials)
                    
                    # Search for the file in the specific folder
                    query = f"'{drive_folder_id}' in parents and name = '{filename}' and trashed = false"
                    results = service.files().list(
                        q=query,
                        fields="files(id, name)"
                    ).execute()
                    
                    items = results.get('files', [])
                    
                    if not items:
                        print(f"{color.RED}File {filename} not found in Google Drive folder{color.END}")
                        Log.write(f"File {filename} not found in Google Drive folder\n")
                    else:
                        file_id = items[0]['id']
                        
                        # Download the file
                        request = service.files().get_media(fileId=file_id)
                        file_handle = io.BytesIO()
                        downloader = MediaIoBaseDownload(file_handle, request)
                        
                        done = False
                        while not done:
                            status, done = downloader.next_chunk()
                        
                        # Process the downloaded file
                        file_handle.seek(0)
                        backup_data = pd.read_csv(file_handle)
                        
                        if not backup_data.empty:
                            self.data = self._fix_order(backup_data)
                            print(f"Successfully loaded {filename} for Unit {self.unit_no} from Google Drive")
                            Log.write(f"Successfully loaded {filename} for Unit {self.unit_no} from Google Drive\n")
                            return
                        else:
                            print(f"{color.RED}Downloaded file from Google Drive is empty{color.END}")
                            Log.write(f"Downloaded file from Google Drive is empty\n")
                
                except Exception as backup_error:
                    print(f"{color.RED}Failed to download from Google Drive: {str(backup_error)}{color.END}")
                    Log.write(f"Failed to download from Google Drive: {str(backup_error)}\n")
            # Log.write(f"Unit {self.unit_no}: Failed to download data from {url}: {str(e)}\n\n")
            # self.data = None
            # self.errors.append(f"Unit {self.unit_no}: Failed to download data from {url}")
            # # Extract date from URL for failed downloads log
            # date = url.split('/')[-1]
            # Log.record_failed_downloads(self.unit_no, date, url)

    def load_data(self, path:str):
        '''
        Load data from a csv file or a directory of csv files
        Used for testing purposes

        param: path: str: path to the csv file or directory
        '''
        if os.path.isdir(path):
            for dir_name in os.listdir(path):
                if dir_name == f'{self.unit_no}':
                    dir_path = os.path.join(path, dir_name)
                    all_files = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if (os.path.isfile(os.path.join(dir_path, f)) and f.endswith('.csv'))]
                    all_files.sort(key=self._natural_sort_key)
                    all_files = [self._fix_order(pd.read_csv(f)) for f in all_files]
                    self.data = pd.concat((f for f in all_files), ignore_index=True)
        else:
            self.data = self._fix_order(pd.read_csv(path))
        if self.data is None:
            return False
        return True

    def download_minute_data(self, date=yesterday):
        '''
        Download data for the last day (minute data)
        '''
        # date in YYYY-MM-DD format
        url = f'http://{self.ip_address}:{self.port}/index.php/pages/export/exportDaily/{self.serial}/{date}'
        self.datatype = "Minute"
        self._download(url)

    def download_hour_data(self, date=last_month):
        '''
        Download data for the specified month (hourly data)
        
        param: date: str: date in YYYY-MM format
        '''
        url = f'http://{self.ip_address}:{self.port}/index.php/pages/export/exportMonthly/{self.serial}/{date}'
        self.datatype = "Hour"
        self._download(url)

    def check_space(self):
        '''
        Check the space available on the sd card
        '''
        url = f'http://{self.ip_address}:{self.port}/index.php/powerdisplay/getmainwatts'
        page = urlopen(url)
        html_bytes = page.read()
        html = html_bytes.decode("utf-8")
        soup = BeautifulSoup(html, 'html.parser')
        outer_span = soup.find_all("span", title='\\"Total')[-1]
        
        inner_span = outer_span.find("span")
        space = inner_span.text.split('<')[0]
        if is_float(space) and float(space) > 1 and float(space) < 40:
            Log.write(f"Unit {self.unit_no}: {space} GB left on the SD card")
            print(f"Unit {self.unit_no}: {space} GB left on the SD card")
        else:
            # self.errors += [f"Unit {self.unit_no}: Less than 1GB of space available on the SD card"]
            Log.write(f"Unit {self.unit_no}: Less than 1GB of space available on the SD card: {space} GB")
            print(f"{color.RED}Unit {self.unit_no}: {space} GB left on the SD card, clear storage{color.END}")
            body = f"Unit {self.unit_no}: Less than 1GB of space available on the SD card\n\n{space} GB left\n\nhttp://{self.ip_address}:{self.port}"
            send_email(subject=f"Maple West SD Card Storage Almost Full", body=body)

    def check_status(self):
        '''
        Check the status of the dashbox
        '''
        url = f'http://{self.ip_address}:{self.port}/index.php/powerdisplay/getmainwatts'
        page = urlopen(url)
        html_bytes = page.read()
        html = html_bytes.decode("utf-8")
        soup = BeautifulSoup(html, 'html.parser')
        status_logo = soup.find("img")
        # print(status_logo)
        if status_logo:
            img_src = status_logo['src']
            # print(img_src)
            if 'green' in img_src:
                Log.write(f"Unit {self.unit_no}: Dashbox Status OK")
                print(f"{color.GREEN}Unit {self.unit_no}: Dashbox Status OK{color.END}")
            else:
                Log.write(f"Unit {self.unit_no}: Dashbox Status Error")
                print(f"{color.RED}Unit {self.unit_no}: Dashbox Status Error{color.END}")
                body = f"Unit {self.unit_no}: Dashbox Status Error\n\nhttp://{self.ip_address}:{self.port}"
                send_email(subject=f"Maple West Dashbox Status Errors Detected", body=body)
                # self.errors.append(f"Unit {self.unit_no}: Dashbox Status Error")
        else:
            print("Something went wrong with status check")
        # print(status_logo)

    def check_quality(self, save_files:bool, date=yesterday):
        '''
        Check the quality of the data using the rules provided

        param: rules: list[DataQualityRule]: list of rules to be applied
        return: None
        '''
        if self.data is None:
            return self.errors, self.warnings
        
        Log.write(f"Checking Unit {self.unit_no}: {self.ip_address}:{self.port}")
        print(f"Checking Unit {self.unit_no}: {self.ip_address}:{self.port}")
        missing_row_errors, missing_row_warnings, bad_indices = check_missing_rows(self.data, self.unit_no)
        self.errors += missing_row_errors
        self.warnings += missing_row_warnings

        if (self.datatype == "Hour"):
            if save_files:
                last_month = (datetime.today() - relativedelta(months=1)).strftime('%Y-%m')
                if not os.path.exists(f'{self.datatype}_Data/UNIT {self.unit_no}'):
                    os.makedirs(f'{self.datatype}_Data/UNIT {self.unit_no}')
                self.data.to_csv(f'{self.datatype}_Data/UNIT {self.unit_no}/Unit_{self.unit_no}_{last_month}.csv', index=False)
            return

        energy_errors, energy_warnings = check_total_energy(self.data, self.unit_no)
        self.errors += energy_errors
        self.warnings += energy_warnings

        for channel in self.channels:
            if self.channels[channel] == True:
                # use the channel check quality function
                channel_errors, channel_warnings = channels[channel].check_channel(self.data, self.unit_no, bad_indices)
                self.errors += channel_errors
                self.warnings += channel_warnings
        if len(self.errors) == 0 and len(self.warnings) == 0:
            print(f"{color.GREEN}Unit {self.unit_no}: Passed all systems checks{color.END}")
            Log.write(f"Unit {self.unit_no}: Passed all systems checks")

        if save_files:
            if not os.path.exists(f'{self.datatype}_Data/UNIT {self.unit_no}'):
                os.makedirs(f'{self.datatype}_Data/UNIT {self.unit_no}')
            self.data.to_csv(f'{self.datatype}_Data/UNIT {self.unit_no}/Unit_{self.unit_no}_{str(date)}.csv', index=False)
        Log.write("\n")
        return self.errors, self.warnings
