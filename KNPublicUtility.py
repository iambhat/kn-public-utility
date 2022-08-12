"""
KNPublicUtility : Knoema Public Dataset Automation Utility Class

Which has written to handle generic task across all the python script by calling methods this class provides.
Developer : #0097 : Shreehari
Version : v1.5
Released on : 04-08-2022
Detailed Docs : https://docs.google.com/document/d/1H2P7CPcfuGk3VzF6DyJg7K0Rj8TfxhpWAwyb9fPp_7I
#----------------------------------------------------------------------------------------------------------------------------------------------------
The Methods can be Accessed From the ETL Tool are:
1. ProxyRotate() --> Uses the WebShare Rotating proxy
2. ProxyCall()--> Can able to filter Region wise Proxy IPs, Region Codes (ex:IN,GB,US..) must be given as an argument in function call.
3. GetProxyIP() --> Returns the Proxy IP's alone if required in ETLs.
4. JsonToDataFrame() --> Can process JSON url or the JSON data as well.
5. GetIndex() --> Returns the [[row,column]] index value/s of the string pattern anywhere in the dataframe.
6. ?
#----------------------------------------------------------------------------------------------------------------------------------------------------
"""

import json
import os
import random
import sys
import time
import warnings
import functools
import inspect
from copy import deepcopy

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from requests.auth import HTTPDigestAuth

print = functools.partial(print, flush=True)
warnings.simplefilter("ignore")


class KNPublicUtility:
    def __init__(self):
        self.class_name = type(self).__name__
        self.Load_Json_File('api_init')

        self.proxiesList = []
        self.ps = True
        self.pi = 0
        self.LI = 0


    def Load_Json_File(self,data_in=None):
        SPath = os.path.dirname(os.path.realpath(__file__))
        
        creds_json_path = os.path.join(SPath, 'KNPUCreds.json')
        if not os.path.exists(creds_json_path):
            print('>> "Credential Json File" NOT Found, Please check')
            sys.exit()

        with open(creds_json_path) as File:
            creds_data = json.load(File)
            
        if data_in == 'api_init':
            self.Initialize_URL(creds_data['proxy_api_url'])
        elif data_in == 'IPs':
            return creds_data['rotating_proxies']

        return None


    def Initialize_URL(self, api_urls):
    
        self.geoNodeUrl = api_urls.get('geoNodeUrl')
        self.advancedProxies = api_urls.get('advancedProxies')
        self.freeProxyWorld = api_urls.get('freeProxyWorld')
        self.freeProxyList = api_urls.get('freeProxyList')

        return True


    def Assign_Options(self, **kwargs):
        options = {
            'headers': None,
            'verify': True,
            'timeout': 30
        }
        options.update(kwargs)

        return options
        
        
    def LoadProxyServer(self, pServer, region, ptype):
    
        region = region.upper()
        ptype = ptype.lower()
        
        def geoNodeProxy():
            proxyList = list()
            mText = "GeoNodeProxy"
            nonlocal ptype
            try:
                if not ptype: ptype = 'http,https'
                payload = {
                    "limit" : 500,
                    "page" : 1,
                    "sort_by" : "lastChecked",
                    "sort_type" : "desc",
                    "country" : region,
                    "protocols" : ptype
                }
                resp = requests.get(self.geoNodeUrl, params = payload)
                res_string = resp.json()
                df = pd.DataFrame(pd.json_normalize(res_string['data']))
                df['PROXY'] = df['ip'] + ':' + df['port']
                proxyList = df['PROXY'].tolist()
            except Exception as E:
                pass

            return mText,proxyList

        def freeProxyWorld():
            proxyList = list()
            mText = "FreeProxyWorld"
            try:
                payload = {
                    "country" : region,
                    "type" : ptype,
                    "page" : 1
                }
                resp = requests.get(self.freeProxyWorld, params = payload)
                Soup = BeautifulSoup(resp.text, 'html.parser')
                data = Soup.find('table', {'class': 'layui-table'})
                df = pd.DataFrame(pd.read_html(str(data))[0])
                df = df[df['IP adress'].str.contains('|'.join(['adsbygoogle'])) == False]
                df['Proxy'] = df['IP adress'] + ':' + df['Port']
                proxyList = df['Proxy'].tolist()
            except Exception as E:
                pass

            return mText,proxyList

        def freeProxyList():
            proxyList = list()
            mText = "FreeProxyList"
            try:
                res = requests.get(self.freeProxyList)
                proxy = pd.read_html(res.text)
                P_list = proxy[0]
                if region:
                    P_list = P_list[P_list['Code'].isin([region])]
                if ptype == 'https':
                    P_list = P_list[P_list['Https'].isin(['yes'])]
                elif ptype == 'http':
                    P_list = P_list[P_list['Https'].isin(['no'])]
                P_list['Proxy'] = P_list['IP Address'] + ':' + P_list['Port'].astype(str).replace('.0', '')
                proxyList = P_list['Proxy'].tolist()
            except Exception as E:
                pass

            return mText,proxyList
            
        def advancedProxies():
            proxyList = list()
            mText = "AdvancedProxyList"
            try:
                resp = requests.get(self.advancedProxies)
                PList = pd.json_normalize(resp.json(), 'protocols',  ['ip', 'port', ['location', 'isocode']],record_prefix='_')
                if region:
                    PList = PList[PList['location.isocode'].isin([region])]
                cptype = 'https'
                if ptype: cptype = ptype
                PList = PList[PList['_type'].isin([cptype])]
                PList['proxy'] = PList['ip'].astype(str)+':'+PList['port'].astype(str)
                proxyList = PList['proxy'].tolist()
            except Exception as E:
                pass
            
            return mText,proxyList
            
        return eval(pServer)()


    def ProxyCall(self, url, region='', ptype='', **kwargs):

        options = self.Assign_Options(**kwargs)

        response = None
        total_iteration = 0
        count_failed_ip = 0
        proxy_servers = ['geoNodeProxy', 'freeProxyWorld', 'freeProxyList']

        if region == '' and ptype == '' and not self.proxiesList:
            response = self.ProxyRotate(url, **kwargs)
        
        if response is None and not self.proxiesList:
            print(f'\n<> {self.class_name}.{inspect.currentframe().f_code.co_name}')
            print(">> Searching for Free Proxy IP's to Connect Source...")

        while response is None:
            if self.proxiesList:
                try:
                    proxies = {"http": "http://" + self.proxiesList[self.pi], "https": "http://" + self.proxiesList[self.pi]}
                    response = requests.get(url, proxies=proxies, **options)
                    if response.ok : print(">> Successfully Fetched Data using the IP : '{}'".format(self.proxiesList[self.pi]))
                    if response.status_code != 200:
                        response = None
                        continue

                except Exception as E:
                    time.sleep(2)
                    count_failed_ip += 1
                    del self.proxiesList[self.pi]
                    if (count_failed_ip%10 == 0):
                        print(f'>> With {count_failed_ip} more IPs Failed to Extract Data | Checking with Other {len(self.proxiesList)} IPs')
                    #print(">> Failed Proxy IP : ",self.proxiesList[self.pi])
                    if self.proxiesList:
                        self.pi = random.randint(0, len(self.proxiesList) - 1)
                    response = None
                    continue
            else:
                if total_iteration == 3:
                    print('\n>> Failed: No Valid IPs Available... Please Re-run After Sometime..')
                    sys.exit()
                else:
                    mText, self.proxiesList = self.LoadProxyServer(proxy_servers[self.LI], region, ptype)
                    self.LI += 1
                    print("\n>> Loaded {} Proxy IPs from {}".format(len(self.proxiesList),mText))
                    if self.LI == len(proxy_servers):
                        total_iteration += 1
                        self.LI = 0

        return response


    def ProxyRotate(self, url, **kwargs):
    
        if self.ps:
            print(f'\n<> {self.class_name}.{inspect.currentframe().f_code.co_name}')
            print(">> Trying to Connect Source using WebShare Rotating Proxies..!!")
            self.ps = False

        count = 0
        response = None
        options = self.Assign_Options(**kwargs)

        RotatingProxies = self.Load_Json_File('IPs')
        RotatingProxies = [f"http://{proxies['username']}:{proxies['password']}@{proxies['IP']}:{proxies['port']}" for proxies in RotatingProxies]
        
        while response is None:
            if count == 2:
                print(">> No WebShare Rotating Proxies are Valid..!!")
                response = None
                break
            for proxy in RotatingProxies:
                proxies_ = {"http": proxy}
                try:
                    response = requests.get(url=url, proxies=proxies_, **options)
                    if response.status_code == 200:
                        print(">> Proxy Applied Successfully..!")
                        response = response
                        break
                    else:
                        response = None
                        response.raise_for_status()
                except:
                    #print(">> Refreshing Proxy IPs")
                    time.sleep(5)
                    pass
            count += 1
        
        return response


    def GetProxyIP(self,IP_Count=None, region='', ptype=''):
        if self.ps:
            print(f'\n<> {self.class_name}.{inspect.currentframe().f_code.co_name}')
            self.ps = False
        
        proxiesList = []
        pServerMap = {
            "S1":"geoNodeProxy",
            "S2":"freeProxyWorld",
            "S3":"freeProxyList",
            "S4":"advancedProxies"
        }
        
        for ps in ['S1','S2','S3','S4']:
            _, proxiesList1 = self.LoadProxyServer(pServerMap.get(ps), region, ptype)
            proxiesList += proxiesList1
            #print(f"{ps} = {len(proxiesList1)}")              
        
        if isinstance(IP_Count, str):
            IP_Count = int(IP_Count)
        
        if not IP_Count:
            IP_Count = len(proxiesList)
        
        proxiesList = proxiesList[:IP_Count]
        print(f">> Total Proxy IPs Returned : {len(proxiesList)}")

        return proxiesList


    def JsonToDataFrame(self, JData):
        if self.ps:
            print(f'\n<> {self.class_name}.{inspect.currentframe().f_code.co_name}')
            print(f'>> Processing the Json Data to Export as DataFrame')
            self.ps = False
        
        def cross_join(left, right):
            new_rows = [] if right else left
            for left_row in left:
                for right_row in right:
                    temp_row = deepcopy(left_row)
                    for key, value in right_row.items():
                        temp_row[key] = value
                    new_rows.append(deepcopy(temp_row))
            return new_rows

        def flatten_list(data):
            for elem in data:
                if isinstance(elem, list):
                    yield from flatten_list(elem)
                else:
                    yield elem

        def json_to_dataframe(data_in):
            def flatten_json(data, prev_heading=''):
                if isinstance(data, dict):
                    rows = [{}]
                    for key, value in data.items():
                        rows = cross_join(rows, flatten_json(value, prev_heading + ' ' + key))
                elif isinstance(data, list):
                    rows = []
                    for i in range(len(data)):
                        [rows.append(elem) for elem in flatten_list(flatten_json(data[i], prev_heading))]
                else:
                    rows = [{prev_heading[1:]: data}]
                return rows

            return pd.DataFrame(flatten_json(data_in))
     
        
        if isinstance(JData, str):
            response = requests.get(JData)
            if response.status_code == 200:
                json_data = response.json()
            else:
                print(f'>> Failed...! The Json API Response Status : {response.status_code}')
                sys.exit()
        else:
            json_data = JData
            
        DataFrame = json_to_dataframe(json_data)

        return DataFrame


    def GetIndex(self, Data, Pattern):

        try:
            Pattern = Pattern.strip().upper()
            index_ = Data.apply(lambda x: x.astype(str).str.upper().str.strip().str.match(Pattern)).values.nonzero()
            indexList = np.transpose(index_).tolist()
        except:
            print(">> No Matching Pattern String in the DataFrame")
            sys.exit()

        return indexList


if __name__ == '__main__':
    KNPU = KNPublicUtility()


