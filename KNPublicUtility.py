"""
KNPublicUtility : Knoema Public Dataset Automation Utility Class

Which has written to handle generic task across all the python script by calling methods this class provides.
"""

import functools
import json
import os
import sys
import time
import warnings
from copy import deepcopy

import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.auth import HTTPDigestAuth
from urllib3 import Retry

print = functools.partial(print, flush=True)
warnings.simplefilter("ignore")


class KNPublicUtility:
    def __init__(self):
        self.ClassName = type(self).__name__ + '.'

    def Load_Credentials(self):
        SPath = os.path.dirname(os.path.realpath(__file__))
        Credentials_Path = os.path.join(SPath, 'KNPU_Creds.json')
        if not os.path.exists(Credentials_Path):
            print('>> "Credentials File" NOT Found, Please check')
            sys.exit()

        with open(Credentials_Path) as File:
            Creds_Dict = json.load(File)

        username = Creds_Dict['username'].strip()
        password = Creds_Dict['password'].strip()
        ip = Creds_Dict['IP'].strip()
        port = Creds_Dict['port'].strip()

        return username, password, ip, port

    def requests_retry_session(self,
                               retries=2,
                               backoff_factor=0.3,
                               status_forcelist=(500, 502, 504),
                               session=None,
                               ):
        """
        This is the support method for the ProxyCall(), which
        handles the 500 series error during the source url request
        """

        session = session or requests.Session()
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def OpenProxy(self, URL, headers=None, verify=True):

        def geoNodeProxy():
            proxyList = list()
            try:
                resp = requests.get("https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps")
                res_string = resp.json()
                df = pd.DataFrame(pd.json_normalize(res_string['data']))
                df['PROXY'] = df['ip'] + ':' + df['port']
                proxyList = df['PROXY'].tolist()
            except Exception as E:
                pass

            return proxyList

        def freeProxyList():
            proxyList = list()
            try:
                res = requests.get('https://free-proxy-list.net/')
                proxy = pd.read_html(res.text)
                P_list = proxy[0]

                P_list['Proxy'] = P_list['IP Address'] + ':' + P_list['Port'].astype(str).replace('.0', '')
                proxyList = P_list['Proxy'].tolist()
            except Exception as E:
                pass

            return proxyList

        flag = 0
        response = None
              
        for j in range(4):
            
            ProxyList = geoNodeProxy() + freeProxyList()
            print("Total {} Proxy IPs Fetched from server".format(len(ProxyList)))

            for i,proxy in enumerate(ProxyList):
                proxies = {"http": "http://" + proxy, "https": "http://" + proxy}
                try:
                    response = requests.get(URL, proxies=proxies, headers=headers, verify=verify, timeout = 40)
                    if response.status_code == 200:
                        print('Proxy Aplied: ', proxies)
                        flag = 1
                except Exception as e:
                    del ProxyList[i]
                    time.sleep(1)
                    
                if flag == 1:
                    break
                
            if flag == 1:
                break
                    
        else:
            print("Failed..!! No Proxies are Available or Proxy Server is Down...Retry Again..!!")
            sys.exit()

        return response

    def ProxyCall(self, url, headers=None, verify=True):
        """
        The first callable method of the class, which takes below parameters,
        url : required field
        verify : bool (optional) by default it is set to True and can be overridden other boolean value
        headers : (optional) takes dictionary of header values

        Returns response on success of the request with proxies 
        """

        response = None

        username, password, ip, port = self.Load_Credentials()
        proxies_ = {"http": 'http://' + ip + ':' + port}
        auth_ = HTTPDigestAuth(username, password)

        try:
            response = self.requests_retry_session().get(url=url, proxies=proxies_, verify=verify, auth=auth_,
                                                         headers=headers)
            if response.status_code == 200:
                print(">> Proxy Applied.!")
                response = response
            else:
                response.raise_for_status()
        except:
            print(">> Searching for Free Proxy IP's...")
            response = self.OpenProxy(url, headers=headers, verify=verify)
            if response is None:
                print(">> No Proxies are Available.!!")
                sys.exit()

        return response

    def GetProxyIP(self):
        """
        The method can be called to access the dictionary of Proxy and authentications,
        returning values
        proxy : host and port
        username : username
        passwrd : password
        """

        username, password, ip, port = self.Load_Credentials()
        print(" Proxy IP Accessed.")

        proxyIP = {"http": "http://" + ip + ":" + port + "@" + username + ":" + password}

        return proxyIP

    def JsonToDataFrame(self, url, headers=None, verify=True):
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

        response = requests.get(url, headers=headers, verify=verify)
        if response.status_code == 200:
            json_data = response.json()
        else:
            print("Failed...Status Code of Requested URL is ",response.status_code)
            sys.exit()

        DataFrame = json_to_dataframe(json_data)

        return DataFrame


if __name__ == '__main__':
    KNPU = KNPublicUtility()
