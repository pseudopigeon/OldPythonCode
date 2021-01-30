import time as _time
import datetime as _dt
import requests as _re
import pandas as _pd
import numpy as _np
from bs4 import BeautifulSoup as _bs


base_url = r'https://www.sec.gov/cgi-bin/browse-edgar'



#class TickerBase():
#    def __init__(self, ticker):
#        self.ticker = str(ticker.upper())
        #self._cik = get_cik(self) ### required -> CIK number of company queried
        #self._company_url = request_url()

_base_url = 'https://www.sec.gov/cgi-bin/browse-edgar' ### base URL for SEC edgar
_cik_url = 'https://www.sec.gov/files/company_tickers.json'

        #def get_company_url(self, action='getcompany', owner='exclude', **kwargs):

            #return request_url(self.ticker, action='getcompany', owner='exclude',
                    #**kwargs)
def get_doc_details(ticker):
    file_url, soup = request_url(ticker)
    doc_table = soup.find_all('table', class='tableFile2')
    return df


def request_url(ticker, action='getcompany', owner='exclude', type=None,
                    dateb=None, start=None, output=None, count=None,
                    **kwargs):
    cik = get_cik(ticker)
    params = {'action': action,
                'CIK': cik,
                'owner': owner}
    if type is not None:
        params['type'] = type
    elif dateb is not None:
        params['dateb'] = dateb
    elif start is not None:
        params['start'] = start
    elif output is not None:
        params['output'] = output
    elif count is not None:
        params['count'] = count
    else:
        pass

    response = _re.get(url = _base_url, params=params)
    soup = _bs(response.content, 'html.parser')
    print('Request Succesful')
    #print(response.url)
    return response.url, soup



def get_cik(ticker):
    r = _re.get(_cik_url)
    data = r.json()
    df = _pd.DataFrame.from_dict(data, orient='index')
    df.columns = ['CIK', 'ticker', 'name']
    cik = df.loc[(df['ticker'] == ticker), 'CIK']
    print(cik)
    return cik


if __name__ == '__main__':
    request_url('MSFT', type='10-K')



#download_tickers_sec(edgar_ticker_url)
#def download_sec_form_metadata(cik, action type=None, file_date=None):
