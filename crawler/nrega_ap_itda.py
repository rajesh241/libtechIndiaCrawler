import pandas as pd
import requests

def fetch_report(report_type, id,date = '20/06/2020'):
    url = 'http://www.nrega.ap.gov.in/Nregs/'
    #print('Fetching URL[%s] for cookies' % url)
    with requests.Session() as session:
        response = session.get(url)
        cookies = session.cookies

        headers = {
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        }
    if report_type == 'R14.5':
        params = (
            ('requestType', 'SmartCardreport_engRH'),
            ('actionVal', 'DelayPay'),
            ('id', id),
            ('type', '-1'),
            ('Date', '-1'),
            ('File', ''),
            ('Agency', ''),
            ('listType', ''),
            ('yearMonth', '-1'),
            ('ReportType', 'Program : ALL'),
            ('flag', '-1'),
            ('Rtype', ''),
            ('Date1', '-1'),
            ('wtype', ''),
            ('ytype', ''),
            ('Date2', '-1'),
            ('ltype', ''),
            ('year', '2020-2021'),
            ('program', 'ALL'),
            ('fileName', id),
            ('stype', ''),
            ('ptype', ''),
            ('lltype', ''),
        )
    elif report_type == 'R14.21(A)':
        params = (
            ('requestType', 'SmartCardreport_engRH'),
            ('actionVal', 'UIDSeedingLink'),
            ('id', id),
            ('type', ''),
            ('Date', '-1'),
            ('File', ''),
            ('Agency', ''),
            ('listType', ''),
            ('yearMonth', '-1'),
            ('ReportType', ''),
            ('flag', 'UIDLink'),
            ('Rtype', ''),
            ('Date1', '-1'),
            ('wtype', ''),
            ('ytype', ''),
            ('Date2', '-1'),
            ('ltype', ''),
            ('year', ''),
            ('program', ''),
            ('fileName', id),
            ('stype', ''),
            ('ptype', ''),
            ('lltype', ''),
        )
    elif report_type== 'R14.37':
        params = (
            ('requestType', 'SmartCardreport_engRH'),
            ('actionVal', 'NEFMS'),
            ('id', id),
            ('type', ''),
            ('Date', '-1'),
            ('File', ''),
            ('Agency', ''),
            ('listType', ''),
            ('yearMonth', '-1'),
            ('ReportType', ''),
            ('flag', '-1'),
            ('Rtype', '-1'),
            ('Date1', '-1'),
            ('wtype', '-1'),
            ('ytype', '-1'),
            ('Date2', '-1'),
            ('ltype', '-1'),
            ('year', ''),
            ('program', ''),
            ('fileName', id),
            ('stype', '-1'),
            ('ptype', '-1'),
            ('lltype', 'ITDA'),
        )
    elif report_type == 'R3.17':
        params = (
            ('requestType', 'PRReportsRH'),
            ('actionVal', 'DailyLabour'),
            ('id', id),
            ('type', '-1'),
            ('type1', ''),
            ('dept', ''),
            ('fromDate', ''),
            ('toDate', ''),
            ('Rtype', ''),
            ('reportGroup', ''),
            ('fto', ''),
            ('LinkType', '-1'),
            ('rtype', ''),
            ('reptype', ''),
            ('date', date),
            ('program', ''),
            ('type2', ''),
            ('type3', ''),
        )

    response = session.get('http://www.nrega.ap.gov.in/Nregs/FrontServlet', headers=headers, params=params, cookies=cookies, verify=False)

    df = pd.read_html(response.content)[0]

    return df

def fetch_R14_5(block,gp = ''):
    print('Fetching Suspended Payments Delay Analysis Report')
    id = '03' + block + gp
    return fetch_report('R14.5', id)

def fetch_R14_21(block,gp=''):
    print('Fetching Wage seekers identified as NOT ENROLLED report')
    id = '03' + block + gp
    return fetch_report('R14.21(A)', id)
def fetch_R14_37(block,gp = ''):
    print('Fetching Payment Pending and Rejected payments report')
    raw_id = '03' + block + gp
    id = '~'.join(raw_id[i:i + 2] for i in range(0, len(raw_id), 2))
    return fetch_report('R14.37',id)
def fetch_R3_17(block,gp = ''):
    print(f'Fetching Labor reported report')
    id = '03' + block + gp
    return fetch_report('R3.17',id,date='19/06/2020')

df1 = fetch_R3_17(block='11')
print(df1.head())