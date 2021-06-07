import streamlit as st
import subprocess
import requests

st.header('Initiate the crawls')

location_type = st.selectbox('Select the location type',['panchayat','block','district','state','country'],index = 1)

ls_or_lc_widget = st.selectbox('Is it for one location or for a sample',['By Tag','By Code'])

if ls_or_lc_widget == 'By Tag':
    ls_or_lc = '-ls'
    tag_name_or_code = st.selectbox('Select the tag name',['Select','APITDA','RJODJH'])
    if tag_name_or_code == 'Select':
        st.stop()
else:
    ls_or_lc = '-lc'
    tag_name_or_code = st.text_input('Enter the location code')
    if tag_name_or_code == '':
        st.stop()

response = requests.get('https://backend.libtech.in/api/public/reportagg/?limit=10000')
results = response.json()['results']

report_types = []
for res in results:
    report_types.append(res['report_type'])

report_types.insert(0,'Select')
report_name = st.selectbox('Select the report name',report_types,index=0)
if report_name == 'Select':
    st.stop()

source_website = st.selectbox('AP or NIC website',['Select','AP','NIC'])
if source_website == 'Select':
    st.stop()
elif source_website  == 'AP':
    nic_param = '-notnic'
else:
    nic_param = ''

cmd = f'source ~/.bashrc ; python debug.py -p -lt {location_type} {ls_or_lc} {tag_name_or_code} -fn {report_name} -fd {nic_param}'
if st.button('Initate the crawl'):  
    subprocess.call(cmd,shell=True)
