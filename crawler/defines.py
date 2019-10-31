from pathlib import Path
import json
homeDir = str(Path.home())
jsonConfigFile=f"{homeDir}/.libtech/crawlerConfig.json"
with open(jsonConfigFile) as config_file:
  config=json.load(config_file)
baseURL=config['baseURL']
apiusername=config['apiusername']
apipassword=config['apipassword']
AUTHENDPOINT='%s/auth/jwt/' % (baseURL)
REPORTURL='%s/api/report/' % (baseURL)
LOCATIONDATASTATUSURL='%s/api/dataStatus/' % (baseURL)
LOCATIONURL='%s/api/location/' % (baseURL)
GETREPORTURL="%s/api/getReport/" % (baseURL)
TASKQUEUEURL='%s/api/queue/' % (baseURL)
AWS_DATA_BUCKET="libtech-india-data"
AWS_PROFILE_NAME="libtechIndia"
AWS_DATA_BUCKET_BASEURL="https://libtech-india-data.s3.ap-south-1.amazonaws.com/"
NICSearchIP="mnregaweb4.nic.in"
NICSearchURL="http://mnregaweb4.nic.in/netnrega/nregasearch1.aspx"
NICStatURL="http://mnregaweb4.nic.in/netnrega/all_lvl_details_dashboard_new.aspx"
JharkhandPDSBaseURL="https://aahar.jharkhand.gov.in/secc_cardholders/searchRationResults"
musterReDownloadThreshold=30
defaultReDownloadThreshold=30
APSTATECODE='02'

reportReDownloadThresholds=dict(
                 jobcardRegister=30,
                 nicGlanceStats=30,
                 blockRejectedTransactions=30,
                 apBlockRejectedTransactions=1,
                 musterDownload=30,
                 musterTransactions=60,
                 detailWorkPayment=30,
                 downloadMusters=0,
                 )
reportReDownloadThresholdsCurYear=dict(
                 jobcardRegister=30,
                 nicGlanceStats=30,
                 blockRejectedTransactions=10,
                 apBlockRejectedTransactions=1,
                 musterTransactions=10,
                 detailWorkPayment=10,
                 musterDownload=10,
                 downloadMusters=0,
                 )
              
