baseURL="http://b.libtech.in:9696"
apiusername='apimanager'
apipassword='sharedata'
AUTHENDPOINT='%s/auth/jwt/' % (baseURL)
REPORTURL='%s/api/report/' % (baseURL)
LOCATIONURL='%s/api/location/' % (baseURL)
TASKQUEUEURL='%s/api/queue/' % (baseURL)
AWS_DATA_BUCKET="libtech-india-data"
AWS_PROFILE_NAME="libtechIndia"
AWS_DATA_BUCKET_BASEURL="https://libtech-india-data.s3.ap-south-1.amazonaws.com/"
NICSearchIP="mnregaweb4.nic.in"
NICSearchURL="http://mnregaweb4.nic.in/netnrega/nregasearch1.aspx"
NICStatURL="http://mnregaweb4.nic.in/netnrega/all_lvl_details_dashboard_new.aspx"
musterReDownloadThreshold=3
defaultReDownloadThreshold=30


reportReDownloadThresholds=dict(
                 jobcardRegister=30,
                 nicGlanceStats=30,
                 )
              
