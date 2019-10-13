from commons import getFullFinYear,getCurrentFinYear,validateAndSave,correctDateFormat,getjcNumber,getFilePath,saveReport,getCenterAlignedHeading,stripTableAttributes,stripTableAttributesPreserveLinks,getFinYear,getDateObj,htmlWrapperLocal,getAWSFileURL,getReportFileURL,daysSinceModifiedS3,getDefaultStartFinYear,createUpdateDjangoReport,getShortFinYear,getLocationDict,getReportURL,isReportUpdated,getChildLocations,getReportDF,computePercentage,saveLocationStatus
from defines import NICSearchIP,NICSearchURL,musterReDownloadThreshold,REPORTURL,LOCATIONURL,JharkhandPDSBaseURL
from aws import awsInit,uploadS3
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from slugify import slugify
import datetime 
from urllib import parse
import re
from threading import Thread
import threading
from queue import Queue
import time
import decimal
import numpy as np

def libtechQueueManager(logger,jobList,num_threads=100):
  error=None
  q = Queue(maxsize=0)
  rq = Queue(maxsize=0) # This is result Queue
  i=0
  for job in jobList:
    q.put(job)
  
  for i in range(num_threads):
    name="libtechWorker%s" % str(i)
    worker = Thread(name=name,target=libtechQueueWorker, args=(logger,q,rq))
    worker.setDaemon(True)
    worker.start()

  q.join()
  for i in range(num_threads):
    q.put(None)
  resultArray=[]
  while not rq.empty():
    result = rq.get()
    if result is not None:
      resultArray.extend(result)
    
  return resultArray


def libtechQueueWorker(logger,q,rq):
  name = threading.currentThread().getName()
  while True:
    obj=q.get()
    if obj is None:
      break
    funcName=obj['funcName']
    funcArgs=obj['funcArgs']
    logger.info("Queue Size %s Thread %s  job %s " % (str(q.qsize()),name,funcName))
    logger.warning("Queue Size %s Thread %s  job %s " % (str(q.qsize()),name,funcName))
    try:
   #   result = getattr(queueMethods, funcName)(logger,funcArgs)
      result = globals()[funcName](logger,funcArgs,threadName=name)
      rq.put(result)
    except Exception as e:
      logger.error(e, exc_info=True)
    time.sleep(3)
    q.task_done()

def panchayatReferenceDocument(logger,locationCode,startFinYear=None,endFinYear=None):
  if startFinYear is None:
    startFinYear=getDefaultStartFinYear()
  if endFinYear is None:
    endFinYear=getCurrentFinYear()
  ldict=getLocationDict(logger,locationCode=locationCode)
  locationType=ldict.get("locationType",None)
  if locationType != 'panchayat':
    return None
  blockCode=ldict.get("blockCode",None)
  glanceURL=nicGlanceStats(logger,blockCode)
  detailWorkPayment(logger,locationCode,startFinYear=startFinYear,endFinYear=endFinYear,num_threads=100)
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    csvArray=[]
    accuracy=getAccuracy(logger,locationCode,finyear=finyear)
    saveLocationStatus(logger,locationCode,finyear,accuracy) 
    a=["accuracy",accuracy]
    csvArray.append(a)
    workPaymentURL=getReportFileURL(logger,ldict,"detailWorkPayment",finyear)
    a=["workPaymentReport",workPaymentURL]
    csvArray.append(a) 
    df = pd.DataFrame(csvArray)
    url=saveReport(logger,ldict,"panchayatReferenceDocument",finyear,df)
  return ''
    
def getAccuracy(logger,locationCode,startFinYear=None,endFinYear=None,finyear=None):
  if startFinYear is None:
    startFinYear=getDefaultStartFinYear()
  if endFinYear is None:
    endFinYear=getCurrentFinYear()
  if finyear is not None:
    finArray=[finyear]
  else:
    finArray=[]
    for finyear in range(int(startFinYear),int(endFinYear)+1):
      finArray.append(str(finyear))

  ldict=getLocationDict(logger,locationCode=locationCode)
  blockCode=ldict.get("blockCode",None)
  glanceURL=nicGlanceStats(logger,blockCode)
  logger.info(glanceURL)
  statDF=getReportDF(logger,locationCode=blockCode,reportType="nicGlanceStats") 
  logger.info(statDF.columns)
  slug='persondays-generated-so-far'
  statDF=statDF.fillna(0)
  statDF['panchayatCode'] = statDF['panchayatCode'].astype(int)
  statDF['panchayatCode'] = statDF['panchayatCode'].astype(str)
  statDF['finyear'] = statDF['finyear'].astype(int)
  statDF['finyear'] = statDF['finyear'].astype(str)

  for finyear in finArray:
    finyear=str(finyear)
    selectedRow=statDF.loc[(statDF['finyear']== finyear) & (statDF['slug'] == slug) & (statDF['panchayatCode'] == locationCode)]
    nicWorkDays=0
    if len(selectedRow == 1):
      nicWorkDays=selectedRow.iloc[0]['value']
      try:
        nicWorkDays=int(nicWorkDays)
      except:
        nicWorkDays=0

    wpDF=getReportDF(logger,locationCode=locationCode,reportType="detailWorkPayment",finyear=finyear) 
    libtechWorkDays=Total = wpDF['daysWorked'].sum()
    accuracy=computePercentage(libtechWorkDays,nicWorkDays)
    logger.info(f"nic work days {nicWorkDays} libtech work days {libtechWorkDays} accuracy {accuracy}")
  return accuracy 
def crawlLocation(logger,locationCode,startFinYear=None,endFinYear=None):
  if startFinYear is None:
    startFinYear=getDefaultStartFinYear()
  if endFinYear is None:
    endFinYear=getCurrentFinYear()
  ##First lets get the stat #Assuming this is for panchayat
  ldict=getLocationDict(logger,locationCode=locationCode)
  locationType=ldict.get("locationType",None)
  blockCode=ldict.get("blockCode",None)
  parentLocation=ldict.get("parentLocation",None)
  parent_ldict=getLocationDict(logger,locationID=parentLocation)
  parentLocationCode=parent_ldict.get("code",None)
  logger.info(parentLocationCode)
  glanceURL=nicGlanceStats(logger,parentLocationCode)

  #Get Jobcard Register only for block and panchayats
  if locationType == 'panchayat':
    locationCodeArray=[locationCode]
  elif (locationType == "block"):
    locationCodeArray=getChildLocations(logger,locationCode)
  else:
    locationCodeArrapy=[]

  for eachCode in locationCodeArray:
    jobcardRegister(logger,eachCode)
  
  if ( (locationType == 'block') or (locationType == 'panchayat')):
    downloadBlockRejectedPayments(logger,blockCode=blockCode,startFinYear=startFinYear,endFinYear=endFinYear)

  for eachCode in locationCodeArray:
    detailWorkPayment(logger,eachCode,startFinYear=None,endFinYear=None)

def detailWorkPayment1(logger,locationCode,startFinYear=None,endFinYear=None):
  logger.info(f"Executing Muster Transactions for {locationCode} with {startFinYear}-{endFinYear}")
  musterTransactions(logger,locationCode,startFinYear=None,endFinYear=None,num_threads=100)
  ldict=getLocationDict(logger,locationCode=locationCode)
  locationType=ldict.get("locationType",None)
  blockCode=ldict.get("blockCode",None)
  if locationType != "panchayat":
    return None
  if startFinYear is None:
    startFinYear=getDefaultStartFinYear()
  if endFinYear is None:
    endFinYear=getCurrentFinYear()

  jobcardRegister(logger,locationCode)
  blockRejectedTransactions(logger,blockCode,startFinYear=startFinYear,endFinYear=endFinYear)
  downloadMusters(logger,locationCode,startFinYear=startFinYear)
  jobList=[]
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    finyear=str(finyear)
    funcArgs=[locationCode,finyear]
    p={}
    p['funcName']="createWorkPaymentReport"
    p['funcArgs']=funcArgs
    jobList.append(p)
  libtechQueueManager(logger,jobList,num_threads=10)
  return ''
def updateWorkPaymentReport(logger,locationCode,finyear=None):
  ldict=getLocationDict(logger,locationCode=locationCode)
  #Get Rejected Transactions
  blockCode=ldict.get("blockCode",None)
  if finyear is not None:
    logger.info(finyear)
    reportType="blockRejectedTransactions"
    rejectedDF=getReportDF(logger,locationCode=blockCode,reportType=reportType,finyear=finyear) 
    logger.info(rejectedDF.columns)
    reportType="detailWorkPayment"
    wpDF=getReportDF(logger,locationCode=locationCode,reportType=reportType,finyear=finyear) 
    if ( (wpDF is not None) and (rejectedDF is not None)):
      for index, row in wpDF.iterrows():
        jobcard=row['jobcard']
        name=row['name']
        workerCode="%s_%s" % (jobcard,name)
        musterNo=row['musterNo']
        matchedDF=rejectedDF.loc[(rejectedDF['workerCode'] == workerCode) & (rejectedDF['musterNo'] == musterNo)]
        matchedRows=len(matchedDF)
        rejWagelists=''
        rejFTOs=''
        rejReasons=''
        rejProcessDate=''
        rejStatus=''
        if matchedRows > 0:
          matchedDF = matchedDF.replace(np.nan, '', regex=True)
          for i, row in matchedDF.iterrows():
            rejWagelists+=f"{row['wagelistNo']} |"
            rejFTOs+=f"{row['ftoNo']} |"
            rejReasons+=f"{row['rejectionReason']} |"
            rejProcessDate+=f"{row['processDate']} |"
            rejStatus+=f"{row['status']} |"
          rejReasons=rejReasons.rstrip("|")
          rejWagelists=rejWagelists.rstrip("|")
          rejFTOs=rejFTOs.rstrip("|")
          rejProcessDate=rejProcessDate.rstrip("|")
          rejStatus=rejStatus.rstrip("|")
          wpDF.at[index, 'rejWagelists'] = rejWagelists
          wpDF.at[index, 'rejFTOs'] = rejFTOs
          wpDF.at[index, 'rejProcessDate'] = rejProcessDate
          wpDF.at[index, 'rejStatus'] = rejStatus
          wpDF.at[index, 'rejReasons'] = rejReasons
    reportType="detailWorkPayment"
    url=saveReport(logger,ldict,reportType,str(finyear),wpDF)
    logger.info(url)
          
	  
def temp(logger,locationCode,startFinYear=None,endFinYear=None):
  if endFinYear is None:
    endFinYear=getCurrentFinYear()
  if startFinYear is None:
    startFinYear=getDefaultStartFinYear()
  ldict=getLocationDict(logger,locationCode=locationCode)
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    logger.info(finyear)
    wpDF=None
    reportType="jobcardRegister"
    jobcardRegisterURL=getReportFileURL(logger,ldict,reportType,'')
    logger.info(jobcardRegisterURL)
    try:
      jobcardDF=pd.read_csv(jobcardRegisterURL)
    except:
      jrError=jobcardRegister(logger,locationCode)
      if jrError is not None:
        error=f"could not download JobcardRegister for {ldict}"
        logger.error(error)
        return error
      else:
        jobcardDF=pd.read_csv(jobcardRegisterURL)
    i=0
    for index, row in jobcardDF.iterrows():
      i=i+1
      jobcard=row['jobcard']
      filepath=getFilePath(logger,ldict)
      filename="%sDATA/jobcards/%s.csv" % (filepath,slugify(jobcard))
      awsURL=getAWSFileURL(filename)
      logger.info(awsURL)
      if i == 5:
        break
      try:
        jcWPDF=pd.read_csv(awsURL)
      except:
        jcWPDF=None
      if jcWPDF is not None:
        logger.info("not None")
        partDF=jcWPDF.loc[jcWPDF['finyear'] == finyear]
        logger.info(f"part DF shape = {partDF.shape}")
        partDF=partDF.reset_index(drop=True)
        if wpDF is None:
          wpDF = partDF
        else:
          wpDF=wpDF.append(partDF, ignore_index=True)
    reportType="detailWorkPayment"
    url=saveReport(logger,ldict,reportType,str(finyear),wpDF)
    logger.info(url)
    updateWorkPaymentReport(logger,locationCode,finyear=finyear)
 
def createWorkPaymentReport(logger,funcArgs,threadName=''):
  logger.info("I am here")
  locationCode=funcArgs[0]
  finyear=funcArgs[1]
  ldict=getLocationDict(logger,locationCode=locationCode)
  wpDF=None
  if finyear is not None:
    reportType="jobcardRegister"
    jobcardDF=getReportDF(logger,locationCode=locationCode,reportType=reportType) 
    if jobcardDF is None:
      return None

    i=0
    for index, row in jobcardDF.iterrows():
      i=i+1
      jobcard=row['jobcard']
      filepath=getFilePath(logger,ldict)
      filename="%sDATA/jobcards/%s.csv" % (filepath,slugify(jobcard))
      awsURL=getAWSFileURL(filename)
      logger.info(awsURL)
      if i == 5:
        break
      #logger.info(awsURL)
      try:
        jcWPDF=pd.read_csv(awsURL)
      except:
        jcWPDF=None
      if jcWPDF is not None:
        partDF=jcWPDF.loc[jcWPDF['finyear'] == finyear]
        partDF=partDF.reset_index(drop=True)
        logger.info(f"part DF is {partDF.head()}")
        if wpDF is None:
          wpDF = partDF
        else:
          wpDF=wpDF.append(partDF, ignore_index=True)
    if wpDF is not None:
      reportType="detailWorkPayment"
      url=saveReport(logger,ldict,reportType,str(finyear),wpDF)
      logger.info(url)
      updateWorkPaymentReport(logger,locationCode,finyear=finyear)

def mergeTransactions(logger,funcArgs,threadName="default"):
  #workerDF  row.state,row.district,row.block,row.panchayat,row.village,row.stateCode,row.districtCode,row.blockCode,row.panchayatCode,row.jobcard,row.headOfHousehold,row.issue Date,row.caste,row.jcNo,row.applicantNo,row.name,row.age,row.gender,row.fatherHusbandName,row.isDeleted,row.isMinority,row.isDisabled,
  # worker DF Headers 'state','district','block','panchayat','village','stateCode','districtCode','blockCode','panchayatCode','jobcard','headOfHousehold','issue Date','caste','jcNo','applicantNo','name','age','gender','fatherHusbandName','isDeleted','isMinority','isDisabled',
  locationHeaders=['state','district','block','panchayat','village','stateCode','districtCode','blockCode','panchayatCode']
  baseRowHeaders=['jobcard','applicantNo','name','gender','age','accountNo','finAgency','aadharNo','workerCode','finyear','daysAllocated','amountDue']
  workerRowHeaders=['headOfHousehold','issue Date','caste','jcNo','fatherHusbandName','isDeleted','isMinority','isDisabled']
  musterRowHeaders=['musterPanchayatName','musterPanchayatCode','localWorkSite','m_finyear','musterNo','workCode','workName','dateFrom','dateTo','paymentDate','musterIndex','m_accountNo','m_bankName','m_branchName','m_branchCode','dayWage','daysProvided','daysWorked','totalWage','wagelistNo','creditedDate']
  rejectedRowHeaders=["rejWagelists","rejFTOs","rejReasons","rejProcessDate","rejStatus","rejCount"]
  ftoHeader=["wagelistIssueDate","ftoNo","ftoFinYear","secondSignatoryDate"]
  ftoRow=["","","",""]
  csvArray=[]
  txnDict=funcArgs[0]
  workerDF=funcArgs[1]
  musterDF=funcArgs[2]
  rejectedDF=funcArgs[3]
  workerCode=txnDict.get("workerCode",None)
  musterNo=txnDict.get("musterNo",None)
  jobcard=txnDict.get("jobcard",None)
  name=txnDict.get("name",None)
  baseRow=[txnDict.jobcard,txnDict.applicantNo,txnDict.name,txnDict.gender,txnDict.age,txnDict.accountNo,txnDict.finAgency,txnDict.aadharNo,txnDict.workerCode,txnDict.finyear,txnDict.daysAllocated,txnDict.amountDue]

  selectedRow=workerDF.loc[(workerDF['jobcard'] == jobcard) & (workerDF['name'] == name)]
  #logger.info(len(selectedRow))
  if len(selectedRow) > 0:
    workerRow=selectedRow.iloc[0][ workerRowHeaders ].values.flatten().tolist()
    locationRow=selectedRow.iloc[0][ locationHeaders ].values.flatten().tolist()
  #logger.info(workerRow)

  selectedRow=musterDF.loc[(musterDF['workerCode'] == workerCode) & (musterDF['musterNo'] == musterNo)]
  if len(selectedRow) > 0:
    musterRow=selectedRow.iloc[0][ musterRowHeaders ].values.flatten().tolist()
  #logger.info(musterRow)


  matchedDF=rejectedDF.loc[(rejectedDF['workerCode'] == workerCode) & (rejectedDF['musterNo'] == musterNo)]
  matchedRows=len(matchedDF)
  rejWagelists=''
  rejFTOs=''
  rejReasons=''
  rejProcessDate=''
  rejStatus=''
  rejCount=0
  if matchedRows > 0:
    matchedDF.to_csv(f"/tmp/{musterNo}.csv")
    matchedDF = matchedDF.replace(np.nan, '', regex=True)
    for i, row in matchedDF.iterrows():
      rejWagelists+=f"{row['wagelistNo']} |"
      rejFTOs+=f"{row['ftoNo']} |"
      rejReasons+=f"{row['rejectionReason']} |"
      rejProcessDate+=f"{row['processDate']} |"
      rejStatus+=f"{row['status']} |"
      if row['status'] == "Rejected":
        rejCount=rejCount+1
    rejReasons=rejReasons.rstrip("|")
    rejWagelists=rejWagelists.rstrip("|")
    rejFTOs=rejFTOs.rstrip("|")
    rejProcessDate=rejProcessDate.rstrip("|")
    rejStatus=rejStatus.rstrip("|")
  rejectedRow=[rejWagelists,rejFTOs,rejReasons,rejProcessDate,rejStatus,rejCount]
  #logger.info(f"rejected row is {rejectedRow}")
  
  a=locationRow+baseRow+workerRow+musterRow+ftoRow+rejectedRow
  headers=locationHeaders+baseRowHeaders+workerRowHeaders+musterRowHeaders+rejectedRowHeaders
  csvArray.append(a)
  return csvArray
def printCols(logger,df):
  s=''
  h=''
  for col in df.columns:
    logger.info(col)
    s+="row.%s," % col
    h+="'%s'," % col
  logger.info(s)
  logger.info(h)
def detailWorkPayment(logger,locationCode,startFinYear=None,endFinYear=None,num_threads=100):
  locationHeaders=['state','district','block','panchayat','village','stateCode','districtCode','blockCode','panchayatCode']
  baseRowHeaders=['jobcard','applicantNo','name','gender','age','accountNo','finAgency','aadharNo','workerCode','finyear','daysAllocated','amountDue']
  workerRowHeaders=['headOfHousehold','issue Date','caste','jcNo','fatherHusbandName','isDeleted','isMinority','isDisabled']
  musterRowHeaders=['musterPanchayatName','musterPanchayatCode','localWorkSite','m_finyear','musterNo','workCode','workName','dateFrom','dateTo','paymentDate','musterIndex','m_accountNo','m_bankName','m_branchName','m_branchCode','dayWage','daysProvided','daysWorked','totalWage','wagelistNo','creditedDate']
  ftoHeaders=["wagelistIssueDate","ftoNo","ftoFinYear","secondSignatoryDate"]
  rejectedRowHeaders=["rejWagelists","rejFTOs","rejReasons","rejProcessDate","rejStatus","rejCount"]
  headers=locationHeaders+baseRowHeaders+workerRowHeaders+musterRowHeaders+ftoHeaders+rejectedRowHeaders
  ldict=getLocationDict(logger,locationCode=locationCode)
  locationType=ldict.get("locationType",None)
  blockCode=ldict.get("blockCode",None)
  if locationType != "panchayat":
    return None
  if startFinYear is None:
    startFinYear=getDefaultStartFinYear()
  if endFinYear is None:
    endFinYear=getCurrentFinYear()

  downloadBlockRejectedPayments(logger,blockCode=blockCode,startFinYear=startFinYear,endFinYear=endFinYear)
  musterTransactions(logger,locationCode,startFinYear=startFinYear,endFinYear=endFinYear,num_threads=100)

  rejectedDF=None
  musterDF=None
  workerDF=getReportDF(logger,locationCode=locationCode,reportType="workerRegister",index_col=0) 
  rejDFs=[]
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    finyear=str(finyear)
    reportType="blockRejectedTransactions"
    df=getReportDF(logger,locationCode=blockCode,reportType=reportType,finyear=finyear,index_col=0) 
    if df is not None:
      logger.info(f"DF shape {df.shape} for finyear {finyear}")
      rejDFs.append(df)
  rejDF=pd.concat(rejDFs,ignore_index=True)
  rejDF.to_csv("/tmp/rejDF.csv")
  logger.info(f"Saape of combined DF is {rejDF.shape}")
  rejectedDF=rejDF.drop_duplicates()

  for finyear in range(int(startFinYear),int(endFinYear)+1):
    updateStatus,reportURL=isReportUpdated(logger,'detailWorkPayment',locationCode,finyear=finyear)
    if updateStatus == False:
      jcDF=getReportDF(logger,locationCode=locationCode,reportType="jobcardTransactions",finyear=finyear)
      musterDF=getReportDF(logger,locationCode=locationCode,reportType="musterTransactions",finyear=finyear)
   #   logger.info(jcDF.head())
      jobList=[]
      for index,row in jcDF.iterrows():
        funcArgs=[row,workerDF,musterDF,rejectedDF]
        funcName="mergeTransactions" 
        p={}
        p['funcName']=funcName
        p['funcArgs']=funcArgs
        jobcard=row['jobcard']
        jobList.append(p)
      logger.info(f"Lenght of jobList is {len(jobList)}")
      reportType="detailWorkPayment" 
      resultArray=libtechQueueManager(logger,jobList,num_threads=num_threads)
      wpDF = pd.DataFrame(resultArray, columns =headers)
      wpDF=wpDF.sort_values(['jcNo','dateFrom'])
      url=saveReport(logger,ldict,reportType,finyear,wpDF)
  return ''
def musterTransactions(logger,locationCode,startFinYear=None,endFinYear=None,num_threads=100):
  logger.info(f"Executing Muster Transactions for {locationCode} with {startFinYear}-{endFinYear}")
  locationArrayLabel=["state","district","block","panchayat","village","stateCode","districtCode","blockCode","panchayatCode","musterPanchayatName","musterPanchayatCode","localWorkSite"]
  musterArrayLabel=["finyear","m_finyear","musterNo","workCode","workName","dateFrom","dateTo","paymentDate"] 
  detailArrayLabel=["musterIndex","workerCode","jobcard","name","m_accountNo","m_bankName","m_branchName","m_branchCode","dayWage","daysProvided","daysWorked","totalWage","wagelistNo","creditedDate"]
  headers=locationArrayLabel+musterArrayLabel+detailArrayLabel
  ldict=getLocationDict(logger,locationCode=locationCode)
  locationType=ldict.get("locationType",None)
  blockCode=ldict.get("blockCode",None)
  if locationType != "panchayat":
    return None
  if startFinYear is None:
    startFinYear=getDefaultStartFinYear()
  if endFinYear is None:
    endFinYear=getCurrentFinYear()
  jobcardTransactions(logger,locationCode,startFinYear=startFinYear,endFinYear=endFinYear,num_threads=100)
  
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    updateStatus,reportURL=isReportUpdated(logger,'musterTransactions',locationCode,finyear=finyear)
    logger.info(f"Update status for musterTransactions for finyear {finyear} is {updateStatus}")
    if updateStatus == False:
      jobList=[]
      finyear=str(finyear)
      logger.info(finyear)
      reportType='jobcardTransactions'
      txnDF=getReportDF(logger,locationCode=locationCode,reportType=reportType,finyear=finyear)
      logger.info(txnDF.head())
      urls=txnDF.musterURL.unique()
      logger.info(f"Number of Musters to be downloaded is {len(urls)}")
      for url in urls:
        funcArgs=[ldict,url,finyear]
        funcName="getMusterDF"
        p={}
        p['funcName']="getMusterDF"
        p['funcArgs']=funcArgs
        jobList.append(p)
      reportType="musterTransactions" 
      resultArray=libtechQueueManager(logger,jobList,num_threads=num_threads)
      wpDF = pd.DataFrame(resultArray, columns =headers)
      url=saveReport(logger,ldict,reportType,finyear,wpDF)
  return ''
def jobcardTransactions(logger,locationCode,startFinYear=None,endFinYear=None,num_threads=100):
  logger.info(f"Executing Jobcard Transactions for {locationCode} with {startFinYear}-{endFinYear}")
  workerArrayLabel=["jobcard","applicantNo","name","gender","age","accountNo","finAgency","aadharNo"]
  txnArrayLabel=["workerCode","finyear","workName","workDate","daysAllocated","amountDue","musterNo","musterURL"]
  headers=workerArrayLabel+txnArrayLabel
  ldict=getLocationDict(logger,locationCode=locationCode)
  locationType=ldict.get("locationType",None)
  blockCode=ldict.get("blockCode",None)
  if locationType != "panchayat":
    return None
  if startFinYear is None:
    startFinYear=getDefaultStartFinYear()
  endFinYear=getCurrentFinYear() # We will over ride the endFinYear because we would anyways download the jobards page and might as well parse it.
  finalStatus=True
  
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    updateStatus,reportURL=isReportUpdated(logger,'jobcardTransactions',locationCode,finyear=finyear)
    if updateStatus == False:
      finalStatus=False
  logger.info(f"Update Status if Jobcard Transaction is {updateStatus}")
  if finalStatus == False:
    jobcardRegister(logger,locationCode)
    reportType="jobcardRegister"
    jobcardDF=getReportDF(logger,locationCode=locationCode,reportType=reportType) 
    if jobcardDF is None:
      return None
    i=0
    jobList=[]
    for index, row in jobcardDF.iterrows():
      i=i+1
      jobcard=row['jobcard']
      funcArgs=[startFinYear,ldict,jobcard]
      p={}
      p['funcName']="fetchJobcardDetails"
      p['funcArgs']=funcArgs
      jobList.append(p)
      #logger.info(f"{index}-{p}")
      if i == 10000:
        break
    reportType="jobcardTransactions" 
    resultArray=libtechQueueManager(logger,jobList,num_threads=num_threads)
    wpDF = pd.DataFrame(resultArray, columns =headers)
    for finyear in range(int(startFinYear),int(endFinYear)+1):
      finyear=str(finyear)
      df=wpDF.loc[(wpDF['finyear'] == finyear) ]
      url=saveReport(logger,ldict,reportType,finyear,df)
  return ''

 #csvArray=[]
 #header=["header"]
 #a=[str(datetime.datetime.now)]
 #csvArray.append(a)
 #reportType='downloadJobards'
 #for finyear in range(int(startFinYear),int(endFinYear)+1):
 #  df = pd.DataFrame(csvArray, columns =header)
 #  url=saveReport(logger,ldict,reportType,finyear,df)


def downloadMusters(logger,locationCode,startFinYear=None,endFinYear=None,num_threads=100):
  
  ldict=getLocationDict(logger,locationCode=locationCode)
  locationType=ldict.get("locationType",None)
  blockCode=ldict.get("blockCode",None)
  if locationType != "panchayat":
    return None
  if startFinYear is None:
    startFinYear=getDefaultStartFinYear()
  if endFinYear is None:
    endFinYear=getCurrentFinYear()

  jobcardRegister(logger,locationCode)
  blockRejectedTransactions(logger,blockCode,startFinYear=startFinYear,endFinYear=endFinYear)

  updateStatus,reportURL=isReportUpdated(logger,'downloadMusters',locationCode,finyear=startFinYear)
  if updateStatus:
    return reportURL

  rejDFs=[]
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    finyear=str(finyear)
    reportType="blockRejectedTransactions"
    df=getReportDF(logger,locationCode=blockCode,reportType=reportType,finyear=finyear,index_col=0) 
    if df is not None:
      logger.info(f"DF shape {df.shape} for finyear {finyear}")
      rejDFs.append(df)
  rejDF=pd.concat(rejDFs,ignore_index=True)
  rejDF.to_csv("/tmp/rejDF.csv")
  logger.info(f"Saape of combined DF is {rejDF.shape}")
  rejDF=rejDF.drop_duplicates()
 
  jobcardRegister(logger,locationCode)
  blockRejectedTransactions(logger,blockCode,startFinYear=startFinYear,endFinYear=endFinYear)
  reportType="jobcardRegister"
  jobcardDF=getReportDF(logger,locationCode=locationCode,reportType=reportType) 
  if jobcardDF is None:
    return None
  logger.info(jobcardDF.head())
  jobList=[]
  i=0
  for index, row in jobcardDF.iterrows():
    i=i+1
    jobcard=row['jobcard']
    village=row['village']
    caste=row['caste']
    headOfHousehold=row['headOfHousehold']
    jcNo=row['jcNo']
    jdict={
      'jobcard':jobcard,
      'village':village,
      'caste':caste,
      'headOfHousehold':headOfHousehold,
      'jcNo':jcNo,

            }
    funcArgs=[startFinYear,ldict,jdict,rejDF]
    p={}
    p['funcName']="getJobcardDetails"
    p['funcArgs']=funcArgs
    jobList.append(p)
    #logger.info(f"{index}-{p}")
    if i == 10000:
      break
 # logger.warning(jobList)    
 # num_threads=5 
  
  libtechQueueManager(logger,jobList,num_threads=num_threads)

  csvArray=[]
  header=["header"]
  a=[str(datetime.datetime.now)]
  csvArray.append(a)
  reportType='downloadMusters'
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    df = pd.DataFrame(csvArray, columns =header)
    url=saveReport(logger,ldict,reportType,finyear,df)

def jobcardRegister(logger,locationCode,startFinYear=None,endFinYear=None):
  error=None
  reportType="jobcardRegister"
  reportName="Jobcard Register"
  updateStatus,reportURL=isReportUpdated(logger,reportType,locationCode)
  if updateStatus:
    return reportURL
  ldict=getLocationDict(logger,locationCode=locationCode)
  myhtml=getJobcardRegister(logger,ldict)
  myhtml=myhtml.replace(b'</nobr><br>',b',')
  myhtml=myhtml.replace(b"bordercolor='#111111'>",b"bordercolor='#111111'><tr>")
  myhtml=validateAndSave(logger,ldict,myhtml)
  reportURL=processJobcardRegister(logger,ldict,myhtml)
  return reportURL

def getJobcardRegister(logger,ldict):
  stateCode=ldict.get("stateCode",None)
  fullDistrictCode=ldict.get("districtCode",None)
  fullBlockCode=ldict.get("blockCode",None)
  fullPanchayatCode=ldict.get("panchayatCode",None)
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  stateName=ldict.get("stateName",None)
  panchayatName=ldict.get("panchayatName",None)
  crawlIP=ldict.get("crawlIP",None)
  finyear=getCurrentFinYear()
  fullfinyear=getFullFinYear(finyear) 
  logger.debug("Processing StateCode %s, fullDistrictCode : %s, fullBlockCode : %s, fullPanchayatCode: %s " % (stateCode,fullDistrictCode,fullBlockCode,fullPanchayatCode))
  panchayatPageURL="http://%s/netnrega/IndexFrame.aspx?lflag=eng&District_Code=%s&district_name=%s&state_name=%s&state_Code=%s&block_name=%s&block_code=%s&fin_year=%s&check=1&Panchayat_name=%s&Panchayat_Code=%s" % (crawlIP,fullDistrictCode,districtName,stateName,stateCode,blockName,fullBlockCode,fullfinyear,panchayatName,fullPanchayatCode)
#  panchayatPageURL=panchayatPageURL.replace(" ","+")
  panchayatDetailURL="http://%s/netnrega/Citizen_html/Panchregpeople.aspx" % crawlIP
  logger.debug(panchayatPageURL)
  logger.debug(panchayatDetailURL)
  #Starting the Download Process
  url="http://nrega.nic.in/netnrega/home.aspx"
  logger.info(panchayatPageURL)
  #response = requests.get(url, headers=headers, params=params)
  response = requests.get(panchayatPageURL)
  myhtml=str(response.content)
  splitString="Citizen_html/Panchregpeople.aspx?lflag=eng&fin_year=%s&Panchayat_Code=%s&type=a&Digest=" % (fullfinyear,fullPanchayatCode)
  myhtmlArray=myhtml.split(splitString)
  myArray=myhtmlArray[1].split('"')
  digest=myArray[0]
  cookies = response.cookies
  logger.debug(cookies)
  headers = {
    'Host': crawlIP,
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:54.0) Gecko/20100101 Firefox/54.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
#    'Referer': panchayatPageURL,
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

  params = (
    ('lflag', 'eng'),
    ('fin_year', fullfinyear),
    ('Panchayat_Code', fullPanchayatCode),
    ('type', 'a'),
    ('Digest', digest),

  )

  response=requests.get(panchayatDetailURL, headers=headers, params=params, cookies=cookies)
  logger.debug("Downloaded StateCode %s, fullDistrictCode : %s, fullBlockCode : %s, fullPanchayatCode: %s " % (stateCode,fullDistrictCode,fullBlockCode,fullPanchayatCode))
  return response.content

def processJobcardRegister(logger,ldict,myhtml):
    error=None
    finyear=''
    df=None
    filepath=getFilePath(logger,ldict)
    stateCode=ldict.get("stateCode",None)
    districtCode=ldict.get("districtCode",None)
    blockCode=ldict.get("blockCode",None)
    panchayatCode=ldict.get("panchayatCode",None)
    stateName=ldict.get("stateName",None)
    districtName=ldict.get("districtName",None)
    blockName=ldict.get("blockName",None)
    panchayatName=ldict.get("panchayatName",None)
    csvArray=[]
    jobcardCsvArray=[]
    locationArrayLabel=["state","district","block","panchayat","village","stateCode","districtCode","blockCode","panchayatCode"]
    jobcardArrayLabel=["jobcard","headOfHousehold","issue Date","caste","jcNo"]
    workerArrayLabel=["applicantNo","name","age","gender","fatherHusbandName","isDeleted","isMinority","isDisabled"]
    header=locationArrayLabel+jobcardArrayLabel+workerArrayLabel
    jobcardheader=locationArrayLabel+jobcardArrayLabel
    reportType="workerRegister"
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    myTable=htmlsoup.find('table',id="myTable")
    jobcardPrefix=ldict.get("stateShortCode",None)+"-"
    logger.debug(jobcardPrefix)
    if myTable is not None:
      logger.debug("Found the table")
      rows=myTable.findAll('tr')
      headOfHousehold=''
      applicantNo=0
      fatherHusbandName=''
      village=''
      villageDict={}
      for row in rows:
        if "Villages : " in str(row):
          logger.debug("Village Name Found")
          cols=row.findAll('td')
          villagestr=cols[0].text.lstrip().rstrip()
          villageName=villagestr.replace("Villages :" ,"").lstrip().rstrip()
        if jobcardPrefix in str(row):
          locationArray=[stateName,districtName,blockName,panchayatName,villageName,stateCode,districtCode,blockCode,panchayatCode]
          isDeleted=False
          isDisabled=False
          isMinority=False
          cols=row.findAll('td')
          rowIndex=cols[0].text.lstrip().rstrip()
          jobcard=cols[9].text.lstrip().rstrip().split(",")[0]
          if len(cols[9].text.lstrip().rstrip().split(",")) > 1:
            issueDateString=cols[9].text.lstrip().rstrip().split(",")[1]
          else:
            issueDateString=''
          gender=cols[6].text.lstrip().rstrip()
          age=cols[7].text.lstrip().rstrip()
          applicationDateString=cols[8].text.lstrip().rstrip()
          remarks=cols[10].text.lstrip().rstrip()
          disabledString=cols[11].text.lstrip().rstrip()
          minorityString=cols[12].text.lstrip().rstrip()
          name=cols[4].text.lstrip().rstrip()
          name=name.rstrip('*')
          name=name.rstrip().strip()
          #logger.debug("Processing %s - %s " % (jobcard,name))
          issueDate=correctDateFormat(issueDateString)
          applicationDate=correctDateFormat(applicationDateString)
          if cols[1].text.lstrip().rstrip() != '':
            headOfHousehold=cols[1].text.lstrip().rstrip()
            caste=cols[2].text.lstrip().rstrip()
            applicantNo=1
            fatherHusbandName=cols[5].text.lstrip().rstrip()
            jobcardArray=[jobcard,headOfHousehold,str(issueDate),caste,str(getjcNumber(jobcard))]
            a=locationArray+jobcardArray
            jobcardCsvArray.append(a)
          else:
            applicantNo=applicantNo+1

          if '*' in name:
            isDeleted=True
          if disabledString == 'Y':
            isDisabled=True
          if minorityString == 'Y':
            isMinority=True
          workerArray=[str(applicantNo),name,str(age),gender,fatherHusbandName,str(isDeleted),str(isMinority),str(isDisabled)]
          a=locationArray+jobcardArray+workerArray
          csvArray.append(a)
    df = pd.DataFrame(csvArray, columns =header)
    url=saveReport(logger,ldict,reportType,finyear,df)
    logger.info(url)
   #filename="%s%s_%s.csv" % (filepath,reportType,slugify(panchayatName))
   #response=uploadS3(logger,filename,df=df)
    reportType="jobcardRegister"
    df = pd.DataFrame(jobcardCsvArray, columns =jobcardheader)
    url=saveReport(logger,ldict,reportType,finyear,df)
    logger.info(url)
    return url
def updateJobcardDF(logger,locationCode,rejDF=None,jcDF=None,startFinYear=None,jobcard=None):
  if ( (jcDF is not None) and (rejDF is not None)):
    logger.info(f"Shape of JC DF is {jcDF.shape}")
    if jcDF is not None:
      for index, row in jcDF.iterrows():
        jobcard=row['jobcard']
        name=row['name']
        workerCode="%s_%s" % (jobcard,name)
        musterNo=row['musterNo']
        matchedDF=rejDF.loc[(rejDF['workerCode'] == workerCode) & (rejDF['musterNo'] == musterNo)]
        matchedRows=len(matchedDF)
        rejWagelists=''
        rejFTOs=''
        rejReasons=''
        rejProcessDate=''
        rejStatus=''
        rejCount=0
        if matchedRows > 0:
          matchedDF.to_csv(f"/tmp/{musterNo}.csv")
          matchedDF = matchedDF.replace(np.nan, '', regex=True)
          for i, row in matchedDF.iterrows():
            rejWagelists+=f"{row['wagelistNo']} |"
            rejFTOs+=f"{row['ftoNo']} |"
            rejReasons+=f"{row['rejectionReason']} |"
            rejProcessDate+=f"{row['processDate']} |"
            rejStatus+=f"{row['status']} |"
            if row['status'] == "Rejected":
              rejCount=rejCount+1
          rejReasons=rejReasons.rstrip("|")
          rejWagelists=rejWagelists.rstrip("|")
          rejFTOs=rejFTOs.rstrip("|")
          rejProcessDate=rejProcessDate.rstrip("|")
          rejStatus=rejStatus.rstrip("|")
        jcDF.at[index, 'rejCount'] = rejCount
        jcDF.at[index, 'rejWagelists'] = rejWagelists
        jcDF.at[index, 'rejFTOs'] = rejFTOs
        jcDF.at[index, 'rejProcessDate'] = rejProcessDate
        jcDF.at[index, 'rejStatus'] = rejStatus
        jcDF.at[index, 'rejReasons'] = rejReasons
  else:
    jcDF=None

  return jcDF 

def updateJobcardDF1(logger,locationCode,rejDF=None,startFinYear=None,jobcard=None):
#  jobcard='RJ-272100309802522500/263'
  jobcardArray=[]
  if jobcard is None:
    reportType="jobcardRegister"
    jobcardDF=getReportDF(logger,locationCode=locationCode,reportType=reportType) 
    for index,row in jobcardDF.iterrows():
      jobcard=row['jobcard']
      jobcardArray.append(jobcard)
  else:
    jobcardArray.append(jobcard)
  logger.info(locationCode)
  ldict=getLocationDict(logger,locationCode=locationCode)
  filepath=getFilePath(logger,ldict)
  blockCode=ldict.get("blockCode",None)
  if startFinYear is None:
    startFinYear=getDefaultStartFinYear()
  endFinYear=getCurrentFinYear()

  if rejDF is None:
    rejDFs=[]
    for finyear in range(int(startFinYear),int(endFinYear)+1):
      finyear=str(finyear)
      reportType="blockRejectedTransactions"
      df=getReportDF(logger,locationCode=blockCode,reportType=reportType,finyear=finyear,index_col=0) 
      if df is not None:
        logger.info(f"DF shape {df.shape} for finyear {finyear}")
        rejDFs.append(df)
    rejDF=pd.concat(rejDFs,ignore_index=True)
    rejDF.to_csv("/tmp/rejDF.csv")
    logger.info(f"Saape of combined DF is {rejDF.shape}")
    rejDF=rejDF.drop_duplicates()
    logger.info(f"Shape after removing duplicates of combined DF is {rejDF.shape}")
#  if jobcard is not None:
  for jobcard in jobcardArray:
    filename="%sDATA/jobcards/%s.csv" % (filepath,slugify(jobcard))
    logger.info(filename) 
    jcDF=getReportDF(logger,filename=filename)
    logger.info(f"Shape of JC DF is {jcDF.shape}")
    if jcDF is not None:
      for index, row in jcDF.iterrows():
        jobcard=row['jobcard']
        name=row['name']
        workerCode="%s_%s" % (jobcard,name)
        musterNo=row['musterNo']
        matchedDF=rejDF.loc[(rejDF['workerCode'] == workerCode) & (rejDF['musterNo'] == musterNo)]
        matchedRows=len(matchedDF)
        rejWagelists=''
        rejFTOs=''
        rejReasons=''
        rejProcessDate=''
        rejStatus=''
        rejCount=0
        if matchedRows > 0:
          matchedDF.to_csv(f"/tmp/{musterNo}.csv")
          matchedDF = matchedDF.replace(np.nan, '', regex=True)
          for i, row in matchedDF.iterrows():
            rejWagelists+=f"{row['wagelistNo']} |"
            rejFTOs+=f"{row['ftoNo']} |"
            rejReasons+=f"{row['rejectionReason']} |"
            rejProcessDate+=f"{row['processDate']} |"
            rejStatus+=f"{row['status']} |"
            if row['status'] == "Rejected":
              rejCount=rejCount+1
          rejReasons=rejReasons.rstrip("|")
          rejWagelists=rejWagelists.rstrip("|")
          rejFTOs=rejFTOs.rstrip("|")
          rejProcessDate=rejProcessDate.rstrip("|")
          rejStatus=rejStatus.rstrip("|")
        jcDF.at[index, 'rejCount'] = rejCount
        jcDF.at[index, 'rejWagelists'] = rejWagelists
        jcDF.at[index, 'rejFTOs'] = rejFTOs
        jcDF.at[index, 'rejProcessDate'] = rejProcessDate
        jcDF.at[index, 'rejStatus'] = rejStatus
        jcDF.at[index, 'rejReasons'] = rejReasons
    return jcDF 
   #fileURL=uploadS3(logger,filename,df=jcDF)
   #logger.info(fileURL)

def processJobcard(logger,ldict,jobcard,myhtml,jdict=None,startFinYear=None):
  error=None
  df=None
  endFinYear=getCurrentFinYear()
  if startFinYear is None:
    startFinYear = str ( int(endFinYear) -1 )
  workerArrayLabel=["jobcard","applicantNo","name","gender","age","accountNo","finAgency","aadharNo"]
  txnArrayLabel=["workerCode","finyear","workName","workDate","daysAllocated","amountDue","musterNo","musterURL"]
  wpArrayLabel=workerArrayLabel+txnArrayLabel
  wpArray=[]
  blankWorkerArray=[jobcard,"","","","","","",""]

  htmlsoup=BeautifulSoup(myhtml,"lxml")
  workerTable=htmlsoup.find("table",id="workerTable")
  workerDict={}
  workerArray=[]
  if workerTable is not None:
    rows=workerTable.findAll("tr")
    for row in rows:
      cols=row.findAll("td")
      applicantNo=cols[0].text.lstrip().rstrip()
      if applicantNo.isdigit():
        name=cols[1].text.lstrip().rstrip()
        workerCode="%s_%s" % (jobcard,name)
        gender=cols[2].text.lstrip().rstrip()
        age=cols[3].text.lstrip().rstrip()
        accountNo=cols[4].text.lstrip().rstrip()
        bankPost=cols[5].text.lstrip().rstrip()
        aadharNo=cols[6].text.lstrip().rstrip()
        a=[jobcard,applicantNo,name,gender,age,accountNo,bankPost,aadharNo]
        workerDict[workerCode]=a
  workTable=htmlsoup.find("table",id="workTable")
  lastWorkDateDict={}
  defaultLastWorkDate=getDateObj("01/01/1970")
  if workTable is not None:
    rows=workTable.findAll('tr')
    previousName=None
    for row in rows:
      if "Date from which Employment Availed" not in str(row):
        cols=row.findAll('td')
        name=cols[1].text.lstrip().rstrip()
        if name == "":
          name=previousName
        else:
          previousName=name
        srno=cols[0].text.lstrip().rstrip()
        #logger.info("processing %s %s" % (srno,name))
        workDateString=cols[2].text.lstrip().rstrip()
        workDate=getDateObj(workDateString)
        workDateMinusFour=workDate-datetime.timedelta(days=4)
        daysAllocated=cols[3].text.lstrip().rstrip()
        workName=cols[4].text.lstrip().rstrip()
        amountDue=cols[6].text.lstrip().rstrip()
        finyear=str(getFinYear(dateObj=workDate))
        musterLink=cols[5].find('a')
        if musterLink is not None:
          musterURL=musterLink['href']
        else:
          musterURL=None
        musterNo=cols[5].text.lstrip().rstrip()
        #logger.info(finyear)
         
        if int(finyear) >= int(startFinYear):
          workerCode="%s_%s" % (jobcard,name)
          txnArray=[workerCode,finyear,workName,workDate,daysAllocated,amountDue,musterNo,musterURL]
          workerArray=workerDict.get(workerCode,blankWorkerArray)
          a=workerArray+txnArray
          wpArray.append(a)
  return wpArray


def fetchJobcardDetails(logger,funcArgs,threadName='default'):
  startFinYear=funcArgs[0]
  ldict=funcArgs[1]
  jobcard=funcArgs[2]
  error,outhtml=downloadJobcard(logger,ldict,jobcard)
  jcArray=[]
  if error is not None:
    return jcArray 
  jcArray= processJobcard(logger,ldict,jobcard,outhtml,startFinYear=startFinYear)
  return jcArray
def getJobcardDetails(logger,funcArgs,threadName="default"):
  startFinYear=funcArgs[0]
  ldict=funcArgs[1]
  jdict=funcArgs[2]
  rejDF=funcArgs[3]
  locationCode=ldict.get("code",None)
  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  panchayatCode=ldict.get("panchayatCode",None)
  stateName=ldict.get("stateName",None)
  ldict=funcArgs[1]
  jdict=funcArgs[2]
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  panchayatName=ldict.get("panchayatName",None)
  #logger.warning(jdict)
  jobcard=jdict.get("jobcard",None)
  village=jdict.get("village",None)
  caste=jdict.get("caste",None)
  headOfHousehold=jdict.get("headOfHousehold",None)
  jcNo=jdict.get("jcNo",None)
  #This will get all the jobcard Details
  error=None
  df=None
  #logger.debug("This loop will get all the Jobcard Details")
  error,outhtml=downloadJobcard(logger,ldict,jobcard)
  if error is not None:
    return error,df
  error,df= processJobcard(logger,ldict,jobcard,outhtml,jdict=jdict,startFinYear=startFinYear)
  if df is not None:
   if rejDF is not None:
     df=updateJobcardDF(logger,locationCode,rejDF=rejDF,jcDF=df)
   filepath=getFilePath(logger,ldict)
   filename="%sDATA/jobcards/%s.csv" % (filepath,slugify(jobcard))
   fileURL=uploadS3(logger,filename,df=df)
   logger.warning(f"Jobcard Details {jobcard} -{fileURL}") 
   return error,df

def processJobcard1(logger,ldict,jobcard,myhtml,jdict=None,startFinYear=None):
  wpDF=None
  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  panchayatCode=ldict.get("panchayatCode",None)
  stateName=ldict.get("stateName",None)
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  panchayatName=ldict.get("panchayatName",None)
  locationArrayLabel=["state","district","block","panchayat","stateCode","districtCode","blockCode","panchayatCode"]
  locationArray=[stateName,districtName,blockName,panchayatName,stateCode,districtCode,blockCode,panchayatCode]
  jobcardArrayLabel=["village","jobcard","jcNo","headOfHousehold","caste"]
  workerArrayLabel=["applicantNo","name","gender","age","accountNo","finAgency","aadharNo"]
  txnArrayLabel=['finyear','musterNo','workCode','workName','dateFrom','dateTo','paymentDate','musterIndex','m_accountNo','m_bankName','m_branchName','m_branchCode','dayWage','daysProvided','daysWorked','totalWage','creditedDate']
  wpArrayLabel=locationArrayLabel+jobcardArrayLabel+workerArrayLabel+txnArrayLabel
  wpArray=[]
  if jdict is not None:
    jobcard=jdict.get("jobcard",None)
    village=jdict.get("village",None)
    caste=jdict.get("caste",None)
    headOfHousehold=jdict.get("headOfHousehold",None)
    jcNo=jdict.get("jcNo",None)
    jobcardArray=[village,jobcard,jcNo,headOfHousehold,caste]
  else:
    jobcardArray=[""]*5

  blankWorkerArray=[""]*7

  crawlIP=ldict.get("crawlIP",None)
  panchayatCode=ldict.get("panchayatCode",None)
  jobcardURL="http://%s/citizen_html/jcr.asp?reg_no=%s&panchayat_code=%s" % (NICSearchIP,jobcard,panchayatCode)
  error=None
  df=None
  endFinYear=getCurrentFinYear()
  if startFinYear is None:
    startFinYear = str ( int(endFinYear) -1 )
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  workerTable=htmlsoup.find("table",id="workerTable")
  workerDict={}
  workerArray=[]
  if workerTable is not None:
    rows=workerTable.findAll("tr")
    for row in rows:
      cols=row.findAll("td")
      applicantNo=cols[0].text.lstrip().rstrip()
      if applicantNo.isdigit():
        name=cols[1].text.lstrip().rstrip()
        workerCode="%s_%s" % (jobcard,name)
        gender=cols[2].text.lstrip().rstrip()
        age=cols[3].text.lstrip().rstrip()
        accountNo=cols[4].text.lstrip().rstrip()
        bankPost=cols[5].text.lstrip().rstrip()
        aadharNo=cols[6].text.lstrip().rstrip()
        a=[applicantNo,name,gender,age,accountNo,bankPost,aadharNo]
        workerDict[workerCode]=a
  workTable=htmlsoup.find("table",id="workTable")
  lastWorkDateDict={}
  defaultLastWorkDate=getDateObj("01/01/1970")
  if workTable is not None:
    rows=workTable.findAll('tr')
    previousName=None
    for row in rows:
      if "Date from which Employment Availed" not in str(row):
        cols=row.findAll('td')
        name=cols[1].text.lstrip().rstrip()
        if name == "":
          name=previousName
        else:
          previousName=name
        srno=cols[0].text.lstrip().rstrip()
        #logger.info("processing %s %s" % (srno,name))
        workDateString=cols[2].text.lstrip().rstrip()
        workDate=getDateObj(workDateString)
        workDateMinusFour=workDate-datetime.timedelta(days=4)
        daysAllocated=cols[3].text.lstrip().rstrip()
        workName=cols[4].text.lstrip().rstrip()
        amountDue=cols[6].text.lstrip().rstrip()
        finyear=str(getFinYear(dateObj=workDate))
        #logger.info(finyear)
         
        if int(finyear) >= int(startFinYear):
          dwd=None
          myMuster=None
          code="%s_%s" % (name,workDateString)
          workerCode="%s_%s" % (jobcard,name)
          workerArray=workerDict.get(workerCode,blankWorkerArray)
          musterLink=cols[5].find('a')
          if musterLink is not None:
            musterURL=musterLink['href']
            #logger.debug(f"Muster URL is {musterURL}")
            error,txnDict=getWorkerTransaction(logger,ldict,musterURL,workerCode)
            if error is None:
              txnArray=[finyear,txnDict['musterNo'],txnDict['workCode'],txnDict['workName'],txnDict['dateFrom'],txnDict['dateTo'],txnDict['paymentDate'],txnDict['musterIndex'],txnDict['m_accountNo'],txnDict['m_bankName'],txnDict['m_branchName'],txnDict['m_branchCode'],txnDict['dayWage'],txnDict['daysProvided'],txnDict['daysWorked'],txnDict['totalWage'],txnDict['creditedDate']]
              a=locationArray+jobcardArray+workerArray+txnArray
              wpArray.append(a)
            else:
              logger.error(f"Muster download failed with error {error} musterlink {musterURL} jobcardURL {jobcardURL}")
          else:
            logger.error(f"Muster link is None, sr no {srno} workDate {workDateString} jobcardURL {jobcardURL}")
           #musterHTML=downloadMuster(logger,ldict,musterURL)
           #with open("/tmp/m.html","wb") as f:
           #  f.write(musterHTML)
           #df=processMuster(logger,ldict,musterHTML)
  if len(wpArray) == 0:
    wpDF=None
  else:
    wpDF = pd.DataFrame(wpArray, columns =wpArrayLabel)
  return error,wpDF


def validateJobcardHTML(logger,jobcard,myhtml):
  error=None
  demandTable=None
  jobcardTable=None
  workTable=None
  workerTable=None
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  tables=htmlsoup.findAll("table")
  for table in tables:
    if "Date from which employment requested" in str(table):
      demandTable=table
    elif "Aadhar No" in str(table):
      workerTable=table
    elif "Date from which Employment Availed" in str(table):
      workTable=table
    elif jobcard in str(table):
      jobcardTable=table
  if jobcardTable is None:
    error="Jobcard Table not found"
  if workerTable is None:
    error="Worker Table not found"
  elif demandTable is None:
    error="demandTable not found"
  elif workTable is None:
    error="workTable not found"
  return error,jobcardTable,workerTable,demandTable,workTable


def downloadJobcard(logger,ldict,jobcard):
  error=None
  outhtml=None
  panchayatCode=ldict.get("panchayatCode",None)
  crawlIP=ldict.get("crawlIP",None)
  url="http://%s/citizen_html/jcr.asp?reg_no=%s&panchayat_code=%s" % (NICSearchIP,jobcard,panchayatCode)
  #logger.debug(f"Jobcard Download URL is {url}")
  r=requests.get(url)
  if r.status_code != 200:
    error=f"Unable to download Jobcard {jobcard} status code {r.status_code}"
    return error,outhtml
  myhtml=r.content
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  error,jobcardTable,workerTable,demandTable,workTable=validateJobcardHTML(logger,jobcard,myhtml)
  if error is not None:
    return error,outhtml
  outhtml=''
  outhtml+=getCenterAlignedHeading("Jobcard Summary Table")
  outhtml+=stripTableAttributes(jobcardTable,"jobcardTable")
  outhtml+=getCenterAlignedHeading("Worker Summary Table")
  outhtml+=stripTableAttributes(workerTable,"workerTable")
  outhtml+=getCenterAlignedHeading("Demand Details")
  outhtml+=stripTableAttributes(demandTable,"demandTable")
  outhtml+=getCenterAlignedHeading("Work Details")
  baseURL="http://%s/placeHodler1/placeHolder2/" % (crawlIP)  
  outhtml+=stripTableAttributesPreserveLinks(workTable,"workTable",baseURL)
  title=''
  outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
 #try:
 #  outhtml=outhtml.encode("UTF-8")
 #except:
 #  outhtml=outhtml
  return error,outhtml

def getWorkerTransaction(logger,ldict,musterURL,workerCode,forceDownload=None):
  musterDF=None
  error=None
  txnDict=None
  downloadedNow=False
  parsedURL=parse.urlsplit(musterURL)
  queryDict=dict(parse.parse_qsl(parsedURL.query))
  musterNo=queryDict.get('msrno',None)
  fullFinYear=queryDict.get('finyear',None)
  filepath=getFilePath(logger,ldict,locationType='block')
  musterFileName="%sDATA/musters/%s/%s.csv" % (filepath,fullFinYear,musterNo)
  awsMusterURL=getAWSFileURL(musterFileName)
  daysDiff=daysSinceModifiedS3(logger,musterFileName)
  if daysDiff is not None:
    musterDF=pd.read_csv(awsMusterURL)
    isBlank=(musterDF['creditedDate'] == "").any()
    isNone=musterDF['creditedDate'].isnull().any()
    if ((isBlank or isNone) and (daysDiff >= musterReDownloadThreshold)):
      reDownload=True
    else:
      reDownload=False
  else:
    reDownload =True
  if ((reDownload == True) or ( forceDownload==True)):
    error=downloadAndSaveMuster(logger,ldict,musterURL)
    if error is None:
      musterDF=pd.read_csv(awsMusterURL)
  if musterDF is None:
    error=f"Coulnd now Download Muster {musterURL}"
  else:
    partDF=musterDF.loc[musterDF['workerCode'] == workerCode]
    noOfRows=partDF.shape[0]
    if noOfRows != 1:
      error=f"worker Code {workerCode} and muster {musterURL} have either 0 or more than one match"
    else:
      txnDict=partDF.to_dict('records')[0]

  return error,txnDict
def getMusterDF(logger,funcArgs,threadName="default"):
  ldict=funcArgs[0]
  musterURL=funcArgs[1]
  infinyear=funcArgs[2]
  newMusterFormat=True
  error=None
  df=None
  csvArray=[]
  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  panchayatCode=ldict.get("panchayatCode",None)
  stateName=ldict.get("stateName",None)
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  panchayatName=ldict.get("panchayatName",None)
  locationArrayLabel=["state","district","block","panchayat","village","stateCode","districtCode","blockCode","panchayatCode","musterPanchayatName","musterPanchayatCode","localWorkSite"]
  musterArrayLabel=["finyear","m_finyear","musterNo","workCode","workName","dateFrom","dateTo","paymentDate"] 
  detailArrayLabel=["musterIndex","workerCode","jobcard","name","m_accountNo","m_bankName","m_branchName","m_branchCode","dayWage","daysProvided","daysWorked","totalWage","wagelistNo","creditedDate"]
  columnLabels=locationArrayLabel+musterArrayLabel+detailArrayLabel
  locationArray=[stateName,districtName,blockName,panchayatName,"",stateCode,districtCode,blockCode,panchayatCode]

  parsedURL=parse.urlsplit(musterURL)
  queryDict=dict(parse.parse_qsl(parsedURL.query))
  musterNo=queryDict.get('msrno',None)
  fullFinYear=queryDict.get('finyear',None)
  finyear=fullFinYear[-2:] 
  dateFromString=queryDict.get('dtfrm',None)
  dateFrom=getDateObj(dateFromString)
  dateToString=queryDict.get('dtto',None)
  dateTo=getDateObj(dateToString)
  workCode=queryDict.get('workcode',None)
  workName=queryDict.get('wn',None)
  musterPanchayatCode=queryDict.get('panchayat_code',None)
  if musterPanchayatCode == panchayatCode:
    localWorkSite=True
  else:
    localWorkSite=False
  if musterPanchayatCode is not None:
    mlDict=getLocationDict(logger,locationCode=musterPanchayatCode)
    musterPanchayatName=mlDict.get("panchayatName",None)
  locationArray=[stateName,districtName,blockName,panchayatName,"",stateCode,districtCode,blockCode,panchayatCode,musterPanchayatName,musterPanchayatCode,localWorkSite]
  musterHTML=fetchNewMuster(logger,ldict,musterURL,finyear,musterNo,workCode)
  if musterHTML is None:
    mdict={
        'musterNo' : musterNo,
        'finyear' : finyear,
        'workCode': workCode,
            }
    musterHTML=fetchOldMuster(logger,ldict,mdict)
    if musterHTML is None:
      error=f"Count not download muster with URL {musterURL}"
      return csvArray
    else:
      newMusterFormat=False
  error,musterSummaryTable,musterTable=validateMusterHTML(logger,ldict,musterHTML,workCode)
  if error is not None:
    return csvArray 
  title="Muster Details" 
  myhtml=''
  myhtml+=getCenterAlignedHeading("Muster Summary Table")
  myhtml+=stripTableAttributes(musterSummaryTable,"musterSummary")
  myhtml+=getCenterAlignedHeading("Muster Detail Table")
  myhtml+=stripTableAttributes(musterTable,"musterDetails")
  myhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=myhtml)

  stateShortCode=ldict.get("stateShortCode",None)
  musterStartAttendanceColumn=4
  musterEndAttendanceColumn=19
  isComplete=False
  remarks=''
  allWorkerFound=True
  allWagelistFound=True
  allWDFound=True
  isComplete=True

  htmlsoup=BeautifulSoup(myhtml,"lxml")
  ptds=htmlsoup.find_all("td", text=re.compile("Payment Date"))
  paymentDate=None
  if len(ptds) == 1:
    ptdText=ptds[0].text
    paymentDateString=ptdText.split(":")[1].lstrip().rstrip()
    paymentDate=getDateObj(paymentDateString)
  musterArray=[infinyear,finyear,musterNo,workCode,workName,dateFrom,dateTo,paymentDate] 
  mytable=htmlsoup.find('table',id="musterDetails")
  rows  = mytable.findAll('tr')
  sharpeningIndex=None
  if newMusterFormat == False:
    sharpeningIndex=getSharpeningIndex(logger,rows[0])
    if sharpeningIndex is None:
      error="Sharpening Index not Found"
    musterEndAttendanceColumn=sharpeningIndex-5

  
  reMatchString="%s-" % (stateShortCode)
  for row in rows:
    wdRemarks=''
    cols=row.findAll("td")
      
    if len(cols) > 7:
      nameandjobcard=cols[1].text.lstrip().rstrip()
      if stateShortCode in nameandjobcard:
        musterIndex=cols[0].text.lstrip().rstrip()
        nameandjobcard=nameandjobcard.replace('\n',' ')
        nameandjobcard=nameandjobcard.replace("\\","")
        nameandjobcardarray=re.match(r'(.*)'+reMatchString+'(.*)',nameandjobcard)
        name_relationship=nameandjobcardarray.groups()[0]
        name=name_relationship.split("(")[0].lstrip().rstrip()
        jobcard=reMatchString+nameandjobcardarray.groups()[1].lstrip().rstrip()
        if newMusterFormat==True:
          totalWage=cols[24].text.lstrip().rstrip()
          accountNo=cols[25].text.lstrip().rstrip()
          dayWage=cols[21].text.lstrip().rstrip()
          daysWorked=cols[20].text.lstrip().rstrip()
          wagelistNo=cols[29].text.lstrip().rstrip()
          bankName=cols[26].text.lstrip().rstrip()
          branchName=cols[27].text.lstrip().rstrip()
          branchCode=cols[28].text.lstrip().rstrip()
          creditedDateString=cols[30].text.lstrip().rstrip()
        else:
          totalWage=cols[sharpeningIndex+1].text.lstrip().rstrip()
          accountNo=""
          dayWage=cols[sharpeningIndex-3].text.lstrip().rstrip()
          daysWorked=cols[sharpeningIndex-4].text.lstrip().rstrip()
          wagelistNo=cols[sharpeningIndex+5].text.lstrip().rstrip()
          bankName=cols[sharpeningIndex+2].text.lstrip().rstrip()
          branchName=cols[sharpeningIndex+3].text.lstrip().rstrip()
          branchCode=cols[sharpeningIndex+4].text.lstrip().rstrip()
          creditedDateString=cols[sharpeningIndex+7].text.lstrip().rstrip()


        creditedDate=getDateObj(creditedDateString)

        daysProvided=0
        for attendanceIndex in range(int(musterStartAttendanceColumn),int(musterEndAttendanceColumn)+1):
          if cols[attendanceIndex].text.lstrip().rstrip() != "":
            daysProvided=daysProvided+1

        workerCode="%s_%s" % (jobcard,name)
        detail=[musterIndex,workerCode,jobcard,name,accountNo,bankName,branchName,branchCode,dayWage,daysProvided,daysWorked,totalWage,wagelistNo,creditedDate]
        a=locationArray+musterArray+detail
        csvArray.append(a)
  return csvArray

def downloadAndSaveMuster(logger,ldict,musterURL):
  newMusterFormat=True
  error=None
  df=None
  csvArray=[]
  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  panchayatCode=ldict.get("panchayatCode",None)
  stateName=ldict.get("stateName",None)
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  panchayatName=ldict.get("panchayatName",None)
  locationArrayLabel=["state","district","block","panchayat","village","stateCode","districtCode","blockCode","panchayatCode"]
  musterArrayLabel=["m_finyear","musterNo","workCode","workName","dateFrom","dateTo","paymentDate"] 
  detailArrayLabel=["musterIndex","workerCode","jobcard","name","m_accountNo","m_bankName","m_branchName","m_branchCode","dayWage","daysProvided","daysWorked","totalWage","creditedDate"]
  columnLabels=locationArrayLabel+musterArrayLabel+detailArrayLabel
  locationArray=[stateName,districtName,blockName,panchayatName,"",stateCode,districtCode,blockCode,panchayatCode]

  parsedURL=parse.urlsplit(musterURL)
  queryDict=dict(parse.parse_qsl(parsedURL.query))
  musterNo=queryDict.get('msrno',None)
  fullFinYear=queryDict.get('finyear',None)
  finyear=fullFinYear[-2:] 
  dateFromString=queryDict.get('dtfrm',None)
  dateFrom=getDateObj(dateFromString)
  dateToString=queryDict.get('dtto',None)
  dateTo=getDateObj(dateToString)
  workCode=queryDict.get('workcode',None)
  workName=queryDict.get('wn',None)
  musterPanchayatCode=queryDict.get('panchayat_code',None)
  filepath=getFilePath(logger,ldict,locationType='block')
  musterFileName="%sDATA/musters/%s/%s.csv" % (filepath,fullFinYear,musterNo)
  awsMusterURL=getAWSFileURL(musterFileName)
  musterHTML=fetchNewMuster(logger,ldict,musterURL,finyear,musterNo,workCode)
  if musterHTML is None:
    mdict={
        'musterNo' : musterNo,
        'finyear' : finyear,
        'workCode': workCode,
            }
    musterHTML=fetchOldMuster(logger,ldict,mdict)
    if musterHTML is None:
      error=f"Count not download muster with URL {musterURL}"
      return error
    else:
      newMusterFormat=False
  error,musterSummaryTable,musterTable=validateMusterHTML(logger,ldict,musterHTML,workCode)
  if error is not None:
    return error,df,downloadedNow
  title="Muster Details" 
  myhtml=''
  myhtml+=getCenterAlignedHeading("Muster Summary Table")
  myhtml+=stripTableAttributes(musterSummaryTable,"musterSummary")
  myhtml+=getCenterAlignedHeading("Muster Detail Table")
  myhtml+=stripTableAttributes(musterTable,"musterDetails")
  myhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=myhtml)

  stateShortCode=ldict.get("stateShortCode",None)
  musterStartAttendanceColumn=4
  musterEndAttendanceColumn=19
  isComplete=False
  remarks=''
  finyear=''
  allWorkerFound=True
  allWagelistFound=True
  allWDFound=True
  isComplete=True

  htmlsoup=BeautifulSoup(myhtml,"lxml")
  ptds=htmlsoup.find_all("td", text=re.compile("Payment Date"))
  paymentDate=None
  if len(ptds) == 1:
    ptdText=ptds[0].text
    paymentDateString=ptdText.split(":")[1].lstrip().rstrip()
    paymentDate=getDateObj(paymentDateString)
  musterArray=[finyear,musterNo,workCode,workName,dateFrom,dateTo,paymentDate] 
  mytable=htmlsoup.find('table',id="musterDetails")
  rows  = mytable.findAll('tr')
  sharpeningIndex=None
  if newMusterFormat == False:
    sharpeningIndex=getSharpeningIndex(logger,rows[0])
    if sharpeningIndex is None:
      error="Sharpening Index not Found"
    musterEndAttendanceColumn=sharpeningIndex-5

  
  reMatchString="%s-" % (stateShortCode)
  for row in rows:
    wdRemarks=''
    cols=row.findAll("td")
      
    if len(cols) > 7:
      nameandjobcard=cols[1].text.lstrip().rstrip()
      if stateShortCode in nameandjobcard:
        musterIndex=cols[0].text.lstrip().rstrip()
        nameandjobcard=nameandjobcard.replace('\n',' ')
        nameandjobcard=nameandjobcard.replace("\\","")
        nameandjobcardarray=re.match(r'(.*)'+reMatchString+'(.*)',nameandjobcard)
        name_relationship=nameandjobcardarray.groups()[0]
        name=name_relationship.split("(")[0].lstrip().rstrip()
        jobcard=reMatchString+nameandjobcardarray.groups()[1].lstrip().rstrip()
        if newMusterFormat==True:
          totalWage=cols[24].text.lstrip().rstrip()
          accountNo=cols[25].text.lstrip().rstrip()
          dayWage=cols[21].text.lstrip().rstrip()
          daysWorked=cols[20].text.lstrip().rstrip()
          wagelistNo=cols[29].text.lstrip().rstrip()
          bankName=cols[26].text.lstrip().rstrip()
          branchName=cols[27].text.lstrip().rstrip()
          branchCode=cols[28].text.lstrip().rstrip()
          creditedDateString=cols[30].text.lstrip().rstrip()
        else:
          totalWage=cols[sharpeningIndex+1].text.lstrip().rstrip()
          accountNo=""
          dayWage=cols[sharpeningIndex-3].text.lstrip().rstrip()
          daysWorked=cols[sharpeningIndex-4].text.lstrip().rstrip()
          wagelistNo=cols[sharpeningIndex+5].text.lstrip().rstrip()
          bankName=cols[sharpeningIndex+2].text.lstrip().rstrip()
          branchName=cols[sharpeningIndex+3].text.lstrip().rstrip()
          branchCode=cols[sharpeningIndex+4].text.lstrip().rstrip()
          creditedDateString=cols[sharpeningIndex+7].text.lstrip().rstrip()


        creditedDate=getDateObj(creditedDateString)

        daysProvided=0
        for attendanceIndex in range(int(musterStartAttendanceColumn),int(musterEndAttendanceColumn)+1):
          if cols[attendanceIndex].text.lstrip().rstrip() != "":
            daysProvided=daysProvided+1

        workerCode="%s_%s" % (jobcard,name)
        detail=[musterIndex,workerCode,jobcard,name,accountNo,bankName,branchName,branchCode,dayWage,daysProvided,daysWorked,totalWage,wagelistNo,creditedDate]
        a=locationArray+musterArray+detail
        csvArray.append(a)
  return csvArray
  df = pd.DataFrame(csvArray, columns =columnLabels)
  logger.info(musterFileName)
  musterCSV=uploadS3(logger,musterFileName,df=df)
  logger.info(musterCSV)
  return error
def validateMusterHTML(logger,ldict,myhtml,workCode):
  error=None
  stateShortCode=ldict.get("stateShortCode",None)
  musterSummaryTable=None
  musterTable=None
  jobcardPrefix="%s-" % (stateShortCode)
  htmlsoup=BeautifulSoup(myhtml,"lxml")
  tables=htmlsoup.findAll('table')
  for table in tables:
    if workCode in str(table):
      musterSummaryTable=table
    elif jobcardPrefix in str(table):
      musterTable=table
  if musterSummaryTable is None:
    error="Muster Summary Table not found"
  elif musterTable is None:
    error="Muster Table not found"
  return error,musterSummaryTable,musterTable

def fetchNewMuster(logger,ldict,musterURL,finyear,musterNo,workCode):
  myhtml=None
  crawlIP=ldict.get("crawlIP",None)
  panchayatCode=ldict.get("panchayatCode",None)
  stateShortCode=ldict.get("stateShortCode",None)
  r=requests.get(musterURL)
  #logger.info(r.status_code)
  if r.status_code==200:
    cookies=r.cookies
    headers = {
    'Host': crawlIP,
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
     }
    #logger.info(pobj.panchayatCode[:7]) 
    params = (
      ('bc', panchayatCode[:7]),
      ('fy', getFullFinYear(finyear)),
      ('q', '%s----' % (musterNo)),
      ('sh', stateShortCode),
      ('t', 'D'),
      ('wc', '%s$$$' % (workCode)),
     )
    url2="http://%s/citizen_html/msrLogic.asp?q=%s----&t=D&fy=%s&bc=%s&sh=%s&wc=%s$$$&sn=&dn=&bn=&pn=" % (crawlIP,musterNo,getFullFinYear(finyear),panchayatCode[:7],stateShortCode,workCode)
    #logger.info(url2)
    #response = requests.get('http://%s/citizen_html/msrLogic.asp' % (pobj.crawlIP), headers=headers, params=params, cookies=cookies)
    response = requests.get(url2, headers=headers, cookies=cookies)
    if response.status_code == 200:
      myhtml=response.content
  return myhtml

def searchMusterURL(logger,ldict,mdict):
  musterURL=None
  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  panchayatCode=ldict.get("panchayatCode",None)
  stateName=ldict.get("stateName",None)
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  panchayatName=ldict.get("panchayatName",None)
  stateShortCode=ldict.get("stateShortCode",None)
  musterNo=mdict.get("musterNo",None)
  finyear=mdict.get("finyear",None)
  workCode=mdict.get("workCode",None)
  digest=getMusterDigest(logger,ldict,musterNo)
  fullFinYear=getFullFinYear(finyear)
  #logger.info(digest)
  searchURL="http://%s/netnrega/master_search1.aspx?flag=2&wsrch=msr&district_code=%s&state_name=%s&district_name=%s&short_name=%s&srch=%s&Digest=%s" % (NICSearchIP,districtCode,stateName,districtName,stateShortCode,musterNo,digest)
  logger.info(searchURL)
  r=requests.get(searchURL)
  if r.status_code == 200:
    shtml=r.content
    ssoup=BeautifulSoup(shtml,"lxml")
    validation = ssoup.find(id='__EVENTVALIDATION').get('value')
    viewState = ssoup.find(id='__VIEWSTATE').get('value')
    headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
}


    data = {
  '__EVENTTARGET': 'ddl_yr',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState,
  '__VIEWSTATEENCRYPTED': '',
  '__EVENTVALIDATION': validation,
  'ddl_yr': fullFinYear, 
   }   
    cookies=r.cookies
    #logger.info(cookies)
    response = requests.post(searchURL, headers=headers,cookies=cookies, data=data)
    logger.info(response.status_code)
    if response.status_code == 200:
      myhtml=response.content.decode("utf-8")
      musterregex=re.compile(r'<input+.*?"\s*/>+',re.DOTALL)
      myhtml=re.sub(musterregex,"",myhtml)
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      allLinks=htmlsoup.find_all("a", href=re.compile("musternew.aspx"))
      for a in allLinks:
        #if pobj.panchayatCode in a['href']:
        if workCode.replace("/","%2f") in a['href']:
          musterURL="http://%s/netnrega/%s" % (NICSearchIP,a['href'])
  return musterURL

def getMusterDigest(logger,ldict,musterNo):
  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  panchayatCode=ldict.get("panchayatCode",None)
  stateName=ldict.get("stateName",None)
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  panchayatName=ldict.get("panchayatName",None)
  stateShortCode=ldict.get("stateShortCode",None)
  searchURL="http://mnregaweb4.nic.in/netnrega/nregasearch1.aspx"
  myhtml,viewState,validation,cookies=nicRequest(logger,searchURL,requestType="GET")
  #logger.info(validation)
  ##logger.info(viewState)
  #logger.info(cookies)
  headers = {
    'Host': 'mnregaweb4.nic.in',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Referer': searchURL,
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
}

  data = {
  '__EVENTTARGET': 'ddl_search',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState, 
  '__VIEWSTATEENCRYPTED': '',
  '__EVENTVALIDATION': validation, 
  'ddl_search': 'MusterRoll',
  'txt_keyword2': '',
  'searchname': '',
  'hhshortname': '',
  'districtname': '',
  'statename': '',
  'lab_lang': ''
}
  myhtml,viewState,validation,cookies=nicRequest(logger,searchURL,headers=headers,cookies=cookies,data=data)
  data = {
  '__EVENTTARGET': 'ddl_state',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState, 
  '__VIEWSTATEENCRYPTED': '',
  '__EVENTVALIDATION': validation, 
  'ddl_search': 'MusterRoll',
  'ddl_state': stateCode,
  'txt_keyword2': '',
  'searchname': 'MusterRoll',
  'hhshortname': '',
  'districtname': '',
  'statename': '',
  'lab_lang': ''
}
  myhtml,viewState,validation,cookies=nicRequest(logger,searchURL,headers=headers,cookies=cookies,data=data)
  data = {
  '__EVENTTARGET': 'ddl_district',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState, 
  '__VIEWSTATEENCRYPTED': '',
  '__EVENTVALIDATION': validation, 
  'ddl_search': 'MusterRoll',
  'ddl_state': stateCode,
  'ddl_district': districtCode,
  'txt_keyword2': '',
  'searchname': 'MusterRoll',
  'hhshortname': stateShortCode,
  'districtname': '',
  'statename': stateName,
  'lab_lang': ''
}
  myhtml,viewState,validation,cookies=nicRequest(logger,searchURL,headers=headers,cookies=cookies,data=data)
  data = {
  '__EVENTTARGET': '',
  '__EVENTARGUMENT': '',
  '__LASTFOCUS': '',
  '__VIEWSTATE': viewState, 
  '__VIEWSTATEENCRYPTED': '',
  '__EVENTVALIDATION': validation, 
  'ddl_search': 'MusterRoll',
  'ddl_state': stateCode,
  'ddl_district': districtCode,
  'txt_keyword2': musterNo,
  'btn_go': 'GO',
  'searchname': 'MusterRoll',
  'hhshortname': stateShortCode,
  'districtname': districtName,
  'statename': stateName,
  'lab_lang': 'kruti dev 010'
}
  myhtml,viewState,validation,cookies=nicRequest(logger,searchURL,headers=headers,cookies=cookies,data=data)
  digest=re.search(r'Digest=(.*?)\'\,', myhtml.decode('UTF-8')).group(1)
  return digest 

def nicRequest(logger,url,cookies=None,data=None,headers=None,requestType=None):
  myhtml=None
  viewState=None
  validation=None
  rcookies=None
  if requestType == "GET":
    r=requests.get(url)
  else:
    r = requests.post(url, headers=headers, cookies=cookies, data=data)
  if r.status_code == 200:
    myhtml=r.content
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
    viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
    rcookies=r.cookies
    
  return myhtml,viewState,validation,rcookies

def fetchOldMuster(logger,ldict,mdict):
  myhtml=None
  musterURL=searchMusterURL(logger,ldict,mdict)
  #logger.info(musterURL)
  if musterURL is not None:
    r=requests.get(musterURL)
    cookies=r.cookies
    time.sleep(3)
    r=requests.get(musterURL,cookies=cookies)
    if r.status_code == 200:
      myhtml=r.content
  return myhtml

def getSharpeningIndex(logger,row):
  #logger.info(row)
  sharpeningIndex=None
  if "Sharpening Charge" in str(row):
    cols=row.findAll("th")
    i=0
    for col in cols:
      if "Sharpening Charge" in col.text:
        sharpeningIndex=i
      i=i+1 
  return sharpeningIndex

def crawlFTORejectedPayment(logger,pobj,finyear):
  urls,urlsRejected=getFTOListURLs(logger,pobj,finyear)
  logger.info(urls)
  logger.info("Printing Rejected URLs")
  logger.info(urlsRejected)
  reportType="rejectedPaymentHTML"
  reportName="Rejected Payment HTML"
  reportThreshold = datetime.datetime.now() - datetime.timedelta(days=3)
  isUpdated=isReportUpdated(logger,pobj,finyear,reportType,locationType='block',reportThreshold=reportThreshold)
  if isUpdated == False:
    jobcardPrefix="%s%s" % (pobj.stateShortCode,pobj.stateCode)
    locationName="%s-%s-%s" % (pobj.stateName,pobj.districtName,pobj.blockName)
    filename="%s_%s_%s_%s.html" % (reportType,pobj.blockSlug,pobj.blockCode,finyear)
    filepath=pobj.blockFilepath.replace("filename",filename)
    outhtml=''
    outhtml+=getCenterAlignedHeading(locationName)
    outhtml+=getCenterAlignedHeading("Financial Year: %s " % (getFullFinYear(finyear)))
    baseURL="http://%s/netnrega/FTO/" % (pobj.crawlIP)
    for url in urlsRejected:
      r=requests.get(url)
      if r.status_code == 200:
        myhtml=r.content
        error,myTable=validateNICReport(logger,pobj,myhtml,jobcardPrefix=jobcardPrefix)
        if myTable is not None:
          logger.info("Found the table")
          outhtml+=stripTableAttributesPreserveLinks(myTable,"myTable",baseURL)
    outhtml=htmlWrapperLocal(title=reportName, head='<h1 aling="center">'+reportName+'</h1>', body=outhtml)
    savePanchayatReport(logger,pobj,finyear,reportType,outhtml,filepath,locationType='block')
    processBlockRejectedPayment(logger,pobj,finyear)

  reportName="FTO List"
  reportType="ftoList"
  reportThreshold = datetime.datetime.now() - datetime.timedelta(days=3)
  isUpdated=isReportUpdated(logger,pobj,finyear,reportType,locationType='block',reportThreshold=reportThreshold)
  if isUpdated == False:
    if len(urls) == 0:
      error="No FTO URL Found for finyear %s " % (finyear)
      return error
    outhtml=""
    outhtml+="<html><body><table>"
    outhtml+="<tr><th>%s</th><th>%s</th></tr>" %("ftoNo","paymentMode")
    for url in urls:
      logger.info(url)
      r=requests.get(url)
      if r.status_code == 200:
        myhtml=r.content
        htmlsoup=BeautifulSoup(myhtml,"lxml")
        try:
            table=htmlsoup.find('table',bordercolor="black")
            rows = table.findAll('tr')
            errorflag=0
        except:
          status=0
          errorflag="Unable to find table in url %s " % (url)
        if errorflag==0:
          for tr in rows:
            cols = tr.findAll('td')
            if "FTO No" in str(tr):
              logger.debug("Found the header row")
            else:
              ftoNo=cols[1].text.lstrip().rstrip()
              ftoFound=1
              if pobj.stateShortCode in ftoNo:
                #logger.info(cols[1])
                ftoRelativeURL=cols[1].find("a")['href']
                urlPrefix="http://mnregaweb4.nic.in/netnrega/FTO/"
                ftoURL="%s%s" % (urlPrefix,ftoRelativeURL)
                #logger.info(ftoURL)
                paymentMode=cols[2].text.lstrip().rstrip()
                secondSignatoryDateString=cols[3].text.lstrip().rstrip()
                secondSignatoryDate=getDateObj(secondSignatoryDateString)
                ftoArray=ftoNo.split("_")
                firstSignatoryDateString=ftoArray[1][:6]
                firstSignatoryDate=getDateObj(firstSignatoryDateString,dateFormat='%d%m%y')
                try:
                  myFTO=FTO.objects.create(block=pobj.block,finyear=finyear,ftoNo=ftoNo)
                except:
                  myFTO=FTO.objects.filter(block=pobj.block,finyear=finyear,ftoNo=ftoNo).first()
                outhtml+="<tr><td>%s</td><td>%s</td></tr>" %(ftoNo,paymentMode)
                myFTO.secondSignatoryDate=secondSignatoryDate
                myFTO.firstSignatoryDate=firstSignatoryDate
                myFTO.ftoNo=ftoNo
                myFTO.paymentMode=paymentMode
                myFTO.ftoURL=ftoURL
                myFTO.save() 
    outhtml+="</table></body></html>"
    error=validateAndSave(logger,pobj,outhtml,reportName,reportType,finyear=finyear,locationType="block",tableIdentifier="Financial Institution",validate=False)

def getFTOListURLs(logger,pobj,finyear):
  urls=[]
  urlsRejected=[]
  mru=MISReportURL.objects.filter(state__code=pobj.stateCode,finyear=finyear).first()
  urlPrefix="http://mnregaweb4.nic.in/netnrega/FTO/"
  fullFinYear=getFullFinYear(finyear)
  if mru is not None:
    url=mru.ftoURL
    logger.info(url)
    r=requests.get(url)
    if r.status_code==200:
      s="district_code=%s" % (pobj.districtCode)
      myhtml=r.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      a=htmlsoup.find("a", href=re.compile(s))
      url="%s%s" % (urlPrefix,a['href'])
      logger.info(url)
      r=requests.get(url)
      if r.status_code==200:
        cookies=r.cookies
        bankURL=None
        postURL=None
        coBankURL=None
        bankURLRejected=None
        postURLRejected=None
        coBankURLRejected=None
        s="block_code=%s" % (pobj.blockCode)
        s="block_code=%s&fin_year=%s&typ=sec_sig" % (pobj.blockCode,fullFinYear)
        myhtml=r.content
        htmlsoup=BeautifulSoup(myhtml,"lxml")
        try:
          validation = htmlsoup.find(id='__EVENTVALIDATION').get('value',None)
        except:
          validation=''
        viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
        a=htmlsoup.find("a", href=re.compile(s))
        if a is not None:
          bankURL="%s%s" % (urlPrefix,a['href'])
          urls.append(bankURL)
        logger.info(bankURL)
        #Lets get rejected Payment URL
        sr="&block_code=%s&fin_year=%s&typ=R" % (pobj.blockCode,fullFinYear)
        a=htmlsoup.find("a", href=re.compile(sr))
        if a is not None:
          bankURLRejected="%s%s" % (urlPrefix,a['href'])
          urlsRejected.append(bankURLRejected)
        
        data = {
           '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$RBtnLstIsEfms$2',
           '__EVENTARGUMENT': '',
           '__LASTFOCUS': '',
           '__VIEWSTATE': viewState,
           '__VIEWSTATEENCRYPTED': '',
           '__EVENTVALIDATION': validation,
           'ctl00$ContentPlaceHolder1$RBtnLst': 'W',
           'ctl00$ContentPlaceHolder1$RBtnLstIsEfms': 'C',
           'ctl00$ContentPlaceHolder1$HiddenField1': ''
        }

        response = requests.post(url,cookies=cookies, data=data)
        if response.status_code==200:
          myhtml=response.content
          htmlsoup=BeautifulSoup(myhtml,"lxml")
          a=htmlsoup.find("a", href=re.compile(s))
          if a is not None:
            coBankURL="%s%s" % (urlPrefix,a['href'])
            urls.append(coBankURL)
          a=htmlsoup.find("a", href=re.compile(sr))
          if a is not None:
            coBankURLRejected="%s%s" % (urlPrefix,a['href'])
            urlsRejected.append(coBankURLRejected)
        logger.info(coBankURL)

        data = {
           '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$RBtnLstIsEfms$2',
           '__EVENTARGUMENT': '',
           '__LASTFOCUS': '',
           '__VIEWSTATE': viewState,
           '__VIEWSTATEENCRYPTED': '',
           '__EVENTVALIDATION': validation,
           'ctl00$ContentPlaceHolder1$RBtnLst': 'W',
           'ctl00$ContentPlaceHolder1$RBtnLstIsEfms': 'P',
           'ctl00$ContentPlaceHolder1$HiddenField1': ''
        }

        response = requests.post(url,cookies=cookies, data=data)
        if response.status_code==200:
          myhtml=response.content
          htmlsoup=BeautifulSoup(myhtml,"lxml")
          a=htmlsoup.find("a", href=re.compile(s))
          if a is not None:
            postURL="%s%s" % (urlPrefix,a['href'])
            urls.append(postURL)
          a=htmlsoup.find("a", href=re.compile(sr))
          if a is not None:
            postURLRejected="%s%s" % (urlPrefix,a['href'])
            urlsRejected.append(postURLRejected)
        logger.info(postURL)

  return urls,urlsRejected

def downloadBlockRejectedPayments(logger,blockCode=None,stateCode=None,limit=10000,num_threads=50,startFinYear=None,endFinYear=None):
  jobList=[]
  locationType='block'
  if startFinYear is None:
    startFinYear=getDefaultStartFinYear()
  if endFinYear is None:
    endFinYear=getCurrentFinYear()
  if blockCode is not None:
    url=f"{LOCATIONURL}?locationType={locationType}&code={blockCode}"
  elif stateCode is None:
    url=f"{LOCATIONURL}?locationType={locationType}&limit={limit}"
  else:
    url=f"{LOCATIONURL}?locationType={locationType}&stateCode={stateCode}&limit={limit}"
  logger.info(url)
  r=requests.get(url)
  if r.status_code == 200:
    data=r.json()
    results=data['results']
    for ldict in results:
      for finyear in range(int(startFinYear),int(endFinYear)+1):
        funcArgs=[ldict,finyear]
        p={}
        p['funcName']="getBlockRejectedTransactions"
        p['funcArgs']=funcArgs
        jobList.append(p)
  libtechQueueManager(logger,jobList,num_threads=num_threads)

def blockRejectedTransactions(logger,locationCode,startFinYear=None,endFinYear=None):
  if startFinYear is None:
    startFinYear=getDefaultStartFinYear()
  if endFinYear is None:
    endFinYear=getCurrentFinYear()
  jobList=[]
  ldict=getLocationDict(logger,locationCode=locationCode)
  locationType=ldict.get("locationType",None)
  if locationType != "block":
    return None
  
  for finyear in range(int(startFinYear),int(endFinYear)+1):
    finyear=str(finyear)
    funcArgs=[ldict,finyear]
    p={}
    p['funcName']="getBlockRejectedTransactions"
    p['funcArgs']=funcArgs
    jobList.append(p)
  libtechQueueManager(logger,jobList,num_threads=10)
  return '' 
def getBlockRejectedTransactions(logger,funcArgs,threadName=''):
  debug=None
  csvArray=[]
  searchString='Rejected_ref_no_detail.aspx'
  urlPrefix='http://mnregaweb4.nic.in/netnrega/FTO/Rejected_ref_no_detail.aspx?'
  ldict=funcArgs[0]
  finyear=funcArgs[1]

  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  panchayatCode=ldict.get("panchayatCode",'')
  stateName=ldict.get("stateName",None)
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  panchayatName=ldict.get("panchayatName",'')
  
  reportType="blockRejectedTransactions"
  updateStatus,reportURL=isReportUpdated(logger,reportType,blockCode,finyear=finyear)
  if updateStatus:
    return reportURL
  villageName=''
  rejDetailsLabels=['finAgency','workerCode','jobcard','name','applicantNo','workCode','workName','musterNo','wagelistNo','ftoNo','referenceNo','processDate','status','rejectionReason']
  locationArrayLabel=["state","district","block","panchayat","village","stateCode","districtCode","blockCode","panchayatCode"]
  header=locationArrayLabel+rejDetailsLabels
  locationArray=[stateName,districtName,blockName,panchayatName,villageName,stateCode,districtCode,blockCode,panchayatCode]


  toProcessDict={}
  reportType='NICRejectedTransactionsPostURL'
  reportURL=getReportURL(logger,reportType,ldict=ldict,finyear=finyear)
  if reportURL is not None:
    toProcessDict['post']=reportURL

  reportType='NICRejectedTransactionsURL'
  reportURL=getReportURL(logger,reportType,ldict=ldict,finyear=finyear)
  if reportURL is not None:
    toProcessDict['bank']=reportURL

  reportType='NICRejectedTransactionsCoBankURL'
  reportURL=getReportURL(logger,reportType,ldict=ldict,finyear=finyear)
  if reportURL is not None:
    toProcessDict['coBank']=reportURL

  #logger.info(toProcessDict)
  for finAgency,reportURL in toProcessDict.items():
    #logger.info(finAgency)
    logger.info(f"Thread-{threadName} Report URL {reportURL}")
    r=requests.get(reportURL)
    if r.status_code == 200:
      myhtml=r.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      links=htmlsoup.findAll("a")
      for link in links:
        href=link['href']
        if (searchString in href):
          parsedURL=parse.urlsplit(href)
          queryDict=dict(parse.parse_qsl(parsedURL.query, keep_blank_values=True))
          urlParams=parse.urlencode(queryDict)
          transactionURL=urlPrefix+urlParams
          logger.info(f" Thread {threadName} Transaction URL is {transactionURL}")
          r1=requests.get(transactionURL)
          if r1.status_code == 200:
            txnhtml=r1.content
            soup=BeautifulSoup(txnhtml,"lxml")
            tables=soup.findAll("table")
            myTable=None
            for table in tables:
              if "Reference No" in str(table):
                myTable=table
            if myTable is not None:
              rows=myTable.findAll('tr')
              for row in rows:
                cols=row.findAll('td')
                if len(cols) > 0:
                  wagelistNo=cols[0].text.lstrip().rstrip()
                  jobcard=cols[1].text.lstrip().rstrip()
                  applicantNo=cols[2].text.lstrip().rstrip()
                  name=cols[3].text.lstrip().rstrip()
                  workCode=cols[4].text.lstrip().rstrip()
                  workName=cols[5].text.lstrip().rstrip()
                  musterNo=cols[6].text.lstrip().rstrip()
                  referenceNo=cols[7].text.lstrip().rstrip()
                  status=cols[8].text.lstrip().rstrip()
                  rejectionReason=cols[9].text.lstrip().rstrip()
                  processDateString=cols[10].text.lstrip().rstrip()
                  processDate=getDateObj(processDateString)
                  ftoNo=cols[11].text.lstrip().rstrip()
                  srNo=cols[12].text.lstrip().rstrip()
                  workerCode="%s_%s" % (jobcard,name)
                  rejArray=[finAgency,workerCode,jobcard,name,applicantNo,workCode,workName,musterNo,wagelistNo,ftoNo,referenceNo,processDate,status,rejectionReason]
                  a=locationArray+rejArray
                  csvArray.append(a)
  reportType="blockRejectedTransactions"
  df = pd.DataFrame(csvArray, columns =header)
  url=saveReport(logger,ldict,reportType,finyear,df)
  logger.warning(f"Thread {threadName} report {url}")
  return url


def getRejectedTransaction(logger,funcArgs):
  debug=None
  csvArray=[]
  searchString='Rejected_ref_no_detail.aspx'
  urlPrefix='http://mnregaweb4.nic.in/netnrega/FTO/Rejected_ref_no_detail.aspx?'
  ldict=funcArgs[0]
  reportURL=funcArgs[1]
  finyear=funcArgs[2]
  logger.info(ldict)
  logger.info(reportURL)
  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  panchayatCode=ldict.get("panchayatCode",'')
  stateName=ldict.get("stateName",None)
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  panchayatName=ldict.get("panchayatName",'')
  villageName=''
  rejDetailsLabels=['workerCode','jobcard','name','applicantNo','workCode','workName','musterNo','wagelistNo','ftoNo','referenceNo','processDate','status','rejectionReason','transactionURL']
  locationArrayLabel=["state","district","block","panchayat","village","stateCode","districtCode","blockCode","panchayatCode"]
  header=locationArrayLabel+rejDetailsLabels
  locationArray=[stateName,districtName,blockName,panchayatName,villageName,stateCode,districtCode,blockCode,panchayatCode]
  r=requests.get(reportURL)
  if r.status_code == 200:
    myhtml=r.content
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    links=htmlsoup.findAll("a")
    for link in links:
      href=link['href']
      if (searchString in href):
        parsedURL=parse.urlsplit(href)
        queryDict=dict(parse.parse_qsl(parsedURL.query, keep_blank_values=True))
        urlParams=parse.urlencode(queryDict)
        transactionURL=urlPrefix+urlParams
        #logger.info(f" Transaction URL is {transactionURL}")
        r1=requests.get(transactionURL)
        if r1.status_code == 200:
          txnhtml=r1.content
          soup=BeautifulSoup(txnhtml,"lxml")
          tables=soup.findAll("table")
          myTable=None
          for table in tables:
            if "Reference No" in str(table):
              myTable=table
          if myTable is not None:
            rows=myTable.findAll('tr')
            for row in rows:
              cols=row.findAll('td')
              if len(cols) > 0:
                wagelistNo=cols[0].text.lstrip().rstrip()
                jobcard=cols[1].text.lstrip().rstrip()
                applicantNo=cols[2].text.lstrip().rstrip()
                name=cols[3].text.lstrip().rstrip()
                workCode=cols[4].text.lstrip().rstrip()
                workName=cols[5].text.lstrip().rstrip()
                musterNo=cols[6].text.lstrip().rstrip()
                referenceNo=cols[7].text.lstrip().rstrip()
                status=cols[8].text.lstrip().rstrip()
                rejectionReason=cols[9].text.lstrip().rstrip()
                processDateString=cols[10].text.lstrip().rstrip()
                processDate=getDateObj(processDateString)
                ftoNo=cols[11].text.lstrip().rstrip()
                srNo=cols[12].text.lstrip().rstrip()
                workerCode="%s_%s" % (jobcard,name)
                rejArray=[workerCode,jobcard,name,applicantNo,workCode,workName,musterNo,wagelistNo,ftoNo,referenceNo,processDate,status,rejectionReason,transactionURL]
                a=locationArray+rejArray
                csvArray.append(a)
  reportType="blockRejectedTransactions"
  df = pd.DataFrame(csvArray, columns =header)
  url=saveReport(logger,ldict,reportType,finyear,df)
  logger.info(url)

def getBlockRejectedURLs(logger,locationType='district',locationCodeParam='block_code',limit=100000,num_threads=50):
  jobList=[]
  searchString="ResponseDetailStatusReport.aspx"
  additionalSearchString='&typ=R'
  reportType='NICFTOStatusHTML'
  url="%s?reportType=%s&location__locationType=%s&limit=%s"  %(REPORTURL,reportType,locationType,str(limit))
  r=requests.get(url)
  jobcardList=[]
  if r.status_code == 200:
    data=r.json()
    results=data['results']
    for res in results:
      logger.info(res)
      reportURL=res['reportURL']
      funcArgs=[reportURL,searchString,locationCodeParam,additionalSearchString]
      p={}
      p['funcName']="getFTOURLs"
      p['funcArgs']=funcArgs
      jobList.append(p)
  libtechQueueManager(logger,jobList,num_threads=num_threads)


def getFTOURLs(logger,funcArgs,threadName=''):
  reportType='NICFTOStatusHTML'
  url=funcArgs[0]
  logger.info(f"baseURL  {url}")
  searchString=funcArgs[1]
  locationCodeParam=funcArgs[2]
  try:
    additionalSearchString=funcArgs[3]
    reportType='NICRejectedTransactionsURL'
    urlPrefix="http://mnregaweb4.nic.in/netnrega/FTO/ResponseDetailStatusReport.aspx?"
  except:
    additionalSearchString=''
    reportType='NICFTOStatusHTML'
    urlPrefix="http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?"
  r=requests.get(url)
  if r.status_code == 200:
    myhtml=r.content
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    #parseFTOLinks(logger,htmlsoup,reportType,urlPrefix,locationCodeParam,searchString,additionalSearchString)
    try:
      validation = htmlsoup.find(id='__EVENTVALIDATION').get('value',None)
    except:
      validation=''
    cookies=r.cookies
    viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
    data = {
       '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$RBtnLstIsEfms$2',
       '__EVENTARGUMENT': '',
       '__LASTFOCUS': '',
       '__VIEWSTATE': viewState,
       '__VIEWSTATEENCRYPTED': '',
       '__EVENTVALIDATION': validation,
       'ctl00$ContentPlaceHolder1$RBtnLst': 'W',
       'ctl00$ContentPlaceHolder1$RBtnLstIsEfms': 'C',
       'ctl00$ContentPlaceHolder1$HiddenField1': ''
        }
    
    response = requests.post(url,cookies=cookies, data=data)
    if response.status_code == 200:
      myhtml=response.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      reportType='NICRejectedTransactionsCoBankURL'
      parseFTOLinks(logger,htmlsoup,reportType,urlPrefix,locationCodeParam,searchString,additionalSearchString)
    data['ctl00$ContentPlaceHolder1$RBtnLstIsEfms']= 'P'
    response = requests.post(url,cookies=cookies, data=data)
    if response.status_code == 200:
      myhtml=response.content
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      reportType='NICRejectedTransactionsPostURL'
      parseFTOLinks(logger,htmlsoup,reportType,urlPrefix,locationCodeParam,searchString,additionalSearchString)


def parseFTOLinks(logger,htmlsoup,reportType,urlPrefix,locationCodeParam,searchString1,searchString2):
    links=htmlsoup.findAll("a")
    for link in links:
      href=link['href']
      if (searchString1 in href):
        if (searchString2 is None) or (searchString2 in href):
          logger.info(href)
          parsedURL=parse.urlsplit(href)
          queryDict=dict(parse.parse_qsl(parsedURL.query, keep_blank_values=True))
          fullFinYear=queryDict.get("fin_year",None)
          finyear=getShortFinYear(fullFinYear)
          locationCode=queryDict.get(locationCodeParam,None)
          urlParams=parse.urlencode(queryDict)
          reportURL=urlPrefix+urlParams
          logger.info(f" Report URL is {reportURL}")
          if locationCode is not None:
            createUpdateDjangoReport(logger,reportType,finyear,reportURL,lcode=locationCode)

def getFTOURLRecursive(logger):
  reportType='NICFTOStatusHTML'
  url="%s?reportType=%s&location__locationType=state&limit=10000"  %(REPORTURL,reportType)
  r=requests.get(url)
  if r.status_code == 200:
    data=r.json()
    results=data['results']
    for res in results:
      logger.info(res)
      reportURL=res['reportURL']
      finyear=res['finyear']
      r=requests.get(reportURL)
      if r.status_code == 200:
        myhtml=r.content
        htmlsoup=BeautifulSoup(myhtml,"lxml")
        links=htmlsoup.findAll("a")
        for link in links:
          href=link['href']
          if "FTOReport.aspx" in href:
            logger.info(href)
            parsedURL=parse.urlsplit(href)
            queryDict=dict(parse.parse_qsl(parsedURL.query))
            locationCode=queryDict.get("district_code",None)
            urlParams=parse.urlencode(queryDict)
            urlPrefix="http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?"
            reportURL=urlPrefix+urlParams
            if locationCode is not None:
              createUpdateDjangoReport(logger,reportType,finyear,reportURL,lcode=locationCode)
  logger.info(url)
def extractMISReportURL(logger):
  startFinYear='16'
  endFinYear=getCurrentFinYear()
  reportType="NICFTOStatusHTML"
  for finyear in range (int(startFinYear), int(endFinYear)+1):
    finyear=str(finyear)
    filename="data/misReport_%s.html" % (finyear)
    with open(filename,"r") as f:
      myhtml=f.read()
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    links=htmlsoup.findAll("a")
    for link in links:
      href=link['href']
      if "FTOReport.aspx" in href:
        logger.info(href)
        r=requests.get(href)
        if r.status_code == 200:
          myhtml=r.content
          htmlsoup=BeautifulSoup(myhtml,"lxml")
          links=htmlsoup.findAll('a')
          for link in links:
            href=link['href']
            if "state_code" in href:
              parsedURL=parse.urlsplit(href)
              queryDict=dict(parse.parse_qsl(parsedURL.query))
              stateCode=queryDict.get("state_code",None)
              urlParams=parse.urlencode(queryDict)
              urlPrefix="http://mnregaweb4.nic.in/netnrega/FTO/FTOReport.aspx?"
              reportURL=urlPrefix+urlParams
              if stateCode is not None:
                createUpdateDjangoReport(logger,reportType,finyear,reportURL,lcode=stateCode)



def nicGlanceStats(logger,locationCode,startFinYear=None,endFinYear=None):
  reportType="nicGlanceStats"
  reportName="NIC At a Glance Stats"
  updateStatus,reportURL=isReportUpdated(logger,reportType,locationCode)
  if updateStatus:
    return reportURL
  csvArray=[]
  locationArrayLabel=["state","district","block","panchayat","village","stateCode","districtCode","blockCode","panchayatCode","locationType"]
  statLabel=["name","slug","finyear","value","textValue"]
  csvHeader=locationArrayLabel+statLabel
  ldict=getLocationDict(logger,locationCode=locationCode)
  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  panchayatCode=ldict.get("panchayatCode",None)
  stateName=ldict.get("stateName",None)
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  panchayatName=ldict.get("panchayatName",None)
  locationType=ldict.get("locationType",None)
  if locationType == 'block':
    targetVar='ddl_pan'
  elif locationType == 'district':
    targetVar='ddl_blk'
  else:
    targetVar='ddl_dist'

  locationArray=["ALL"]
  url=f"{LOCATIONURL}?parentLocation__code={locationCode}&limit=10000"
  r=requests.get(url)
  if r.status_code == 200:
    data=r.json()
    results=data['results']
    for l in results:
      pCode=l.get("code",None)
      if pCode is not None:
        locationArray.append(pCode)
  logger.info(locationArray)
  urlPrefix="http://mnregaweb4.nic.in/netnrega/"
  url="http://mnregaweb4.nic.in/netnrega/all_lvl_details_dashboard_new.aspx"
  headers = {
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
  'Accept-Encoding': 'gzip, deflate',
  'Accept-Language': 'en-GB,en;q=0.5',
  'Connection': 'keep-alive',
  'Host': 'mnregaweb4.nic.in',
  'Referer': url,
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:45.0) Gecko/20100101 Firefox/45.0',
  'Content-Type': 'application/x-www-form-urlencoded',
  }
  r=requests.get(url)
  if r.status_code == 200:
    myhtml=r.content
    htmlsoup=BeautifulSoup(myhtml,"lxml")
    validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
    viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
    data = {
      '__EVENTARGUMENT': '',
      '__LASTFOCUS': '',
      '__VIEWSTATE': viewState,
      '__VIEWSTATEENCRYPTED': '',
      '__EVENTVALIDATION': validation,
      'ddl_state': stateCode,
      '__EVENTTARGET': 'ddl_state',
    }
 
    response = requests.post(url,headers=headers, data=data)
    if response.status_code==200:
      myhtml=response.content
    else:
      myhtml=None
    if ((locationType == 'district') or (locationType == 'block')):
      if myhtml is not None:
        htmlsoup=BeautifulSoup(myhtml,"lxml")
        validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
        viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
        data['ddl_dist']=districtCode
        data['__EVENTTARGET']='ddl_dist'
        data['__VIEWSTATE']= viewState
        data['__EVENTVALIDATION']= validation

        response = requests.post(url,headers=headers, data=data)
        if response.status_code==200:
          myhtml=response.content
        else:
          myhtml=None
    if (locationType == 'block'):
      if myhtml is not None:
        htmlsoup=BeautifulSoup(myhtml,"lxml")
        validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
        viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
        data['ddl_blk']=blockCode
        data['__EVENTTARGET']='ddl_blk'
        data['__VIEWSTATE']= viewState
        data['__EVENTVALIDATION']= validation
        response = requests.post(url,headers=headers, data=data)
        if response.status_code==200:
          myhtml=response.content
        else:
          myhtml=None
    if myhtml is not None:
      htmlsoup=BeautifulSoup(myhtml,"lxml")
      validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
      viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
      for lCode in locationArray:
          data[targetVar]=lCode
          data['__EVENTTARGET']=''
          data['__VIEWSTATE']= viewState
          data['__EVENTVALIDATION']= validation
          data['btproceed']='View Detail'
          response = requests.post(url,headers=headers, data=data)
          if response.status_code==200:
            myhtml=response.content
          else:
            myhtml=None
          if myhtml is not None:
            htmlsoup=BeautifulSoup(myhtml,"lxml")
            #validation = htmlsoup.find(id='__EVENTVALIDATION').get('value')
            #viewState = htmlsoup.find(id='__VIEWSTATE').get('value')
            myiFrame=htmlsoup.find("iframe")
            if myiFrame is not None:
              statsURL=urlPrefix+myiFrame['src']
              if lCode == "ALL":
                lCode1=locationCode
              else:
                lCode1=lCode
              logger.info(statsURL)
              a=getStatArray(logger,statsURL,lCode1)
              csvArray.extend(a)
  df = pd.DataFrame(csvArray, columns =csvHeader)
  finyear=''
  url=saveReport(logger,ldict,reportType,finyear,df)
  logger.info(url)
  return url


def getStatArray(logger,statsURL,locationCode):
  csvArray=[]
  tableIdentifier="Financial Progress"
  ldict=getLocationDict(logger,locationCode=locationCode)
  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  panchayatCode=ldict.get("panchayatCode",None)
  stateName=ldict.get("stateName",None)
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  panchayatName=ldict.get("panchayatName",None)
  locationType=ldict.get("locationType",None)
  locationArray=[stateName,districtName,blockName,panchayatName,"",stateCode,districtCode,blockCode,panchayatCode,locationType]
  r=requests.get(statsURL)
  if r.status_code == 200:
    myhtml=r.content
    outhtml=validateAndSave(logger,ldict,myhtml,tableIdentifier=tableIdentifier)
    mysoup=BeautifulSoup(outhtml,"lxml")
    myTable=mysoup.find("table",id="myTable")
    if myTable is not None:
      rows=myTable.findAll("tr")
      finyearArray=[None]
      basedOnFinYear=False
      finyear=None
      for row in rows:
        cols=row.findAll("td")
        if len(cols) >= 2:
          rowHeader=cols[0].text.lstrip().rstrip()
          if ("II" in rowHeader) and ("Progress" in rowHeader):
            basedOnFinYear=True
            #logger.info(rowHeader)
            d=[]
            for col in cols:
              finyearString=col.text.replace("FY","").lstrip().rstrip()
              #logger.info(finyearString)
              finyear=finyearString[-2:]
              d.append(finyear)
          else:
            rowHeader=cols[0].text.lstrip().rstrip()
            for i in range(1,len(cols)):
              if (basedOnFinYear == False):
                finyear=None
              else:
                finyear=d[i]
              #logger.info(finyear)
              value=cols[i].text.lstrip().rstrip().replace(",","")
              if ( (value != "") and (value.lower() != 'nan')):
            #  if value != "":
                try:
                  b=decimal.Decimal(value)
                  val=b
                except:
                  val=''
                slug=slugify(rowHeader)
                stat=[rowHeader,slug,finyear,val,value]
                csvArray.append(locationArray+stat)
  return csvArray  

def rationList(logger,locationCode,startFinYear=None,endFinYear=None):
  lArray=getChildLocations(logger,locationCode,scheme="pds")
  logger.info(lArray)
  csvArray=[]
  title="PDSList"
  timeout=10
  cardTypeArray=['5','6','7']
  l=['stateName','districtName','blockName','villageName','stateCode','districtCode','blockCode','villageCode']
  a=['rationCardNumber','name','nameHindi','fatherHusbandName','cardType','familyCount','uidCount','dealer','dealerCode','data','mappedStatus']
  colHeaders=l+a

  for lcode in lArray:
    ldict=getLocationDict(logger,locationCode=lcode,scheme="pds")
    districtCode=ldict.get("districtCode",None)
    blockCode=ldict.get("blockCode",None)
    villageCode=ldict.get("code",None)
    myhtml=None
    error=None
    outhtml=''
    r=requests.get(JharkhandPDSBaseURL)
    cookies=r.cookies
    headers = {
          'Origin': 'https://aahar.jharkhand.gov.in',
          'Accept-Encoding': 'gzip, deflate',
          'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
          'Upgrade-Insecure-Requests': '1',
          'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.97 Safari/537.36 Vivaldi/1.94.1008.44',
          'Content-Type': 'application/x-www-form-urlencoded',
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
          'Cache-Control': 'max-age=0',
          'Referer': 'https://aahar.jharkhand.gov.in/secc_cardholders/searchRation',
          'Connection': 'keep-alive',
      }

    for cardType in cardTypeArray:
      data = [
              ('_method', 'POST'),
              ('data[SeccCardholder][rgi_district_code]', districtCode),
              ('data[SeccCardholder][rgi_block_code]', blockCode),
              ('r1', 'dealer'),
              ('data[SeccCardholder][rgi_village_code]', villageCode),
              ('data[SeccCardholder][dealer_id]', ''),
              ('data[SeccCardholder][cardtype_id]', cardType),
              ('data[SeccCardholder][rationcard_no]', ''),
          ]
      response = requests.post('https://aahar.jharkhand.gov.in/secc_cardholders/searchRationResults', headers=headers, cookies=cookies, data=data, timeout=timeout, verify=False)
      if response.status_code == 200:
        myhtml=response.content
        logger.info("able to download list")
        soup=BeautifulSoup(myhtml,"lxml")
        myTable=soup.find('table',id='maintable')
        if myTable is not None:
          outhtml+=getCenterAlignedHeading(cardType)
          outhtml+=stripTableAttributes(myTable,"maintable")
      else:
        error="unable to download"
    outhtml=htmlWrapperLocal(title=title, head='<h1 aling="center">'+title+'</h1>', body=outhtml)
    data=processPDSData(logger,ldict,outhtml)
    csvArray=csvArray+data
  reportURL=''
  if len(csvArray) > 0:
    df=pd.DataFrame(csvArray,columns=colHeaders)
    reportType="rationList"
    finyear=''
    ldict=getLocationDict(logger,locationCode=locationCode,scheme="pds")
    logger.info(ldict)
    reportURL=saveReport(logger,ldict,reportType,finyear,df)
  return reportURL 
def processPDSData(logger,ldict,myhtml):
  csvArray=[]
  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  villageCode=ldict.get("code",None)
  stateName=ldict.get("stateName",None)
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  villageName=ldict.get("name",None)
  l=[stateName,districtName,blockName,villageName,stateCode,districtCode,blockCode,villageCode]
  soup=BeautifulSoup(myhtml,"lxml")
  myTables=soup.findAll('table',id='maintable')

  for myTable in myTables:
    rows=myTable.findAll('tr')
    for row in rows:
      cols=row.findAll('td')
      if len(cols) == 11:
        srNo=cols[0].text.lstrip().rstrip()
        name=cols[2].text.lstrip().rstrip()
        #logger.info(name)
        rationCardNumber=cols[1].text.lstrip().rstrip()
        nameHindi=cols[3].text.lstrip().rstrip()
        fatherHusbandName=cols[4].text.lstrip().rstrip()
        cardType=cols[5].text.lstrip().rstrip()
        familyCount=cols[6].text.lstrip().rstrip()
        uidCount=cols[7].text.lstrip().rstrip()
        dealer=cols[8].text.lstrip().rstrip()
        data=cols[9].text.lstrip().rstrip()
        mappedStatus=cols[10].text.lstrip().rstrip()
        dealerCode=''
        a=[rationCardNumber,name,nameHindi,fatherHusbandName,cardType,familyCount,uidCount,dealer,dealerCode,data,mappedStatus]
        csvArray.append(l+a)
  return csvArray

def apBlockRejectedTransactions(logger,locationCode,startFinYear=None,endFinYear=None,num_threads=100):
  jobList=[]
  ldict=getLocationDict(logger,locationCode=locationCode)
  locationType=ldict.get("locationType",None)
  if locationType != 'block':
    return None
  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  shortBlockCode=blockCode[-2:]
  shortDistrictCode=districtCode[-2:]
  locationCodeArray=getChildLocations(logger,locationCode)
  for panchayatCode in locationCodeArray:
    pdict=getLocationDict(logger,locationCode=panchayatCode)
    shortPanchayatCode=panchayatCode[-2:]
    apLocationCode=f"{shortDistrictCode}~{shortBlockCode}~{shortPanchayatCode}"
    baseURL=f"http://www.nrega.ap.gov.in/Nregs/FrontServlet?requestType=SmartCardreport_engRH&actionVal=NEFMS&id={apLocationCode}&type=&Date=-1&File=&Agency=&listType=&yearMonth=-1&ReportType=&flag=-1&Rtype=-1&Date1=-1&wtype=-1&ytype=-1&Date2=-1&ltype=-1&year=&program=&fileName={apLocationCode}&stype=-1&ptype=-1&lltype=ITDA"
    logger.info(f"AP URL is {baseURL}")
    r=requests.get("http://www.nrega.ap.gov.in/Nregs/")
    cookies=r.cookies
    logger.info(cookies)
    r=requests.get(baseURL,cookies=cookies)
    if r.status_code == 200:
      myhtml=r.content
      mysoup=BeautifulSoup(myhtml,"lxml")
      with open("/tmp/h.html","wb") as f:
        f.write(myhtml)
      tables=mysoup.findAll('table',id="sortable")
      for table in tables:
        logger.info("found tables")
        rows=table.findAll("tr")
        i=0
        for row in rows:
          cols=row.findAll("td")
          if len(cols)> 9:
            rejTrans=cols[8].text.lstrip().rstrip()
            srNo=cols[0].text.lstrip().rstrip()
            if (srNo.isdigit()) and (rejTrans.isdigit()) and (int(rejTrans) > 0):
              a=cols[8].find("a")
              if a is not None:
                href="http://www.nrega.ap.gov.in"+a["href"]
                logger.info(f"rejected Transsions url is {href}")
                funcArgs=[pdict,href]
                p={}
                p['funcName']="apDownloadRejectedURL"
                p['funcArgs']=funcArgs
                jobList.append(p)
          i=i+1         
  locationArrayLabel=["state","district","block","panchayat","village","stateCode","districtCode","blockCode","panchayatCode"]
  mainLabel=['S.No.','Household Code','Worker Code','Beneficiary Name','ePayorder No.','Amount','NREGA Account No.','File Sent Date','Credit Status','Credited Account No.','Credited Bank Name','Credited Bank IIN No.','Bank UTR No.','Remarks']
  headers=locationArrayLabel+mainLabel
  resultArray=libtechQueueManager(logger,jobList,num_threads=num_threads)
  wpDF = pd.DataFrame(resultArray, columns =headers)
  finyear=''
  reportType="apBlockRejectedTransactions"
  url=saveReport(logger,ldict,reportType,finyear,wpDF)

def apDownloadRejectedURL(logger,funcArgs,threadName="default"):
  csvArray=[]
  url=funcArgs[1]
  ldict=funcArgs[0]
  stateCode=ldict.get("stateCode",None)
  districtCode=ldict.get("districtCode",None)
  blockCode=ldict.get("blockCode",None)
  panchayatCode=ldict.get("panchayatCode",None)
  stateName=ldict.get("stateName",None)
  districtName=ldict.get("districtName",None)
  blockName=ldict.get("blockName",None)
  panchayatName=ldict.get("panchayatName",None)
  villageName=''
  l=[stateName,districtName,blockName,panchayatName,villageName,stateCode,districtCode,blockCode,panchayatCode]
  r=requests.get("http://www.nrega.ap.gov.in/Nregs/")
  cookies=r.cookies
  logger.info(cookies)
  r=requests.get(url,cookies=cookies)
  if r.status_code == 200:
    myhtml=r.content
    mysoup=BeautifulSoup(myhtml,"lxml")
    tables=mysoup.findAll("table",id="sortable")
    for table in tables:
      rows=table.findAll("tr")
      for row in rows:
        a=[]
        cols=row.findAll("td")
        if len(cols) > 8:
          for col in cols:
            a.append(col.text.lstrip().rstrip())
          csvArray.append(l+a)
  return csvArray
