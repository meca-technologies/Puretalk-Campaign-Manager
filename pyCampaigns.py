import json
import logging
from requests import get, put
import random

from twilio.twiml.voice_response import Gather, VoiceResponse, Dial
from twilio.rest import Client

import config

# PHONE TIME VALIDATOR
from pyAreadCodes import areaCodeTZ
from pyStateLaws import stateLaws
import datetime
import pytz

import pymongo
from bson.objectid import ObjectId

# SETUP LOGGER
logger = logging.getLogger('Campaigns')

logger.setLevel(logging.DEBUG)
todayFormatted = (datetime.datetime.today()).strftime("%Y-%m-%d")
fh = logging.FileHandler('{}campaigns-{}.py.log'.format(config.logger_path, todayFormatted))
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("[%(asctime)s] - %(name)14s - %(levelname)8s | %(message)s","%Y-%m-%d %H:%M:%S")
fh.setFormatter(formatter)
logger.addHandler(fh)

class campaign:
    def __init__(self, campaign_id, mongoSession):
        try:
            logger.debug('Campaign created for Campaign ID {}'.format(str(campaign_id)))
            try:
                self.mongoSession = mongoSession
                self.campaigns_col = mongoSession['campaigns']
                self.companies_col = mongoSession['companies']
                self.leads_col = mongoSession['leads']
                self.virtual_agents_col = mongoSession['virtual_agents']
                self.twilio_numbers_col = mongoSession['twilio_numbers']
                #self.caller_ids_col = mongoSession['company_caller_ids']
            except:
                logger.debug('\t {} - Failed to connect to mongodb'.format(str(self.campaign_id)))

            self.campaign_id = ObjectId(campaign_id)
            logger.debug('\t {} - campaign_id: {}'.format(str(self.campaign_id), str(self.campaign_id)))

            self.call_queue = []
            self.call_list = []
            self.unactioned = 0
            
            campaign = self.campaigns_col.find_one({"_id":self.campaign_id})
            company = self.companies_col.find_one({"_id":campaign['company_id']})
            self.company_id = company['_id']

            self.account_sid = company['twilio_account_sid']
            logger.debug('\t {} - account_sid: {}'.format(str(self.campaign_id), str(self.account_sid)))

            self.auth_token = company['twilio_auth_token']
            logger.debug('\t {} - auth_token: {}'.format(str(self.campaign_id), str(self.auth_token)))

            self.application_id = company['twilio_application_sid']
            logger.debug('\t {} - application_id: {}'.format(str(self.campaign_id), str(self.application_id)))

            self.getCurrentCallList()

            twilio_settings = self.twilio_numbers_col.find_one({'company_id':ObjectId(company['_id'])})
            
            virtual_agent = self.virtual_agents_col.find_one({'_id':ObjectId(campaign['virtual_agent_id'])})
            self.transfer_number = virtual_agent['phone']
            logger.debug('\t {} - transfer_number: {}'.format(str(self.campaign_id), str(self.transfer_number)))

            self.app_id = virtual_agent['app_id']
            logger.debug('\t {} - app_id: {}'.format(str(self.campaign_id), str(self.app_id)))

            self.inbound_recording = twilio_settings['inbound_recording']
            logger.debug('\t {} - inbound_recording: {}'.format(str(self.campaign_id), str(self.inbound_recording)))

            self.outbound_recording = twilio_settings['outbound_recording']
            logger.debug('\t {} - outbound_recording: {}'.format(str(self.campaign_id), str(self.outbound_recording)))

            logger.debug('\t {} - Grabbing Caller IDs'.format(str(self.campaign_id)))
            #filterBy = {
            #    '_id':campaign['caller_id'],
            #    'verified':True
            #}
            #print(filterBy)
            self.caller_ids = campaign['caller_ids']
            #callerIDs = self.caller_ids_col.find(filterBy)
            #self.caller_ids = []
            #for id in callerIDs:
            #    callerID = ''.join(filter(str.isdigit, id['caller_id']))
            #    if len(callerID) == 10:
            #        callerID = '+1'+callerID
            #    self.caller_ids.append(callerID)
            #    self.caller_ids.append(callerID)
            logger.debug('\t {} - Caller ID List: {}'.format(str(self.campaign_id), str(self.caller_ids)))

            self.process_id = None
            self.can_make_calls = True
            self.errors = 0
        except:
            logger.error('\t {} - Campaign Failed To Be Created No Calls Will Be Made: {}'.format(str(self.campaign_id), str(self.campaign_id)))
            self.can_make_calls = False


    def updateSqlSession(self, mongoSession):
        self.mongoSession = mongoSession
    
    def getCampaignID(self):
        return self.campaign_id
    
    def getCampaignAR(self):
        campaign = self.campaigns_col.find_one({"_id":ObjectId(self.campaign_id)})
        try:
            return campaign['auto_reference_prefix']
        except:
            return None

    def checkCampaignStatus(self):
        logger.debug('{} - Checking Campaign Status'.format(str(self.campaign_id)))
        try:
            campaign = self.campaigns_col.find_one({'_id':self.campaign_id})
            if campaign['status'] == 'paused':
                #return True
                return False
            #return False
            return True
        except:
            logger.error('{} - Lost connection to SQL server'.format(str(self.campaign_id)))
            return 'FAILURE'

    def getCurrentCallList(self):
        try:
            search_query = {
                'campaign_id':self.campaign_id,
                'status':{
                    '$in':['calling', 'queued', 'in-progress', 'ringing']
                }
            }
            currentCalls = self.leads_col.find(search_query)
            logger.debug('\t {} - Checking to see if there is any old calls to account for'.format(str(self.campaign_id)))
            if currentCalls:
                logger.debug('\t\t {} - Calls Found'.format(str(self.campaign_id)))
                for call in currentCalls:
                    logger.debug('\t\t {} - Call ID - {}'.format(str(self.campaign_id), str(call['_id'])))
                    try:
                        try:
                            leadCalls = call['call_logs']
                        except:
                            leadCalls = None
                            logger.error('\t\t {} - No Call Logs'.format(str(self.campaign_id)))
                        if leadCalls:
                            for leadCall in leadCalls:
                                logger.debug('\t\t {} - Call Log Found - {}'.format(str(self.campaign_id), str(leadCall['call_id'])))
                                try:
                                    account_sid = self.account_sid
                                    auth_token = self.auth_token
                                    logger.debug('\t\t {} - Twilio account details - {} - {}'.format(str(self.campaign_id), str(account_sid), str(auth_token)))
                                except:
                                    logger.error('\t\t {} - Failed getting twilio account details'.format(str(self.campaign_id)))
                                try:
                                    try:
                                        client = Client(account_sid, auth_token)

                                        twilCall = client.calls(leadCall['call_id']).fetch()
                                    except:
                                        logger.debug('\t\t {} - Failed to pull calls'.format(str(self.campaign_id)))

                                    search_query = {
                                        'call_logs.call_id':leadCall['call_id']
                                    }
                                    update_query = {
                                        "$set":{
                                            "call_logs.$.status":str(twilCall.status),
                                            "status":str(twilCall.status)
                                        }
                                    }
                                    logger.debug('\t\t {} - Call Updating- {}'.format(str(self.campaign_id), str(update_query)))
                                    if twilCall.status == 'queued':
                                        self.unactioned += 1
                                    try:
                                        self.leads_col.update_one(search_query, update_query)
                                        logger.debug('\t\t {} - Updating Call - {} - To Status - {}'.format(str(self.campaign_id), str(call['_id']), str(twilCall.status)))
                                    except:
                                        logger.error('\t\t {} - Error Updating Call - {} - To Status - {}'.format(str(self.campaign_id), str(call['_id']), str(twilCall.status)))
                                    try:
                                        if twilCall.status == 'failed' or twilCall.status == 'no-answer':
                                            logger.debug('\t\t {} - Creating Call Log - {}'.format(str(self.campaign_id), str(leadCall['call_id'])))
                                            search_query = {
                                                '_id':call['_id']
                                            }
                                            update_query = {
                                                "$push":{
                                                    "call_logs":{
                                                        'lead_id':call['_id'],
                                                        'call_id':str(twilCall.sid),
                                                        'started_on':0,
                                                        'time_taken':0,
                                                        'created_at':str(datetime.datetime.utcnow())[:-7],
                                                        'updated_at':str(datetime.datetime.utcnow())[:-7]
                                                    }
                                                }
                                            }
                                            logger.debug('\t\t Creating Call Logs: {}'.format(str(update_query)))
                                            logger.debug('\t\t Stage Data')
                                            self.leads_col.update_one(search_query, update_query)
                                            logger.debug('\t\t {} - Call Log Created Successfully - {}'.format(str(self.campaign_id), str(leadCall['call_id'])))
                                    except:
                                        logger.error('\t\t {} - Failed To Create Call Log - {}'.format(str(self.campaign_id), str(leadCall['call_id'])))
                                except:
                                    logger.error('\t\t {} - Error Grabbing Call - {}'.format(str(self.campaign_id), str(leadCall['call_id'])))
                    except:
                        logger.error('\t\t {} - Error Grabbing Lead Calls - {}'.format(str(self.campaign_id), str(search_query)))
        except:
            logger.error('\t\t {} - Error Grabbing Queued Calls - {}'.format(str(self.campaign_id), str(search_query)))
    
    def getUnactioned(self):
        try:
            logger.debug('{} - Checking Unactioned Call Queue'.format(str(self.campaign_id)))
            self.unactioned = 0

            remove_list = []
            index = 0
            for number in self.call_queue:
                if number['status'] == 'unactioned' or number['status'] == 'calling':
                    account_sid = self.account_sid
                    auth_token = self.auth_token
                    try:
                        client = Client(account_sid, auth_token)
                        call = client.calls(number['CallSid']).fetch()
                        search_query = {
                            '_id':number['lead_id']
                        }
                        update_query = {
                            "$set":{
                                "status":str(call.status)
                            }
                        }
                        try:
                            self.leads_col.update_one(search_query, update_query)
                            logger.debug('\t {} - Updating Call - {} - To Status - {}'.format(str(self.campaign_id), str(number['lead_id']), str(call.status)))
                        except:
                            logger.error('\t {} - Failed Updating Call - {}'.format(str(self.campaign_id), str(number['lead_id'])))
                    except:
                        logger.error('\t {} - Failed Grabbing Call'.format(str(self.campaign_id)))
                    self.unactioned += 1
                else:
                    remove_list.append(number)
                index+=1

            logger.debug('\t {} - Remove List: {}'.format(str(self.campaign_id), str(remove_list)))
            for number in remove_list:
                self.call_queue.remove(number)

            index = 0
            for number in self.call_queue:
                if number['status'] == 'calling':
                    try:
                        leadDetails = self.leads_col.find_one({'_id':number['lead_id']})
                        if leadDetails['status'] != 'unactioned' and leadDetails['status']  != 'calling' and leadDetails['status']  != 'queued':
                            self.call_queue[index]['status'] = 'actioned'
                    except:
                        logger.error('\t {} - Failed Lead Info - {}'.format(str(self.campaign_id), str(number['lead_id'])))

                index += 1


            logger.debug('\t {} - Updated List: {}'.format(str(self.campaign_id), str(self.call_queue)))
            logger.debug('\t {} - Unactioned Calls: {}'.format(str(self.campaign_id), str(self.unactioned)))
            
            logger.debug('\t {} - New Call Queue: {}'.format(str(self.campaign_id), str(self.call_queue)))
            return self.unactioned
        except:
            logger.error('{} - Lost connection to SQL server'.format(str(self.campaign_id)))
            self.getUnactioned(self)
            campaign = self.campaigns_col.find_one({'_id':ObjectId(self.campaign_id)})
            limit = campaign['num_agents']*campaign['calls_per_agent'] if (campaign['num_agents'] > 0) else campaign['calls_per_agent']
            return limit

    def getLimits(self):
        try:
            campaign = self.campaigns_col.find_one({'_id':ObjectId(self.campaign_id)})
            limit = campaign['calls_per_agent']
            unactioned_calls = self.getUnactioned()
            limit = int(limit) - int(unactioned_calls)

            logger.debug('\t {} - New Limit: {}'.format(str(self.campaign_id), str(limit)))
            return limit
        except:
            return 0

    def loadHopper(self, process_id):
        if self.can_make_calls == True:
            logger.debug('{} - Loading Hopper'.format(str(self.campaign_id)))

            time_now = datetime.datetime.utcnow()
            offset_time = 0
            if time_now.hour == 14:
                offset_time = -5
            elif time_now.hour == 15:
                offset_time = -6
            elif time_now.hour == 16:
                offset_time = -7
            elif time_now.hour == 17:
                offset_time = -8
            elif time_now.hour == 18:
                offset_time = -9
            elif time_now.hour >= 19:
                offset_time = -99
            search_query = {
                'campaign_id':ObjectId(self.campaign_id),
                'status':'unactioned',
                'offset' : { 
                    '$gte' : offset_time
                }
            }
            limit = self.getLimits()
            if limit < 0:
                limit = 0
            if limit > 60:
                limit = 60
            logger.debug('{} - Hopper Limit: {}'.format(str(self.campaign_id), str(limit)))
            leadData = []
            makeCalls = False
            try:
                if limit > 0:
                    reqData = self.leads_col.find(search_query).limit(limit)
                else:
                    reqData = []
                for lead in reqData:
                    leadData.append({
                        'id':lead['_id'],
                        'lead_data':lead['lead_data']
                    })

                phoneNames = ['Phone','phone','Phone Number','phone number','Phone No.','phone no.','Phone No','phone no','cell phone number','Cell Phone Number','contact number','Contact Number','Contact No.','Mobile','mobile','Mobile No',' mobile no','Mobile Number','mobile number','Customer Number','customer number','Customer number','customer Number', 'Telephone No', 'Telephone', 'telephone no', 'telephone', 'Telephone Number' , 'telephone number']
                logger.debug('{} - Iterating through leads'.format(str(self.campaign_id)))
                for item in leadData:
                    makeCalls = True
                    valid = True
                    details = {}
                    details['lead_id'] = item['id']
                    logger.debug('\t {} - Checking Lead: {}'.format(str(self.campaign_id), str(details)))
                    for x in item['lead_data']:
                        try:
                            details[x['field_name']] = x['field_value']
                            logger.debug('\t {} - Iterating through lead details - {}'.format(str(self.campaign_id), str(x['field_name'])))
                            if str(x['field_name']) in phoneNames:
                                refNum = ''.join(filter(str.isdigit, x['field_value']))
                                logger.debug('\t\t {} - Found Phone Number - {}'.format(str(self.campaign_id), str(refNum)))
                                if len(refNum) == 10:
                                    refNum = '+1'+refNum
                                if refNum in self.call_list:
                                    valid = False
                                details['reference_number'] = refNum
                            details['status'] = 'unactioned'
                        except:
                            pass
                    logger.debug('\t {} - New Lead: {}'.format(str(self.campaign_id), str(details)))
                    if valid:
                        self.call_queue.append(details)
                logger.debug('{} - Lead List: {}'.format(str(self.campaign_id), str(self.call_queue)))
            except:
                logger.error('{} - Lost connection to SQL server'.format(str(self.campaign_id)))
            if makeCalls:
                self.createCall()
            else:
                logger.debug('{} - No Calls To Make Go To Sleep'.format(str(self.campaign_id)))
                self.checkCampaignStatus()
                self.getCurrentCallList()
        else:
            logger.debug('{} - No Calls To Make Go To Sleep'.format(str(self.campaign_id)))
            self.checkCampaignStatus()
            self.getCurrentCallList()
   
    def createCall(self):
        logger.debug('{} - Creating Calls'.format(str(self.campaign_id)))
        account_sid = self.account_sid
        auth_token = self.auth_token
        client = Client(account_sid, auth_token)

        twil = '''
        <Response>
            <Dial timeout="30" record="{}"
                recordingStatusCallback="{}recording/callback"
                recordingStatusCallbackEvent="in-progress completed absent">
            <Number
                statusCallbackEvent="initiated ringing answered completed"
                statusCallback="{}calls/events"
                statusCallbackMethod="POST">{}</Number>
            </Dial>
        </Response>
        '''.format(self.outbound_recording, config.webhooks, config.webhooks, self.transfer_number)
        logger.debug('{} - Response: {}'.format(str(self.campaign_id), str(twil)))
        index = 0
        logger.debug('{} - Check Wallet Balance'.format(str(self.campaign_id)))
        company_wallet_balance_col = self.mongoSession['company_wallet_balance']
        balance =  company_wallet_balance_col.find_one({'company_id':ObjectId(self.company_id)})
        try:
            wallet_balance = balance['paid_amount'] - balance['refunded_amount'] - balance['charge_amount']
        except:
            wallet_balance = 0
        logger.debug('{} - Wallet Balance: {}'.format(str(self.campaign_id), str(wallet_balance)))
        if wallet_balance > 20:
            for lead in self.call_queue:
                if lead['status'] == 'unactioned':
                    if(phone_tz_check(lead['reference_number'])):
                        if lead['reference_number'] not in self.call_list:
                            logger.debug('\t {} - Starting call: {}'.format(str(self.campaign_id), str(lead['reference_number'])))
                            try:
                                randomNumber = self.caller_ids[random.randint(0,len(self.caller_ids)-1)]['caller_id']                                
                                logger.debug('\t {} - My Caller ID: {}'.format(str(self.campaign_id), str(randomNumber)))
                                self.call_list.append(lead['reference_number'])
                                call = client.calls.create(
                                                        timeout=28,
                                                        application_sid=self.app_id,
                                                        to=lead['reference_number'],
                                                        from_=randomNumber,
                                                        caller_id=randomNumber,
                                                        record=True,
                                                        recording_status_callback='{}recording/callback'.format(config.webhooks),
                                                        recording_status_callback_method='POST'
                                                )
                                logger.debug('{} - Calling: {}'.format(str(self.campaign_id), str(lead['reference_number'])))
                                search_query = {
                                    '_id':ObjectId(lead['lead_id'])
                                }
                                update_query = {
                                    '$set':{
                                        'status':'calling',
                                        'created_at':str(datetime.datetime.utcnow())[:-7],
                                        'updated_at':str(datetime.datetime.utcnow())[:-7]
                                    }
                                }
                                logger.debug('\t {} - Search Query: {}'.format(str(self.campaign_id), str(search_query)))
                                logger.debug('\t {} - Update Query: {}'.format(str(self.campaign_id), str(update_query)))

                                self.leads_col.update_one(search_query, update_query)

                                logger.debug('\t {} - Parent Call SID: {}'.format(str(self.campaign_id), str(call.parent_call_sid)))
                                logger.debug('\t {} - Call Made SID: {}'.format(str(self.campaign_id), str(call.sid)))

                                update_query = {
                                    '$set':{
                                        'interested':'not_interested'
                                    },
                                    '$push':{
                                        'call_logs':{
                                            'lead_id':ObjectId(lead['lead_id']),
                                            'call_id':str(call.sid),
                                            'reference_number':lead['reference_number'],
                                            'answered':False,
                                            'status':'calling',
                                            'created_at':str(datetime.datetime.utcnow())[:-7],
                                            'updated_at':str(datetime.datetime.utcnow())[:-7]
                                        }
                                    }
                                }
                                self.leads_col.update_one(search_query, update_query)
                                self.call_queue[index]['status'] = 'calling'
                                self.call_queue[index]['CallSid'] = str(call.sid)
                                self.errors = 0
                            except:
                                logger.error('{} - Call Failed Marking Unreachable: {}'.format(str(self.campaign_id), str(lead['reference_number'])))
                                self.errors += 1
                                if self.errors > 5:
                                    logger.error('{} - Too many calls failed turning off calls until fixed'.format(str(self.campaign_id)))
                                    self.can_make_calls = False
                                    self.call_queue = []
                                search_query = {
                                    '_id':ObjectId(lead['lead_id'])
                                }
                                update_query = {
                                    '$set':{
                                        'status':'unreachable',
                                        'created_at':str(datetime.datetime.utcnow())[:-7],
                                        'updated_at':str(datetime.datetime.utcnow())[:-7]
                                    }
                                }
                                self.leads_col.update_one(search_query, update_query)
                        else:
                            logger.error('{} - Found Duplicate Call: {}'.format(str(self.campaign_id), str(lead['reference_number'])))
                            self.call_queue[index]['status'] = 'calling'
                            try:
                                logger.debug('\t {} - Updating Duplicate Call: {}'.format(str(self.campaign_id), str(lead['reference_number'])))
                                search_query = {
                                    '_id':ObjectId(lead['lead_id'])
                                }
                                update_query = {
                                    '$set':{
                                        'status':'calling',
                                        'created_at':str(datetime.datetime.utcnow())[:-7],
                                        'updated_at':str(datetime.datetime.utcnow())[:-7]
                                    }
                                }
                                self.leads_col.update_one(search_query, update_query)
                            except:
                                logger.error('\t {} - Failed Updating Duplicate Call: {}'.format(str(self.campaign_id), str(lead['reference_number'])))
                    else:
                        logger.debug('\t {} - Time Zone Area Code Past Legal Time: {}'.format(str(self.campaign_id), str(lead['reference_number'])))
                        self.call_queue[index]['status'] = 'disallowed'
                            
                    index += 1
        else:
            logger.debug('{} - Not enough money in the wallet'.format(str(self.campaign_id)))
        logger.debug('{} - Done Making Calls Go To Sleep'.format(str(self.campaign_id)))
        self.checkCampaignStatus()
        self.getCurrentCallList()

def formatStringToJSON(x):
    x = str((x).decode("utf-8"))
    leadData = None
    try:
        leadData = json.loads(x)
        print('Properly Loaded')
        return json.loads(leadData)
    except:
        try:
            leadData = str(x).replace("'\"", "'\\\"")
            leadData = str(leadData).replace("\"'", "\\\"'")
            leadData = str(leadData).replace("'", '"')
            return json.loads(leadData)
        except:
            leadData = str(x).replace("'\"", "'\\\"")
            leadData = str(leadData).replace("\"'", "\\\"'")
            leadData = str(leadData).replace("'", '"')
    return None

## THIS FUNCTION WORKS BY REFERENCING TWO DICTIONARIES WE HAVE SAVED 
## ONE IS FOR STORING ALL AREA CODES AND WHICH TIMEZONE AND STATE THEY BELONG TO
## THE OTHER IS FOR STORING STATE LAWS ABOUT AUTO DIAL TIMES
## WE GRAB THE PHONE NUMBER AND TAKE THE AREA CODE OUT OF THAT
## THEN WE FIND WHAT STATE IT BELONGS TO
## LASTLY WE CHECK THE LAWS FOR WHAT TIME WE CAN CALL FOR THAT STATE AND WHAT DAY IT IS NOW IN THAT TIMEZONE
## WE RETURN TRUE OR FALSE OF WHETHER OR NOT WE CAN MAKE THE CALL
def phone_tz_check(phoneNumber):
    try:
        utcnow = pytz.timezone('utc').localize(datetime.datetime.utcnow()) # generic time

        areaCode = phoneNumber[2:5]
        state = None
        tz = None
        startTime = None
        endTime = None
        for area in areaCodeTZ:
            if str(area['area_code']) == areaCode:
                state = area['state']
                tz = area['time_zone']
                break
        here = utcnow.astimezone(pytz.timezone(tz)).replace(tzinfo=None)
        day = here.weekday()
        for law in stateLaws:
            if law['State'] == state:
                enabled = law['days'][day]['allowed']
                startTime = law['days'][day]['start_full']
                endTime = law['days'][day]['end_full']
                break

        if enabled == 0:
            return False

        startHour = startTime.split(':')[0]
        startMin = startTime.split(':')[1]

        endHour = endTime.split(':')[0]
        endMin = endTime.split(':')[1]
        startTime = here.replace(hour=int(startHour), minute=int(startMin))
        endTime = here.replace(hour=int(endHour), minute=int(endMin))

        if here >= startTime and here < endTime:
            return True
        return False
    except:
        return True