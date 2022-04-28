import datetime
import multiprocessing
import config

from pyCampaigns import campaign
import logging

import pymongo
from bson.objectid import ObjectId

# SETUP LOGGER
logger = logging.getLogger('Campaign Manager')

logger.setLevel(logging.DEBUG)
todayFormatted = (datetime.datetime.today()).strftime("%Y-%m-%d")
fh = logging.FileHandler('{}campaign-manager-{}.py.log'.format(config.logger_path, todayFormatted))
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("[%(asctime)s] - %(name)14s - %(levelname)8s | %(message)s","%Y-%m-%d %H:%M:%S")
fh.setFormatter(formatter)
logger.addHandler(fh)


def connectToDB():
    client = pymongo.MongoClient("mongodb+srv://admin:QM6icvpQ6SlOveul@cluster0.vc0rw.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
    mongoDB = client['jamesbon']
    return mongoDB

def getSchedules():
    mongoDB = connectToDB()
    campaigns_col = mongoDB['campaigns']
    search_query = {
        'status':{
            '$nin':['completed', 'paused']
        },
    }
    campaigns = campaigns_col.find(search_query)
    activeCampaignList = []
    for campaign in campaigns:
        activeCampaignList.append(campaign['_id'])

    todayIndex = datetime.datetime.today().weekday()
    days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
    search_query = {
        "campaign_id":{
            "$in":activeCampaignList
        },
        "day_value":days[todayIndex+1],
    }
    campaign_schedule_col = mongoDB['campaign_schedule']
    schedules = campaign_schedule_col.find(search_query)

    scheduleData = {}
    for schedule in schedules:
        search_query = {
            "_id":schedule['campaign_id']
        }
        currCampaign = campaigns_col.find_one(search_query)
        if not scheduleData.get(str(schedule['campaign_id'])):
            scheduleData[str(schedule['campaign_id'])] = {
                'status':None
            }
        scheduleData[str(schedule['campaign_id'])]['day_value'] = schedule['day_value']
        scheduleData[str(schedule['campaign_id'])]['campaign_id'] = schedule['campaign_id']
        if schedule['status'] == 'start':
            scheduleData[str(schedule['campaign_id'])]['start_hour'] = schedule['hour']
            scheduleData[str(schedule['campaign_id'])]['start_minute'] = schedule['minute']
        elif schedule['status'] == 'stop':
            scheduleData[str(schedule['campaign_id'])]['stop_hour'] = schedule['hour']
            scheduleData[str(schedule['campaign_id'])]['stop_minute'] = schedule['minute']
        scheduleData[str(schedule['campaign_id'])]['status'] = str(currCampaign['status'])
        scheduleData[str(schedule['campaign_id'])]['enabled'] = schedule['enabled']
    
    logger.debug("Schedules: {}".format(str(scheduleData)))
    return scheduleData


if __name__ == "__main__":
    mongoDB = connectToDB()
    campaigns_col = mongoDB['campaigns']
    print('Run')

    ## FIRST WE WANT TO GRAB ALL THE CAMPAIGNS THAT ARE CURRENTLY STARTED
    filter_by = {
        'status':'started',
        #'status':'paused',
        #'_id':ObjectId('61b3718b8b23496c01858888')
    }
    campaigns = campaigns_col.find(filter_by)

    # WE HAVE TWO LISTS HERE ONE FOR HOLDING THE CAMPAIGN OBJECTS AND THEIR IDS
    campaignClassList = []
    campaignIDList = []

    # GO THROUGH EACH CAMPAIGN THAT IS STARTED AND ADD IT TO THE LISTS ABOVE
    for camp in campaigns:
        campaignClassList.append(campaign(camp['_id'],mongoDB))
        campaignIDList.append(camp['_id'])

    # SET OUR LOOP INTERVALS IN SECONDS
    interval = 30
    schedule_interval = 30
    curr_time = datetime.datetime.now()

    # THESE TWO VARIABLES ARE TO TRACK WHEN WE RUN
    # EITHER THE DIALER OR SCHEDULER NEXT
    next_time_hopper = curr_time
    next_time_scheduler = curr_time

    # START OUR LOOP
    while True:
        # UPDATE OUR CURRENT TIME
        curr_time = datetime.datetime.now()

        # CHECK TO SEE IF TIMES UP TO RUN THE DIALER
        if curr_time >= next_time_hopper:
            # BIND SESSION TO DB
            #session = scoped_session(sessionmaker(bind=engine))

            # SET THE NEXT TIME TO RUN THE DIALER
            time_change = datetime.timedelta(seconds=interval)
            next_time_hopper = curr_time + time_change # THESE TWO LISTS ARE TO TRACK THE CAMPAIGNS 
            # AND THEIR IDS TO REMOVE IF THE CAMPAIGN HAS STOPPED
            removeList = []
            removeIDList = []
            # GO THROUGH EACH CAMPAIGN OBJECT AND SEE IF IT IS STILL RUNNING
            # IF IT ISNT ADD ADD TO LIST TO BE REMOVED
            for camp in campaignClassList:
                camp.updateSqlSession(mongoDB)
                try:
                    if camp.checkCampaignStatus() == False:
                        logger.debug('\t Removed: {}'.format(camp.getCampaignID()))
                        removeList.append(camp)
                        removeIDList.append(camp.getCampaignID())
                    elif camp.checkCampaignStatus() == 'FAILURE':
                        mongoDB = connectToDB()
                        camp.updateSqlSession(mongoDB)
                except:
                    mongoDB = connectToDB()
                    camp.updateSqlSession(mongoDB)

            # GO THROUGH AND REMOVE AND CAMPAIGNS THAT HAVE STOPPED
            logger.debug("Campaign Set To Be Removed: {}".format(str(removeIDList)))
            for x in removeList:
                logger.debug("Campaign: {}".format(str(x.getCampaignID())))
                campaignClassList.remove(x)
                del x
            for x in removeIDList:
                campaignIDList.remove(x)

                
            logger.debug("Current Campaign List: {}".format(str(campaignIDList)))

            logger.debug("Run Hoppers")
            jobs = []
            for i in range(len(campaignClassList)):
                campaignClassList[i].updateSqlSession(mongoDB)
                p = multiprocessing.Process(target=campaignClassList[i].loadHopper(i))
                jobs.append(p)
                p.start()
            logger.debug("Hoppers Triggered To Run")
            
            logger.debug("Grabbing Campaigns That have started")

            search_query = {
                'status':'started',
                #'status':'paused',
                #'_id':ObjectId('61b3718b8b23496c01858888')
            }
            campaigns = campaigns_col.find(search_query)
            logger.debug("Checking to see if we have any new campaigns that started")
            for camp in campaigns:
                if camp['_id'] not in campaignIDList:
                    logger.debug("Add Campaign ID: {} to our list".format(str(camp['_id'])))
                    try:
                        campaignClassList.append(campaign(camp['_id'], mongoDB))
                        campaignIDList.append(camp['_id'])
                    except:
                        pass
                    logger.debug("New Campaign List: {}".format(str(campaignIDList)))

        if curr_time >= next_time_scheduler:
            time_change = datetime.timedelta(seconds=schedule_interval)
            next_time_scheduler = curr_time + time_change
            data = getSchedules()
            now = datetime.datetime.utcnow()
            currHour = int(now.hour)
            currMin = int(now.minute)
            logger.debug("Current Time: {}:{}".format(str(currHour), str(currMin)))
            for x in data:
                if data[x]['status'] != 'paused':
                    if currHour>= data[x]['stop_hour'] or currHour < data[x]['start_hour'] or data[x]['enabled'] == False:
                        if currMin>= data[x]['stop_minute']:
                            logger.debug("Past Stopping Time Check If Running: {}".format(str(data[x]['campaign_id'])))
                            if data[x]['status'] != 'stopped':
                                logger.debug("Campaign is running we need to stop it: {}".format(str(data[x]['campaign_id'])))
                                updateData = {
                                    "$set":{
                                        'status':'stopped'
                                    }
                                }
                                logger.debug("Stopping Campaign: {}".format(data[x]['campaign_id']))
                                search_query = {
                                    '_id':data[x]['campaign_id']
                                }
                                logger.debug("Starting Campaign with search query: {}".format(str(search_query)))
                                campaigns_col.update_one(search_query, updateData)
                                logger.debug("Campaign Stopped: {}".format(data[x]['campaign_id']))

                    elif currHour>= data[x]['start_hour']:
                        if currMin>= data[x]['start_minute']:
                            logger.debug("Past Starting Time Check If Running: {}".format(str(data[x]['campaign_id'])))
                            if data[x]['status'] != 'started':
                                logger.debug("Campaign is not running we need to start it: {}".format(str(data[x]['campaign_id'])))
                                updateData = {
                                    "$set":{
                                        'status':'started'
                                    }
                                }
                                logger.debug("Starting Campaign: {}".format(data[x]['campaign_id']))
                                search_query = {
                                    '_id':data[x]['campaign_id']
                                }
                                logger.debug("Starting Campaign with search query: {}".format(str(search_query)))
                                campaigns_col.update_one(search_query, updateData)
                                logger.debug("Campaign Started: {}".format(data[x]['campaign_id']))
                else:
                    logger.debug("Campaign is paused do nothing: {}".format(data[x]['campaign_id']))