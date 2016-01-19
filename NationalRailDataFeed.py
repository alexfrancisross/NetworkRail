import logging
from time import sleep

import stomp
import json
import psycopg2
import time, datetime

#connect to database
try:
    conn = psycopg2.connect("dbname='<YOUR DATABASE NAME>' user='<YOUR USER NAME>' host='<YOUR HOST NAME>' password='<YOUR PASSWORD>'")
except:
    print "I am unable to connect to the database"

NETWORK_RAIL_AUTH = ('<YOUR NETWORK RAIL USERNAME>', '<YOUR NETWORK RAIL PASSWORD>')

class Listener(object):

    def __init__(self, mq):
        self._mq = mq

    def on_message(self, headers, message):
        #print headers
        #print message
        message_array = json.loads(message)

        for s in message_array:
            if s['header']['msg_type'] == '0003': #if message type is 0003 (Train Movement)
                print s['body']

                event_type = s['body']['event_type']

                if s['body']['gbtt_timestamp'] == '':
                    gbtt_timestamp = None
                else:
                    gbtt_timestamp = str(datetime.datetime.fromtimestamp(float(s['body']['gbtt_timestamp'])/1000))

                original_loc_stanox = s['body']['original_loc_stanox']

                if s['body']['planned_timestamp'] == '':
                    planned_timestamp = None
                else:
                    planned_timestamp = str(datetime.datetime.fromtimestamp(float(s['body']['planned_timestamp'])/1000))

                timetable_variation = s['body']['timetable_variation']

                if s['body']['original_loc_timestamp'] == '':
                    original_loc_timestamp = None
                else:
                    original_loc_timestamp = str(datetime.datetime.fromtimestamp(float(s['body']['original_loc_timestamp'])/1000))

                current_train_id = s['body']['current_train_id']
                delay_monitoring_point = s['body']['delay_monitoring_point']
                next_report_run_time = s['body']['next_report_run_time']
                reporting_stanox = s['body']['reporting_stanox']

                if s['body']['actual_timestamp'] == '':
                    actual_timestamp = None
                else:
                    actual_timestamp = str(datetime.datetime.fromtimestamp(float(s['body']['actual_timestamp'])/1000))

                correction_ind = s['body']['correction_ind']
                event_source = s['body']['event_source']
                train_file_address = s['body']['train_file_address']
                platform =  s['body']['platform']
                division_code = s['body']['division_code']
                train_terminated = s['body']['train_terminated']
                train_id = s['body']['train_id']
                offroute_ind = s['body']['offroute_ind']
                variation_status = s['body']['variation_status']
                train_service_code = s['body']['train_service_code']
                toc_id = s['body']['toc_id']
                loc_stanox = s['body']['loc_stanox']

                if s['body']['auto_expected'] == '':
                    auto_expected = None
                else:
                    auto_expected = s['body']['auto_expected']
                direction_ind = s['body']['direction_ind']
                route = s['body']['route']
                planned_event_type = s['body']['planned_event_type']
                next_report_stanox = s['body']['next_report_stanox']
                line_ind = s['body']['line_ind']

                c = conn.cursor()
                #insert data into database
                c.execute("INSERT INTO NationalRail (event_type, gbtt_timestamp, original_loc_stanox, planned_timestamp, timetable_variation, original_loc_timestamp, current_train_id, delay_monitoring_point, next_report_run_time, reporting_stanox, actual_timestamp, correction_ind, event_source, train_file_address, platform, division_code, train_terminated, train_id, offroute_ind, variation_status, train_service_code, toc_id, loc_stanox, auto_expected, direction_ind, route, planned_event_type, next_report_stanox, line_ind) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (event_type, gbtt_timestamp, original_loc_stanox, planned_timestamp, timetable_variation, original_loc_timestamp, current_train_id, delay_monitoring_point, next_report_run_time, reporting_stanox, actual_timestamp, correction_ind, event_source, train_file_address, platform, division_code, train_terminated, train_id, offroute_ind, variation_status, train_service_code, toc_id, loc_stanox, auto_expected, direction_ind, route, planned_event_type, next_report_stanox, line_ind))

                conn.commit()

        self._mq.ack(id=headers['message-id'], subscription=headers['subscription'])

while True:
    try:
        mq = stomp.Connection(host_and_ports=[('datafeeds.networkrail.co.uk', 61618)],
                              keepalive=True,
                              vhost='datafeeds.networkrail.co.uk',
                              heartbeats=(100000, 50000))

        mq.set_listener('', Listener(mq))

        mq.start()
        mq.connect(username=NETWORK_RAIL_AUTH[0],
                   passcode=NETWORK_RAIL_AUTH[1],
                   wait=True)

        mq.subscribe('/topic/TRAIN_MVT_ALL_TOC', 'test-vstp', ack='client-individual')

        while mq.is_connected():
            sleep(1)

    except:
        # Oh well, reconnect and keep going
        continue


