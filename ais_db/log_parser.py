"""
  AIS parse log to DB.
"""
# import os
import sys
import sqlite3
from pathlib import Path
from sqlite3 import Error as SQL_Error, Connection

from pyais import decode
#  from pyais import messages

# , IterMessages

from pyais.exceptions import MissingMultipartMessageException

# fom pyais.exceptions import TooManyMessagesException, MissingMultipartMessageException
# from pyais.messages import NMEAMessage, ANY_MESSAGE


DEFAULT_DB_FILE = 'ais-data.db'


def get_db_connection(db_file):
    """ Get a connection to the database. """
    try:
        conn: Connection = sqlite3.connect(db_file)
    except SQL_Error as error:
        raise SystemExit("\n\nERROR getting database connection - {}".format(error))
    return conn


def add_field_data(con, s_data):
    """ Add a row to the nemaTable with the sentence fields. """
    sql = '''
        insert into rawFields(sentence_id, field1, field2, field3, field4, field5, field6, field7)
        values(:sentence_id, :field1, :field2, :field3, :field4, :field5, :field6, :field7)
    '''
    s_id = get_last_sentence_id(con)
    fields = s_data.split(",")

    if len(fields) == 7:
        try:
            sql_args = {
                'sentence_id': s_id, 'field1': fields[0], 'field2': fields[1], 'field3': fields[2],
                'field4': fields[3], 'field5': fields[4], 'field6': fields[5], 'field7': fields[6]
            }
            cur = con.cursor()
            cur.execute(sql, sql_args)
            con.commit()
        except SQL_Error as error:
            raise SystemExit(
                '\n\nERROR during insert = {}\n\n'.format(error)
            )
    else:
        print("Unable to split sentence data. This should not happen.")


def add_raw_data(con, s_type, s_data, has_error):
    """ Add a new sentence to the database raw table. """
    sql = '''
        insert into rawData(s_type, s_data, location_id, hasError)
        values(:s_type, :s_data, :location_id, :hasError)
    '''
    #  s_row_id = 0
    try:
        sql_args = {'s_type': s_type, 's_data': s_data, 'location_id': 1, 'hasError': has_error}
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nERROR during insert = {}\n\n'.format(error)
        )

    add_field_data(con, s_data)


def get_last_sentence_id(con):
    """ Get the last rowid from the raw data table. """
    sentence_id = 0
    sql = ' select max(sentence_id) from rawData '
    try:
        cur = con.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            sentence_id = 1
        else:
            sentence_id = row[0]
    except SQL_Error as error:
        print('ERROR: {}'.format(error))
    return sentence_id


def add_class_a_position_report(con, data):
    """ Add the decoded data to an A position report. Types 1, 2, and 3. """
    sql = '''
        insert into classAPositionReport(
            sentence_id, msg_type, repeat, mmsi, status, turn, speed,
            accuracy, lon, lat, course, heading, second, maneuver, raim, radio, 
            syncState, slotTimeout, subMessage 
        )
        values(
            :sentence_id, :msg_type, :repeat, :mmsi, :status, :turn, :speed,
            :accuracy, :lon, :lat, :course, :heading, :second, :maneuver, :raim, :radio,
            :syncState, :slotTimeout, :subMessage
        )
    '''

    sentence_id = get_last_sentence_id(con)
    try:
        sql_args = {
            'sentence_id': sentence_id, 'msg_type': data.msg_type,
            'repeat': data.repeat, 'mmsi': data.mmsi, 'status': data.status,
            'turn': data.turn, 'speed': data.speed, 'accuracy': data.accuracy,
            'lon': data.lon, 'lat': data.lat, 'course': data.course, 'heading': data.heading,
            'second': data.second, 'maneuver': data.maneuver, 'raim': data.raim,
            'radio': data.radio, 'syncState': data.sync_state, 'slotTimeout': data.slot_timeout,
            'subMessage': data.sub_message
        }
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nERROR during insert = {}\n\n'.format(error)
        )


def add_base_station_report(con, data):
    """ Add the decoded data to a base_station_report.  Type 4. """
    sql = '''
        insert into baseStationReport(
            sentence_id, msg_type, repeat, mmsi, year, month, day, hour, minute, second, accuracy,
            lon, lat, epfd, spare_1, raim, radio
        ) VALUES (
            :sentence_id, :msg_type, :repeat, :mmsi, :year, :month, :day, :hour, :minute, :second, :accuracy, 
            :lon, :lat, :epfd, :spare_1, :raim, :radio
        )
    '''
    sentence_id = get_last_sentence_id(con)
    try:
        sql_args = {
            'sentence_id': sentence_id, 'msg_type': data.msg_type, 'repeat': data.repeat, 'mmsi': data.mmsi,
            'year': data.year, 'month': data.month, 'day': data.day, 'hour': data.hour, 'minute': data.minute,
            'second': data.second, 'accuracy': data.accuracy, 'lon': data.lon, 'lat': data.lat,
            'epfd': data.epfd, 'spare_1': data.spare_1, 'raim': data.raim, 'radio': data.radio
        }
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nERROR during insert = {}\n\n'.format(error)
        )


def add_class_b_position_report(con, data):
    """ Add the decoded data to an B position report. Type 18. """
    sql = '''
        insert into classBPositionReport(
            sentence_id, msg_type, repeat, mmsi, speed, accuracy, lon, lat, 
            course, heading, second, cs, display, dsc, band, msg22, assigned,
            raim, radio
        )
        values(
            :sentence_id, :msg_type, :repeat, :mmsi, :speed, :accuracy, :lon, :lat, 
            :course, :heading, :second, :cs, :display, :dsc, :band, :msg22, :assigned,  
            :raim, :radio
        )
    '''
    sentence_id = get_last_sentence_id(con)
    try:
        sql_args = {
            'sentence_id': sentence_id, 'msg_type': data.msg_type,
            'repeat': data.repeat, 'mmsi': data.mmsi,
            'speed': data.speed, 'accuracy': data.accuracy,
            'lon': data.lon, 'lat': data.lat, 'course': data.course,
            'heading': data.heading, 'second': data.second,
            'cs': data.cs, 'display': data.display, 'dsc': data.dsc,
            'band': data.band, 'msg22': data.msg22,
            'assigned': data.assigned,
            'raim': data.raim, 'radio': data.radio
        }
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nclassBPositionReport - ERROR during insert = {}\n\n'.format(error)
        )


def add_extended_class_b_position_report(con, data):
    """ Add extended class b position report. Type 19. """
    sql = '''
        insert into extendedClassBPositionReport(
            sentence_id, msg_type, repeat, reserved_1, speed, accuracy, lon, lat, course, heading, 
            second, reserved_2, shipname, ship_type, to_bow, to_stern, to_port, to_starboard, epfd, 
            raim, dte, assigned, spare_1
        )
        values (
            :sentence_id, :msg_type, :repeat, :reserved_1, :speed, :accuracy, :lon, :lat, :course, :heading, 
            :second, :reserved_2, :shipname, :ship_type, :to_bow, :to_stern, :to_port, :to_starboard, :epfd, 
            :raim, :dte, :assigned, :spare_1
        )
    '''
    sentence_id = get_last_sentence_id(con)
    try:
        sql_args = {
            'sentence_id': sentence_id, 'msg_type': data.msg_type,
            'repeat': data.repeat, 'reserved_1': data.reserved_1,
            'speed': data.speed, 'accuracy': data.accuracy,
            'lon': data.lon, 'lat': data.lat, 'course': data.course,
            'heading': data.heading, 'second': data.second,
            'reserved_2': data.reserved_2, 'shipname': data.shipname,
            'ship_type': data.ship_type, 'to_bow': data.to_bow, 'to_stern': data.to_stern,
            'to_port': data.to_port, 'to_starboard': data.to_starboard,
            'epfd': data.epfd, 'raim': data.raim, 'dte': data.dte,
            'assigned': data.assigned, 'spare_1': data.spare_1
        }
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nextendedClassBPositionReport - ERROR during insert = {}\n\n'.format(error)
        )


def get_last_static_data_report_id(con):
    """ Get the last rowid from staticDataReport table. """
    sql = ' select max(report_id) from staticDataReport '
    try:
        cur = con.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            report_id = 1
        else:
            report_id = row[0]
    except SQL_Error as error:
        raise SystemExit(
            print('ERROR: {}'.format(error))
        )
    return report_id


def get_last_row_id(con, table_name):
    """ Generic function to get the last rowid for a given table. """
    sql = ' select max(rowid) from ' + table_name
    try:
        cur = con.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            row_id = 1
        else:
            row_id = row[0]
    except SQL_Error as error:
        raise SystemExit(
            print('\n\nERROR: {}'.format(error))
        )
    return row_id


def update_static_data_report_child(con, parent_id, child_id):
    """ Update the child id in the staticDataReport table after the child was inserted. """
    sql = '''
        update staticDataReport set child_id = :child_id
        where report_id = :parent_id
    '''
    try:
        sql_args = {'child_id': child_id, 'parent_id': parent_id}
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            print('\n\nstaticDataReport update - ERROR: {}'.format(error))
        )


def add_static_data_report(con, data):
    """ Add a static data report that is split based on the part number. Type: 24X """
    sql = '''
        insert into staticDataReport(
            sentence_id, msg_type, repeat, mmsi, partno, child_id
        )
        values(
            :sentence_id, :msg_type, :repeat, :mmsi, :partno, :child_id
        )
    '''
    sentence_id = get_last_sentence_id(con)
    child_id = 0
    try:
        sql_args = {
            'sentence_id': sentence_id, 'msg_type': data.msg_type, 'repeat': data.repeat,
            'mmsi': data.mmsi, 'partno': data.partno, 'child_id': child_id
        }
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nstaticDataReport - ERROR during insert = {}\n\n'.format(error)
        )

    parent_id = get_last_static_data_report_id(con)

    if data.partno == 0:
        add_static_data_report_a(con, sentence_id, parent_id, data)
        child_id = get_last_row_id(con, 'staticDataReportA')
        update_static_data_report_child(con, parent_id, child_id)
    elif data.partno == 1:
        add_static_data_report_b(con, sentence_id, parent_id, data)
        child_id = get_last_row_id(con, 'staticDataReportB')
        update_static_data_report_child(con, parent_id, child_id)
    else:
        print("WARNING invalid partno in data - {}".format(data.partno))


def add_static_data_report_a(con, sentence_id, parent_id, data):
    """ Add the static data report. Type: 24A. """
    sql = '''
        insert into staticDataReportA(
            sentence_id, parent_id, mmsi, shipname, spare_1
        )
        values(
            :sentence_id, :parent_id, :mmsi, :shipname, :spare_1
        ) 
    '''
    try:
        sql_args = {
            'sentence_id': sentence_id,  'parent_id': parent_id, 'mmsi': data.mmsi,
            'shipname': data.shipname, 'spare_1': data.spare_1
        }
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nstaticDataReportA - ERROR during insert = {}\n\n'.format(error)
        )


def add_static_data_report_b(con, sentence_id, parent_id, data):
    """ Add the static data report. Type: 24B. """
    sql = '''
        insert into staticDataReportB(
            sentence_id, parent_id, mmsi, ship_type, vendorid, model, serial, callsign, 
            to_bow, to_stern, to_port, to_starboard, spare_1
        )
        values(
            :sentence_id, :parent_id, :mmsi, :ship_type, :vendorid, :model, :serial, :callsign, 
            :to_bow, :to_stern, :to_port, :to_starboard, :spare_1
        ) 
    '''
    try:
        sql_args = {
            'sentence_id': sentence_id, 'parent_id': parent_id, 'mmsi': data.mmsi, 'ship_type': data.ship_type,
            'vendorid': data.vendorid, 'model': data.model, 'serial': data.serial,
            'callsign': data.callsign, 'to_bow': data.to_bow, 'to_stern': data.to_stern,
            'to_port': data.to_port, 'to_starboard': data.to_starboard, 'spare_1': data.spare_1
        }
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nstaticDataReportB - ERROR during insert = {}\n\n'.format(error)
        )


def add_class_a_static_voyage_data(con, data):
    """ Add the class a static voyage data report. Type: 5"""
    sql = '''
        insert into classAStaticVoyageData(
            sentence_id, msg_type, mmsi, ais_version, imo, callsign, shipname, 
            ship_type, to_bow, to_stern, to_port, to_starboard, epfd, 
            month, day, hour, minute, draught, destination, dte, spare_1
        )
        values(
            :sentence_id, :msg_type, :mmsi, :ais_version, :imo, :callsign, :shipname, 
            :ship_type, :to_bow, :to_stern, :to_port, :to_starboard, :epfd, 
            :month, :day, :hour, :minute, :draught, :destination, :dte, :spare_1
        )
    '''
    sentence_id = get_last_sentence_id(con)
    try:
        sql_args = {
            'sentence_id': sentence_id, 'msg_type': data.msg_type, 'mmsi': data.mmsi, 'ais_version': data.ais_version,
            'imo': data.imo, 'callsign': data.callsign, 'shipname': data.shipname, 'ship_type': data.ship_type,
            'to_bow': data.to_bow, 'to_stern': data.to_stern, 'to_port': data.to_port,
            'to_starboard': data.to_starboard, 'epfd': data.epfd, 'month': data.month, 'day': data.day,
            'hour': data.hour, 'minute': data.minute, 'draught': data.draught, 'destination': data.destination,
            'dte': data.dte, 'spare_1': data.spare_1
        }
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nclassAStaticVoyageData - ERROR during insert = {}\n\n'.format(error)
        )


def add_aid_to_navigation_report(con, data):
    """ Add the aid to navigation report. Type: 21 """
    sql = '''
        insert into aidToNavigationReport(
            sentence_id, msg_type, repeat, mmsi, aid_type, name, accuracy, lon, lat, 
            to_bow, to_stern, to_port, to_starboard, epfd, second, off_position, 
            reserved_1, raim, virtual_aid, assigned, spare_1, name_ext
        )
        values(
            :sentence_id, :msg_type, :repeat, :mmsi, :aid_type, :name, :accuracy, :lon, :lat, 
            :to_bow, :to_stern, :to_port, :to_starboard, :epfd, :second, :off_position, 
            :reserved_1, :raim, :virtual_aid, :assigned, :spare_1, :name_ext
        ) 
    '''
    sentence_id = get_last_sentence_id(con)
    try:
        sql_args = {
            'sentence_id': sentence_id, 'msg_type': data.msg_type, 'repeat': data.repeat,
            'mmsi': data.mmsi, 'aid_type': data.aid_type, 'name': data.name, 'accuracy': data.accuracy,
            'lon': data.lon, 'lat': data.lat, 'to_bow': data.to_bow, 'to_stern': data.to_stern,
            'to_port': data.to_port, 'to_starboard': data.to_starboard, 'epfd': data.epfd,
            'second': data.second, 'off_position': data.off_position, 'reserved_1': data.reserved_1,
            'raim': data.raim, 'virtual_aid': data.virtual_aid, 'assigned': data.assigned,
            'spare_1': data.spare_1, 'name_ext': data.name_ext
        }
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\naidToNavigationReport- ERROR during insert = {}\n\n'.format(error)
        )


def add_standard_sar_aircraft_position_report(con, data):
    """ Add standard SAR aircraft position report. Type: 9 """
    sql = '''
        insert into standardSARAircraftPositionReport(
            sentence_id, msg_type, repeat, mmsi, alt, speed, accuracy, 
            lon, lat, course, second, reserved_1, dte, spare_1, 
            assigned, raim, radio
        )
        values (
            :sentence_id, :msg_type, :repeat, :mmsi, :alt, :speed, :accuracy, 
            :lon, :lat, :course, :second, :reserved_1, :dte, :spare_1, 
            :assigned, :raim, :radio      
        ) 
    '''
    sentence_id = get_last_sentence_id(con)
    try:
        sql_args = {
            'sentence_id': sentence_id, 'msg_type': data.msg_type, 'repeat': data.repeat,
            'mmsi': data.mmsi, 'alt': data.alt, 'speed': data.speed, 'accuracy': data.accuracy,
            'lon': data.lon, 'lat': data.lat, 'course': data.course, 'second': data.second,
            'reserved_1': data.reserved_1, 'dte': data.dte, 'spare_1': data.spare_1,
            'assigned': data.assigned, 'raim': data.raim, 'radio': data.radio
        }
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nstandardSARAircraftPositionReport - ERROR during insert = {}\n\n'.format(error)
        )


def add_decoded_data(con, data):
    """
    Use this function to store appropriate record based on type.
    We only care about the types stored below.
    """

    class_a_reports = [1, 2, 3]
    base_station_report = [4]
    class_a_static_voyage_data = [5]
    standard_sar_aircraft_position_report = [9]
    class_b_report = [18]
    extended_class_b_position_report = [19]
    aid_to_navigation_report = [21]
    static_data_report = [24]

    msg_type = data.msg_type

    if msg_type in class_a_reports:
        add_class_a_position_report(con, data)
    elif msg_type in base_station_report:
        add_base_station_report(con, data)
    elif msg_type in class_b_report:
        add_class_b_position_report(con, data)
    elif msg_type in extended_class_b_position_report:
        add_extended_class_b_position_report(con, data)
    elif msg_type in static_data_report:
        add_static_data_report(con, data)
    elif msg_type in class_a_static_voyage_data:
        add_class_a_static_voyage_data(con, data)
    elif msg_type in aid_to_navigation_report:
        add_aid_to_navigation_report(con, data)
    elif msg_type in standard_sar_aircraft_position_report:
        add_standard_sar_aircraft_position_report(con, data)

    """
        print("Unhandled report type {}.".format(data.msg_type))
    """


def parse_ais_file(con, ais_file_name):
    count = 0
    with open(ais_file_name) as file_in:
        try:
            for line in file_in:
                count += 1
                if line.startswith("!AI"):
                    try:
                        decoded = decode(line)
                        add_raw_data(con, decoded.msg_type, line, 0)
                        add_decoded_data(con, decoded)
                    except MissingMultipartMessageException as warning:
                        # print("WARNING! {}".format(warning))
                        # Warning during decode so set error flag.
                        add_raw_data(con, decoded.msg_type, line, 1)
                    # print(decoded)
                    # print(line, end='')
                if count % 100 == 0:
                    print(".", end='')
        except IOError as error:
            raise SystemExit('ERROR while reading file = {}'.format(error))


def main(ais_file_name, db_file):
    """ Run as a program. """
    conn = get_db_connection(db_file)
    if conn is not None:
        file_path = Path(ais_file_name)
        if file_path.is_file():
            print("is a file: {}".format(file_path))
        if file_path.exists():
            parse_ais_file(conn, file_path)
        else:
            raise SystemExit(
                'Unable to read from file: {}'.format(ais_file_name)
            )
    else:
        raise SystemExit(
            'Unable to create or open database file: {}'.format(db_file)
        )

    try:
        conn.close()
    except SQL_Error as error:
        raise SystemExit('ERROR while closing the database connection: {}'.format(error))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: {} {} {}", sys.argv[0], "/path/to/ais_log_file", "/path/to/ais.db")
        sys.exit(1)
    if len(sys.argv) == 3:
        database_file = sys.argv[2]
    else:
        database_file = DEFAULT_DB_FILE
    main(sys.argv[1], database_file)
