"""
  AIS parse log to DB.
"""
# import os
import sys
import sqlite3
from pathlib import Path
from sqlite3 import Error as SQL_Error, Connection

from pyais import decode

# , IterMessages

from pyais.exceptions import MissingMultipartMessageException

# fom pyais.exceptions import TooManyMessagesException, MissingMultipartMessageException
# from pyais.messages import NMEAMessage, ANY_MESSAGE


DB_FILE = 'ais-data.db'


def get_db_connection(db_file):
    """ Get a connection to the database. """
    try:
        conn: Connection = sqlite3.connect(db_file)
    except SQL_Error as error:
        raise SystemExit("\n\nERROR getting database connection - {}".format(error))
    return conn


def create_tables(con):
    """ Create tables if they don't exist. """
    tables = [
        """
        create table if not exists dataCollection
        (
            collection_id integer not null
                constraint data_collection_pk
                primary key autoincrement,
            location_id   integer not null
        );
        """,
        """
        create table if not exists dataLocation
        (
            location_id integer not null
                constraint data_location_pk
                primary key autoincrement,
            site_name        text    not null,
            latitude    real default 0.0,
            longitude   real default 0.0,
            description text
        )
        """,

        """
            create table if not exists rawData
            (
                sentence_id integer not null
                    constraint rawData_pk
                    primary key autoincrement,
                s_type        integer not null,
                s_data        TEXT    not null,
                location_id integer not null,
                hasError    integer default 0     
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS classAPositionReport 
            (                
                sentence_id INTEGER NOT NULL,
                msg_type INTEGER NOT NULL,
                repeat INTEGER,
                mmsi INTEGER,
                status INTEGER,
                turn  TEXT,
                speed REAL default 0.0,
                accuracy TEXT,
                lon REAL default 0.0,
                lat REAL default 0.0,
                course REAL,
                heading REAL,
                second INTEGER,
                maneuver INTEGER,
                spare_1 TEXT,
                raim INTEGER,
                radio INTEGER,
                syncState INTEGER default 0, 
                slotTimeout INTEGER default 0,
                subMessage INTEGER default 0
              )
        """,
        """
            create table if not exists classBPositionReport
            (
                sentence_id INTEGER NOT NULL,
                msg_type INTEGER NOT NULL,
                repeat INTEGER,
                mmsi INTEGER,
                reserved_1 TEXT,
                speed REAL default 0.0,
                accuracy TEXT,
                lon REAL default 0.0,
                lat REAL default 0.0,
                course REAL,
                heading REAL,
                second INTEGER,
                reserved_2 TEXT,
                cs TEXT,
                display TEXT,
                dsc TEXT,
                band TEXT,
                msg22 TEXT,
                assigned TEXT,
                raim TEXT,
                radio INTEGER,
                syncState INTEGER default 0,
                slotTimeout INTEGER default 0,
                subMessage INTEGER default 0
            )
        """
    ]
    try:
        cur = con.cursor()
        for sql in tables:
            # print("executing: {}\n".format(sql))
            cur.execute(sql)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            "\n\nERROR while creating table(s) - {}\n\n".format(error)
        )


def add_raw_data(con, s_type, s_data, has_error):
    """ Add a new sentence to the database raw table. """
    sql = '''
        insert into rawData(s_type, s_data, location_id, hasError)
        values(:s_type, :s_data, :location_id, :hasError)
    '''
    try:
        sql_args = {'s_type': s_type, 's_data': s_data, 'location_id': 1, 'hasError': has_error}
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nERROR during insert = {}\n\n'.format(error)
        )


def get_last_sentence_id(con):
    """ Get the last rowid from the raw data table. """
    sentence_id = 0
    sql = ''' select max(sentence_id) from rawData '''
    try:
        cur = con.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        value = row[0]
        if value is None:
            sentence_id = 1
        else:
            sentence_id = row[0]
    except SQL_Error as error:
        print('ERROR: {}'.format(error))
    return sentence_id


def add_class_a_position_report(con, data):
    """ Add the decoded data to an A position report. """
    sql = '''
        insert into classAPositionReport(
            sentence_id, msg_type, repeat, mmsi, status, turn, speed,
            accuracy, lon, lat, course, heading, second, maneuver, raim, radio
        )
        values(
            :sentence_id, :msg_type, :repeat, :mmsi, :status, :turn, :speed,
            :accuracy, :lon, :lat, :course, :heading, :second, :maneuver, :raim, :radio
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
            'radio': data.radio
                    }
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nERROR during insert = {}\n\n'.format(error)
        )


def add_class_b_position_report(con, data):
    """ Add the decoded data to an B position report. """
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
            '\n\nERROR during insert = {}\n\n'.format(error)
        )


def add_base_station_report(con, data):
    """ Add the decoded data to a base_station_report. """
    print("Base station report.")


def add_decoded_data(con, data):
    """
    Use this function to store appropriate record based on type.
    We only care about the types stored below.
    """

    class_a_reports = [1, 2, 3]
    class_b_report = [18]
    base_station_report = [4]

    msg_type = data.msg_type

    if msg_type in class_a_reports:
        add_class_a_position_report(con, data)
    elif msg_type in class_b_report:
        add_class_b_position_report(con, data)
    """    
    elif msg_type in base_station_report:
        add_base_station_report(con, data)
    else:
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


def main(ais_file_name):
    """ Run as a program. """
    conn = get_db_connection(DB_FILE)
    if conn is not None:
        create_tables(conn)

        file_path = Path(ais_file_name)

        if file_path.is_file():
            print("is a file: {}".format(file_path))

        # if os.path.exists(ais_file_name):

        if file_path.exists():
            parse_ais_file(conn, file_path)
        else:
            raise SystemExit(
                'Unable to read from file: {}'.format(ais_file_name)
            )
    else:
        raise SystemExit(
            'Unable to create or open database file: {}'.format(DB_FILE)
        )

    try:
        conn.close()
    except SQL_Error as error:
        raise SystemExit('ERROR while closing the database connection: {}'.format(error))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage:", sys.argv[0], "/path/to/ais_log_file")
        sys.exit(1)
    main(sys.argv[1])
