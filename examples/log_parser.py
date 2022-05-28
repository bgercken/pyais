"""
  AIS parse log to DB.
"""
import os
import sys
import sqlite3
from pathlib import Path
from sqlite3 import Error as SQL_Error, Connection

from pyais import decode, IterMessages

from pyais.exceptions import TooManyMessagesException, MissingMultipartMessageException
from pyais.messages import NMEAMessage, ANY_MESSAGE


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
                location_id integer not null
            )
        """,
        """
            CREATE TABLE IF NOT EXISTS classAPositionReport 
            (                
                msgID INTEGER NOT NULL,
                repeatIndicator INTEGER,
                userID INTEGER,
                navigationalStatus INTEGER,
                longitude REAL, 
                latitude REAL,
                courseOverGround REAL,
                trueHeading REAL,
                timestamp INTEGER,
                specialManeuverIndicator INTEGER,
                spare TEXT,
                raimFlag INTEGER,
                syncState INTEGER, 
                slotTimeout INTEGER,
                subMessage INTEGER
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


def add_raw_data(con, s_type, s_data):
    """ Add a new sentence to the database raw table. """
    sql = '''
        insert into rawData(s_type, s_data, location_id)
        values(:s_type, :s_data, :location_id)
    '''
    try:
        sql_args = {'s_type': s_type, 's_data': s_data, 'location_id': 1}
        cur = con.cursor()
        cur.execute(sql, sql_args)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            '\n\nERROR during insert = {}\n\n'.format(error)
        )


def parse_ais_file(con, ais_file_name):
    print("type something we are paying you...")

    with open(ais_file_name) as file_in:
        try:
            for line in file_in:
                if line.startswith("!AI"):
                    try:
                        decoded = decode(line)
                        add_raw_data(con, decoded.msg_type, line)
                    except MissingMultipartMessageException as warning:
                        print("WARNING! {}".format(warning))

                    print(decoded)
                    print(line, end='')
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
