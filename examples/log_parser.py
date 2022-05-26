"""
  AIS parse log to DB.
"""
import os
import sys
import sqlite3
from sqlite3 import Error as SQL_Error, Connection

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
            CREATE TABLE IF NOT EXISTS classAPositionReport (
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
            print("executing: {}\n".format(sql))
            cur.execute(sql)
        con.commit()
    except SQL_Error as error:
        raise SystemExit(
            "\n\nERROR while creating table(s) - {}\n\n".format(error)
        )


def parse_ais_file(con, ais_file_name):
    print("type something we are paying you...")


def main(ais_file_name):
    """ Run as a program. """
    conn = get_db_connection(DB_FILE)
    if conn is not None:
        create_tables(conn)
        if os.path.exists(ais_file_name):
            parse_ais_file(conn, ais_file_name)
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
