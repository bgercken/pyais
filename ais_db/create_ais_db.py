import sys
import sqlite3
# from pathlib import Path
from sqlite3 import Error as SQL_Error, Connection
from dataclasses import dataclass


@dataclass
class AisMessageType:
    message_id: int
    name: str
    description: str


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
         create table if not exists aisMessageTypes
         (
             message_id integer not null,
             name text not null,
             description text not null        
         )
         """,
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
            create table if not exists rawFields
            (
                sentence_id integer not null
                    constraint rawFields_rawData_sentence_id_fk
                    references rawData,
                field1      text    not null,
                field2      text not null,
                field3      text not null,
                field4      text,
                field5      text,
                field6      text    not null,
                field7      text    not null
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


def add_ais_message_types(con):
    data = [AisMessageType(1, 'Position Report', 'Scheduled position report; Class A shipborne mobile equipment'),
            AisMessageType(2, 'Position Report',
                           'Assigned scheduled position report; Class A shipborne mobile equipment'),
            AisMessageType(3, 'Position Report',
                           'Special position report, response to interrogation; Class A shipborne mobile ' +
                           'equipment'),
            AisMessageType(4, 'Base station report', 'Position, UTC, date and current slot number of base station'),
            AisMessageType(5, 'Static and voyage related data', 'Scheduled static and voyage related vessel data ' +
                           'report, Class A shipborne mobile equipment'),
            AisMessageType(6, 'Binary addressed message', 'Binary data for addressed communication'),
            AisMessageType(7, 'Binary acknowledgement', 'Acknowledgement of received addressed binary data'),
            AisMessageType(8, 'Binary broadcast message', 'Binary data for broadcast communication'),
            AisMessageType(9, 'Standard SAR aircraft position report', 'Position report for airborne stations ' +
                           'involved in SAR operations only'),
            AisMessageType(10, 'UTC/date inquiry', 'Request UTC and date'),
            AisMessageType(11, 'UTC/date response', 'Current UTC and date if available'),
            AisMessageType(12, 'Addressed safety related message', 'Safety related data for addressed ' +
                           'communication'),
            AisMessageType(13, 'Safety related acknowledgement', 'Acknowledgement of received addressed safety ' +
                           'related message'),
            AisMessageType(14, 'Safety related broadcast message', 'Safety related data for broadcast ' +
                           'communication'),
            AisMessageType(15, 'Interrogation', 'Request for a specific message type can result in multiple ' +
                           'responses from one or several stations'),
            AisMessageType(16, 'Assignment mode command', 'Assignment of a specific report behaviour by ' +
                           'competent authority using a Base station'),
            AisMessageType(17, 'DGNSS broadcast binary message', 'DGNSS corrections provided by a base station'),
            AisMessageType(18, 'Standard Class B equipment position report', 'Standard position report for ' +
                           'Class B shipborne mobile equipment to be used instead of Messages 1, 2, 3'),
            AisMessageType(19, 'Extended Class B equipment position report', 'No longer required. Extended ' +
                           'position report for Class B shipborne mobile equipment; contains additional ' +
                           'static information'),
            AisMessageType(20, 'Data link management message', 'Reserve slots for Base station(s)'),
            AisMessageType(21, 'Aids-to-navigation report', 'Position and status report for aids-to-navigation'),
            AisMessageType(22, 'Channel management', 'Management of channels and transceiver modes by a ' +
                           'Base station'),
            AisMessageType(23, 'Group assignment command', 'Assignment of a specific report behaviour by ' +
                           'competent authority using a Base station to a specific group of mobiles'),
            AisMessageType(24, 'Static data report', 'Additional data assigned to an MMSI' +
                           'Part A: Name Part B: Static Data'),
            AisMessageType(25, 'Single slot binary message', 'Short unscheduled binary data transmission ' +
                           'Broadcast or addressed'),
            AisMessageType(26, 'Multiple slot binary message with Communications State', 'Scheduled binary data ' +
                           'transmission Broadcast or addressed'),
            AisMessageType(27, 'Position report for long range applications', 'Class A and Class B "SO" ' +
                           'shipborne mobile equipment outside base station coverage')]

    sql = '''
        insert into aisMessageTypes(message_id, name, description)
        values(:message_id, :name, :description)
    '''

    for o in data:
        print(o)
        try:
            sql_args = {'message_id': o.message_id, 'name': o.name, 'description': o.description}
            cur = con.cursor()
            cur.execute(sql, sql_args)
            con.commit()
        except SQL_Error as error:
            raise SystemExit('\n\nERROR inserting values - {}'.format(error))


def populate_ais_message_types(con):
    """ A function to populate the AIS message type table if it is empty. """
    message_id = 0
    sql = '''
        select message_id from aisMessageTypes where message_id = 27;
        '''
    try:
        cur = con.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            message_id = 0
        else:
            message_id = row[0]
    except SQL_Error as error:
        raise SystemExit('\n\nERROR while populating message table - {}\n\n'.format(error))

    print('message_id {}'.format(message_id))

    if message_id != 27:
        add_ais_message_types(con)


def main(database_name):
    conn = get_db_connection(database_name)
    create_tables(conn)
    populate_ais_message_types(conn)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage:", sys.argv[0], "/path/to/db_file")
        sys.exit(1)
    main(sys.argv[1])
