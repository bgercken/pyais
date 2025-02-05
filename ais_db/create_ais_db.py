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


@dataclass
class APositionReportMessage:
    field: int
    parameter: str
    bits: int
    value: str
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
            create table if not exists baseStationReport
            (
                sentence_id integer not null,
                msg_type    integer not null,
                repeat      integer,
                mmsi        integer,
                year        integer,
                month       integer,
                day         integer,
                hour        integer,
                minute      integer,
                second      integer,
                accuracy    integer,
                lon         real,
                lat         real,
                epfd        text,
                spare_1     text,
                raim        integer,
                radio       integer
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
        """,
        """
            create table if not exists extendedClassBPositionReport
            (
                sentence_id  integer not null,
                msg_type     integer not null,
                repeat       integer,
                reserved_1   integer,
                speed        real,
                accuracy     integer,
                lon          real,
                lat          real,
                course       real,
                heading      integer,
                second       integer,
                reserved_2   integer,
                shipname     text,
                ship_type    integer,
                to_bow       integer,
                to_stern     integer,
                to_port      integer,
                to_starboard integer,
                epfd         integer,
                raim         integer,
                dte          integer,
                assigned     integer,
                spare_1      text
            )
        """,
        """
            create table if not exists staticDataReport
            (
                report_id   integer
                    constraint staticReportData_pk
                        primary key autoincrement,
                sentence_id integer not null,
                msg_type    integer not null,
                repeat      integer,
                mmsi        integer not null,
                partno      integer,
                child_id    integer
            )
        """,
        """
            create table if not exists staticDataReportA
            (
                sentence_id integer not null,
                parent_id   integer not null,
                mmsi        integer not null, 
                shipname    text,
                spare_1     text
            )
        """,
        """
            create table if not exists staticDataReportB
            (
                sentence_id  integer not null,
                parent_id    integer not null,
                mmsi         integer not null,
                ship_type    integer,
                vendorid     text,
                model        integer,
                serial       integer,
                callsign     text,
                to_bow       integer,
                to_stern     integer,
                to_port      integer,
                to_starboard integer,
                spare_1      text
            )
        """,
        """
            create table if not exists classAStaticVoyageData
            (
                sentence_id  integer not null,
                msg_type     integer not null,
                mmsi         integer,
                ais_version  integer,
                imo          integer,
                callsign     text,
                shipname     text,
                ship_type    integer,
                to_bow       integer,
                to_stern     integer,
                to_port      integer,
                to_starboard integer,
                epfd         integer,
                month        integer,
                day          integer,
                hour         integer,
                minute       integer,
                draught      real,
                destination  text,
                dte          integer,
                spare_1      text
            )
        """,
        """
            create table if not exists aidToNavigationReport
            (
                sentence_id  integer not null,
                msg_type     integer not null,
                repeat       integer,
                mmsi         integer,
                aid_type     integer,
                name         text,
                accuracy     integer,
                lon          real,
                lat          real,
                to_bow       integer,
                to_stern     integer,
                to_port      integer,
                to_starboard integer,
                epfd         integer,
                second       integer,
                off_position integer,
                reserved_1   integer,
                raim         integer,
                virtual_aid  integer,
                assigned     integer,
                spare_1      text,
                name_ext     text
            )
        """,
        """
            create table if not exists standardSARAircraftPositionReport
            (
                sentence_id integer not null,
                msg_type    integer not null,
                repeat      integer,
                mmsi        integer,
                alt         integer,
                speed       real,
                accuracy    integer,
                lon         real,
                lat         real,
                course      real,
                second      integer,
                reserved_1  integer,
                dte         integer,
                spare_1     text,
                assigned    integer,
                raim        integer,
                radio       integer
            )
        """,
        """
            create table if not exists classAPositionReportMessages
            (
                message_id  integer not null
                    constraint classAPositionReportMessages_pk
                        primary key autoincrement,
                field       integer not null,
                parameter   text    not null,
                bits        integer not null,
                value       text not null,
                description text    not null
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


def add_a_position_report_messages(con):
    data = [
        APositionReportMessage(1, 'Message ID', 6, '1', 'Identifier for this message (1, 2 or 3)'),
        APositionReportMessage(1, 'Message ID', 6, '2', 'Identifier for this message (1, 2 or 3)'),
        APositionReportMessage(1, 'Message ID', 6, '3', 'Identifier for this message (1, 2 or 3)'),
        APositionReportMessage(2, 'Repeat indicator', 2, '0',
                               'Default. Used by the repeater to indicate how many times a message ' +
                               'has been repeated. (0-3)'),
        APositionReportMessage(2, 'Repeat indicator', 2, '1', 'Repeat once.'),
        APositionReportMessage(2, 'Repeat indicator', 2, '2', 'Repeat twice.'),
        APositionReportMessage(2, 'Repeat indicator', 2, '3', 'Do not repeat anymore.'),
        APositionReportMessage(3, 'User ID', 30, 'N/A', 'MMSI number'),
        APositionReportMessage(4, 'Navigation Status', 4, '0', 'under way using engine'),
        APositionReportMessage(4, 'Navigation Status', 4, '1', 'at anchor'),
        APositionReportMessage(4, 'Navigation Status', 4, '2', 'not under command'),
        APositionReportMessage(4, 'Navigation Status', 4, '3', 'restricted maneuverability'),
        APositionReportMessage(4, 'Navigation Status', 4, '4', 'constrained by her draught'),
        APositionReportMessage(4, 'Navigation Status', 4, '5', 'moored'),
        APositionReportMessage(4, 'Navigation Status', 4, '6', 'aground'),
        APositionReportMessage(4, 'Navigation Status', 4, '7', 'engaged in fishing'),
        APositionReportMessage(4, 'Navigation Status', 4, '8', 'under way sailing'),
        APositionReportMessage(4, 'Navigation Status', 4, '9',
                               'reserved for future amendment of navigational status for ships carrying DG, HS, ' +
                               'or MP, or IMO hazard or pollutant category C, high speed craft (HSC)'),
        APositionReportMessage(4, 'Navigation Status', 4, '10',
                               'reserved for future amendment of navigational status for ships carrying ' +
                               'dangerous goods (DG), harmful substances (HS) or marine pollutants (MP), ' +
                               'or IMO hazard or pollutant category A, wing in ground (WIG)'),
        APositionReportMessage(4, 'Navigation Status', 4, '11', 'power-driven vessel towing astern (regional use)'),
        APositionReportMessage(4, 'Navigation Status', 4, '12',
                               'power-driven vessel pushing ahead or towing alongside (regional use)'),
        APositionReportMessage(4, 'Navigation Status', 4, '13', 'reserved for future use'),
        APositionReportMessage(4, 'Navigation Status', 4, '14', 'AIS-SART (active), MOB-AIS, EPIRB-AIS'),
        APositionReportMessage(4, 'Navigation Status', 4, '15',
                               'undefined = default (also used by AIS-SART, MOB-AIS and EPIRB-AIS under test)'),
        APositionReportMessage(5, 'Rate of turn', 8, '0 to +126', 'turning right at up to 708 deg per min or higher'),
        APositionReportMessage(5, 'Rate of turn', 8, '0 to -126', 'turning left at up to 708 deg per min or higher'),
        APositionReportMessage(5, 'Rate of turn', 8, '+127', 'turning right at more than 5 deg per 30 s'),
        APositionReportMessage(5, 'Rate of turn', 8, '-127', 'turning left at more than 5 deg per 30 s'),
        APositionReportMessage(5, 'Rate of turn', 8, '-128', '(80 hex) indicates no turn information ' +
                               'available (default)'),
        APositionReportMessage(6, 'SOG', 10, '0-102.1 knots', 'Speed over ground in 1/10 knot steps'),
        APositionReportMessage(6, 'SOG', 10, '102.2', '102.2 knots or higher'),
        APositionReportMessage(6, 'SOG', 10, '102.3', 'not available (range of parameter is 0-102.3)'),
        APositionReportMessage(7, 'Position Accuracy', 1, '0', 'low > 10m (default)'),
        APositionReportMessage(7, 'Position Accuracy', 1, '1', 'high <= 10m'),
        APositionReportMessage(8, 'Longitude', 28, '+/-180 deg', 'Longitude in 1/10,000 min East=positive, ' +
                               'W=negative'),
        APositionReportMessage(8, 'Longitude', 28, '181', 'not available = default (6791AC0h)'),
        APositionReportMessage(9, 'Latitude', 27, '+/-90 deg', 'Latitude in 1/10,000 min North=positive, ' +
                               'South=negative '),
        APositionReportMessage(9, 'Latitude', 27, '91', 'not available = default (3412140h)'),
        APositionReportMessage(10, 'COG', 12, '0-3599', 'Course over ground in 1/10'),
        APositionReportMessage(10, 'COG', 12, '3600', 'not available = default (E10h)'),
        APositionReportMessage(10, 'COG', 12, '3601-4095', 'should not be used - out of range'),
        APositionReportMessage(11, 'True heading', 9, '0-350', 'Degrees'),
        APositionReportMessage(11, 'True heading', 9, '511', 'not available = default'),
        APositionReportMessage(12, 'Time stamp', 6, '0-59', 'UTC second when the report was generated by the ' +
                               'electronic position system (EPFS)'),
        APositionReportMessage(12, 'Time stamp', 6, '60', 'Time stamp not available = default'),
        APositionReportMessage(12, 'Time stamp', 6, '61', 'If positioning system is in manual input mode'),
        APositionReportMessage(12, 'Time stamp', 6, '62', 'If electronic position fixing system operates in ' +
                               'estimated (dead reckoning) mode'),
        APositionReportMessage(12, 'Time stamp', 6, '63', 'If the positioning system is inoperative'),
        APositionReportMessage(13, 'special maneuver indicator', 2, '0', 'not available = default'),
        APositionReportMessage(13, 'special maneuver indicator', 2, '1', 'not engaged in special maneuver'),
        APositionReportMessage(13, 'special maneuver indicator', 2, '2', 'engaged in special maneuver'),
        APositionReportMessage(14, 'Spare', 3, '0', 'Not used - should be set to zero'),
        APositionReportMessage(15, 'RAIM-flag', 1, '0', 'RAIM not in use = default (Receiver Autonomous ' +
                               'Integrity Monitoring)'),
        APositionReportMessage(15, 'RAIM-flag', 1, '1', 'RAIM in use'),
        APositionReportMessage(16, 'Communication state', 19, 'Value', 'See communicationStateMessages table')
    ]

    sql = '''
        insert into classAPositionReportMessages(
            field, parameter, bits, value, description
        )
        values(
            :field, :parameter, :bits, :value, :description
        )
    '''
    for o in data:
        print(o)
        try:
            sql_args = {
                'field': o.field, 'parameter': o.parameter, 'bits': o.bits,
                'value': o.value, 'description': o.description
            }
            cur = con.cursor()
            cur.execute(sql, sql_args)
            con.commit()
        except SQL_Error as error:
            raise SystemExit('\n\nERROR inserting values - {}'.format(error))


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


def populate_a_position_report_messages(con):
    """ A function to check for and then call populate a position report messages if needed. """

    sql = '''
        select count(*) from classAPositionReportMessages;
    '''
    try:
        cur = con.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            message_count = 0
        else:
            message_count = row[0]
    except SQL_Error as error:
        raise SystemExit('\n\nERROR while populating message table - {}\n\n'.format(error))

    if message_count == 0:
        add_a_position_report_messages(con)


def main(database_name):
    conn = get_db_connection(database_name)
    create_tables(conn)
    populate_ais_message_types(conn)
    populate_a_position_report_messages(conn)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage:", sys.argv[0], "/path/to/db_file")
        sys.exit(1)
    main(sys.argv[1])
