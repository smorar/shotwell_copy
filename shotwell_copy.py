#!/usr/bin/python3
import sys
import os.path
import argparse
import sqlite3

def initialize_output_database(output_file):
    # Create new sqlite database to store exported data
    conn = sqlite3.connect(output_file)
    c = conn.cursor()
    sql_create_phototable = """
    CREATE TABLE PhotoTable 
    (id INTEGER PRIMARY KEY, 
    filename TEXT UNIQUE NOT NULL, 
    width INTEGER, 
    height INTEGER, 
    filesize INTEGER, 
    timestamp INTEGER, 
    exposure_time INTEGER, 
    orientation INTEGER, 
    original_orientation INTEGER, 
    import_id INTEGER, 
    event_id INTEGER, 
    transformations TEXT, 
    md5 TEXT, 
    thumbnail_md5 TEXT, 
    exif_md5 TEXT, 
    time_created INTEGER, 
    flags INTEGER DEFAULT 0, 
    rating INTEGER DEFAULT 0, 
    file_format INTEGER DEFAULT 0, 
    title TEXT, 
    backlinks TEXT, 
    time_reimported INTEGER, 
    editable_id INTEGER DEFAULT -1, 
    metadata_dirty INTEGER DEFAULT 0, 
    developer TEXT, 
    develop_shotwell_id INTEGER DEFAULT -1, 
    develop_camera_id INTEGER DEFAULT -1, 
    develop_embedded_id INTEGER DEFAULT -1, 
    comment TEXT)
    """
    c.execute(sql_create_phototable)
    conn.commit()
    sql_create_eventtable = """
    CREATE TABLE EventTable 
    (id INTEGER PRIMARY KEY, 
    name TEXT, 
    primary_photo_id INTEGER, 
    time_created INTEGER,
    primary_source_id TEXT,
    comment TEXT)
    """
    c.execute(sql_create_eventtable)
    conn.commit()
    conn.close()

def process_databases(input_file, output_file):
    ifile_uri = ('file:{0}?mode=ro').format(input_file)
    conn_ifile = sqlite3.connect(ifile_uri, uri=True)
    conn_ofile = sqlite3.connect(output_file)
    c_ifile = conn_ifile.cursor()
    c_ifile2 = conn_ifile.cursor()
    c_ofile = conn_ofile.cursor()

    sql_select_transformations = "SELECT * FROM PhotoTable WHERE transformations IS NOT NULL"
    for row in c_ifile.execute(sql_select_transformations):
        sql_record_exists = ('SELECT EXISTS(SELECT 1 FROM PhotoTable WHERE filename="{0}")').format(row[1])
        c_ofile.execute(sql_record_exists)
        if ((c_ofile.fetchone()[0]) == 1):
            # update record
            transformations_str = row[11]
            transformations_str = transformations_str.replace("\\n", "'||char(10)||'")
            sql_update_transformations = ('UPDATE PhotoTable SET transformations = "{0}" WHERE filename="{1}"').format(transformations_str, row[1])
            c_ofile.execute(sql_update_transformations)
            conn_ofile.commit()
        else:
            # copy record
            lst = list(row)
            lst[0] = None
            record_str = tuple(lst).__str__()
            record_str = record_str.replace("None", "NULL")
            record_str = record_str.replace("\\n", "'||char(10)||'")
            sql_copy_record = ("INSERT INTO PhotoTable VALUES {0}").format(record_str)
            c_ofile.execute(sql_copy_record)
            conn_ofile.commit()

    sql_select_ratings = "SELECT * FROM PhotoTable WHERE rating > 0"
    for row in c_ifile.execute(sql_select_ratings):
        sql_record_exists = ('SELECT EXISTS(SELECT 1 FROM PhotoTable WHERE filename="{0}")').format(row[1])
        c_ofile.execute(sql_record_exists)
        if ((c_ofile.fetchone()[0]) == 1):
            # update record
            sql_update_ratings = ('UPDATE PhotoTable SET rating={0} WHERE filename="{1}"').format(row[17], row[1])
            c_ofile.execute(sql_update_ratings)
            conn_ofile.commit()
        else:
            # copy record
            lst = list(row)
            lst[0] = None
            record_str = tuple(lst).__str__()
            record_str = record_str.replace("None", "NULL")
            record_str = record_str.replace("\\n", "'||char(10)||'")
            sql_copy_record = ("INSERT INTO PhotoTable VALUES {0}").format(record_str)
            c_ofile.execute(sql_copy_record)
            conn_ofile.commit()
    
    processed_inputeventids = []
    sql_iterate_itable = "SELECT filename, exposure_time, orientation, import_id, event_id FROM PhotoTable"
    for row in c_ifile.execute(sql_iterate_itable):
        sql_record_exists = ('SELECT EXISTS(SELECT 1 FROM PhotoTable WHERE filename="{0}")').format(row[0])
        c_ofile.execute(sql_record_exists)
        if ((c_ofile.fetchone()[0]) == 1):
            print("matched 1")
            # matched file in input and output database
            sql_update_data = ('UPDATE PhotoTable SET exposure_time={0}, orientation={1}, import_id={2} WHERE filename="{3}"').format(row[1], row[2], row[3], row[0])
            c_ofile.execute(sql_update_data)
            conn_ofile.commit()
            sql_get_current_oevent_id = ('SELECT event_id FROM PhotoTable WHERE filename="{0}"').format(row[0])
            c_ofile.execute(sql_get_current_oevent_id)
            current_oevent_id = c_ofile.fetchone()[0]
            # TODO - group photos by event
            if processed_inputeventids.count(row[4]) == 0:
                processed_inputeventids.append(row[4])
                sql_get_input_event = ('SELECT * FROM EventTable WHERE id={0}').format(row[4])
                c_ifile2.execute(sql_get_input_event)
                event_data = c_ifile2.fetchone()
                sql_check_output_event = ('SELECT EXISTS(SELECT 1 FROM EventTable WHERE time_created={0})').format(event_data[3])
                c_ofile.execute(sql_check_output_event)
                if ((c_ofile.fetchone()[0]) == 1):
                    # Event exists. Update all pictures with this event id
                    sql_get_output_eventid = ('SELECT id FROM EventTable WHERE time_created={0}').format(event_data[3])
                    c_ofile.execute(sql_get_output_eventid)
                    output_eventid = c_ofile.fetchone()[0]
                    sql_update_event_id = ('UPDATE PhotoTable SET event_id={0} WHERE event_id={1}').format(output_eventid, current_oevent_id)
                    c_ofile.execute(sql_update_event_id)
                    conn_ofile.commit()
                else:
                    # Event doesn't exist. Create and then update all pictures with created id
                    print("event doesn't exist")
                    event = list(event_data)
                    event[0] = None
                    event_str = tuple(event).__str__()
                    event_str = event_str.replace("None", "NULL")
                    sql_copy_event = ("INSERT INTO EventTable VALUES {0}").format(event_str)
                    c_ofile.execute(sql_copy_event)
                    conn_ofile.commit()
                    sql_obtain_id = "SELECT last_insert_rowid()"
                    c_ofile.execute(sql_obtain_id)
                    output_eventid = c_ofile.fetchone()[0]
                    sql_update_event_id = ('UPDATE PhotoTable SET event_id={0} WHERE event_id={1}').format(output_eventid, current_oevent_id)
                    c_ofile.execute(sql_update_event_id)
                    conn_ofile.commit()

    conn_ifile.close()
    conn_ofile.close()

def main():
    parser = argparse.ArgumentParser(prog="shotwell_copy")
    parser.add_argument('-i', '--input', help="Input file")
    parser.add_argument('-o', '--output', help="Output file")
    args = parser.parse_args()
    
    if ((args.output != None)):
        output_file = None
        input_file = None
        
        if os.path.isfile(args.output):
            output_file = args.output
        else:
            print("Output file does not exist, creating new sqlite database")
            initialize_output_database(args.output)
            output_file = args.output

        if args.input == None:
            if os.path.isfile(os.path.expanduser("~/.local/share/shotwell/data/photo.db")):
                print("Input file not specified, using default shotwell database")
                input_file = os.path.expanduser("~/.local/share/shotwell/data/photo.db")
            else:
                print("Please specify valid input file")
                parser.print_help()
                sys.exit()
        else:
            if os.path.isfile(args.input):
                input_file = args.input
            else:
                print("Please specify valid input file")
                parser.print_help()
                sys.exit()
            
        print(("Input file: {0}").format(input_file))
        print(("Output file: {0}").format(output_file))

        if input_file != None and output_file != None:
            process_databases(input_file, output_file)
        else:
            print("Please specify valid input and output files")
            parser.print_help()
            sys.exit()
    else:
        print("Please specify valid input and output files")
        parser.print_help()
        sys.exit()

if __name__ == '__main__':
    main()
