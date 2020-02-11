#!/usr/bin/python3
import sys
import os.path
import argparse
import sqlite3

def initialize_output_database(output_file):
    # Create new sqlite database to store exported data
    conn = sqlite3.connect(output_file)
    c = conn.cursor()
    c.execute('''
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
    ''')
    conn.commit()
    conn.close()

def process_databases(input_file, output_file):
    conn_ifile = sqlite3.connect(input_file)
    conn_ofile = sqlite3.connect(output_file)
    c_ifile = conn_ifile.cursor()
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
