'''
Created on Nov 4, 2014

@author: julien
'''
# import modules
import sys
import psycopg2
from random import uniform
#import matplotlib.pyplot as plt

#Constants
db_name = 'julien'
user_name = 'julien'
host = 'localhost'
pwd = 'rr33'
scheme_name = 'public'
table_name_in = "tracts_sb"
output_table_name = 'tracts_sb_dots'
SRID = 4269
ratio_pop = 100
 
def query_postgis(sql_string, data=None):
    #Create the cursor
    cur = conn.cursor()
    # execute the query
    if data:
        cur.execute(sql_string, data)
    else:
        cur.execute(sql_string)
    # store the result of the query into Tuple
    rows = cur.fetchall()
    # closes the connection
    cur.close()
    #return the data
    return rows

def create_table(field_tuple):
    #Create the query
    sql_create = "CREATE TABLE %s.%s (" %(scheme_name, output_table_name)
    sql_fields = ", ".join(field_tuple)
    sql_owner = ") WITH (OIDS=FALSE); ALTER TABLE public.%s OWNER TO julien; CREATE INDEX ind_geom_point ON public.tracts_sb_dots USING gist(geom);" %(output_table_name)
    sql_query = ''.join([sql_create,sql_fields,sql_owner])
    
    #Execute the query
    cur = conn.cursor()
    try:
        cur.execute(sql_query)
    except:
        conn.rollback()
        sql =  "DROP TABLE %s.%s" %(scheme_name, output_table_name)
        cur.execute(sql);
        cur.execute(sql_query)
    conn.commit()
    cur.close()

def insert_many(value_dict):  
    cur = conn.cursor()
    cur.executemany("""INSERT INTO tracts_SB_dots(geoid,namelsad,geom) VALUES (%(geoid)s, %(tract_name)s, ST_GeomFromText(%(geom)s, 4269))""", value_dict)
    #cur.execute("INSERT INTO tracts_SB_dots(geoid,namelsad,geom) VALUES ('123','dede',ST_SetSRID(ST_MakePoint(-119.880634657, 34.4170923867),4269));")
    conn.commit()
    cur.close()
    

def point_generator(nbr_dots, lg_min, lg_max, la_min, la_max):
    point_dict = {}
    if nbr_dots < 1:
        point_dict = None
    else:
        i = nbr_dots
        while i > 0:
        #for i in range(nbr_dots):
            #Create the point from a uniform distribution 
            sample_point = (uniform(long_min, long_max),uniform(lat_min, lat_max))
            #Check if the point is within the tract:
            query_test_in = """SELECT ST_WITHIN(ST_GeomFromText('POINT(%(lat)s %(lon)s)', %(SRID)s), %(field_geom)s) FROM %(table)s WHERE %(field_id)s = %(value_id)s;"""
            #SELECT ST_WITHIN(ST_GeomFromText('POINT(34.42 -119.82)', 4269),tracts_sb.geom) from tracts_sb WHERE geoid10 = '06083980000';
            data_test_in = {'lat':sample_point[0], 'lon':sample_point[1], 'SRID':SRID, 'table':table_name_in,'field_geom':"%s.geom" %(table_name_in), 'field_id': "geoid10", 'value_id': "'"+dict_row['geoid']+"'"}
            query = query_test_in % data_test_in
            test = query_postgis(query)
            if test[0][0] is True:
                #Add the point location to the dictionary
                point_dict[i] = "POINT(%s %s)" %(sample_point[0], sample_point[1])
                #Update the dot count
                i = i-1
            else:
                continue
    #return the dictionary to main 
    return point_dict


#==========================================================================================================


# create connection to database
try:
    conn = psycopg2.connect("dbname= %s host=%s user=%s password=%s" %(db_name, host, user_name, pwd))   
except:
    print "I am unable to connect to the database"
    exit

# create output table
fields = ('gid SERIAL PRIMARY KEY', 'geoid character varying(11)', 'namelsad character varying(20)', 'geom geometry(Point,4269)')
create_table(fields)

query = "SELECT gid, geoid10, namelsad10, dp0010001/%s AS nbr_dots, BOX2D(geom) as BBOX from %s.%s;" %(ratio_pop, scheme_name, table_name_in)
data_all = query_postgis(query)
data_dict = {}
for data in data_all:
    dict_row = {}
    #Get the tract ids:
    dict_row['keyid'] = data[0]
    dict_row['geoid'] = data[1]
    dict_row['tract_name'] = data[2]
    dict_row['dots_number'] = int(data[3])
    #Get the bounding box
    bbox = data[4][4:-1]
    corners = bbox.split(',')
    long_min,lat_min = map(float,corners[0].split(' '))
    long_max,lat_max = map(float,corners[1].split(' '))
    #Generate the point coordinates
    points = point_generator(dict_row['dots_number'], long_min, long_max, lat_min, lat_max)
    
    if points:
        #Create the dictionary tuple for the insertion
        data_insert = []
        for v in points.itervalues():
            dict_row['geom'] = v
            data_insert.append(dict_row.copy())
        
        #data_dict[data[0]] = 
        insert_many(tuple(data_insert))

#close the connection
conn.close()
print "All Done!"