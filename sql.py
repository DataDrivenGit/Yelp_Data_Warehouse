import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_tip_table_drop = "DROP TABLE IF EXISTS staging_tip"
staging_user_table_drop = "DROP TABLE IF EXISTS staging_user"
staging_review_table_drop = "DROP TABLE IF EXISTS staging_review"
staging_business_table_drop = "DROP TABLE IF EXISTS staging_business"
dim_users_table_drop = "DROP TABLE IF EXISTS dim_users"
dim_location_table_drop = "DROP TABLE IF EXISTS dim_location"
dim_business_table_drop = "DROP TABLE IF EXISTS dim_business"
dim_datetime_table_drop = "DROP TABLE IF EXISTS dim_datetime"
fact_tip_table_drop = "DROP TABLE IF EXISTS fact_tip"
fact_review_table_drop = "DROP TABLE IF EXISTS fact_review"

# CREATE TABLES
staging_tip_table_create= ("""
                            CREATE TABLE IF NOT EXISTS staging_tip (
                            ST_user_id VARCHAR(256),
                            ST_business_id VARCHAR(256),
                            text VARCHAR(65535),
                            ST_date TIMESTAMP,
                            compliment_count INTEGER )
                            """)
            
staging_user_table_create= ("""
                            CREATE TABLE IF NOT EXISTS staging_user (
                            SU_user_id VARCHAR(256),
                            name VARCHAR(256),
                            review_count INTEGER,
                            average_stars REAL,
                            yelping_since TIMESTAMP )
                            """)

staging_review_table_create= ("""
                            CREATE TABLE IF NOT EXISTS staging_review (
                            SR_review_id VARCHAR(256),
                            SR_user_id VARCHAR(256),
                            SR_business_id VARCHAR(256),
                            stars INTEGER,
                            useful INTEGER ,
                            funny INTEGER,
                            cool INTEGER,
                            text VARCHAR(65535),
                            SR_date TIMESTAMP )
                            """)

staging_business_table_create= ("""
                            CREATE TABLE IF NOT EXISTS staging_business (
                            SB_business_id VARCHAR(256),
                            name VARCHAR(256),
                            address VARCHAR(256),
                            city VARCHAR(256),
                            state VARCHAR(32) ,
                            postal_code VARCHAR(32),
                            latitude REAL,
                            longitude REAL,
                            stars REAL, 
                            review_count INTEGER,
                            is_open INTEGER,
                            BusinessAcceptsCreditCards VARCHAR(256), 
                            BikeParking VARCHAR(256),
                            GoodForKids VARCHAR(256),
                            BusinessParking VARCHAR(256),
                            ByAppointmentOnly VARCHAR(256),
                            RestaurantsPriceRange2 VARCHAR(256),
                            categories VARCHAR(1024),
                            Monday VARCHAR(256),
                            Tuesday VARCHAR(256),
                            Wednesday VARCHAR(256),
                            Thursday VARCHAR(256),
                            Friday VARCHAR(256),
                            Saturday VARCHAR(256),
                            Sunday VARCHAR(256) )
                            """)
                
dim_users_table_create = ("""
                   CREATE TABLE IF NOT EXISTS dim_users (
                        DU_user_id VARCHAR(256),
                        name VARCHAR(256),
                        yelping_since TIMESTAMP,
                        week_since_active INTEGER,
                        PRIMARY KEY (DU_user_id)
                   )
                    """)

dim_location_table_create = ("""
                   CREATE TABLE IF NOT EXISTS dim_location (
                    DL_location_id VARCHAR(256) NOT NULL,
                    postal_code VARCHAR(128) NOT NULL,
                    city VARCHAR(128),
                    state VARCHAR(128) ,
                    PRIMARY KEY (DL_location_id)
                   )
                    """)

dim_business_table_create = ("""
                   CREATE TABLE IF NOT EXISTS dim_business (
                    DB_business_id VARCHAR(256) NOT NULL,
                    name VARCHAR(256) NOT NULL,
                    is_open INTEGER,
                    address VARCHAR(1024),
                    BusinessAcceptsCreditCards VARCHAR(256), 
                    BikeParking VARCHAR(256),
                    GoodForKids VARCHAR(256),
                    BusinessParking VARCHAR(256),
                    ByAppointmentOnly VARCHAR(256),
                    RestaurantsPriceRange2 VARCHAR(256),
                    categories VARCHAR(1024),
                    PRIMARY KEY (DB_business_id)
                   )
                    """)

dim_datetime_table_create = ("""
                    CREATE TABLE IF NOT EXISTS dim_datetime
                    (
                        DD_datetime TIMESTAMP NOT NULL,
                        hour INTEGER NOT NULL,
                        minute INTEGER NOT NULL,
                        day INTEGER NOT NULL,
                        month INTEGER NOT NULL,
                        year INTEGER NOT NULL,
                        quarter INTEGER NOT NULL,
                        weekday INTEGER NOT NULL,
                        yearday INTEGER NOT NULL,
                        PRIMARY KEY (DD_datetime)
                    )
                    """)

fact_tip_table_create = ("""
                    CREATE TABLE IF NOT EXISTS fact_tip
                    (
                    FT_tip_id VARCHAR(32) NOT NULL,
                    FT_user_id VARCHAR(256) NOT NULL,
                    FT_business_id VARCHAR(256) NOT NULL,
                    FT_location_id VARCHAR(256),
                    text VARCHAR(65535),
                    FT_datetime TIMESTAMP,
                    compliment_count INTEGER,
                    PRIMARY KEY (FT_tip_id)
                    )
                    """)

fact_review_table_create = ("""
                    CREATE TABLE IF NOT EXISTS fact_review
                    (
                    FR_review_id VARCHAR(256) NOT NULL,
                    FR_user_id VARCHAR(256) NOT NULL,
                    FR_business_id VARCHAR(256) NOT NULL,
                    FR_location_id VARCHAR(256),
                    stars DOUBLE PRECISION,
                    useful INTEGER,
                    funny INTEGER,
                    cool INTEGER,
                    text VARCHAR(65535),
                    FR_datetime TIMESTAMP,
                    PRIMARY KEY (FR_review_id)
                    )
                    """)
 

# STAGING TABLES
staging_tip_copy = ("""
                    copy staging_tip from {}
                    iam_role {}
                    json{};
                    """).format(config['S3']['TIP_DATA'] ,\
                                config['IAM_ROLE']['ARN'], config['S3']['TIP_JSON_PATH'])

staging_user_copy = ("""
                    copy staging_user from {}
                    iam_role {}
                    delimiter ','
                    """).format(config['S3']['USER_DATA'] ,\
                                config['IAM_ROLE']['ARN'])

staging_review_copy = ("""
                    copy staging_review from {}
                    iam_role {}
                    json{};
                    """).format(config['S3']['REVIEW_DATA'] ,\
                                config['IAM_ROLE']['ARN'], config['S3']['REVIEW_JSON_PATH'])
            

staging_business_copy = ("""
                    copy staging_business from {}
                    iam_role {}
                    json{};
                    """).format(config['S3']['BUSINESS_DATA'] ,\
                    config['IAM_ROLE']['ARN'], config['S3']['BUSINESS_JSON_PATH'])

# DIMENTION TABLES
dim_users_table_insert = ("""
                        INSERT INTO dim_users(
                        DU_user_id ,
                        name ,
                        yelping_since ,
                        week_since_active) 

                        SELECT 
                        SU_user_id as DU_user_id,
                        name,
                        yelping_since,
                        datediff(week, yelping_since , current_date)
                        FROM staging_user
                        where SU_user_id IS NOT NULL
                        """)

dim_location_table_insert = ("""
                        INSERT INTO dim_location(
                        DL_location_id,
                        postal_code ,
                        city ,
                        state                 
                        )
                        
                        SELECT 
                        DISTINCT md5(postal_code || city || state) as DL_location_id,
                        postal_code AS postal_code ,  
                        city,
                        state 
                        FROM staging_business
                        where postal_code IS NOT NULL
                        """)

dim_business_table_insert = ("""
                        INSERT INTO dim_business(
                        DB_business_id,
                        name, 
                        is_open ,
                        address ,
                        BusinessAcceptsCreditCards, 
                        BikeParking ,
                        GoodForKids ,
                        BusinessParking ,
                        ByAppointmentOnly ,
                        RestaurantsPriceRange2 ,
                        categories                        
                         )
                        
                        SELECT 
                        SB_business_id as DB_business_id,
                        name,
                        is_open ,
                        address ,
                        BusinessAcceptsCreditCards, 
                        BikeParking ,
                        GoodForKids ,
                        BusinessParking ,
                        ByAppointmentOnly ,
                        RestaurantsPriceRange2 ,
                        categories
                        FROM staging_business
                        """)

dim_datetime_table_insert = ("""
                        INSERT INTO dim_datetime(
                        DD_datetime ,
                        hour ,
                        minute ,
                        day ,
                        month ,
                        year ,
                        quarter ,
                        weekday ,
                        yearday 
                        )
                        
                        SELECT 
                        a.datetime as DD_datetime,
                        extract(hour from a.datetime ) as hour,
                        extract(minute from a.datetime ) as minute,
                        extract( day from a.datetime ) as day,
                        extract( month from a.datetime ) as month,
                        extract( year from  a.datetime ) as year,
                        extract( qtr  from  a.datetime ) as quarter,
                        extract( weekday from a.datetime ) as weekday,
                        extract( yearday from a.datetime ) as yearday
                        FROM (
                            SELECT yelping_since as datetime
                            from staging_user
                            group by yelping_since
                            UNION
                            select SR_date as datetime
                            from staging_review
                            group by SR_date
                            UNION
                            select ST_date as datetime
                            from staging_tip
                            group by ST_date
                        ) a
                        where a.datetime is not null

                        """)

# FACT TABLES
fact_tip_table_insert = ("""
                        INSERT INTO fact_tip(
                        FT_tip_id,
                        FT_user_id,
                        FT_business_id,
                        FT_location_id,
                        text,
                        FT_datetime,
                        compliment_count                
                        )
                        
                        SELECT 
                        DISTINCT md5(ST_user_id || ST_business_id || ST_date) as FT_tip_id,
                        T.ST_user_id AS FT_user_id,
                        T.ST_business_id as FT_business_id,
                        L.DL_location_id as FT_location_id,
                        T.text,
                        T.ST_date as FT_datetime,
                        T.compliment_count
                        FROM staging_tip AS T
                        inner JOIN staging_business AS B 
                        ON T.ST_business_id = B.SB_business_id
                        INNER JOIN dim_location AS L 
                        ON B.postal_code = L.postal_code AND B.city = L.city AND B.state = L.state
                        """)

fact_review_table_insert = ("""
                        INSERT INTO fact_review(
                        FR_review_id,
                        FR_user_id,
                        FR_business_id,
                        FR_location_id,
                        stars,
                        useful,
                        funny,
                        cool,
                        text,
                        FR_datetime                
                        )
                        
                        SELECT 
                        SR_review_id AS FR_review_id,
                        SR_user_id AS FR_user_id,
                        SR_business_id AS FR_business_id,
                        DL_location_id AS FR_location_id,
                        R.stars,
                        R.useful,
                        R.funny,
                        R.cool,
                        R.text,
                        SR_date AS FR_datetime
                        FROM staging_review AS R
                        inner JOIN staging_business AS B 
                        ON R.SR_business_id = B.SB_business_id
                        INNER JOIN dim_location AS L 
                        ON B.postal_code = L.postal_code AND B.city = L.city AND B.state = L.state
                        """)

# TEST
fact_review_table_test = "SELECT * FROM fact_review LIMIT 5"
fact_tip_table_test     = "SELECT * FROM fact_tip LIMIT 5"
dim_datetime_table_test     = "SELECT * FROM dim_datetime LIMIT 5"
dim_business_table_test   = "SELECT * FROM dim_business LIMIT 5"
dim_location_table_test      = "SELECT * FROM dim_location LIMIT 5"
dim_users_table_test      = "SELECT * FROM dim_users LIMIT 5"


# QUERY LISTS

drop_table_queries = [ staging_tip_table_drop,staging_user_table_drop,staging_review_table_drop,
                        staging_business_table_drop,dim_users_table_drop, dim_location_table_drop, 
                        dim_business_table_drop, dim_datetime_table_drop, fact_tip_table_drop,fact_review_table_drop]

create_table_queries = [ staging_tip_table_create, staging_user_table_create, staging_review_table_create,
                         staging_business_table_create, dim_users_table_create, dim_location_table_create,
                         dim_business_table_create, dim_datetime_table_create, fact_tip_table_create,fact_review_table_create ]

copy_table_queries = [ staging_tip_copy, staging_user_copy, staging_review_copy, staging_business_copy]

insert_Dim_queries = [ dim_users_table_insert, dim_location_table_insert ,dim_business_table_insert, dim_datetime_table_insert  ]

insert_fact_queries = [fact_review_table_insert , fact_tip_table_insert ]

test_queries = [fact_review_table_test, fact_tip_table_test, dim_datetime_table_test , dim_business_table_test, dim_location_table_test,dim_users_table_test ]
