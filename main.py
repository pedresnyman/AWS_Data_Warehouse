from create_services import create_services
from create_tables import main_tables_create
from delete_services import delete_services
from aws_services import DB
from etl import main_etl
import configparser

if __name__ == "__main__":

    create_services()
    main_tables_create()
    main_etl()

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Testing to check if there is data in all created tables
    redshift_db = DB()

    tables = [
        'time',
        'artists',
        'songs',
        'users',
        'songplays',
        'staging_songs',
        'staging_events',
    ]
    query = """
    select top 10 * from {} 
    """

    test_dict = {}
    try:
        for table in tables:
            test_dict[table] = redshift_db._get_db_sql(query.format(table))
    except Exception as e:
        print(e)

    if len(test_dict) == 7:
        print(test_dict)
        print('Creation of AWS services and loading of data into Redshift from S3 is successful!!')
        delete_services()

    print('All processes finished!!')
