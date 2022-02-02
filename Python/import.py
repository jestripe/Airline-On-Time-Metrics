import pandas as pd
import zipfile
import psycopg2
import sqlalchemy as sqla
from sqlalchemy import create_engine

psql_engine =create_engine('postgresql+psycopg2://postgres:PASSWORD@localhost/airlineontimemetrics')


