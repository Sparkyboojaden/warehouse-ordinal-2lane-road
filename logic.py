from hashlib import algorithms_available
import pyodbc
import pandas as pd
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy as sa
import duckdb as quack
import numpy as np
from config import dms_server, dms_db, dms_user, dms_pass
from config import pg_user, pg_pass, pg_host, pg_db, pg_db2
from config import oxford_server, oxford_db, oxford_user, oxford_pass

conn_str = f'postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}/{pg_db2}'
#create engine
engine = sa.create_engine(conn_str, pool_pre_ping=True)
conn = engine.connect()

sql = f"""
SELECT
    location,
    area,
    "locationType",
    SUBSTRING(location,1,4) as loc,
    aisle,
    bay,
    "row",
    "column",
    ordinal
FROM
    public."cfgLocations"
WHERE
    (
        (
            aisle BETWEEN 'AF' AND 'AK'
            AND (
                (aisle = 'AF' AND bay IN ('9','10','11','12','13','14','15','16'))
            )
        )
        OR (aisle = 'AK' AND bay IN ('1','2','3','4','5','6','7','8'))
        OR aisle BETWEEN 'AG' AND 'AJ'
    )
    AND "locationType" = 'P'
ORDER BY
    CASE SUBSTRING(location,1,4)
        WHEN 'AF09' THEN 1
        WHEN 'AG08' THEN 2
        WHEN 'AF10' THEN 3
        WHEN 'AG07' THEN 4
        WHEN 'AF11' THEN 5
        WHEN 'AG06' THEN 6
        WHEN 'AF12' THEN 7
        WHEN 'AG05' THEN 8
        WHEN 'AF13' THEN 9
        WHEN 'AG04' THEN 10
        WHEN 'AF14' THEN 11
        WHEN 'AG03' THEN 12
        WHEN 'AF15' THEN 13
        WHEN 'AG02' THEN 14
        WHEN 'AF16' THEN 15
        WHEN 'AG01' THEN 16
        WHEN 'AG09' THEN 17
        WHEN 'AH08' THEN 18
        WHEN 'AG10' THEN 19
        WHEN 'AH07' THEN 20
        WHEN 'AG11' THEN 21
        WHEN 'AH06' THEN 22
        WHEN 'AG12' THEN 23
        WHEN 'AH05' THEN 24
        WHEN 'AG13' THEN 25
        WHEN 'AH04' THEN 26
        WHEN 'AG14' THEN 27
        WHEN 'AH03' THEN 28
        WHEN 'AG15' THEN 29
        WHEN 'AH02' THEN 30
        WHEN 'AG16' THEN 31
        WHEN 'AH01' THEN 32
        WHEN 'AH09' THEN 33
        WHEN 'AI08' THEN 34
        WHEN 'AH10' THEN 35
        WHEN 'AI07' THEN 36
        WHEN 'AH11' THEN 37
        WHEN 'AI06' THEN 38
        WHEN 'AH12' THEN 39
        WHEN 'AI05' THEN 40
        WHEN 'AH13' THEN 41
        WHEN 'AI04' THEN 42
        WHEN 'AH14' THEN 43
        WHEN 'AI03' THEN 44
        WHEN 'AH15' THEN 45
        WHEN 'AI02' THEN 46
        WHEN 'AH16' THEN 47
        WHEN 'AI01' THEN 48
        WHEN 'AI09' THEN 49
        WHEN 'AJ08' THEN 50
        WHEN 'AI10' THEN 51
        WHEN 'AJ07' THEN 52
        WHEN 'AI11' THEN 53
        WHEN 'AJ06' THEN 54
        WHEN 'AI12' THEN 55
        WHEN 'AJ05' THEN 56
        WHEN 'AI13' THEN 57
        WHEN 'AJ04' THEN 58
        WHEN 'AI14' THEN 59
        WHEN 'AJ03' THEN 60
        WHEN 'AI15' THEN 61
        WHEN 'AJ02' THEN 62
        WHEN 'AI16' THEN 63
        WHEN 'AJ01' THEN 64
        WHEN 'AJ09' THEN 65
        WHEN 'AK08' THEN 66
        WHEN 'AJ10' THEN 67
        WHEN 'AK07' THEN 68
        WHEN 'AJ11' THEN 69
        WHEN 'AK06' THEN 70
        WHEN 'AJ12' THEN 71
        WHEN 'AK05' THEN 72
        WHEN 'AJ13' THEN 73
        WHEN 'AK04' THEN 74
        WHEN 'AJ14' THEN 75
        WHEN 'AK03' THEN 76
        WHEN 'AJ15' THEN 77
        WHEN 'AK02' THEN 78
        WHEN 'AJ16' THEN 79
        WHEN 'AK01' THEN 80
        ELSE 81
    END,
    ordinal ASC;

"""

df = pd.read_sql(sql,conn)
df['new_ordinal'] = (df.index + 1) * 100

df.to_excel("new_ordinals.xlsx")