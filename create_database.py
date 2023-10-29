import os
import pandas as pd
from tqdm import tqdm
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, ForeignKey, PrimaryKeyConstraint

# Define constants
FOLDER_NAME = 'data'
DB_CREDS = {
    'HOST': "localhost", 'PORT': "6543",
    'USER': "postgres", 'PASSWORD': "postgres", 'DBNAME': "evision_10"
}

# Initialize database connection and create engine
def init_db():
    from psycopg2 import connect
    conn = connect(
        host=DB_CREDS['HOST'], port=DB_CREDS['PORT'],
        user=DB_CREDS['USER'], password=DB_CREDS['PASSWORD']
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("SELECT datname FROM pg_database;")
    if DB_CREDS['DBNAME'] not in [db[0] for db in cursor.fetchall()]:
        cursor.execute(f"CREATE DATABASE {DB_CREDS['DBNAME']};")
    cursor.close()
    conn.close()
    return create_engine(f"postgresql+psycopg2://{DB_CREDS['USER']}:{DB_CREDS['PASSWORD']}@{DB_CREDS['HOST']}:{DB_CREDS['PORT']}/{DB_CREDS['DBNAME']}")

engine = init_db()
metadata = MetaData()

# Define and create tables
def define_table(name, columns, primary_keys=None):
    table = Table(name, metadata, *(columns + [PrimaryKeyConstraint(*primary_keys)] if primary_keys else columns))
    table.drop(engine, checkfirst=True)
    table.create(engine)
    return table

tables = {
    'Token': define_table('Token', [
        Column('token_address', String), Column('decimals', Integer),
        Column('logo', String), Column('name', String), Column('symbol', String)
    ], ['token_address']),
    'User': define_table('User', [Column('ethereum_address', String)], ['ethereum_address']),
    'LiquidityPool': define_table('LiquidityPool', [
        Column('liquidity_pool_address', String), Column('token0_address', String),
        Column('token1_address', String), Column('sqrtPriceX96', Float), Column('liquidity', Float)
    ], ['liquidity_pool_address']),
    'Swap': define_table('Swap', [
        Column('timestamp', String), Column('tx_hash', String), Column('log_index', Integer),
        Column('pool_contract_address', String, ForeignKey('LiquidityPool.liquidity_pool_address')),
        Column('amount0', String), Column('amount1', String), Column('sqrt_price_x96', String),
        Column('liquidity', String), Column('tick', String),
        Column('ethereum_address', String, ForeignKey('User.ethereum_address'))
    ], ['tx_hash']),
    'CreatePool': define_table('CreatePool', [
        Column('timestamp', String), Column('tx_hash', String), Column('log_index', Integer),
        Column('factory_contract_address', String),
        Column('pool_contract_address', String, ForeignKey('LiquidityPool.liquidity_pool_address')),
        Column('fee', Integer), Column('token0_address', String), Column('token1_address', String),
        Column('ethereum_address', String, ForeignKey('User.ethereum_address'))
    ], ['tx_hash']),
    'Mint': define_table('Mint', [
        Column('timestamp', String), Column('tx_hash', String), Column('log_index', Integer),
        Column('pool_contract_address', String, ForeignKey('LiquidityPool.liquidity_pool_address')),
        Column('tick_lower', String), Column('tick_upper', String), Column('amount', String),
        Column('amount0', String), Column('amount1', String),
        Column('ethereum_address', String, ForeignKey('User.ethereum_address'))
    ], ['tx_hash']),
    'Burn': define_table('Burn', [
        Column('timestamp', String), Column('tx_hash', String), Column('log_index', Integer),
        Column('pool_contract_address', String, ForeignKey('LiquidityPool.liquidity_pool_address')),
        Column('tick_lower', String), Column('tick_upper', String), Column('amount', String),
        Column('amount0', String), Column('amount1', String),
        Column('ethereum_address', String, ForeignKey('User.ethereum_address'))
    ], ['tx_hash']),
}

# Load CSVs and populate tables
for table_name in tqdm(tables.keys(), desc="Tables"):
    tqdm.write(f"Loading data into {table_name} table...")
    df = pd.read_csv(os.path.join(FOLDER_NAME, f'uniswap-v3-{table_name.lower()}.csv'))
    if 'block_number' in df.columns:
        df = df.drop(columns=['block_number'])
    if 'pool_contract_address' in df.columns:
        df['pool_contract_address'] = df['pool_contract_address'].str.lower()
    df.to_sql(table_name, engine, if_exists="append", index=False)
    tqdm.write(f"Data loaded into {table_name} successfully.")