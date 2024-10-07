"""
Makes copies of all sqlite databases in input_sqlite to output_sqlite and orders their tables by period/vintage. Checks for period-vintage errors.
"""

import sqlite3
import os
import pandas as pd
import shutil

this_dir = os.path.realpath(os.path.dirname(__name__)) + "/"
input_dir = this_dir + "input_sqlite/"
output_dir = this_dir + "output_sqlite/"


def process_databases():

    databases = _get_sqlite_databases()

    for database in databases:

        print(f"Processing {database}...")
        process_database(database)

    print("\nFinished.\n")



def process_database(database: str):

    shutil.copy(input_dir + f"{database}.sqlite", output_dir + f"{database}_sorted.sqlite")

    conn = sqlite3.connect(output_dir + f"{database}_sorted.sqlite")
    curs = conn.cursor()

    model_periods = [fetch[0] for fetch in curs.execute("SELECT t_periods FROM time_periods WHERE flag='f';").fetchall()]
    existing_periods = [fetch[0] for fetch in curs.execute("SELECT t_periods FROM time_periods WHERE flag='e';").fetchall()]

    df_eff = pd.read_sql_query("SELECT * FROM Efficiency;", conn)
    df_exs = pd.read_sql_query("SELECT * FROM ExistingCapacity", conn).set_index(['regions','tech','vintage'])['exist_cap']
    df_life = pd.read_sql_query("SELECT * FROM LifetimeTech;", conn).set_index(['regions','tech'])['life']

    df_eff['errors'] = ''
    for idx, row in df_eff.iterrows():
        if row['vintage'] in existing_periods:
            try:
                cap = df_exs.loc[(row['regions'], row['tech'], row['vintage'])]
                if pd.isna(cap) or cap == 0:
                    df_eff.loc[idx, 'errors'] += '~exs; '
            except:
                df_eff.loc[idx, 'errors'] += '~exs; '
                pass
    
    df_invalid = df_eff.loc[df_eff['errors'] != '']
    if len(df_invalid) > 0:
        print(f"\nInvalid rows detected in Efficiency table:")
        print(df_invalid)

    # Get all tables with tech and region indices
    all_tables = [fetch[0] for fetch in curs.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()]

    # Get columns for each table
    t_cols = {}
    for table in all_tables: t_cols[table] = [description[0] for description in curs.execute(f"SELECT * FROM '{table}'").description]

    for table in all_tables:

        cols = t_cols[table]

        # Intexed by periods and vintage
        if 'periods' in cols and 'vintage' in cols:

            df: pd.DataFrame = pd.read_sql_query(f"SELECT * FROM {table} ORDER BY regions ASC, tech ASC, periods ASC, vintage ASC;", conn)
            curs.execute(f"DELETE FROM {table}")
            conn.commit()
            df.to_sql(table, conn, if_exists='append', index=False)
            conn.commit()

            # Row validation
            df['life'] = 40
            for idx, row in df.iterrows():
                try: df.loc[idx, 'life'] = df_life.loc[(row['regions'], row['tech'])]
                except: pass

            df['errors'] = ''
            df.loc[df['vintage'] > df['periods'], 'errors'] += 'v>p; '
            df.loc[df['vintage'] + df['life'] <= df['periods'], 'errors'] += 'v+l<=p; '
            df.loc[~df[['regions','tech','vintage']].apply(tuple, axis=1).isin(df_eff[['regions','tech','vintage']].apply(tuple, axis=1)), 'errors'] += '~eff; '
            df.loc[~df['periods'].isin(model_periods), 'errors'] += '~per; '

            df_invalid = df.loc[~(df['errors']=='')]

            if len(df_invalid) > 0:
                print(f"\nInvalid rows detected in table {table}:")
                print(df_invalid)

        elif 'periods' in cols:

            if 'tech' in cols: df: pd.DataFrame = pd.read_sql_query(f"SELECT * FROM {table} ORDER BY regions ASC, tech ASC, periods ASC;", conn)
            else: df: pd.DataFrame = pd.read_sql_query(f"SELECT * FROM {table} ORDER BY regions ASC, periods ASC;", conn)
            curs.execute(f"DELETE FROM {table}")
            conn.commit()
            df.to_sql(table, conn, if_exists='append', index=False)
            conn.commit()

            if 'tech' not in cols: continue # gen group tables have no tech

            # Row validation
            df['errors'] = ''
            df.loc[~df['periods'].isin(model_periods), 'errors'] += '~per; '

            df_invalid = df.loc[~(df['errors']=='')]

            if len(df_invalid) > 0:
                print(f"\nInvalid rows detected in table {table}:")
                print(df_invalid)

        elif 'vintage' in cols:

            df: pd.DataFrame = pd.read_sql_query(f"SELECT * FROM {table} ORDER BY regions ASC, tech ASC, vintage ASC;", conn)
            curs.execute(f"DELETE FROM {table}")
            conn.commit()
            df.to_sql(table, conn, if_exists='append', index=False)
            conn.commit()

            # Row validation
            df['errors'] = ''
            df.loc[~df[['regions','tech','vintage']].apply(tuple, axis=1).isin(df_eff[['regions','tech','vintage']].apply(tuple, axis=1)), 'errors'] += '~eff; '

            df_invalid = df.loc[~(df['errors']=='')]

            if len(df_invalid) > 0:
                print(f"\nInvalid rows detected in table {table}:")
                print(df_invalid)

    conn.close()



# Collects sqlite databases into a database of name: path
def _get_sqlite_databases():

    databases = []

    for dirs in os.walk(input_dir):
        files = dirs[2]

        for file in files:
            split = os.path.splitext(file)
            if split[1] == '.sqlite': databases.append(split[0])

    return databases



if __name__ == "__main__":

    process_databases()