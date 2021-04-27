import pandas as pd
from function_database import Queries
import uuid


def extract():
    """
    Remove all line and data unnecessary and rename columns default of spreadsheet.

    :return: Dataframe from excel production_2019.xlsx
    :rtype: pd.DataFrame

    """
    df = pd.read_excel('production_2019.xlsx', header=[4])[:28]
    df.drop(index=0, inplace=True)
    new_columns = [
        'state', 'april_area', 'may_area', 'june_area', 'april_production', 'may_production', 'june_production'
    ]
    df.columns = new_columns
    return df


def transform_df_from_excel(dataframe):
    """
    Performs the necessary treatment on the dataframe, such as changing column names and adjusting values.

    :param dataframe: Dataframe from excel production_2019.xlsx
    :type dataframe: pd.DataFrame
    :return: clean dataframe.
    :rtype: pd.DataFrame

    """
    columns = ['april_area', 'may_area', 'june_area', 'april_production', 'may_production', 'june_production']
    dataframe[columns] = dataframe[columns].apply(lambda x: x.str.replace(" ", ""), axis=1)
    dataframe[columns] = dataframe[columns].apply(pd.to_numeric)
    return dataframe


def unpivot(dataframe, columns, value_name):
    """
    transform the dataframe to its normal form.

    :param dataframe: Dataframe from excel production_2019.xlsx
    :type dataframe: pd.DataFrame
    :param columns: column name that you wish transform, the list should have only 4 element and some of these elements
    must be a string call state.
    :type columns: list
    :param value_name: name you wish used in column of values.
    :return: dataframe unpivot
    :rtype: pd.DataFrame

    """
    return pd.melt(
        dataframe[columns].rename(columns={columns[1]: '2019-04', columns[2]: '2019-05', columns[3]: '2019-06'}),
        id_vars=['state'],
        value_vars=['2019-04', '2019-05', '2019-06'],
        var_name='time',
        value_name=value_name
    )


def transform(dataframe):
    """
    Function main of transformations necessary.

    :param dataframe: Dataframe from excel production_2019.xlsx
    :type dataframe: pd.DataFrame
    :return: The transformed dataframe.
    :type dataframe: pd.DataFrame

    """
    transform_df_from_excel(dataframe)
    area = unpivot(dataframe, ['state', 'april_area', 'may_area', 'june_area'], 'area_ha')
    production = unpivot(dataframe, ['state',  'april_production', 'may_production', 'june_production'], 'production_t')
    return area.merge(production, how='inner', on=['state', 'time'])


def load(dataframe):
    """
    Insert data in database.

    :param dataframe: Dataframe from excel production_2019.xlsx
    :type dataframe: pd.DataFrame

    """
    for row in dataframe.to_dict('records'):
        row_insert = list(row.values())
        row_insert.append(str(uuid.uuid1()))
        print(row_insert)
        Queries.commit_query(f"""
            insert into agriculturalProduction (state, time, area_ha, production_t, id)
            values {tuple(row_insert)}
        """)


if __name__ == '__main__':
    df = extract()
    df = transform(df)
    load(df)
