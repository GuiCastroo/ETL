import pandas as pd
from function_database import Queries
import uuid


def extract_dfs():
    """
    Extract dfs of excel from  PIB_Cepea.xlsx.

    :return: dict with all dataframe
    :rtype: dict

    """
    df = pd.read_excel('PIB_Cepea.xlsx', header=[7])[:24]
    number = 0
    dict_df = dict()
    list_dfs = [
        'agronegócio', 'ramo_agricola', 'ramo_pecuário', 'agronegócio_pib',
        'ramo_agricola_pib', 'ramo_pecuário_pib'
    ]
    year = df.iloc[:, 0]
    df.drop(columns=['Unnamed: 0'], inplace=True)
    for unit in list_dfs:
        state = number
        number += 6
        dict_df[unit] = df.iloc[:, state:number]
        dict_df[unit].dropna(axis=1, inplace=True)
        dict_df[unit] = dict_df[unit].iloc[:, :-1]
        dict_df[unit]['year'] = year
    return dict_df


def unpivot_all_dfs(dfs):
    """
    transform the dataframe to its normal form.

    :param dfs: dict with all dataframe
    :type: dfs: dict
    :return: the dataframes unpivot
    :rtype dict

    """
    dict_dataframe = dict()
    for key, value in dfs.items():
        value.columns = ['insumos', 'agropecuaria', 'industria', 'servicos', 'year']
        value = pd.melt(
            dfs[key],
            id_vars=['year'],
            value_vars=['insumos', 'agropecuaria', 'industria', 'servicos'],
            var_name='segment',
            value_name='pib' if key[-3:] == 'pib' else 'pib-income'
        )
        value['category'] = key[:-4] if key[-3:] == 'pib' else key
        dict_dataframe[key] = value
    return dict_dataframe


def joins(dfs):
    """
    Union all dataframes.

    :param dfs: dict with all dataframe
    :type: dfs: dict
    :return: complete dataframe.
    :rtype: pd.Dataframe

    """
    df_agro = dfs['agronegócio'].merge(dfs['agronegócio_pib'], how='inner', on=['year', 'segment', 'category'])
    df_agricultural = dfs['ramo_agricola'].merge(
        dfs['ramo_agricola_pib'], how='inner', on=['year', 'segment', 'category']
    )
    df_alivestock = dfs['ramo_pecuário'].merge(
        dfs['ramo_pecuário_pib'], how='inner', on=['year', 'segment', 'category']
    )
    frames = [df_agro, df_agricultural, df_alivestock]
    return pd.concat(frames)


def transform(dfs):
    """
    Function main of transformations necessary.

    :param dataframe: Dataframe from function transform
    :type dataframe: pd.DataFrame
    :return: The transformed dataframe.
    :type dataframe: pd.DataFrame

    """
    dfs = unpivot_all_dfs(dfs)
    return joins(dfs)


def load(df):
    """
    Insert data in database.

    :param dataframe: Dataframe from function transform
    :type dataframe: pd.DataFrame

    """
    for row in df.to_dict('records'):
        row_insert = list(row.values())
        row_insert.append(str(uuid.uuid1()))
        print(row_insert)
        Queries.commit_query(f"""
            insert into PIB (year, segment, pibIcome, category, pib, id)
            values {tuple(row_insert)}
        """)


if __name__ == '__main__':
    dfs = extract_dfs()
    df = transform(dfs)
    load(df)