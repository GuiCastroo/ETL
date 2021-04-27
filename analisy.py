from function_database import Queries
import pandas as pd
import pib
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

region_co = Queries.select(
    """
    select sum(production_t) 
    from agriculturalProduction 
    where state in ('GOIÁS', 'MATO GROSSO DO SUL', 'MATO GROSSO', 'DISTRITO FEDERAL')
    """,
    fetch=None
)[0]

all_Region = Queries.select("select sum(production_t) from agriculturalProduction", fetch=None)[0]

rate_region_co = region_co/all_Region


state = Queries.select("""
    select 
        state
    from (select state, production_t/area_ha as prod  from agriculturalProduction) as area_prod 
    order by prod  desc 
    limit 1
    """, fetch=None
)[0]

sp_avg_production = Queries.select(
    "select avg(production_t) from agriculturalProduction where state = 'SÃO PAULO'", fetch=None
)[0]


dfs = pib.extract_dfs()
ramo_agricola_pib = dfs['ramo_agricola_pib']
ramo_pecuário_pib = dfs['ramo_pecuário_pib']
ramo_pecuário_pib['industria_agricola'] = ramo_agricola_pib['(C) Indústria.4']


ramo_pecuário_pib[
    ['(A) Insumos.5', '(B) Agropecuária .5', '(C) Indústria.5', '(D) Serviços.5', 'industria_agricola']
].corr()

sns.heatmap(
    ramo_pecuário_pib[
        ['(A) Insumos.5', '(B) Agropecuária .5', '(C) Indústria.5', '(D) Serviços.5', 'industria_agricola']
    ].corr(),
    vmin=-1, vmax=1, annot=True
)
plt.show()



