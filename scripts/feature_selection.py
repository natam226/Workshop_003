import pandas as pd
from sklearn.model_selection import train_test_split
import os

from streaming import kafka_producer
from sklearn.model_selection import train_test_split 


def load_and_clean_data():
    # Cargar archivos
    df2015 = pd.read_csv('../data/2015.csv')
    df2016 = pd.read_csv('../data/2016.csv')
    df2017 = pd.read_csv('../data/2017.csv')
    df2018 = pd.read_csv('../data/2018.csv')
    df2019 = pd.read_csv('../data/2019.csv')

    # Renombrar columnas
    df2015 = df2015.rename(columns={
        'Country':'country', 'Region':'region', 'Happiness Rank':'happiness_rank',
        'Happiness Score':'happiness_score', 'Standard Error':'standard_error',
        'Economy (GDP per Capita)':'gdp_per_capita', 'Family':'social_support',
        'Health (Life Expectancy)':'life_expectancy', 'Freedom':'freedom',
        'Trust (Government Corruption)':'corruption', 'Generosity':'generosity',
        'Dystopia Residual':'dystopia_residual'
    })

    df2016 = df2016.rename(columns={
        'Country':'country', 'Region':'region', 'Happiness Rank':'happiness_rank',
        'Happiness Score':'happiness_score', 'Lower Confidence Interval':'lower_confidence_interval',
        'Upper Confidence Interval':'upper_confidence_interval',
        'Economy (GDP per Capita)':'gdp_per_capita', 'Family':'social_support',
        'Health (Life Expectancy)':'life_expectancy', 'Freedom':'freedom',
        'Trust (Government Corruption)':'corruption', 'Generosity':'generosity',
        'Dystopia Residual':'dystopia_residual'
    })

    df2017 = df2017.rename(columns={
        'Country':'country', 'Happiness.Rank':'happiness_rank',
        'Happiness.Score':'happiness_score', 'Whisker.high':'whisker_high',
        'Whisker.low':'whisker_low', 'Economy..GDP.per.Capita.':'gdp_per_capita',
        'Family':'social_support', 'Health..Life.Expectancy.':'life_expectancy',
        'Freedom':'freedom', 'Generosity':'generosity',
        'Trust..Government.Corruption.':'corruption', 'Dystopia.Residual':'dystopia_residual'
    })

    df2018 = df2018.rename(columns={
        'Overall rank':'happiness_rank', 'Country or region':'country',
        'Score':'happiness_score', 'GDP per capita':'gdp_per_capita',
        'Social support':'social_support', 'Healthy life expectancy':'life_expectancy',
        'Freedom to make life choices':'freedom', 'Generosity':'generosity',
        'Perceptions of corruption':'corruption'
    })

    df2019 = df2019.rename(columns={
        'Overall rank':'happiness_rank', 'Country or region':'country',
        'Score':'happiness_score', 'GDP per capita':'gdp_per_capita',
        'Social support':'social_support', 'Healthy life expectancy':'life_expectancy',
        'Freedom to make life choices':'freedom', 'Generosity':'generosity',
        'Perceptions of corruption':'corruption'
    })

    # Añadir año
    for df, year in zip([df2015, df2016, df2017, df2018, df2019], [2015, 2016, 2017, 2018, 2019]):
        df['year'] = year

    # Quitar columnas irrelevantes
    df2015.drop(['standard_error'], axis=1, inplace=True)
    df2016.drop(['lower_confidence_interval', 'upper_confidence_interval'], axis=1, inplace=True)
    df2017.drop(['whisker_high', 'whisker_low'], axis=1, inplace=True)

    # Reordenar columnas
    cols = ['country', 'happiness_rank', 'happiness_score', 'gdp_per_capita', 'social_support',
            'life_expectancy', 'freedom', 'corruption', 'generosity', 'year']
    df2015 = df2015[cols + ['dystopia_residual']]
    df2016 = df2016[cols + ['dystopia_residual']]
    df2017 = df2017[cols + ['dystopia_residual']]
    df2018 = df2018[cols]
    df2019 = df2019[cols]

    # Concatenar todos los años
    return pd.concat([df2015, df2016, df2017, df2018, df2019], ignore_index=True)


def assign_regions(df):
    # Diccionario simplificado de países por continente (debes completarlo tú)
    region_countries = {
        'Africa': [...],
        'Asia': [...],
        'Europe': [...],
        'North America': [...],
        'South America': [...],
        'Oceania': [...]
    }
    country_region_mapping = {
        country: region
        for region, countries in region_countries.items()
        for country in countries
    }
    df['region'] = df['country'].map(country_region_mapping)
    return df

df = load_and_clean_data()
df = assign_regions(df)

# Eliminar columnas innecesarias
df = df.drop(columns=['happiness_rank', 'dystopia_residual', 'year', 'generosity'])

# Imputar valores nulos en corrupción
df['corruption'] = df.groupby('country')['corruption'].transform(lambda x: x.fillna(x.mean()))
df.dropna(inplace=True)

# Separar en X e y
X = df.drop(columns=['country', 'happiness_score'])
y = df['happiness_score']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1425)


# Función principal si se ejecuta directamente
if __name__ == "__main__":
    kafka_producer(X_test)
