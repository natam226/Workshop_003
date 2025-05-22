import pandas as pd
from sklearn.model_selection import train_test_split
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from streaming import kafka_producer
from sklearn.model_selection import train_test_split 


def load_and_clean_data():
    df2015 = pd.read_csv('../data/2015.csv')
    df2016 = pd.read_csv('../data/2016.csv')
    df2017 = pd.read_csv('../data/2017.csv')
    df2018 = pd.read_csv('../data/2018.csv')
    df2019 = pd.read_csv('../data/2019.csv')

    df2015 = df2015.rename(columns={'Country':'country', 'Region':'region', 'Happiness Rank':'happiness_rank', 'Happiness Score':'happiness_score', 'Standard Error':'standard_error', 'Economy (GDP per Capita)':'gdp_per_capita', 'Family':'social_support', 'Health (Life Expectancy)':'life_expectancy', 'Freedom':'freedom', 'Trust (Government Corruption)':'corruption', 'Generosity':'generosity', 'Dystopia Residual':'dystopia_residual'})
    df2016 = df2016.rename(columns={'Country':'country', 'Region':'region', 'Happiness Rank':'happiness_rank', 'Happiness Score':'happiness_score', 'Lower Confidence Interval':'lower_confidence_interval', 'Upper Confidence Interval':'upper_confidence_interval', 'Economy (GDP per Capita)':'gdp_per_capita', 'Family':'social_support', 'Health (Life Expectancy)':'life_expectancy', 'Freedom':'freedom', 'Trust (Government Corruption)':'corruption', 'Generosity':'generosity', 'Dystopia Residual':'dystopia_residual'})
    df2017 = df2017.rename(columns={'Country':'country', 'Happiness.Rank':'happiness_rank', 'Happiness.Score':'happiness_score', 'Whisker.high':'whisker_high', 'Whisker.low':'whisker_low', 'Economy..GDP.per.Capita.':'gdp_per_capita', 'Family':'social_support', 'Health..Life.Expectancy.':'life_expectancy', 'Freedom':'freedom', 'Generosity':'generosity', 'Trust..Government.Corruption.':'corruption', 'Dystopia.Residual':'dystopia_residual'})
    df2018 = df2018.rename(columns={'Overall rank':'happiness_rank', 'Country or region':'country', 'Score':'happiness_score', 'GDP per capita':'gdp_per_capita', 'Social support':'social_support', 'Healthy life expectancy':'life_expectancy', 'Freedom to make life choices':'freedom', 'Generosity':'generosity', 'Perceptions of corruption':'corruption'})
    df2019 = df2019.rename(columns={'Overall rank':'happiness_rank', 'Country or region':'country', 'Score':'happiness_score', 'GDP per capita':'gdp_per_capita', 'Social support':'social_support', 'Healthy life expectancy':'life_expectancy', 'Freedom to make life choices':'freedom', 'Generosity':'generosity', 'Perceptions of corruption':'corruption'})

    for df, year in zip([df2015, df2016, df2017, df2018, df2019], [2015, 2016, 2017, 2018, 2019]):
        df['year'] = year

    df2015.drop(['standard_error'], axis=1, inplace=True)
    df2016.drop(['lower_confidence_interval', 'upper_confidence_interval'], axis=1, inplace=True)
    df2017.drop(['whisker_high', 'whisker_low'], axis=1, inplace=True)

    cols = ['country', 'happiness_rank', 'happiness_score', 'gdp_per_capita', 'social_support',
            'life_expectancy', 'freedom', 'corruption', 'generosity', 'year']
    df2015 = df2015[cols + ['dystopia_residual']]
    df2016 = df2016[cols + ['dystopia_residual']]
    df2017 = df2017[cols + ['dystopia_residual']]
    df2018 = df2018[cols]
    df2019 = df2019[cols]

    return pd.concat([df2015, df2016, df2017, df2018, df2019], ignore_index=True)


def assign_regions(df):
    region_countries = {
        'Africa': [
            'Mauritius', 'Nigeria', 'Somaliland region', 'Kenya', 'Zambia', 'Zimbabwe',
            'Liberia', 'Namibia', 'Somalia', 'South Africa', 'Niger',
            'Congo (Kinshasa)', 'Uganda', 'Mozambique', 'Senegal', 'Gabon',
            'Tanzania', 'Madagascar', 'Central African Republic', 'Chad',
            'Ethiopia', 'Mauritania', 'Malawi', 'Sierra Leone',
            'Congo (Brazzaville)', 'Botswana', 'Mali', 'Angola', 'Benin',
            'Burkina Faso', 'Rwanda', 'Togo', 'Burundi', 'South Sudan',
            'Gambia', 'Lesotho', 'Swaziland', 'Cameroon', 'Comoros', 'Ghana',
            'Djibouti', 'Guinea', 'Algeria', 'Morocco', 'Tunisia', 'Libya', 'Egypt', 'Sudan',
            'Ivory Coast', 'Somaliland Region'
        ],

        'Asia': [
            'Israel', 'United Arab Emirates', 'Oman', 'Saudi Arabia', 'Kuwait',
            'Bahrain', 'Qatar', 'Jordan', 'Lebanon', 'Turkey', 'Iran', 'Iraq',
            'Palestinian Territories', 'Yemen', 'Syria', 'Afghanistan',
            'Bhutan', 'Bangladesh', 'India', 'Nepal', 'Pakistan', 'Sri Lanka',
            'Uzbekistan', 'Kazakhstan', 'Turkmenistan', 'Kyrgyzstan', 'Tajikistan',
            'Taiwan', 'Japan', 'South Korea', 'Hong Kong', 'Mongolia', 'China',
            'Taiwan Province of China', 'Hong Kong S.A.R., China',
            'Singapore', 'Thailand', 'Vietnam', 'Malaysia', 'Indonesia',
            'Philippines', 'Laos', 'Myanmar', 'Cambodia',
            'Armenia', 'Azerbaijan', 'Georgia', 'United Arab Emirates'
        ],

        'Europe': [
            'Switzerland', 'Iceland', 'Denmark', 'Norway', 'Finland', 'Netherlands',
            'Sweden', 'Luxembourg', 'Ireland', 'Belgium', 'United Kingdom',
            'Austria', 'Germany', 'France', 'Malta', 'Spain', 'Italy', 'Portugal',
            'Greece', 'Cyprus', 'Czech Republic', 'Slovakia', 'Poland', 'Hungary',
            'Slovenia', 'Croatia', 'Bosnia and Herzegovina', 'Estonia', 'Lithuania',
            'Latvia', 'Romania', 'Bulgaria', 'Serbia', 'Montenegro',
            'North Macedonia', 'Albania', 'Kosovo', 'Ukraine', 'Belarus',
            'Moldova', 'Russia', 'Northern Cyprus', 'North Cyprus', 'Macedonia'
        ],

        'North America': [
            'Canada', 'United States', 'Mexico', 'Costa Rica', 'Panama',
            'El Salvador', 'Guatemala', 'Trinidad and Tobago', 'Trinidad & Tobago',
            'Jamaica', 'Dominican Republic', 'Nicaragua', 'Honduras',
            'Haiti', 'Belize', 'Cuba', 'Puerto Rico'
        ],

        'South America': [
            'Chile', 'Argentina', 'Uruguay', 'Colombia', 'Suriname', 'Ecuador',
            'Bolivia', 'Peru', 'Paraguay', 'Venezuela', 'Brazil'
        ],

        'Oceania': [
            'Australia', 'New Zealand'
        ],
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

df = df.drop(columns=['happiness_rank', 'dystopia_residual', 'year', 'generosity'])

df['corruption'] = df.groupby('country')['corruption'].transform(lambda x: x.fillna(x.mean()))
df.dropna(inplace=True)

X = df.drop(columns=['country', 'happiness_score'])
y = df['happiness_score']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1425)


if __name__ == "__main__":
    kafka_producer(X_test, y_test)
