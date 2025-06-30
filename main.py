```
# %%
from IPython.display import display, HTML
display(HTML("<style>.container {Width: 90% !important;}</style>"))
 
# %%
import psycopg2
import os
import pandas as pd
import numpy as np
import warnings; warnings.filterwarnings(action='ignore')
from rosreestr2coord import Area
import time
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from shapely import geometry
import pickle
import re
import datetime
import shapely.wkt
#import osmnx as ox
from geopy.distance import geodesic as GD
from shapely import geometry
 
from geopy.geocoders import Nominatim
import geocoder as gc
import random
import time
from datetime import datetime
from collections import Counter
 
import pandas as pd
from collections import OrderedDict
from geopy.extra.rate_limiter import RateLimiter
from geopy.point import Point
import Levenshtein
from fuzzywuzzy import fuzz
import re
import shapely.wkt
import folium
import geopandas as gpd
 
from rutermextract import TermExtractor
import pymorphy2
morph = pymorphy2.MorphAnalyzer()
 
from tqdm.auto import tqdm, trange
tqdm.pandas()
 
# %%
pd.options.display.max_colwidth = 1000
%matplotlib inline
 
# %% [markdown]
# # Загружаем файлы
 
# %% [markdown]
# ## Other
 
# %%
main_path = "Other" ## Считываем файлы с аналогами помесячно в массив 
data = []
for file in tqdm(os.listdir(main_path)):
    path = os.path.join(main_path, file)
    table = pd.read_excel(path)
    table['Имя файла'] = file
    if 'Координаты объекта' not in table.columns:
        table['Координаты объекта'] = None
    data.append(table)
 
# %%
df_Other = pd.concat(data).reset_index(drop=True)
 
# %% [markdown]
# ## SpecialBusinessObjects
 
# %%
main_path = "SpecialBusinessObjects" ## Считываем файлы с аналогами помесячно в массив 
data = []
for file in tqdm(os.listdir(main_path)):
    path = os.path.join(main_path, file)
    table = pd.read_excel(path)
    table['Имя файла'] = file
    if 'Координаты объекта' not in table.columns:
        table['Координаты объекта'] = None
    data.append(table)
 
# %%
df_SpecialBusinessObjects = pd.concat(data).reset_index(drop=True)
 
# %% [markdown]
# ## Объединение 2х типов файлов
 
# %%
df = pd.concat([df_Other, df_SpecialBusinessObjects], ignore_index=True)
 
# %%
print('Общее количество записей:', len(df))
 
# %%
## не можем использовать 'ID на источнике' потому что не являеься в общем пуле уникальным идентификатором, поэтому мучаемся с индексами строк (главное нигде не закосячить)
print('кол-во уникальных значений поля "ID на источнике":', len(df['ID на источнике'].unique()))
print('кол-во записей в исходном датафрейме:', len(df))
 
# %%
## в столбце "текст объявления" встречаются ячейки, начинающиеся со знака "=", поэтому при выгрузки в эксель файл, он думает, что это формула, поэтому исправляем, убирая знак "="
 
df['Текст объявления'] = df['Текст объявления'].str.replace('=', '')
 
# %%
## более 65 530 url не выгрузится в эксель файл, поэтому пока что поставим первым символом пробел (актуально для квартир и земли, для остальных сегментов - только с течением времени)
 
df['URL'] = ' '+df['URL']
 
# %% [markdown]
# # Причесываем файл
 
# %%
## унифицируем дату
df['Дата предложения'] = pd.to_datetime(df['Дата предложения'], dayfirst=True).dt.date
df['Дата объявления'] = pd.to_datetime(df['Дата объявления'], dayfirst=True).dt.date
 
## некоторые объекты содержат в площади строковое значение, изменим на площадь комнаты 
df.loc[df['Цена(₽/м²)'].str.lower() == 'смотреть цена указана за кв.м или за весь объект', 'Цена(₽/м²)'] = None
df.loc[df['Цена(₽/м²)'].str.lower() == 'смотреть, цена указана за кв.м или за весь объект', 'Цена(₽/м²)'] = None
 
df['Цена(₽/м²)'] = df['Цена(₽/м²)'].replace(',', '.', regex=True)
df['Площадь объекта(м²)'] = df['Площадь объекта(м²)'].replace(',', '.', regex=True)
 
df['Цена(₽/м²)'] = df['Цена(₽/м²)'].astype('float')
df['Площадь объекта(м²)'] = df['Площадь объекта(м²)'].astype('float')
 
# %% [markdown]
# # Выберем только продажу
 
# %%
## контроль количества записей
 
control_count_result = pd.pivot_table(df, 
                                      values='Дата объявления', 
                                      index='Имя файла',
                                      aggfunc='count').rename(columns = {'Дата объявления': 'исходное количество записей'})
 
print('Кол-во файлов (строк) за период:', len(control_count_result))
 
control_count_result
 
# %%
## контроль количества записей
 
control_count_temp = pd.pivot_table(df[df['Тип предложения'] != 'продажа'], 
                                    values='Дата объявления', 
                                    index='Имя файла',
                                    aggfunc='count').rename(columns = {'Дата объявления': 'предложения аренда'})
 
control_count_result = pd.merge(control_count_result, control_count_temp,
                                left_on = 'Имя файла',
                                right_on = 'Имя файла',
                                how = 'left').fillna(0)
 
print('Кол-во файлов (строк) за период:', len(control_count_result))
 
control_count_result
 
# %%
df = df[df['Тип предложения'] == 'продажа'].reset_index(drop=True)
 
print('Кол-во строк в датафрейме:', len(df))
 
# %% [markdown]
# # Убираем записи с нулевой площадью и стоимостью
 
# %% [markdown]
# ## Затянем информацией площадь*УПС, если суммарная стоимость равна 0
 
# %%
df['Цена(₽)'] = df['Цена(₽)'].astype('float')
df['Цена(₽/м²)'] = df['Цена(₽/м²)'].astype('float')
df['Площадь объекта(м²)'] = df['Площадь объекта(м²)'].astype('float')
 
df['Цена(₽)'] = df['Цена(₽)'].fillna(0)
df['Цена(₽/м²)'] = df['Цена(₽/м²)'].fillna(0)
df['Площадь объекта(м²)'] = df['Площадь объекта(м²)'].fillna(0)
 
df['temp'] = df['Цена(₽/м²)'] * df['Площадь объекта(м²)']
df.loc[df['Цена(₽)'] == 0, 'Цена(₽)'] = df['temp']
df = df.drop(columns = 'temp')
 
df['Стоимость за квадратный метр'] = np.round(df['Цена(₽)']/df['Площадь объекта(м²)'],2)
 
# %%
len(df)
 
# %% [markdown]
# ## Удаляем записи с нулевой площадью и стоимостью
 
# %%
## контроль количества записей
 
control_count_temp = pd.pivot_table(df[df['Цена(₽)'] == 0], 
                                    values='Дата объявления', 
                                    index='Имя файла',
                                    aggfunc='count').rename(columns = {'Дата объявления': 'нет цены (цена нулевая)'})
 
control_count_result = pd.merge(control_count_result, control_count_temp,
                                left_on = 'Имя файла',
                                right_on = 'Имя файла',
                                how = 'left').fillna(0)
 
print('Кол-во файлов (строк) за период:', len(control_count_result))
 
control_count_result
 
# %%
df[df['Цена(₽)'] == 0].to_excel('not_use_files_for_statistic//предложения с пустой или нулевой ценой.xlsx')
 
# %%
df = df[df['Цена(₽)'] != 0].reset_index(drop=True)
 
# %%
## контроль количества записей
 
control_count_temp = pd.pivot_table(df[df['Площадь объекта(м²)'] == 0], 
                                    values='Дата объявления', 
                                    index='Имя файла',
                                    aggfunc='count').rename(columns = {'Дата объявления': 'нет площади (нулевая площадь)'})
 
control_count_result = pd.merge(control_count_result, control_count_temp,
                                left_on = 'Имя файла',
                                right_on = 'Имя файла',
                                how = 'left').fillna(0)
 
print('Кол-во файлов (строк) за период:', len(control_count_result))
 
control_count_result
 
# %%
df[df['Площадь объекта(м²)'] == 0].to_excel('not_use_files_for_statistic//предложения с пустой или нулевой площадью.xlsx')
 
# %%
df = df[df['Площадь объекта(м²)'] != 0].reset_index(drop=True)
 
# %%
print('кол-во строк:', len(df))
 
# %% [markdown]
# # Определяем координаты
 
# %% [markdown]
# ## Затянем выгрузкой Брянска
 
# %%
## часть координат уже подгружена в исходниках, сделаем из этого словарь и распространим на все записи
 
dict_coordinates = dict(df[df['Координаты объекта'].notnull()][['Адрес из источника', 'Координаты объекта']].values)
df['Координаты объекта temp'] = df['Адрес из источника'].apply(lambda x: dict_coordinates.get(x, '-'))
 
# %%
df[['Координаты объекта temp_x', 'Координаты объекта temp_y']] = df['Координаты объекта temp'].str.split(',', expand= True )
 
df['Координаты объекта temp_x'] = df['Координаты объекта temp_x'].str.replace('{"lat":','')
df['Координаты объекта temp_y'] = df['Координаты объекта temp_y'].str.replace('"lon":','').str.replace('}','')
 
df['Координаты объекта temp_2'] = str('(')+df['Координаты объекта temp_x'].astype('str')+', '+df['Координаты объекта temp_y'].astype('str')+')'
df.loc[df['Координаты объекта temp_2'] !='(-, None)', 'Комментарий по координатам'] = 'выгрузил Брянск'
 
df = df.drop(columns = ['Координаты объекта temp_x', 'Координаты объекта temp_y'])
 
# %% [markdown]
# ## Выгрузим оставшиеся координаты самостоятельно
 
# %% [markdown]
# ### Формируем строковую переменную адреса
 
# %%
## уберем слово "поселок" в столбце "населенный пункт" (примеры: поселок Врангель, поселок Ливадия)
 
df['Населенный пункт'] = df['Населенный пункт'].str.replace('поселок ', '')
 
# %%
## причешем район для адресов, в которых ни город, а населенный пункт (возможно в будущем нуждно будет дополнить)
 
df.loc[(df['Населенный пункт'] != None) & (df['Город'] == 'Находка'), 'Район субъекта РФ'] = 'Находкинский'
df.loc[(df['Населенный пункт'] != None) & (df['Город'] == 'Владивосток'), 'Район субъекта РФ'] = 'Владивостокский'
df.loc[(df['Населенный пункт'] != None) & (df['Город'] == 'Партизанск'), 'Район субъекта РФ'] = 'Партизанский'
df.loc[(df['Населенный пункт'] != None) & (df['Город'] == 'Дальнегорск'), 'Район субъекта РФ'] = 'Дальнегорский'
df.loc[(df['Населенный пункт'] != None) & (df['Город'] == 'Дальнереченск'), 'Район субъекта РФ'] = 'Дальнереченский'  ##Это неточность, есть дальнереченский район. Возможно, влияние отсутствует
df.loc[(df['Населенный пункт'] != None) & (df['Город'] == 'Фокино'), 'Район субъекта РФ'] = 'ЗАТО Фокино'
df.loc[(df['Населенный пункт'] != None) & (df['Город'] == 'Арсеньев'), 'Район субъекта РФ'] = 'Арсеньевский'
df.loc[(df['Населенный пункт'] != None) & (df['Город'] == 'Артем'), 'Район субъекта РФ'] = 'Артемовский'
df.loc[(df['Населенный пункт'] != None) & (df['Город'] == 'Лесозаводск'), 'Район субъекта РФ'] = 'Лесозаводский'
df.loc[(df['Населенный пункт'] != None) & (df['Город'] == 'Уссурийск'), 'Район субъекта РФ'] = 'Уссурийский'
 
# %%
df['Адрес из источника'] = df['Адрес из источника'].str.lower()
 
# %%
## собираем адрес из детальной раскладки
 
temp = []
 
for i in tqdm(range(0, len(df))):
    if str(df['Комментарий по координатам'][i]) == 'nan':
        if str(df['Населенный пункт'][i]) != 'nan':
            temp.append(str(df['Регион'][i])+' край, '+str(df['Район субъекта РФ'][i])+', '+str(df['Населенный пункт'][i])+', '+str(df['Улица'][i])+', '+str(df['Номер дома'][i]))
        else:
            temp.append(str(df['Регион'][i])+' край, '+str(df['Город'][i])+', '+str(df['Улица'][i])+', '+str(df['Номер дома'][i]))
    else:
        temp.append('-')
            
df['Адрес_temp'] = temp
 
# %%
temp = []
 
for i in tqdm(range(0, len(df))):
    if df['Адрес_temp'][i] != '-':
        if str(df['Блок'][i]) == 'nan':
            temp.append(df['Адрес_temp'][i])
        else:
            temp.append(df['Адрес_temp'][i]+str(' к.')+str(df['Блок'][i]))
    else:
        temp.append(df['Адрес_temp'][i])
        
df['Адрес_temp'] = temp
 
# %%
## список адресов, которые идут с корпусом, при этом в разложении адреса нет корпуса (Брянск не смотрим, так как они берут указанные координаты в объявлении, а не от адреса)
 
list_unique_dom_slash = list(df[(df['Адрес из источника'].str.contains('/')) & (df['Блок'].isnull()) & (df['Комментарий по координатам'] != 'выгрузил Брянск')]['Адрес из источника'].unique())
 
# %%
temp_dom = []
temp_address = []
 
for i in tqdm(list_unique_dom_slash):
    if (len(i) - i.find('/')) <= 4:
        if len(i.split(' ')[-1]) <= 7: ## экспертное число
            temp_address.append(i)
            temp_dom.append(i.split(' ')[-1])
 
# %%
df_temp = pd.DataFrame({'Адрес из источника': temp_address,
                        'Номер дома': temp_dom})
 
df_temp_dict = dict(df_temp[['Адрес из источника', 'Номер дома']].values)
 
# %%
df.loc[df['Адрес из источника'].isin(list(df_temp_dict.keys())), 'temp'] = 1
df['Номер дома temp'] = df['Адрес из источника'].apply(lambda x: df_temp_dict.get(x, None))
 
# %%
df['Номер дома temp'] = df['Номер дома temp'].fillna(df['Номер дома'])
 
# %%
#df = df.drop(columns = ['Номер дома'])
df = df.rename(columns = {'Номер дома': 'Номер дома исход',
                          'Номер дома temp': 'Номер дома'})
 
# %%
## собираем адрес из детальной раскладки
 
for i in tqdm(range(0, len(df))):
    if df['temp'][i] == 1:
        temp = df['Адрес_temp'][i].split(',')
        temp = temp[:-1]
        df['Адрес_temp'][i] = (','.join(temp+[' '+str(df['Номер дома'][i])]))
 
# %%
df = df.drop(columns = ['temp'])
 
# %% [markdown]
# ### Геокодируем адрес
 
# %%
list_contents = os.listdir('support_files//Координаты//')
path = 'support_files//Координаты//'
 
# %%
temp = []
for i in list_contents:
    if ('адреса_' in i):
        temp.append(i)
        
temp
 
# %%
data_coord = [pd.read_excel(path+temp[i]) for i in range(0,len(temp))]
 
df_coords = pd.concat(data_coord)
print('исходное кол-во строк по файлам', len(df_coords))
 
df_coords = df_coords.sort_values('Координаты финал')
df_coords = df_coords.drop_duplicates('Адрес_temp').reset_index(drop=True)
print('кол-во строк после удаления дублей', len(df_coords))
 
df_coords['Адрес_temp'] = df_coords['Адрес_temp'].str.lower()
 
#dict_df_address= dict(df_coords[['Адрес_temp', 'Координаты финал']].values)
 
# %%
## убираем те, что разобраны не до улицы (нам важно, чтобы адрес был разобран до дома)
 
df_coords['temp_Nominatim'] = (df_coords['Адрес Nominatim'].str[:1]).str.isdigit()
df_coords.loc[(df_coords['Координаты финал'] != 'Координаты неопределены') & (df_coords['temp_Nominatim'] == False) & (df_coords['Схожесть arcgis и arcgis reverse'] != 100), 'Координаты финал'] = 'Координаты неопределены'
 
df_coords['temp_arcgis'] = (df_coords['Адрес arcgis reverse'].str[:1]).str.isdigit()
df_coords.loc[(df_coords['temp_arcgis'] == False) & (df_coords['Схожесть arcgis и arcgis reverse'] == 100), 'Координаты финал'] = 'Координаты неопределены'
 
# %%
dict_df_address = dict(df_coords[['Адрес_temp', 'Координаты финал']].values)
 
# %%
df_sorted = df.sort_values('Адрес_temp', ascending=False)
 
df_sorted = df_sorted.drop_duplicates('Адрес из источника')
 
df_sorted_dict = dict(df_sorted[['Адрес из источника', 'Адрес_temp']].values)
 
df['Адрес_temp_1'] = df['Адрес из источника'].apply(lambda x: df_sorted_dict.get(x, '-'))
 
#df = df.drop(columns = 'Адрес_temp')
#
#df = df.rename(columns = {'Адрес_temp_1': 'Адрес_temp'})
 
# %%
temp = []
 
for i in tqdm(range(0, len(df))):
    if df['Адрес_temp_1'][i] != '-': ## df['Адрес_temp_1'][i] == '-' означает, что координаты выгрузил Брянск
        temp.append(dict_df_address.get(df['Адрес_temp_1'][i].lower(), '-'))
    else:
        temp.append('разбору не подлежит')
        
        
df['Координаты объекта temp_3'] = temp
 
# %%
df = df.drop(columns = 'Адрес_temp')
 
df = df.rename(columns = {'Адрес_temp_1': 'Адрес_temp'})
 
# %%
## заполним поля координат и комментарии к ним информацией, которую дотянули сами
 
df.loc[df['Координаты объекта temp_3'] == 'Координаты неопределены', 'Координаты объекта temp_2'] = 'Координаты неопределены'
df.loc[df['Координаты объекта temp_3'] == 'Координаты неопределены', 'Комментарий по координатам'] = 'Координаты неопределены'
 
df.loc[(df['Координаты объекта temp_3'] != 'разбору не подлежит') & (df['Координаты объекта temp_3'] != 'Координаты неопределены') & (df['Координаты объекта temp_3'] != '-'), 'Координаты объекта temp_2'] = df['Координаты объекта temp_3']
df.loc[(df['Координаты объекта temp_3'] != 'разбору не подлежит') & (df['Координаты объекта temp_3'] != 'Координаты неопределены') & (df['Координаты объекта temp_3'] != '-'), 'Комментарий по координатам'] = 'выгрузили самостоятельно'
 
# %%
## пул записей, по которым нет возможности полностью идентифицировать адреса
 
df.loc[(df['Координаты объекта temp_2'] == '(-, None)') & (df['Адрес_temp'].str.contains('nan')), 'Комментарий по координатам'] = 'невозможно полностью идентифицировать адрес'
df.loc[(df['Координаты объекта temp_2'] == '(-, None)') & (df['Адрес_temp'].str.contains('nan')), 'Координаты объекта temp_2'] = 'Координаты неопределены'
 
# %%
address_unique = df[(df['Координаты объекта temp_2'] == '(-, None)')]['Адрес_temp'].unique()
print('кол-во адресов для проработки:', len(address_unique))
 
# %%
if len(address_unique) > 0:
    location_start_list = []
    location_Nominatim_list = []
    coordinates_Nominatim_list = []
    location_Nominatim_reverse_list = []
    fuzz_Nominatim_list = []
    
    location_arcgis_list = []
    coordinates_arcgis_list = []
    location_arcgis_reverse_list = []
    fuzz_arcgis_list = []
    
    print('получение координат:')
    for i in tqdm(address_unique):
        location_start_list.append(i)
    
        try:
            ## получаем координаты по адресу через Nominatim
            geolocator = Nominatim(user_agent="my_request_"+str(random.randint(0,10000)))
            location = geolocator.geocode(i, exactly_one=False, timeout=20)
            ### выбираем из предложенных вариантов то, где первым будет стоять номер дома, а не название организации
            temp = []
            for j in location:
                temp.append(j[0])     
            index_temp = np.argsort(temp)[0]
            
            location_Nominatim = location[index_temp]
            if location_Nominatim.raw['name'] != '':
                ## нужно исключить наименование предприятий из адресной строки
                address_Nominatim = location_Nominatim.address[location_Nominatim.address.find(location_Nominatim.raw['name'])+len(location_Nominatim.raw['name'])+2:]
            else:
                address_Nominatim = location[index_temp].address
                
            location_Nominatim_list.append(address_Nominatim)
            coordinates_Nominatim_list.append((location[index_temp].latitude, location[index_temp].longitude))
        except:    
            location_Nominatim_list.append('-')
            coordinates_Nominatim_list.append('-')  
            
        try:   
            ## реверс геокодирования
            geolocator = Nominatim(user_agent="my_request_"+str(random.randint(0,10000)))
            address_Nominatim_reverse = geolocator.reverse(str(location[index_temp].latitude)+', '+str(location[index_temp].longitude))
            if address_Nominatim_reverse.raw['name'] != '':
                ## нужно исключить наименование предприятий из адресной строки
                address_Nominatim_reverse = address_Nominatim_reverse.address[address_Nominatim_reverse.address.find(address_Nominatim_reverse.raw['name'])+len(address_Nominatim_reverse.raw['name'])+2:]
                location_Nominatim_reverse_list.append(address_Nominatim_reverse)
            else:
                address_Nominatim_reverse = address_Nominatim_reverse.address
                location_Nominatim_reverse_list.append(address_Nominatim_reverse)
        except:    
            location_Nominatim_reverse_list.append('-')
        
        try:
            # сравним Nominatim и Nominatim reverse
            fuzz_score_Nominatim = fuzz.token_sort_ratio(address_Nominatim, address_Nominatim_reverse)
            fuzz_Nominatim_list.append(fuzz_score_Nominatim)
        except:    
            fuzz_score_Nominatim = '-'
            fuzz_Nominatim_list.append(fuzz_score_Nominatim)
        
        
        if fuzz_score_Nominatim != 100:
            try:
                ## получаем координаты по адресу через arcgis
                g = gc.arcgis(i, addressdetails=True, language='ru')
                address_arcgis = g.json['address']
                address_arcgis = address_arcgis[:address_arcgis.find("Приморский край")+len("Приморский край")]
                location_arcgis_list.append(address_arcgis)
                coordinates_arcgis_list.append(tuple([g.y, g.x]))
            except:
                location_arcgis_list.append('-')
                coordinates_arcgis_list.append('-')
                
            try:     
                ## реверс геокодирования
                geolocator = Nominatim(user_agent="my_request_"+str(random.randint(0,10000)))
                address_arcgis_reverse = geolocator.reverse(str(g.y)+', '+str(g.x))
                address_arcgis_reverse_address = address_arcgis_reverse.address[:address_arcgis_reverse.address.find(", Дальневосточный федеральный округ")]
                address_arcgis_reverse_address = ', '.join([i for i in address_arcgis_reverse_address.split(',') if ('городской округ' not in i) & 
                                                                                                                    ('микрорайон' not in i)])
                location_arcgis_reverse_list.append(address_arcgis_reverse_address)
            except:
                location_arcgis_reverse_list.append('-')
                
            try:       
                ## сравним arcgis и arcgis reverse
                fuzz_score_arcgis = fuzz.token_sort_ratio(address_arcgis, address_arcgis_reverse_address)
                fuzz_arcgis_list.append(fuzz_score_arcgis)
            except:
                fuzz_score_arcgis = '-'
                fuzz_arcgis_list.append(fuzz_score_arcgis)
            
        else:   
            location_arcgis_list.append('-')
            coordinates_arcgis_list.append('-')
            location_arcgis_reverse_list.append('-')
            fuzz_score_arcgis = '-'
            fuzz_arcgis_list.append(fuzz_score_arcgis)     
            
            
            
            
    t = pd.DataFrame({'Адрес_temp': location_start_list,
                      'Адрес Nominatim': location_Nominatim_list,
                      'Координаты Nominatim': coordinates_Nominatim_list,
                      'Адрес Nominatim reverse': location_Nominatim_reverse_list,
                      'Схожесть Nominatim и Nominatim reverse': fuzz_Nominatim_list,
                      'Адрес arcgis': location_arcgis_list,
                      'Координаты arcgis': coordinates_arcgis_list,
                      'Адрес arcgis reverse': location_arcgis_reverse_list,
                      'Схожесть arcgis и arcgis reverse': fuzz_arcgis_list
                     })
    
    
    ## считаем расстояние между точками
    temp_list = []
    
    print('считаем расстояние между точками:')
    for i in tqdm(range(0, len(t))):
        if (t['Координаты Nominatim'][i] != '-') & (t['Координаты arcgis'][i] != '-'):
            temp_list.append(GD(Point(tuple(t['Координаты Nominatim'][i])), Point(tuple(t['Координаты arcgis'][i]))).m)
        else:
            temp_list.append('-')   
    t['Расстояние между точками Nominatim и arcgis'] = temp_list
    
    
    t.loc[t['Схожесть Nominatim и Nominatim reverse'] == 100, 'Координаты финал'] = t['Координаты Nominatim']
    t.loc[t['Схожесть arcgis и arcgis reverse'] == 100, 'Координаты финал'] = t['Координаты arcgis']
    t['Координаты финал'] = t['Координаты финал'].fillna('Координаты неопределены')
    
    
    t.loc[(t['Схожесть Nominatim и Nominatim reverse'] == 100) & (t['Координаты Nominatim'] == '-'), 'Координаты финал'] = 'Координаты неопределены'
    t.loc[(t['Схожесть Nominatim и Nominatim reverse'] == 100) & (t['Координаты Nominatim'] == '-'), 'Схожесть Nominatim и Nominatim reverse'] = '-'
    
    
    ## убираем те, что разобраны не до улицы (нам важно, чтобы адрес был разобран до дома)
    t['temp_Nominatim'] = (t['Адрес Nominatim'].str[:1]).str.isdigit()
    t.loc[(t['Координаты финал'] != 'Координаты неопределены') & (t['temp_Nominatim'] == False) & (t['Схожесть arcgis и arcgis reverse'] != 100), 'Координаты финал'] = 'Координаты неопределены'
    
    t['temp_arcgis'] = (t['Адрес arcgis reverse'].str[:1]).str.isdigit()
    t.loc[(t['temp_arcgis'] == False) & (t['Схожесть arcgis и arcgis reverse'] == 100), 'Координаты финал'] = 'Координаты неопределены'
    
    t['Адрес_temp'] = t['Адрес_temp'].str.lower()
    
else:
    print('нет записей для разбора')
 
# %%
list_contents = os.listdir('support_files//Координаты//')
path = 'support_files//Координаты//'
 
temp = []
for i in list_contents:
    if ('адреса_' in i):
        temp.append(i)
        
 
## дописать что если есть адреса, которые не были встречены ранее, и которых нет информации в файлах "адреса"
if len(address_unique) > 0:
#if len(set(t['Адрес_temp']).difference(set(df_coords['Адрес_temp']))) > 0:
    t.to_excel(path + 'адреса_' + str(max([int(re.findall(r'\d+', x)[0]) for x in temp])+1) + '.xlsx')
else:
    print('все адреса уже есть')
 
# %%
## внимание! перепроверить на новых данных
 
if len(address_unique) > 0:
    dict_t = dict(t[['Адрес_temp', 'Координаты финал']].values)
    df['Координаты объекта temp_3'] = df['Адрес_temp'].str.lower().apply(lambda x: dict_t.get(x, None))
    df['Координаты объекта temp_3'] = df['Координаты объекта temp_3'].fillna(df['Координаты объекта temp_2'])
    
    df = df.drop(columns = ['Координаты объекта temp_3'])
 
    df = df.rename(columns = {'Координаты объекта': 'Координаты объекта Брянск',
                              'Координаты объекта temp': 'Координаты объекта Брянск (распространен)',
                              'Координаты объекта temp_2': 'Координаты объекта'})
    
else: ## внимание! перепроверить на новых данных (возможно нужно объединить со строкой выше)
    df = df.drop(columns = ['Координаты объекта temp_3'])
    df = df.rename(columns = {'Координаты объекта': 'Координаты объекта Брянск',
                              'Координаты объекта temp': 'Координаты объекта Брянск (распространен)',
                              'Координаты объекта temp_2': 'Координаты объекта'})
 
# %%
df = df.reset_index(drop=True)
 
# %%
len(df)
 
# %% [markdown]
# ## Округляем координаты (для единообразия)
 
# %%
df[['temp', 'temp_1']] = df['Координаты объекта'].astype('str').str.split(', ', expand=True)
 
df['temp'] = df['temp'].str.replace('(', '')
df['temp_1'] = df['temp_1'].str.replace(')', '')
 
df['temp_1'] = df['temp_1'].fillna('Координаты неопределены')
 
df['temp'] = df['temp'].apply(lambda x: round(float(x),5) if x != 'Координаты неопределены' else 'Координаты неопределены')
df['temp_1'] = df['temp_1'].apply(lambda x: round(float(x),5) if x != 'Координаты неопределены' else 'Координаты неопределены')
 
df['Координаты объекта (округленные)'] = str('(') + df['temp'].astype('str') + ', '+ df['temp_1'].astype('str') + str(')')
 
# %%
## заполним поля координат и комментарии к ним информацией, которую дотянули сами
 
df.loc[df['Координаты объекта (округленные)'] == '(Координаты неопределены, Координаты неопределены)', 'Координаты объекта (округленные)'] = 'Координаты неопределены'
 
df.loc[(df['Координаты объекта (округленные)'] == 'Координаты неопределены') & (df['Комментарий по координатам'].isnull()), 'Комментарий по координатам'] = 'Координаты неопределены'
 
df.loc[(df['Координаты объекта (округленные)'] != 'разбору не подлежит') & (df['Координаты объекта (округленные)'] != 'Координаты неопределены') & (df['Координаты объекта (округленные)'] != '-') & (df['Комментарий по координатам'].isnull()), 'Комментарий по координатам'] = 'выгрузили самостоятельно'
 
# %% [markdown]
# ## Отбрасываем записи без координат
 
# %%
## контроль количества записей
 
control_count_temp = pd.pivot_table(df[df['Комментарий по координатам'] == 'невозможно полностью идентифицировать адрес'], 
                                    values='Дата объявления', 
                                    index='Имя файла',
                                    aggfunc='count').rename(columns = {'Дата объявления': 'невозможно полностью идентифицировать адрес'})
 
control_count_result = pd.merge(control_count_result, control_count_temp,
                                left_on = 'Имя файла',
                                right_on = 'Имя файла',
                                how = 'left').fillna(0)
 
print('Кол-во файлов (строк) за период:', len(control_count_result))
 
control_count_result
 
# %%
df[df['Комментарий по координатам'] == 'невозможно полностью идентифицировать адрес'].to_excel('not_use_files_for_statistic//предложения адрес не идентифицируется полностью.xlsx')
 
# %%
## контроль количества записей
 
control_count_temp = pd.pivot_table(df[df['Комментарий по координатам'] == 'Координаты неопределены'], 
                                    values='Дата объявления', 
                                    index='Имя файла',
                                    aggfunc='count').rename(columns = {'Дата объявления': 'координаты неопределены'})
 
control_count_result = pd.merge(control_count_result, control_count_temp,
                                left_on = 'Имя файла',
                                right_on = 'Имя файла',
                                how = 'left').fillna(0)
 
print('Кол-во файлов (строк) за период:', len(control_count_result))
 
control_count_result
 
# %%
df[df['Комментарий по координатам'] == 'Координаты неопределены'].to_excel('not_use_files_for_statistic//предложения без координат.xlsx')
 
# %%
df = df[(df['Комментарий по координатам'] != 'невозможно полностью идентифицировать адрес') & (df['Комментарий по координатам'] != 'Координаты неопределены')].reset_index(drop=True)
 
# %%
print('кол-во строк:', len(df))
 
# %% [markdown]
# # Идентификация объекта (по сцепке "коордантаты+площадь")
 
# %%
df['сцепка'] = df['Координаты объекта (округленные)'].astype('str')+'_'+df['Площадь объекта(м²)'].astype('str')
 
# %%
df_temp = pd.DataFrame(df['сцепка'].unique(), columns = ['сцепка'])
df_temp['уникальный номер'] = range(0, len(df['сцепка'].unique()))
 
dict_temp = dict(df_temp.values)
 
# %%
df['уникальный номер'] = df['сцепка'].apply(lambda x: dict_temp.get(x, '-'))
 
# %%
df.head(3)
 
# %% [markdown]
# # Идентификация выбросов
 
# %%
## продублируем заголовок и текст объявления, чтобы работать с ними, а исходник остался неизменным
 
df['Заголовок_temp'] = df['Заголовок']
df['Текст объявления_temp'] = df['Текст объявления']
 
df['Заголовок_temp'] = df['Заголовок_temp'].str.lower()
df['Текст объявления_temp'] = df['Текст объявления_temp'].str.lower()
 
# %% [markdown]
# ## торги/доли/обременения
 
# %%
## Из-за проблем с доступом к диску Т необходимо задать альтернативный путь (например, диск Л)
 
#dict_temp = pd.read_excel('T://Рынок//Dashboard_файлообменник для анализа//словари_python ЗУ//Коммерция_продажа//словарь_слова-индикаторы.xlsx')
dict_temp = pd.read_excel('//backserver//G//Рынок//Dashboard_файлообменник для анализа//словари_python ЗУ//Коммерция_продажа//словарь_слова-индикаторы.xlsx')
 
word_in = list(dict_temp[dict_temp['слова-индикаторы'].notnull()]['слова-индикаторы'])
word_out = list(dict_temp[dict_temp['слова-исключения'].notnull()]['слова-исключения'])
 
df_temp = df[df['Текст объявления_temp'].apply(lambda x: any(word.lower() in x.lower() for word in word_in))]
 
list_temp = list(df_temp[~df_temp['Текст объявления_temp'].apply(lambda x: any(word.lower() in x.lower() for word in word_out))].index)
df_outliers = df[df.index.isin(list_temp)]
 
df_outliers['temp'] = df_outliers['Заголовок_temp'].astype('str')+' '+df_outliers['Текст объявления_temp'].astype('str')
 
# %%
list_index = []
list_word = []
 
for i in list(df_outliers.index):
    temp = df_outliers['temp'][i]
    marker = 0
    for j in word_in:
        if j in temp:
            list_index.append(i)
            list_word.append(j)
            marker = 1
            break
    
    if (marker == 0) & (j == word_in[-1]):
        list_index.append(i)
        list_word.append('-')
        
df_temp = pd.DataFrame({'индекс': list_index,
                        'слова-индикаторы': list_word})
 
dict_temp = dict(df_temp[['индекс', 'слова-индикаторы']].values)
 
# %%
df_outliers = df_outliers.reset_index()
df_outliers['слова-индикаторы'] = df_outliers['index'].apply(lambda x: dict_temp.get(x, '-'))
 
# %%
list_temp = list(df_outliers['уникальный номер'])
dict_temp = dict(df_outliers[['Текст объявления_temp', 'слова-индикаторы']].values)
 
# %%
df_outliers = df[df['уникальный номер'].isin(list_temp)].reset_index(drop=True)
 
df_outliers['слова-индикаторы'] = df_outliers['Текст объявления_temp'].apply(lambda x: dict_temp.get(x, 'попал потому что по этому уникальному номеру есть объявления, включающие слова-исключения'))
 
# %%
if len(df_outliers) > 0:
    df_outliers = df_outliers.drop(columns = ['Заголовок_temp', 'Текст объявления_temp', 'temp', 'temp_1'])
    #df_outliers = df_outliers[~(df_outliers['cadnum'].astype('str').isin(list(df_return['cadnum'])))] ## должны учесть, что часть КН возвращаем в рынок
    df_outliers.to_excel('not_use_files_for_statistic//предложения торги_доли_обременения.xlsx', index=False)
    
    ## контроль количества записей
    control_count_temp = pd.pivot_table(df_outliers, 
                                        values='Дата объявления', 
                                        index='Имя файла',
                                        aggfunc='count').rename(columns = {'Дата объявления': 'торги/доли/обременения'})#.reset_index()
    
    control_count_result =  pd.merge(control_count_result, control_count_temp,
                                    #left_on = 'name_file',
                                    #right_on = 'name_file',
                                    right_index = True,
                                    left_index = True,
                                    how = 'left').fillna(0)
    
    df = df[~(df['уникальный номер'].isin(list_temp))].reset_index(drop=True)
 
# %% [markdown]
# ## ОНС/под снос
 
# %%
dict_temp = pd.read_excel('//backserver//G//Рынок\Dashboard_файлообменник для анализа\словари_python ЗУ\Коммерция_продажа\словарь_слова-индикаторы.xlsx', sheet_name='ОНС_под снос')
 
word_in = list(dict_temp[dict_temp['слова-индикаторы'].notnull()]['слова-индикаторы'])
word_out = list(dict_temp[dict_temp['слова-исключения'].notnull()]['слова-исключения'])
 
df_temp = df[df['Текст объявления_temp'].apply(lambda x: any(word.lower() in x.lower() for word in word_in))]
 
list_temp = list(df_temp[~df_temp['Текст объявления_temp'].apply(lambda x: any(word.lower() in x.lower() for word in word_out))].index)
df_outliers = df[df.index.isin(list_temp)]
 
df_outliers['temp'] = df_outliers['Заголовок_temp'].astype('str')+' '+df_outliers['Текст объявления_temp'].astype('str')
 
# %%
list_index = []
list_word = []
 
for i in list(df_outliers.index):
    temp = df_outliers['temp'][i]
    marker = 0
    for j in word_in:
        if j in temp:
            list_index.append(i)
            list_word.append(j)
            marker = 1
            break
    
    if (marker == 0) & (j == word_in[-1]):
        list_index.append(i)
        list_word.append('-')
        
df_temp = pd.DataFrame({'индекс': list_index,
                        'слова-индикаторы': list_word})
 
dict_temp = dict(df_temp[['индекс', 'слова-индикаторы']].values)
 
# %%
df_outliers = df_outliers.reset_index()
df_outliers['слова-индикаторы'] = df_outliers['index'].apply(lambda x: dict_temp.get(x, '-'))
 
# %%
df_outliers['слова-индикаторы']
 
# %%
list_temp = list(df_outliers['уникальный номер'])
dict_temp = dict(df_outliers[['Текст объявления_temp', 'слова-индикаторы']].values)
 
# %%
df_outliers = df[df['уникальный номер'].isin(list_temp)].reset_index(drop=True)
 
df_outliers['слова-индикаторы'] = df_outliers['Текст объявления_temp'].apply(lambda x: dict_temp.get(x, 'попал потому что по этому уникальному номеру есть объявления, включающие слова-исключения'))
 
# %%
if len(df_outliers) > 0:
    df_outliers = df_outliers.drop(columns = ['Заголовок_temp', 'Текст объявления_temp', 'temp', 'temp_1'])
    #df_outliers = df_outliers[~(df_outliers['cadnum'].astype('str').isin(list(df_return['cadnum'])))] ## должны учесть, что часть КН возвращаем в рынок
    df_outliers.to_excel('not_use_files_for_statistic//предложения ОНС_под снос.xlsx', index=False)
    
    ## контроль количества записей
    control_count_temp = pd.pivot_table(df_outliers, 
                                        values='Дата объявления', 
                                        index='Имя файла',
                                        aggfunc='count').rename(columns = {'Дата объявления': 'ОНС/под снос'})#.reset_index()
    
    control_count_result =  pd.merge(control_count_result, control_count_temp,
                                    #left_on = 'name_file',
                                    #right_on = 'name_file',
                                    right_index = True,
                                    left_index = True,
                                    how = 'left').fillna(0)
    
    df = df[~(df['уникальный номер'].isin(list_temp))].reset_index(drop=True)
 
# %% [markdown]
# ## выбросы с остатками товара 
 
# %%
dict_temp = pd.read_excel('//backserver//G//Рынок\Dashboard_файлообменник для анализа\словари_python ЗУ\Коммерция_продажа\словарь_слова-индикаторы.xlsx', sheet_name='остатки товара')
 
word_in = list(dict_temp[dict_temp['слова-индикаторы'].notnull()]['слова-индикаторы'])
word_out = list(dict_temp[dict_temp['слова-исключения'].notnull()]['слова-исключения'])
 
df_temp = df[df['Текст объявления_temp'].apply(lambda x: any(word.lower() in x.lower() for word in word_in))]
 
list_temp = list(df_temp[~df_temp['Текст объявления_temp'].apply(lambda x: any(word.lower() in x.lower() for word in word_out))].index)
df_outliers = df[df.index.isin(list_temp)]
 
df_outliers['temp'] = df_outliers['Заголовок_temp'].astype('str')+' '+df_outliers['Текст объявления_temp'].astype('str')
 
# %%
list_index = []
list_word = []
 
for i in list(df_outliers.index):
    temp = df_outliers['temp'][i]
    marker = 0
    for j in word_in:
        if j in temp:
            list_index.append(i)
            list_word.append(j)
            marker = 1
            break
    
    if (marker == 0) & (j == word_in[-1]):
        list_index.append(i)
        list_word.append('-')
        
df_temp = pd.DataFrame({'индекс': list_index,
                        'слова-индикаторы': list_word})
 
dict_temp = dict(df_temp[['индекс', 'слова-индикаторы']].values)
 
# %%
df_outliers = df_outliers.reset_index()
df_outliers['слова-индикаторы'] = df_outliers['index'].apply(lambda x: dict_temp.get(x, '-'))
 
# %%
list_temp = list(df_outliers['уникальный номер'])
dict_temp = dict(df_outliers[['Текст объявления_temp', 'слова-индикаторы']].values)
 
# %%
df_outliers = df[df['уникальный номер'].isin(list_temp)].reset_index(drop=True)
 
df_outliers['слова-индикаторы'] = df_outliers['Текст объявления_temp'].apply(lambda x: dict_temp.get(x, 'попал потому что по этому уникальному номеру есть объявления, включающие слова-исключения'))
 
# %%
if len(df_outliers) > 0:
    df_outliers = df_outliers.drop(columns = ['Заголовок_temp', 'Текст объявления_temp', 'temp', 'temp_1'])
    #df_outliers = df_outliers[~(df_outliers['cadnum'].astype('str').isin(list(df_return['cadnum'])))] ## должны учесть, что часть КН возвращаем в рынок
    df_outliers.to_excel('not_use_files_for_statistic//предложения с остатками товара.xlsx', index=False)
    
    ## контроль количества записей
    control_count_temp = pd.pivot_table(df_outliers, 
                                        values='Дата объявления', 
                                        index='Имя файла',
                                        aggfunc='count').rename(columns = {'Дата объявления': 'остатки товара'})#.reset_index()
    
    control_count_result =  pd.merge(control_count_result, control_count_temp,
                                    #left_on = 'name_file',
                                    #right_on = 'name_file',
                                    right_index = True,
                                    left_index = True,
                                    how = 'left').fillna(0)
    
    df = df[~(df['уникальный номер'].isin(list_temp))].reset_index(drop=True)
 
# %% [markdown]
# ## выбросы продажа бизнеса 
 
# %%
dict_temp = pd.read_excel('//backserver//G//Рынок\Dashboard_файлообменник для анализа\словари_python ЗУ\Коммерция_продажа\словарь_слова-индикаторы.xlsx', sheet_name='продажа бизнеса')
 
word_in = list(dict_temp[dict_temp['слова-индикаторы'].notnull()]['слова-индикаторы'])
word_out = list(dict_temp[dict_temp['слова-исключения'].notnull()]['слова-исключения'])
 
df_temp = df[df['Текст объявления_temp'].apply(lambda x: any(word.lower() in x.lower() for word in word_in))]
 
list_temp = list(df_temp[~df_temp['Текст объявления_temp'].apply(lambda x: any(word.lower() in x.lower() for word in word_out))].index)
df_outliers = df[df.index.isin(list_temp)]
 
df_outliers['temp'] = df_outliers['Заголовок_temp'].astype('str')+' '+df_outliers['Текст объявления_temp'].astype('str')
 
# %%
list_index = []
list_word = []
 
for i in tqdm(list(df_outliers.index)):
    temp = df_outliers['temp'][i]
    marker = 0
    for j in word_in:
        if j in temp:
            list_index.append(i)
            list_word.append(j)
            marker = 1
            break
    
    if (marker == 0) & (j == word_in[-1]):
        list_index.append(i)
        list_word.append('-')
        
df_temp = pd.DataFrame({'индекс': list_index,
                        'слова-индикаторы': list_word})
 
dict_temp = dict(df_temp[['индекс', 'слова-индикаторы']].values)
 
# %%
df_outliers = df_outliers.reset_index()
df_outliers['слова-индикаторы'] = df_outliers['index'].apply(lambda x: dict_temp.get(x, '-'))
 
# %%
list_temp = list(df_outliers['уникальный номер'])
dict_temp = dict(df_outliers[['Текст объявления_temp', 'слова-индикаторы']].values)
 
# %%
df_outliers = df[df['уникальный номер'].isin(list_temp)].reset_index(drop=True)
 
df_outliers['слова-индикаторы'] = df_outliers['Текст объявления_temp'].apply(lambda x: dict_temp.get(x, 'попал потому что по этому уникальному номеру есть объявления, включающие слова-исключения'))
 
# %%
if len(df_outliers) > 0:
    df_outliers = df_outliers.drop(columns = ['Заголовок_temp', 'Текст объявления_temp', 'temp', 'temp_1'])
    #df_outliers = df_outliers[~(df_outliers['cadnum'].astype('str').isin(list(df_return['cadnum'])))] ## должны учесть, что часть КН возвращаем в рынок
    df_outliers.to_excel('not_use_files_for_statistic//предложения продажа бизнеса.xlsx', index=False)
    
    ## контроль количества записей
    control_count_temp = pd.pivot_table(df_outliers, 
                                        values='Дата объявления', 
                                        index='Имя файла',
                                        aggfunc='count').rename(columns = {'Дата объявления': 'продажа бизнеса'})#.reset_index()
    
    control_count_result =  pd.merge(control_count_result, control_count_temp,
                                    #left_on = 'name_file',
                                    #right_on = 'name_file',
                                    right_index = True,
                                    left_index = True,
                                    how = 'left').fillna(0)
    
    df = df[~(df['уникальный номер'].isin(list_temp))].reset_index(drop=True)
 
# %% [markdown]
# # Определяем территориальную привязку (Регион, НП, микрорайон города)
 
# %%
len(df)
 
# %%
#df = df.iloc[:, :-12]
 
# %%
len(df)
 
# %%
df.columns
 
# %%
df = df.rename(columns = {'Координаты объекта (округленные)': 'Coordinates'})
 
# %%
df['Coordinates'] = df['Coordinates'].astype('str') 
 
# %%
df = df.drop(columns = ['temp_1'])
 
# %%
df['temp'] = df['Coordinates'].apply(lambda x: x.split(',')[0].replace('(', ''))
df['temp_1'] = df['Coordinates'].apply(lambda x: x.split(',')[1].replace(')', ''))
 
# %%
df['Coordinates'] = (list(zip(df['temp_1'].astype('float'), df['temp'].astype('float'))))
 
# %%
df = df.drop(columns = ['temp', 'temp_1'])
 
# %% [markdown]
# ## определение микрорайона по Владивостоку
 
# %%
df_region_Vl_polygon = pd.read_excel('support_files//Полигоны//районы Владивостока.xlsx')
 
df_region_Vl_polygon['Координаты'] = (list(zip(df_region_Vl_polygon['POINT_X'], df_region_Vl_polygon['POINT_Y'])))
 
list_name = []
list_micro_region = []
 
for i in df_region_Vl_polygon['Name_obj'].unique():
    polygon = Polygon(df_region_Vl_polygon[df_region_Vl_polygon['Name_obj'] == i].reset_index()['Координаты'])
    list_name.append(i)
    list_micro_region.append(polygon)
    
df_region_Vl_polygon = pd.DataFrame({'Наименование микрорайона': list_name,
                                     'Полигон': list_micro_region})
 
temp = []
for i in tqdm(range(0, len(df))):
    point = shapely.geometry.Point((df['Coordinates'][i]))
    marker = 0
    for j in range(0, len(df_region_Vl_polygon)):
        polygon = df_region_Vl_polygon['Полигон'][j]
        if polygon.contains(point):
            temp.append(df_region_Vl_polygon['Наименование микрорайона'][j])
            marker = 1
            break
    if (j == (len(df_region_Vl_polygon)-1)) & (marker == 0):
        temp.append('-')
        
df['Микрорайон Владивостока'] = temp
 
# %%
df_region_Vl_polygon
 
# %% [markdown]
# ## определяем район Края
 
# %%
df_region_kray_polygon = pd.read_excel('support_files//Полигоны//районы Края.xlsx')
 
df_region_kray_polygon['Координаты'] = (list(zip(df_region_kray_polygon['POINT_X'], df_region_kray_polygon['POINT_Y'])))
 
list_name = []
list_region = []
for i in df_region_kray_polygon['Name_new'].unique():
    polygon = Polygon(df_region_kray_polygon[df_region_kray_polygon['Name_new'] == i].reset_index()['Координаты'])
    list_name.append(i)
    list_region.append(polygon)
    
df_region_kray_polygon = pd.DataFrame({'Наименование района': list_name,
                                       'Полигон': list_region})
 
temp = []
for i in tqdm(range(0, len(df))):
    point = shapely.geometry.Point(df['Coordinates'][i])
    marker = 0
    for j in range(0, len(df_region_kray_polygon)):
        polygon = df_region_kray_polygon['Полигон'][j]
        if polygon.contains(point):
            temp.append(df_region_kray_polygon['Наименование района'][j])
            marker = 1
            break
    if (j == (len(df_region_kray_polygon)-1)) & (marker == 0):
        temp.append('-')
        
        
df['Район Края'] = temp
df['Район Края_temp'] = df['Район Края'].str.split('_')
df['Район Края Полигоны'] = df['Район Края_temp'].apply(lambda x: x[0])
 
# %%
df = df.drop(columns = ['Район Края_temp'])
 
df = df.rename(columns = {'Район Края': 'Region_Kray_(Polygon)_map',
                          'Микрорайон Владивостока': 'Microraion_Vl_(Polygon)',
                          'Район Края Полигоны': 'Region_Kray_(Polygon)'})
 
df.loc[(df['Region_Kray_(Polygon)'] != 'Владивостокский городской округ') & (df['Microraion_Vl_(Polygon)'] != '-'), 'Microraion_Vl_(Polygon)'] = '-'
 
# %%
## сокращаем район
 
df_region = pd.read_excel('support_files//Словари//словарь_районы.xlsx')
 
df = pd.merge(df, df_region,
              left_on = 'Region_Kray_(Polygon)',
              right_on = 'Region_Kray_(Polygon)',
              how = 'left')
 
# %%
df = df.drop(columns = ['Region_Kray_(Polygon)_map', 'Region_Kray_(Polygon)'])
 
# %% [markdown]
# ## определяем в какой НП попадет
 
# %%
df_NP_polygon = pd.read_excel('support_files//Полигоны//Населенные_пункты_полигоны.xlsx')
 
df_NP_polygon['Координаты'] = (list(zip(df_NP_polygon['POINT_X'], df_NP_polygon['POINT_Y'])))
 
list_name = []
list_NP = []
for i in df_NP_polygon['NAME_new'].unique():
    polygon = Polygon(df_NP_polygon[df_NP_polygon['NAME_new'] == i].reset_index()['Координаты'])
    list_name.append(i)
    list_NP.append(polygon)
    
df_NP_polygon = pd.DataFrame({'Наименование НП': list_name,
                              'Полигон': list_NP})
 
temp = []
for i in tqdm(range(0, len(df))):
    point = shapely.geometry.Point(df['Coordinates'][i])
    marker = 0
    for j in range(0, len(df_NP_polygon)):
        polygon = df_NP_polygon['Полигон'][j]
        if polygon.contains(point):
            temp.append(df_NP_polygon['Наименование НП'][j])
            marker = 1
            break
    if (j == (len(df_NP_polygon)-1)) & (marker == 0):
        temp.append('-')
        
df['NP_(Polygon)'] = temp
 
# %% [markdown]
# # Добавление информации по попаданию в полигоны БЦ
 
# %%
df_business_center = pd.read_excel('support_files\\Полигоны\\Бизнес центры.xlsx')
df_business_center = df_business_center[df_business_center['geometry'] != 'нет такого адреса'].reset_index(drop=True)
 
# %%
## выберем только те НП из общего пула, где есть БЦ (из списка)
 
df_temp = df[df['Region_Kray_(Polygon)_cut'].isin(sorted(list(df_business_center['Город'].unique())))][['Coordinates', 'Region_Kray_(Polygon)_cut']].reset_index(drop=True)
 
# %%
df_temp['Coordinates'] = df_temp['Coordinates'].astype('str')
 
# %%
coord = []
temp_name = []
temp_adress = []
 
for i in tqdm(list(df_temp['Coordinates'].unique())):
    coord.append(i)
    point = shapely.geometry.Point(float(i.split(', ')[0].replace('(', '')), float(i.split(', ')[1].replace(')', '')))
    marker = 0
    for j in range(0, len(df_business_center)):
        polygon = shapely.wkt.loads(df_business_center['geometry'][j])
        if polygon.contains(point):
            temp_name.append(df_business_center['Название'][j])
            temp_adress.append(df_business_center['Адрес'][j])
            marker = 1
            break
    if (j == (len(df_business_center)-1)) & (marker == 0):
        temp_name.append('-')
        temp_adress.append('-')
        
df_temp_business_center = pd.DataFrame({'Coordinates': coord,
                                        'Наименование БЦ': temp_name,
                                        'Адрес БЦ': temp_adress})
 
# %%
len(df_temp)
 
# %%
df_temp = pd.merge(df_temp, df_temp_business_center,
                   left_on = 'Coordinates',
                   right_on = 'Coordinates',
                   how = 'left')
 
# %%
len(df_temp)
 
# %%
dict_name_business_center = dict(df_temp[df_temp['Наименование БЦ'] != '-'][['Coordinates', 'Наименование БЦ']].values)
dict_adress_business_center = dict(df_temp[df_temp['Наименование БЦ'] != '-'][['Coordinates', 'Адрес БЦ']].values)
 
# %%
dict_name_business_center
 
# %%
df['Наименование БЦ'] = df['Coordinates'].apply(lambda x: dict_name_business_center.get(str(x), '-'))
df['Адрес БЦ'] = df['Coordinates'].apply(lambda x: dict_adress_business_center.get(str(x), '-'))
 
# %% [markdown]
# # Добавление информации по попаданию в функциональные зоны
 
# %%
df_polygon_pzz = gpd.read_file("support_files//Полигоны//Territorial_zones_2023_вгс84.shp")
 
# %%
df_polygon_pzz = df_polygon_pzz[df_polygon_pzz['geometry'].notnull()].reset_index(drop=True)
 
# %%
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Анучинский район', 'МО_'] = 'Анучинский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Арсеньевский ГО', 'МО_'] = 'Арсеньев'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Артемовский ГО', 'МО_'] = 'Артем'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'ГО Большой Камень', 'МО_'] = 'Большой Камень'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Владивостокский ГО', 'МО_'] = 'Владивосток'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Дальнегорский ГО', 'МО_'] = 'Дальнегорск'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Дальнереческий ГО', 'МО_'] = 'Дальнереченск'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Дальнереченский ГО', 'МО_'] = 'Дальнереченск'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Дальнереческий район', 'МО_'] = 'Дальнереченский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Дальнереченский район', 'МО_'] = 'Дальнереченский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Кавалеровский район', 'МО_'] = 'Кавалеровский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Кировский район', 'МО_'] = 'Кировский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Красноармейский район', 'МО_'] = 'Красноармейский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Лазовский район', 'МО_'] = 'Лазовский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Лесозаводский ГО', 'МО_'] = 'Лесозаводск'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Михайловский район', 'МО_'] = 'Михайловский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Надеждинский район', 'МО_'] = 'Надеждинский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Находкинский ГО', 'МО_'] = 'Находка'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Октябрьский район', 'МО_'] = 'Октябрьский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Ольгинский район', 'МО_'] = 'Ольгинский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Партизанский ГО', 'МО_'] = 'Партизанск'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Партизанский район', 'МО_'] = 'Партизанский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Пограничный район', 'МО_'] = 'Пограничный'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Пожарский район', 'МО_'] = 'Пожарский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'ГО Спасск-Дальний', 'МО_'] = 'Спасск-Дальний'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Спасский район', 'МО_'] = 'Спасский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Тернейский район', 'МО_'] = 'Тернейский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Уссурийский ГО', 'МО_'] = 'Уссурийск'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Ханкайский район', 'МО_'] = 'Ханкайский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Хасанский район', 'МО_'] = 'Хасанский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Хорольский район', 'МО_'] = 'Хорольский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Черниговский район', 'МО_'] = 'Черниговский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Чугуевский район', 'МО_'] = 'Чугуевский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Шкотовский район', 'МО_'] = 'Шкотовский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'Яковлевский район', 'МО_'] = 'Яковлевский'
df_polygon_pzz.loc[df_polygon_pzz['МО_'] == 'ЗАТО Фокино', 'МО_'] = 'Фокино'
 
# %%
from shapely.geometry import Point ## две разные библиотеки, в начале переопределили друг друга
 
temp = []
for i in tqdm(range(0, len(df))): 
    point = Point(df['Coordinates'][i])
    marker = 0
    region = df['Region_Kray_(Polygon)_cut'][i]
    df_polygon_pzz_temp = df_polygon_pzz[df_polygon_pzz['МО_'] == region].reset_index(drop=True)
    for j in range(0, len(df_polygon_pzz_temp)):
        polygon = df_polygon_pzz_temp['geometry'][j]
        if polygon.contains(point):
            temp.append(df_polygon_pzz_temp['VIEW'][j])
            marker = 1
            break
    if (marker == 0):
        temp.append('-')
        
df['Зона ПЗЗ'] = temp
 
# %% [markdown]
# # Сегментация по видам недвижимости
 
# %%
#df = df.drop(columns = ['temp', 'temp'])
 
# %% [markdown]
# ## словарь на 100% слова определения вида недвижимости
 
# %%
df.loc[df['Текст объявления_temp'].str.contains('продается здание'), 'вид недвижимости (слова-маркеры)'] = 'продается здание'
df.loc[(df['Текст объявления_temp'].str.contains('к продаже здание')) & (df['вид недвижимости (слова-маркеры)'].isnull()), 'вид недвижимости (слова-маркеры)'] = 'к продаже здание'
df.loc[(df['Текст объявления_temp'].str.contains('продам здание')) & (df['вид недвижимости (слова-маркеры)'].isnull()), 'вид недвижимости (слова-маркеры)'] = 'продам здание'
df.loc[(df['Текст объявления_temp'].str.contains('нежилое здание с земельным участком'))  & (df['вид недвижимости (слова-маркеры)'].isnull()), 'вид недвижимости (слова-маркеры)'] = 'нежилое здание с земельным участком'
 
df.loc[df['вид недвижимости (слова-маркеры)'].notnull(), 'вид недвижимости'] =  'здание'
 
# %% [markdown]
# ## затянем по уникальном номеру тип недвижимости, если он определился этапом выше
 
# %%
## пример
 
df[df['уникальный номер'] == 2][['Адрес из источника', 
                                 'уникальный номер', 
                                 'Заголовок_temp', 
                                 'Текст объявления_temp',
                                 'вид недвижимости (слова-маркеры)', 
                                 'вид недвижимости']]
 
# %%
dict_temp = dict(df[df['вид недвижимости'].notnull()].drop_duplicates('уникальный номер')[['уникальный номер', 'вид недвижимости']].values)
 
# %%
df['temp'] = df['уникальный номер'].apply(lambda x: dict_temp.get(x, None))
df['вид недвижимости'] = df['вид недвижимости'].fillna(df['temp'])
df = df.drop(columns = ['temp'])
 
# %% [markdown]
# ## поиск ключевых слов (метод TermExtractor)
 
# %% [markdown]
# ### выделим в заголовках ключевые слова, посчитаем сколько таких слов для каждой строки
 
# %%
## пример работы метода TermExtractor
 
from rutermextract import TermExtractor
term_extractor = TermExtractor()
text = 'Продам кладовку во Владивостоке'
for term in term_extractor(text):
    print(term.normalized)
 
# %%
df_temp.columns
 
# %%
## сформируем список уникальных заголовков, удалим те записи, с которыми уже поработали на прошлом шаге
 
df_temp = df[df['вид недвижимости'].isnull()].reset_index(drop=True)
list_temp = list(df_temp['Заголовок_temp'].unique())
 
# %%
## выделим значимые (основные)слова
 
list_text_head = []
list_word = []
 
for i in tqdm(list_temp):
    list_text_head.append(i)
    temp = []
    term_extractor = TermExtractor()
    for term in term_extractor(i):
        temp.append(term.normalized)
        
    list_word.append(temp)
 
# %%
temp = pd.DataFrame({'Заголовок объявления': list_text_head,
                     'Ключевые слова (словосочетания)': list_word})
 
# %%
temp
 
# %%
## приведение словосочетаний к начальной форме (для исключения вариативности)
 
list_total_temp = []
 
for i in tqdm(range(0, len(temp))):
    list_temp_1 = []
    for j in temp['Ключевые слова (словосочетания)'][i]:
        if len(j.split(' ')) > 1:
            list_temp = []
            for k in j.split(' '):
                list_temp.append(morph.parse(k)[0].normal_form)
            list_temp_1.append(' '.join(list_temp))
        else:
            list_temp_1.append(j)
            
    list_total_temp.append(list_temp_1)
    
    
temp['Ключевые слова (словосочетания) в начальной форме'] = list_total_temp
 
# %%
## для поиска слов-маркеров
 
## l_1 = []
## l_2 = []
## 
## for i in tqdm(range(0, len(temp))):
##     for j in temp['Слова в начальной форме'][i]:
##         if j not in l_1:
##             l_1.append(j)
##             l_2.append(1)
##         else:
##             index = l_1.index(j)
##             l_2[index] += 1
##             
## pd.DataFrame({'слова': l_1,
##               'частота': l_2}).to_excel('проверить_4.xlsx')
 
# %%
## сформируем общий список слов-индикаторов
 
data = []
 
for i in ['здания', 'помещение', 'здание_помещение', 'павильон', 'комплексы', 'исключить', 'здания_земля']:
    table = pd.read_excel('//backserver//G//Рынок\Dashboard_файлообменник для анализа\словари_python ЗУ\Коммерция_продажа\словарь вид недвижимости.xlsx', sheet_name = i)
    data.append(table)
    
dict_word = pd.concat(data).reset_index(drop=True)
 
list_main_word = list(dict_word['слова-индикаторы'])
 
# %%
list_temp = []
 
for i in range(0, len(temp)):
    list_temp.append(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(list_main_word))) 
 
# %%
len(temp)
 
# %%
len(list_temp)
 
# %%
temp['Слова-маркеры (согласно списку) первая итерация'] = list_temp
 
# %%
temp['количество ключевых слов'] = temp['Слова-маркеры (согласно списку) первая итерация'].apply(lambda x: len(x))
 
# %% [markdown]
# ### сопоставим каждому слову значение по виду недвижимости
 
# %%
temp.loc[temp['количество ключевых слов'] == 0, 'вид недвижимости'] = 'неопределено'
 
# %%
## помещения
l_1 = list(dict_word[dict_word['группа'] == 'помещения']['слова-индикаторы'])
 
## здания
l_2 = list(dict_word[dict_word['группа'] == 'здания']['слова-индикаторы'])
 
## павильоны
l_3 = list(dict_word[dict_word['группа'] == 'павильон']['слова-индикаторы'])
 
## комплексы
l_4 = list(dict_word[dict_word['группа'] == 'комплексы']['слова-индикаторы'])
 
## могут быть как зданиями, так и помещениями
l_5 = list(dict_word[dict_word['группа'] == 'могут быть как зданиями, так и помещениями']['слова-индикаторы'])
 
## исключить
l_6 = list(dict_word[dict_word['группа'] == 'исключить']['слова-индикаторы'])
 
## здания_земля
l_7 = list(dict_word[dict_word['группа'] == 'здания_земля']['слова-индикаторы'])
 
# %%
# ранее было так, потом переписала на блок ниже
 
#list_temp = []
#
#for i in tqdm(range(0, len(temp))):
#    if temp['вид недвижимости'][i] != 'неопределено': 
#        if len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_1))) > 0:
#            list_temp.append('помещение')
#        elif len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_2))) > 0:
#            list_temp.append('здание')
#        elif len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_3))) > 0:
#            list_temp.append('павильон')   
#        elif len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_4))) > 0:
#            list_temp.append('комплексы')  
#        elif len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_5))) > 0:
#            list_temp.append('могут быть как зданиями, так и помещениями')  
#        elif len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_6))) > 0:
#            list_temp.append('исключить')  
#    else:
#        list_temp.append('неопределено')
 
# %%
list_result = []
 
for i in tqdm(range(0, len(temp))):
    list_temp = []
    if temp['вид недвижимости'][i] != 'неопределено': 
        if len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_1))) > 0:
            list_temp.append('помещение')
        if len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_2))) > 0:
            list_temp.append('здание')
        if len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_3))) > 0:
            list_temp.append('павильон')   
        if len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_4))) > 0:
            list_temp.append('комплексы')  
        if len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_5))) > 0:
            list_temp.append('могут быть как зданиями, так и помещениями')  
        if len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_6))) > 0:
            list_temp.append('исключить') 
        if len(list(set(temp['Ключевые слова (словосочетания) в начальной форме'][i]) & set(l_7))) > 0:
            list_temp.append('здания_земля') 
        list_result.append(list_temp)
    else:
        list_result.append(['неопределено'])
 
# %%
print('проверка:', len(temp) == len(list_result))
 
# %%
temp['вид недвижимости'] = list_result
 
# %%
temp['кол-во слов-маркеров'] = temp['вид недвижимости'].apply(lambda x: len(x))
 
# %%
list_index = []
list_temp = []
 
for i in tqdm(range(0, len(temp))):
    if temp['кол-во слов-маркеров'][i] == 1:
        list_index.append(i)
        list_temp.append(temp['вид недвижимости'][i][0])
    else:
        if list(set(temp['вид недвижимости'][i]) & set(['здания_земля'])) == ['здания_земля']:
            list_index.append(i)
            list_temp.append('здание')
        elif list(set(temp['вид недвижимости'][i]) & set(['исключить'])) == ['исключить']:
            list_index.append(i)
            list_temp.append('исключить')
        elif list(set(temp['вид недвижимости'][i]) & set(['павильон'])) == ['павильон']:
            list_index.append(i)
            list_temp.append('павильон')
        elif (sorted(list(set(temp['вид недвижимости'][i]) & set(['здание', 'могут быть как зданиями, так и помещениями']))) == ['здание', 'могут быть как зданиями, так и помещениями']) & (len(temp['вид недвижимости'][i]) == 2):
            list_index.append(i)
            list_temp.append('здание')
        elif (sorted(list(set(temp['вид недвижимости'][i]) & set(['помещение', 'могут быть как зданиями, так и помещениями']))) == ['могут быть как зданиями, так и помещениями', 'помещение']) & (len(temp['вид недвижимости'][i]) == 2):
            list_index.append(i)
            list_temp.append('помещение')
        elif sorted(list(set(temp['вид недвижимости'][i]) & set(['помещение', 'здание']))) == ['здание', 'помещение']:
            list_index.append(i)
            list_temp.append('неопределено')
        else:
            list_index.append(i)
            list_temp.append('?')
 
# %%
temp['вид недвижимости_temp'] = list_temp
 
temp['вид недвижимости_temp'] = temp['вид недвижимости_temp'].str.replace('здания_земля', 'здание')
 
# %%
pd.DataFrame(temp.groupby('вид недвижимости_temp')['Заголовок объявления'].count()).reset_index()
 
# %% [markdown]
# ### вторая итерация
 
# %%
temp_1 = temp[temp['вид недвижимости_temp'] == 'неопределено'].reset_index(drop=True)
temp_1 = temp_1.drop(columns = ['Ключевые слова (словосочетания)', 
                                'Ключевые слова (словосочетания)', 
                                'Ключевые слова (словосочетания)', 
                                'вид недвижимости', 
                                'количество ключевых слов',
                                'Ключевые слова (словосочетания) в начальной форме',
                                'Слова-маркеры (согласно списку) первая итерация',
                                'кол-во слов-маркеров'])
temp_1
 
# %%
temp_1['разобрано по словам'] = temp_1['Заголовок объявления'].apply(lambda x: re.sub(r'\W', ' ', x.lower()).replace('  ', ' ').split(' '))
 
# %%
list_total_temp = []
 
for i in tqdm(range(0, len(temp_1))): ## len(temp_1)
    list_temp = []
    for j in temp_1['разобрано по словам'][i]:
        list_temp.append(morph.parse(j)[0].normal_form)
        
    list_total_temp.append(list_temp)
 
# %%
temp_1['разобрано по словам (начальная форма)'] = list_total_temp
 
# %%
temp_1
 
# %%
list_temp = []
 
for i in range(0, len(temp_1)):
    list_temp.append(list(set(temp_1['разобрано по словам (начальная форма)'][i]) & set(list_main_word))) 
 
# %%
temp_1['Слова-маркеры (согласно списку) вторая итерация'] = list_temp
 
# %%
temp_1
 
# %%
list_result = []
 
for i in tqdm(range(0, len(temp_1))):
    list_temp = []
    if len(temp_1['Слова-маркеры (согласно списку) вторая итерация'][i]) > 0:
        if len(list(set(temp_1['Слова-маркеры (согласно списку) вторая итерация'][i]) & set(l_1))) > 0:
            list_temp.append('помещение')
        if len(list(set(temp_1['Слова-маркеры (согласно списку) вторая итерация'][i]) & set(l_2))) > 0:
            list_temp.append('здание')
        if len(list(set(temp_1['Слова-маркеры (согласно списку) вторая итерация'][i]) & set(l_3))) > 0:
            list_temp.append('павильон')   
        if len(list(set(temp_1['Слова-маркеры (согласно списку) вторая итерация'][i]) & set(l_4))) > 0:
            list_temp.append('комплексы')  
        if len(list(set(temp_1['Слова-маркеры (согласно списку) вторая итерация'][i]) & set(l_5))) > 0:
            list_temp.append('могут быть как зданиями, так и помещениями')  
        if len(list(set(temp_1['Слова-маркеры (согласно списку) вторая итерация'][i]) & set(l_6))) > 0:
            list_temp.append('исключить') 
        if len(list(set(temp_1['Слова-маркеры (согласно списку) вторая итерация'][i]) & set(l_7))) > 0:
            list_temp.append('здания_земля') 
        list_result.append(list_temp)
    else:
        list_result.append(['неопределено'])
 
# %%
temp_1['вид недвижимости (вторая итерация)'] = list_result
 
# %%
temp_1['кол-во слов-маркеров'] = temp_1['вид недвижимости (вторая итерация)'].apply(lambda x: len(x))
 
# %%
temp_1
 
# %%
list_index = []
list_temp = []
 
for i in tqdm(range(0, len(temp_1))):
    if temp_1['кол-во слов-маркеров'][i] == 1:
        list_index.append(i)
        list_temp.append(temp_1['вид недвижимости (вторая итерация)'][i][0])
    else:
        if list(set(temp_1['вид недвижимости (вторая итерация)'][i]) & set(['здания_земля'])) == ['здания_земля']:
            list_index.append(i)
            list_temp.append('здание')
        elif list(set(temp_1['вид недвижимости (вторая итерация)'][i]) & set(['исключить'])) == ['исключить']:
            list_index.append(i)
            list_temp.append('исключить')
        elif list(set(temp_1['вид недвижимости (вторая итерация)'][i]) & set(['павильон'])) == ['павильон']:
            list_index.append(i)
            list_temp.append('павильон')
        elif (sorted(list(set(temp_1['вид недвижимости (вторая итерация)'][i]) & set(['здание', 'могут быть как зданиями, так и помещениями']))) == ['здание', 'могут быть как зданиями, так и помещениями']) & (len(temp_1['вид недвижимости (вторая итерация)'][i]) == 2):
            list_index.append(i)
            list_temp.append('здание')
        elif (sorted(list(set(temp_1['вид недвижимости (вторая итерация)'][i]) & set(['помещение', 'могут быть как зданиями, так и помещениями']))) == ['могут быть как зданиями, так и помещениями', 'помещение']) & (len(temp_1['вид недвижимости (вторая итерация)'][i]) == 2):
            list_index.append(i)
            list_temp.append('помещение')
        elif sorted(list(set(temp_1['вид недвижимости (вторая итерация)'][i]) & set(['помещение', 'здание']))) == ['здание', 'помещение']:
            list_index.append(i)
            list_temp.append('неопределено')
        else:
            list_index.append(i)
            list_temp.append('?')
 
# %%
temp_1['вид недвижимости (вторая итерация)_temp'] = list_temp
 
temp_1['вид недвижимости (вторая итерация)_temp'] = temp_1['вид недвижимости (вторая итерация)_temp'].str.replace('здания_земля', 'здание')
 
# %%
dict_temp = dict(temp_1[['Заголовок объявления', 'вид недвижимости (вторая итерация)_temp']].values)
dict_temp_word = dict(temp_1[['Заголовок объявления', 'Слова-маркеры (согласно списку) вторая итерация']].values)
 
# %%
temp['вид недвижимости (вторая итерация)'] = temp['Заголовок объявления'].apply(lambda x: dict_temp.get(x, '-'))
temp['Слова-маркеры (согласно списку) вторая итерация'] = temp['Заголовок объявления'].apply(lambda x: dict_temp_word.get(x, '-'))
 
# %%
temp.loc[temp['вид недвижимости_temp'] == 'неопределено', 'вид недвижимости_temp'] = temp['вид недвижимости (вторая итерация)']
 
# %%
temp = temp.drop(columns = ['вид недвижимости (вторая итерация)', 'количество ключевых слов', 'кол-во слов-маркеров', 'вид недвижимости'])
 
# %%
temp = temp.rename(columns = {'вид недвижимости_temp': 'вид недвижимости'})
 
# %%
pd.DataFrame(temp.groupby('вид недвижимости')['Заголовок объявления'].count()).reset_index()
 
# %% [markdown]
# ### переносим результаты в рабочий датафрейм
 
# %%
dict_temp_1 = dict(temp[['Заголовок объявления', 'вид недвижимости']].values)
dict_temp_2 = dict(temp[['Заголовок объявления', 'Слова-маркеры (согласно списку) первая итерация']].values)
dict_temp_3 = dict(temp[['Заголовок объявления', 'Слова-маркеры (согласно списку) вторая итерация']].values)
 
# %%
df['вид недвижимости'] = df['Заголовок_temp'].apply(lambda x: dict_temp_1.get(x, '-'))
df['cлова-маркеры (согласно списку) первая итерация'] = df['Заголовок_temp'].apply(lambda x: dict_temp_2.get(x, '-'))
df['cлова-маркеры (согласно списку) вторая итерация'] = df['Заголовок_temp'].apply(lambda x: dict_temp_3.get(x, '-'))
 
# %% [markdown]
# ### унифицируем информацию по виду недвижимости в рамках одного уникального номера
 
# %%
number_list = []
temp_list = []
 
for i in list(df['уникальный номер'].unique()):
    temp = len(df[df['уникальный номер'] == i]['вид недвижимости'].unique())
    if temp > 1:
        if list(set(df[df['уникальный номер'] == i]['вид недвижимости']).difference(set(['исключить', 'могут быть как зданиями, так и помещениями', 'неопределено']))) == ['помещение']:
            number_list.append(i)
            temp_list.append('помещение')
        elif list(set(df[df['уникальный номер'] == i]['вид недвижимости']).difference(set(['исключить', 'могут быть как зданиями, так и помещениями', 'неопределено']))) == ['здание']:
            number_list.append(i)
            temp_list.append('здание')
        else:
            number_list.append(i)
            temp_list.append('по уникальному номеру разные заголовки, следовательно разные виды недвижимости: '+ str(list(df[df['уникальный номер'] == 2332]['вид недвижимости'])))
 
# %%
df_temp = pd.DataFrame({'уникальный номер': number_list,
                        'вид недвижимости': temp_list})
dict_temp = dict(df_temp[['уникальный номер', 'вид недвижимости']].values)
 
# %%
df['вид недвижимости temp'] = df['уникальный номер'].apply(lambda x: dict_temp.get(x, None))
df['вид недвижимости temp'] = df['вид недвижимости temp'].fillna(df['вид недвижимости'])
 
# %%
df.loc[df['вид недвижимости (слова-маркеры)'].notnull(), 'вид недвижимости temp'] = 'здание'
df = df.rename(columns = {'вид недвижимости (слова-маркеры)': 'вид недвижимости (слова-маркеры c 100% вероятностью)'})
 
# %%
df = df.drop(columns = ['Заголовок_temp', 'Заголовок_temp', 'Текст объявления_temp', 'вид недвижимости'])
 
# %%
df = df.rename(columns = {'вид недвижимости temp': 'вид недвижимости (метод TermExtractor)'})
 
# %% [markdown]
# ## определение вида недвижимости по тексту объявления
 
# %%
## сформируем общий список слов-индикаторов
 
data = []
data_group = []
 
for i in ['здание', 'помещение', 'комплекс']:
    table = pd.read_excel('support_files\словарь вид недвижимости_по тексту.xlsx', sheet_name = i)
    table['Группа'] = i
    data.append(table)
    
    
dict_word = pd.concat(data).reset_index(drop=True)
 
list_main_word = list(dict_word['Слова-индикаторы'])
 
# %%
list_result_1 = []
list_result_2 = []
list_result_3 = []
 
for i in list(dict_word['Группа'].unique()):
    words = dict_word[dict_word['Группа'] == i].reset_index(drop=True)
    df_temp = df[df['Текст объявления'].apply(lambda x: any(word.lower() in x.lower() for word in list(words['Слова-индикаторы'])))].reset_index(drop=True)
    for j in tqdm(range(0, len(df_temp))):
        list_result_1.append(df_temp['уникальный номер'][j])
        list_result_2.append(i)
        list_result_3.append(re.findall(r"|".join(list(words["Слова-индикаторы"].str.lower())), df_temp['Текст объявления'][j].lower()))
 
# %%
df_temp = pd.DataFrame({'уникальный номер': list_result_1,
                        'группа': list_result_2,
                        'слова-индикаторы': list_result_3})
 
# %%
df_temp = df_temp.drop_duplicates('уникальный номер').reset_index(drop=True)
 
# %%
df_temp = df_temp.rename(columns = {'группа': 'вид недвижимости (словари по тексту)',
                                    'слова-индикаторы': 'cлова-маркеры (словари по тексту)'})
 
# %%
df_temp
 
# %%
len(df)
 
# %%
df = pd.merge(df, df_temp,
              left_on = 'уникальный номер',
              right_on = 'уникальный номер',
              how = 'left')
 
# %%
len(df)
 
# %% [markdown]
# ## определение вида недвижимости по картографическим данным
 
# %% [markdown]
# ### Работаем с ЗУ (первая итерация)
 
# %%
df_coord = pd.read_excel('выгрузка по координатам (переделан).xlsx', sheet_name='общий')
 
# %%
df_coord = df_coord[['коорд', 'NOTE', 'Вид недвижимости']]
 
# %%
df_coord
 
# %%
df_coord['temp'] = df_coord['коорд'].apply(lambda x: x.split(",")[0].replace('(',''))
df_coord['temp_1'] = df_coord['коорд'].apply(lambda x: x.split(",")[1].replace(')',''))
 
# %%
df_coord['коорд'] = list(zip(df_coord['temp_1'].astype('float'), df_coord['temp'].astype('float')))
 
# %%
df_coord['коорд'] = df_coord['коорд'].astype('str')
 
# %%
df_coord_zu = df_coord[df_coord['Вид недвижимости'] == 'ЗУ'].reset_index(drop=True)
 
# %%
# Подключение к бд
connection = psycopg2.connect(
    host = "192.168.1.12",
    user = "select_user",
    password = "12345",
    dbname = "ZU_2022")
 
# Создание курсора отправляющего зарпосы в бд
cur = connection.cursor()
 
# %%
def gkr_zu_2022(cad_num):
    cur.execute(f"""
                select cadastralnum,
                       innercadastralnumbers,
                       codecalcuse
                from bufferbase b 
                left join listforrating as lf on lf.unid = b.listforrating_unid
                left join spr_base sb on sb.unid = b.category 
                where cadastralnum = '{cad_num}'""")
    result = cur.fetchall()
    return result
 
# %%
cad_num_list = list(df_coord_zu['NOTE'].unique())
 
# %%
list_cadnum_zu = []
list_cadnum_oks = []
list_code_zu = []
 
for i in tqdm(cad_num_list):
    for j in gkr_zu_2022(i):
        list_cadnum_zu.append(j[0])
        list_cadnum_oks.append(j[1])
        list_code_zu.append(j[2])
 
df_gkr_temp = pd.DataFrame({'Кадастровый номер ЗУ': list_cadnum_zu,
                            'Кадастровый номер ОКС': list_cadnum_oks,
                            'Код расчета ЗУ': list_code_zu})
 
# %%
df_gkr_temp = df_gkr_temp.drop_duplicates().reset_index(drop=True)
 
# %%
df_gkr_temp
 
# %%
df_gkr_temp['Кадастровый номер ОКС'] = df_gkr_temp['Кадастровый номер ОКС'].fillna('-')
 
# %%
list_zu = []
list_oks = []
list_code_zu = []
 
for i in list(df_gkr_temp['Кадастровый номер ЗУ'].unique()):
    temp = df_gkr_temp[df_gkr_temp['Кадастровый номер ЗУ'] == i].reset_index(drop=True)
    temp = '; '.join(temp['Кадастровый номер ОКС'])
    temp = temp.split('; ')
    temp = list(set(temp))
    temp = [x for x in temp if x is not '-']
    list_zu.append(i)
    list_oks.append(temp)
    list_code_zu.append(df_gkr_temp[df_gkr_temp.index == df_gkr_temp[df_gkr_temp['Кадастровый номер ЗУ'] == i].index[0]]['Код расчета ЗУ'].values[0])
 
# %%
df_temp_zu_2022 = pd.DataFrame({'Кадастровый номер ЗУ': list_zu,
                                'Кадастровый номер ОКС': list_oks,
                                'Код расчета ЗУ': list_code_zu})
 
# %%
df_temp_zu_2022
 
# %%
## возьмем те КН ЗУ, которые не найшли в БД 2022 
cad_num_list = list(df_coord_zu[~df_coord_zu['NOTE'].isin(list(df_temp_zu_2022['Кадастровый номер ЗУ'].unique()))]['NOTE'].unique())
 
# %%
# Подключение к бд
connection_20 = psycopg2.connect(
    host = "192.168.1.12",
    user = "select_user",
    password = "12345",
    dbname = "base_zu_2020")
 
# Создание курсора отправляющего зарпосы в бд
cur = connection_20.cursor()
 
# %%
def gkr_zu_2020(cad_num):
    cur.execute(f"""
                select cadastralnum,
                       innercadastralnumbers,
                       codecalcuse
                from bufferbase b 
                left join listforrating as lf on lf.unid = b.listforrating_unid
                left join spr_base sb on sb.unid = b.category 
                where cadastralnum = '{cad_num}'""")
    result = cur.fetchall()
    return result
 
# %%
list_cadnum_zu = []
list_cadnum_oks = []
list_code_zu = []
 
for i in tqdm(cad_num_list):
    for j in gkr_zu_2020(i):
        list_cadnum_zu.append(j[0])
        list_cadnum_oks.append(j[1])
        list_code_zu.append(j[2])
        
 
df_gkr_temp = pd.DataFrame({'Кадастровый номер ЗУ': list_cadnum_zu,
                            'Кадастровый номер ОКС': list_cadnum_oks,
                            'Код расчета ЗУ': list_code_zu})
 
# %%
df_gkr_temp = df_gkr_temp.sort_values('Кадастровый номер ОКС')
 
# %%
df_gkr_temp = df_gkr_temp.drop_duplicates().reset_index(drop=True)
 
# %%
df_gkr_temp['Кадастровый номер ОКС'] = df_gkr_temp['Кадастровый номер ОКС'].fillna('-')
 
# %%
list_zu = []
list_oks = []
list_code_zu = []
 
for i in list(df_gkr_temp['Кадастровый номер ЗУ'].unique()):
    temp = df_gkr_temp[df_gkr_temp['Кадастровый номер ЗУ'] == i].reset_index(drop=True)
    temp = '; '.join(temp['Кадастровый номер ОКС'])
    temp = temp.split('; ')
    temp = list(set(temp))
    temp = [x for x in temp if x is not '-']
    list_zu.append(i)
    list_oks.append(temp)
    list_code_zu.append(df_gkr_temp[df_gkr_temp.index == df_gkr_temp[df_gkr_temp['Кадастровый номер ЗУ'] == i].index[0]]['Код расчета ЗУ'].values[0])
 
# %%
df_temp_zu_2020 = pd.DataFrame({'Кадастровый номер ЗУ': list_zu,
                                'Кадастровый номер ОКС': list_oks,
                                'Код расчета ЗУ': list_code_zu})
 
# %%
df_temp_zu = pd.concat([df_temp_zu_2022, df_temp_zu_2020], ignore_index=True)
 
# %%
df_temp_zu
 
# %%
# Подключение к бд
connection = psycopg2.connect(
    host = "192.168.1.12",
    user = "select_user",
    password = "12345",
    dbname = "BaseOKS_TEST_2")
 
# Создание курсора отправляющего зарпосы в бд
cur = connection.cursor()
 
# %%
def gkr_oks_test_2(cad_num):
    cur.execute(f"""
                select cadastralnum,
                       area,
                       name,
                       coalesce(valuation_date, '1990-01-01')
                from bufferbase b 
                where (cadastralnum = '{cad_num}') and (okstype = 'Здание') --and (valuation_date is not Null)
                order by coalesce DESC
                limit 1""")
    result = cur.fetchall()
    return result
 
# %%
list_cadnum_zu_result = []
list_code_zu_result = []
list_cadnum_oks_result = []
list_oksarea_result = []
list_oksname_result = []
 
for i in tqdm(range(0, len(df_temp_zu))):
    for j in df_temp_zu['Кадастровый номер ОКС'][i]:
        for k in gkr_oks_test_2(j):
            list_cadnum_zu_result.append(df_temp_zu['Кадастровый номер ЗУ'][i])
            list_code_zu_result.append(df_temp_zu['Код расчета ЗУ'][i])
            list_cadnum_oks_result.append(k[0])
            list_oksarea_result.append(k[1])
            list_oksname_result.append(k[2])
            
df_temp_zu_oks = pd.DataFrame({'Кадастровый номер ЗУ': list_cadnum_zu_result,
                               'Код расчета ЗУ': list_code_zu_result,
                               'Кадастровый номер ОКС': list_cadnum_oks_result,
                               'Площадь ОКС': list_oksarea_result,
                               'Наименование ОКС': list_oksname_result})
 
# %%
df_temp_zu_oks
 
# %%
df_coord_zu_pt = pd.pivot_table(df_coord_zu,
                                index = 'коорд',
                                values = 'NOTE',
                                aggfunc = lambda x: list(set(x))).reset_index()
 
df_coord_zu_pt = df_coord_zu_pt.rename(columns = {'коорд': 'Координаты объекта (округленные)',
                                                  'NOTE': 'КН ЗУ'})
 
# %%
len(df)
 
# %%
df['Coordinates'] = df['Coordinates'].astype('str')
 
# %%
df = pd.merge(df, df_coord_zu_pt[['Координаты объекта (округленные)', 'КН ЗУ']],
              left_on = 'Coordinates',
              right_on = 'Координаты объекта (округленные)',
              how = 'left')
 
# %%
df_temp = df.drop_duplicates('сцепка').reset_index(drop=True)
 
# %%
l_1 = []
l_2 = []
l_3 = []
l_4 = []
 
for i in tqdm(range(0, len(df_temp))): #len(df_temp)
    if str(df_temp['КН ЗУ'][i]) != 'nan':
        for j in df_temp['КН ЗУ'][i]:
            t = df_temp_zu_oks[df_temp_zu_oks['Кадастровый номер ЗУ'] == j].reset_index(drop=True)
            if len(t) > 0:
                for k in range(0, len(t)):
                    if (t['Площадь ОКС'][k]*0.99 <= df_temp['Площадь объекта(м²)'][i]) & (df_temp['Площадь объекта(м²)'][i] <= t['Площадь ОКС'][k]*1.01):
                        l_1.append(df_temp['сцепка'][i])
                        l_2.append(t['Кадастровый номер ОКС'][k])
                        l_3.append(t['Площадь ОКС'][k])
                        l_4.append(t['Наименование ОКС'][k])
 
# %%
data_temp = pd.DataFrame({'сцепка': l_1,
                          'Кадастровый номер ОКС': l_2,
                          'Площадь ОКС': l_3,
                          'Наименование ОКС': l_4,})#.to_excel('проверить.xlsx')
 
# %%
data_temp = pd.pivot_table(data_temp,
                           index = 'сцепка',
                           values = ['Кадастровый номер ОКС', 'Площадь ОКС', 'Наименование ОКС'],
                           aggfunc = lambda x: list(set(x))).reset_index()
 
# %%
len(df)
 
# %%
data_temp
 
# %%
df = pd.merge(df, data_temp,
              left_on = 'сцепка',
              right_on = 'сцепка',
              how = 'left')
 
df.loc[(df['Кадастровый номер ОКС'].astype('str') == 'nan') & (df['КН ЗУ'].astype('str') != 'nan'), 'КН ЗУ'] = None ## уберем подтянутые КН ЗУ, по которым ОКСы не подошли
 
# %%
len(df)
 
# %%
df = df.rename(columns = {'КН ЗУ': 'КН ЗУ (первая итерация)',
                          'Кадастровый номер ОКС': 'Кадастровый номер ОКС (первая итерация)',
                          'Наименование ОКС': 'Наименование ОКС (первая итерация)',
                          'Площадь ОКС': 'Площадь ОКС (первая итерация)'})
 
# %% [markdown]
# ### Работаем с ОКСами (вторая итерация)
 
# %%
df_coord_oks = df_coord[df_coord['Вид недвижимости'] == 'ОКС'].reset_index(drop=True)
 
# %%
df_coord_oks = pd.pivot_table(df_coord_oks,
                              index = 'коорд',
                              values = 'NOTE',
                              aggfunc = lambda x: list(set(x))).reset_index()
 
# %%
list_cadnum_zu_result = []
list_cadnum_oks_result = []
list_oksarea_result = []
list_oksname_result = []
 
for i in tqdm(range(0, len(df_coord_oks))):
    for j in df_coord_oks['NOTE'][i]:
        for k in gkr_oks_test_2(j):
            list_cadnum_zu_result.append(df_coord_oks['коорд'][i])
            list_cadnum_oks_result.append(k[0])
            list_oksarea_result.append(k[1])
            list_oksname_result.append(k[2])
            
df_temp_oks = pd.DataFrame({'коорд': list_cadnum_zu_result,
                            'Кадастровый номер ОКС': list_cadnum_oks_result,
                            'Площадь ОКС': list_oksarea_result,
                            'Наименование ОКС': list_oksname_result})
 
# %%
df_temp = df.drop_duplicates('сцепка').reset_index(drop=True)
 
# %%
l_1 = []
l_2 = []
l_3 = []
l_4 = []
 
for i in tqdm(range(0, len(df_temp))): #len(df_temp)
    t = df_temp_oks[df_temp_oks['коорд'] == df_temp['Координаты объекта (округленные)'][i]].reset_index(drop=True)
    if len(t) > 0:
        for k in range(0, len(t)):
            if (t['Площадь ОКС'][k]*0.99 <= df_temp['Площадь объекта(м²)'][i]) & (df_temp['Площадь объекта(м²)'][i] <= t['Площадь ОКС'][k]*1.01):
                l_1.append(df_temp['сцепка'][i])
                l_2.append(t['Кадастровый номер ОКС'][k])
                l_3.append(t['Площадь ОКС'][k])
                l_4.append(t['Наименование ОКС'][k])
 
# %%
data_temp = pd.DataFrame({'сцепка': l_1,
                          'Кадастровый номер ОКС': l_2,
                          'Площадь ОКС': l_3,
                          'Наименование ОКС': l_4,})#.to_excel('проверить.xlsx')
 
# %%
data_temp = data_temp.rename(columns = {'Кадастровый номер ОКС': 'Кадастровый номер ОКС (вторая итерация)',
                                        'Площадь ОКС': 'Площадь ОКС (вторая итерация)',
                                        'Наименование ОКС': 'Наименование ОКС (вторая итерация)'})
 
# %%
data_temp
 
# %%
len(df)
 
# %%
df = pd.merge(df, data_temp,
              left_on = 'сцепка',
              right_on = 'сцепка',
              how = 'left')
 
# %%
len(df)
 
# %% [markdown]
# ### сведение результатов по данным картографов (третья итерация)
 
# %%
df['Кадастровый номер ОКС (первая итерация)'] = df['Кадастровый номер ОКС (первая итерация)'].astype('str')
df['Наименование ОКС (первая итерация)'] = df['Наименование ОКС (первая итерация)'].astype('str')
df['Площадь ОКС (первая итерация)'] = df['Площадь ОКС (первая итерация)'].astype('str')
 
 
df['Кадастровый номер ОКС (первая итерация)'] = df['Кадастровый номер ОКС (первая итерация)'].str.replace('[', '')
df['Кадастровый номер ОКС (первая итерация)'] = df['Кадастровый номер ОКС (первая итерация)'].str.replace(']', '')
 
df['Наименование ОКС (первая итерация)'] = df['Наименование ОКС (первая итерация)'].str.replace('[', '')
df['Наименование ОКС (первая итерация)'] = df['Наименование ОКС (первая итерация)'].str.replace(']', '')
 
df['Площадь ОКС (первая итерация)'] = df['Площадь ОКС (первая итерация)'].str.replace('[', '')
df['Площадь ОКС (первая итерация)'] = df['Площадь ОКС (первая итерация)'].str.replace(']', '')
 
# %%
df['Кадастровый номер ОКС (вторая итерация)'] = df['Кадастровый номер ОКС (первая итерация)']
df['Площадь ОКС (вторая итерация)'] = df['Площадь ОКС (первая итерация)']
df['Наименование ОКС (вторая итерация)'] = df['Наименование ОКС (первая итерация)']
 
# %%
df['Кадастровой номер ОКС (картографы)'] = df['Кадастровый номер ОКС (вторая итерация)']
df['Площадь ОКС (картографы)'] = df['Площадь ОКС (вторая итерация)']
df.loc[df['Кадастровый номер ОКС (вторая итерация)'].astype('str') != 'nan','Тип ОКС (картографы)'] = 'Здание'
 
# %%
df = df.drop(columns = ['Координаты объекта (округленные)', 
                        #'КН ЗУ (первая итерация)', 
                        'Кадастровый номер ОКС (первая итерация)', 
                        'Наименование ОКС (первая итерация)', 
                        'Площадь ОКС (первая итерация)',
                        'Кадастровый номер ОКС (вторая итерация)',
                        'Площадь ОКС (вторая итерация)',
                        'Наименование ОКС (вторая итерация)'])
 
# %%
df['Кадастровой номер ОКС (картографы)'] = df['Кадастровой номер ОКС (картографы)'].replace('nan', None)
df['Площадь ОКС (картографы)'] = df['Площадь ОКС (картографы)'].replace('nan', None)
 
# %% [markdown]
# ## Выделение помещений по коду ЗУ 02:051 и 02:061
 
# %%
df_temp = df_temp_zu_oks[df_temp_zu_oks['Код расчета ЗУ'].isin(['02:051', '02:061'])]
 
# %%
list_index = []
list_marker = []
 
for i in tqdm(range(0, len(df))):
    list_index.append(i)
    if ((str(df['КН ЗУ (первая итерация)'][i]) != 'None') & (str(df['КН ЗУ (первая итерация)'][i]) != 'nan')):
        temp = df_temp[df_temp['Кадастровый номер ЗУ'].isin(df['КН ЗУ (первая итерация)'][i])].reset_index(drop=True)
        if (len(temp) > 0):
            if sum(df['Площадь объекта(м²)'][i] < temp['Площадь ОКС']) > 0:
                list_marker.append('помещение: расположен на участках 02:051, 02:061 и площадь объекта меньше площади ОКСов')
            else:
                list_marker.append('помещение: расположен на участках 02:051, 02:061 и площадь объекта больше площади ОКСов (не брать для рассмотрения)')
        else:
            list_marker.append('-') 
    else:
        list_marker.append('-')           
 
# %%
df_temp = pd.DataFrame({'индекс': list_index,
                        'маркер temp': list_marker})
 
# %%
df = pd.merge(df, df_temp,
              left_on = df.index,
              right_on = 'индекс',
              how = 'left')
 
# %%
df = df.drop(columns = ['индекс'])
 
# %%
df = df.rename(columns = {'маркер temp': 'вид недвижимости (02:051, 02:061)'})
 
# %% [markdown]
# ## определение вида недвижимости по адресу
 
# %%
df_gkr = pd.read_pickle('base_oks_test_2.pickle')
 
# %%
df_gkr = df_gkr[~df_gkr['codesubgroup'].isin(['0101', '0200'])].reset_index(drop=True)
 
# %%
df_gkr['name'] = df_gkr['name'].fillna('неопределенно')
 
# %%
df_gkr = df_gkr[~df_gkr['name'].str.lower().str.contains('квартира')].reset_index(drop=True)
 
# %%
df.loc[df['Адрес из источника'].str.lower().str.contains('кор.'), 'Тип блока'] = 'кор_самостоятельно'
df.loc[df['Адрес из источника'].str.lower().str.contains('стр.'), 'Тип блока'] = 'стр_самостоятельно'
df.loc[df['Адрес из источника'].str.lower().str.contains('/'), 'Тип блока'] = '/_самостоятельно'
 
# %%
df['temp'] = df['Адрес из источника'].apply(lambda x: x.split('/')[1] if (len(x.split('/')) > 1) and (x.split('/')[1].isdigit()) else '-')
 
# %%
df.loc[df['Тип блока'] == '/_самостоятельно', 'Блок'] = df['temp']
 
# %%
df = df.drop(columns = ['temp'])
 
# %%
df['temp'] = df['Адрес из источника'].apply(lambda x: x.split('кор.')[1] if (len(x.split('кор.')) > 1) and (x.split('кор.')[1].strip().isdigit()) else '-')
 
# %%
df.loc[df['Тип блока'] == 'кор_самостоятельно', 'Блок'] = df['temp']
 
# %%
df = df.drop(columns = ['temp'])
 
# %%
df['temp'] = df['Адрес из источника'].apply(lambda x: x.split('стр.')[1] if (len(x.split('стр.')) > 1) and (x.split('стр.')[1].strip().isdigit()) else '-')
 
# %%
df.loc[df['Тип блока'] == 'стр_самостоятельно', 'Блок'] = df['temp']
 
# %%
df = df.drop(columns = ['temp'])
 
# %%
df['Блок'] = df['Блок'].fillna('-')
 
# %%
df_gkr['level2name'] = df_gkr['level2name'].fillna('-')
 
# %%
df_gkr['level1name'] = df_gkr['level1name'].astype('str')
df_gkr['level2name'] = df_gkr['level2name'].astype('str')
 
df['Номер дома исход'] = df['Номер дома исход'].astype('str')
df['Блок'] = df['Блок'].astype('str')
 
# %%
df_gkr['level1name'] = df_gkr['level1name'].str.lower()
df_gkr['level2name'] = df_gkr['level2name'].str.lower()
 
df['Номер дома исход'] = df['Номер дома исход'].str.lower()
df['Блок'] = df['Блок'].str.lower()
 
# %%
df['Блок'] = df['Блок'].str.strip()
 
# %%
df_gkr = df_gkr.sort_values(['cityname', 'localityname', 'streetname', 'level1name','level2name'])
df_gkr = df_gkr.drop_duplicates(subset=['cadastralnum', 'area', 'districtname', 'cityname', 'localityname', 'streetname', 'level1name', 'level2name'])
 
# %%
df_gkr
 
# %%
df_temp = df.sort_values(['Город', 'Населенный пункт', 'Улица', 'Номер дома исход', 'Блок']).drop_duplicates('уникальный номер').reset_index(drop=True)
 
# %%
## проверям населенные пункты
 
l_1 = []
l_2 = []
l_3 = []
l_4 = []
 
for i in tqdm(range(0, len(df_temp))):
    if (str(df_temp['Населенный пункт'][i]) != 'nan'): ## если значение Населенный пункт пусто, то не имеет смысл его проверять
        t = df_gkr[(df_gkr['localityname'] == df_temp['Населенный пункт'][i]) &\
                   (df_gkr['streetname'] == df_temp['Улица'][i]) &\
                   (df_gkr['level1name'] == df_temp['Номер дома исход'][i]) &\
                   (df_gkr['level2name'] == df_temp['Блок'][i]) &\
                   ((df_gkr['area']*1.01 >= df_temp['Площадь объекта(м²)'][i]) & (df_gkr['area']*0.99 <= df_temp['Площадь объекта(м²)'][i]))].reset_index(drop=True)
        t_equal = df_gkr[(df_gkr['localityname'] == df_temp['Населенный пункт'][i]) &\
                   (df_gkr['streetname'] == df_temp['Улица'][i]) &\
                   (df_gkr['level1name'] == df_temp['Номер дома исход'][i]) &\
                   (df_gkr['level2name'] == df_temp['Блок'][i]) &\
                   (df_gkr['area'] == df_temp['Площадь объекта(м²)'][i])].reset_index(drop=True)
            
        if len(t_equal) > 0: ## добавим только объект, который по площади точно совпал
            if len(list(t_equal['cadastralnum'].unique())) == 1:
                l_1.append(list(t_equal['cadastralnum']))
                l_2.append(list(t_equal['okstype']))
                l_3.append(list(t_equal['area']))
                l_4.append('сопоставился один КН (точное совпадение площади)')
            else:
                l_1.append(list(t_equal['cadastralnum']))
                l_2.append(list(t_equal['okstype']))
                l_3.append(list(t_equal['area']))
                l_4.append('сопоставилось множество КН (точное совпадение площади)')
        elif len(t) > 0: ## добавим объекты, которые по площади схожи
            if len(list(t['cadastralnum'].unique())) == 1:
                l_1.append(list(t['cadastralnum']))
                l_2.append(list(t['okstype']))
                l_3.append(list(t['area']))
                l_4.append('сопоставился один КН (приближенное совпадение площади (1%))')
            else:
                l_1.append(list(t['cadastralnum']))
                l_2.append(list(t['okstype']))
                l_3.append(list(t['area']))
                l_4.append('сопоставилось множество КН (приближенное совпадение площади (1%))сопоставилось множество КН (приближенное совпадение площади (1%))')
        else:
            l_1.append('-')
            l_2.append('-')
            l_3.append('-')
            l_4.append('-')
    else:
        l_1.append('-')
        l_2.append('-')
        l_3.append('-')
        l_4.append('-')
 
# %%
t = pd.DataFrame({'cadastralnum_НП': l_1,
                  'okstype_НП': l_2,
                  'area_НП': l_3,
                  'marker_НП': l_4})
 
# %%
t.loc[t['cadastralnum_НП'].str.lower() != '-']
 
# %%
t['marker_НП'].value_counts()
 
# %%
len(t)
 
# %%
len(df_temp)
 
# %%
df_temp = df_temp.join(t)
 
# %%
len(df_temp)
 
# %%
## проверям города
 
l_1 = []
l_2 = []
l_3 = []
l_4 = []
 
for i in tqdm(range(0, len(df_temp))): # len(df)
    if (str(df_temp['Город'][i]) != 'nan') & (df_temp['Населенный пункт'][i] != '-'): ## если значение Города пусто, то не имеет смысл его проверять
        t = df_gkr[(df_gkr['cityname'] == df_temp['Город'][i]) &\
                   (df_gkr['streetname'] == df_temp['Улица'][i]) &\
                   (df_gkr['level1name'] == df_temp['Номер дома исход'][i]) &\
                   (df_gkr['level2name'] == df_temp['Блок'][i]) &\
                   ((df_gkr['area']*1.01 >= df_temp['Площадь объекта(м²)'][i]) & (df_gkr['area']*0.99 <= df_temp['Площадь объекта(м²)'][i]))].drop_duplicates().reset_index(drop=True)
        t_equal = df_gkr[(df_gkr['cityname'] == df_temp['Город'][i]) &\
                   (df_gkr['streetname'] == df_temp['Улица'][i]) &\
                   (df_gkr['level1name'] == df_temp['Номер дома исход'][i]) &\
                   (df_gkr['level2name'] == df_temp['Блок'][i]) &\
                   (df_gkr['area'] == df_temp['Площадь объекта(м²)'][i])].drop_duplicates().reset_index(drop=True)
        
        if len(t_equal) > 0: ## добавим только объект, который по площади точно совпал
            if len(list(t_equal['cadastralnum'].unique())) == 1:
                l_1.append(list(t_equal['cadastralnum']))
                l_2.append(list(t_equal['okstype']))
                l_3.append(list(t_equal['area']))
                l_4.append('сопоставился один КН (точное совпадение площади)')
            else:
                l_1.append(list(t_equal['cadastralnum']))
                l_2.append(list(t_equal['okstype']))
                l_3.append(list(t_equal['area']))
                l_4.append('сопоставилось множество КН (точное совпадение площади)')
        elif len(t) > 0: ## добавим объекты, которые по площади схожи
            if len(list(t['cadastralnum'].unique())) == 1:
                l_1.append(list(t['cadastralnum']))
                l_2.append(list(t['okstype']))
                l_3.append(list(t['area']))
                l_4.append('сопоставился один КН (приближенное совпадение площади (1%))')
            else:
                l_1.append(list(t['cadastralnum']))
                l_2.append(list(t['okstype']))
                l_3.append(list(t['area']))
                l_4.append('сопоставилось множество КН (приближенное совпадение площади (1%))')
        else:
            l_1.append('-')
            l_2.append('-')
            l_3.append('-')
            l_4.append('-')
    else:
        l_1.append('-')
        l_2.append('-')
        l_3.append('-')
        l_4.append('-')
 
# %%
t = pd.DataFrame({'cadastralnum_город': l_1,
                  'okstype_город': l_2,
                  'area_город': l_3,
                  'marker_город': l_4})
 
# %%
len(df_temp)
 
# %%
t['marker_город'].value_counts()
 
# %%
df_temp = df_temp.join(t)
 
# %%
len(df_temp)
 
# %%
df_temp.loc[df_temp['cadastralnum_город'] == '-', 'cadastralnum_город'] = df_temp['cadastralnum_НП']
df_temp.loc[df_temp['okstype_город'] == '-', 'okstype_город'] = df_temp['okstype_НП']
df_temp.loc[df_temp['area_город'] == '-', 'area_город'] = df_temp['area_НП']
df_temp.loc[df_temp['marker_город'] == '-', 'marker_город'] = df_temp['marker_НП']
 
# %%
df_temp = df_temp.drop(columns = ['cadastralnum_НП', 'okstype_НП', 'area_НП', 'marker_НП'])
 
# %%
l_1 = []
l_2 = []
l_3 = []
l_4 = []
l_5 = []
 
for i in tqdm(range(0, len(df_temp))):
    l_1.append(df_temp['уникальный номер'][i])
    if type(df_temp['cadastralnum_город'][i]) == list:
        if len(list(set(df_temp['cadastralnum_город'][i]))) == 1:
            l_2.append(list(set(df_temp['cadastralnum_город'][i]))[0])
            l_3.append(list(set(df_temp['okstype_город'][i]))[0])
            l_4.append(list(set(df_temp['area_город'][i]))[0])
            l_5.append(df_temp['marker_город'][i])
        else: ## если множество КН соответствует
            l_2.append(df_temp['cadastralnum_город'][i])
            if len(list(set(df_temp['okstype_город'][i]))) == 1:
                l_3.append(list(set(df_temp['okstype_город'][i]))[0])
            else:
                l_3.append(df_temp['okstype_город'][i])
            #if len(list(set(df_temp['area_город'][i]))) == 1:     
            #    l_4.append(list(set(df_temp['area_город'][i]))[0])
            #else:
            l_4.append(df_temp['area_город'][i])
            l_5.append(df_temp['marker_город'][i])
    else:
        l_2.append('-')
        l_3.append('-')
        l_4.append('-')
        l_5.append('-')
 
# %%
t = pd.DataFrame({'уникальный номер': l_1,
                  'Кадастровый номер (сопоставление адресов)': l_2,
                  'Тип ОКС (сопоставление адресов)': l_3,
                  'Площадь ОКС (сопоставление адресов)': l_4,
                  'Маркер (сопоставление адресов)': l_5})
 
# %%
t.loc[t['Тип ОКС (сопоставление адресов)'] == '-', 'Результирующий тип ОКС (сопоставление адресов)'] = '-'
t.loc[t['Тип ОКС (сопоставление адресов)'] == 'Здание', 'Результирующий тип ОКС (сопоставление адресов)'] = 'Здание'
t.loc[t['Тип ОКС (сопоставление адресов)'] == 'Помещение', 'Результирующий тип ОКС (сопоставление адресов)'] = 'Помещение'
t['Результирующий тип ОКС (сопоставление адресов)'] = t['Результирующий тип ОКС (сопоставление адресов)'].fillna('Может быть как зданием, так и помещением')
 
# %%
t
 
# %%
len(df)
 
# %%
df = pd.merge(df, t,
              left_on = 'уникальный номер',
              right_on = 'уникальный номер',
              how = 'left')
 
# %%
len(df)
 
# %%
df.to_excel('проверить_v08.xlsx')
 
# %% [markdown]
# ## Балльная система оценки вида недвижимости
 
# %%
#df = pd.read_excel('проверить_v08.xlsx')
 
# %%
## Объявление необходимых переменных и создание временного датафрейма для работы с ним
okstypes = ['помещение', 'здание', 'павильон', 'комплекс']
methods = ['вид недвижимости (метод TermExtractor)', 'вид недвижимости (словари по тексту)', 'Тип ОКС (картографы)', 'вид недвижимости (02:051, 02:061)', 'Тип ОКС (сопоставление адресов)']
df_okstype = df[methods]
 
# %%
## Причесываем столбцы от заглавных букв и убираем NaN
for column in df_okstype.columns:
    df_okstype[column] = df_okstype[column].str.lower()
df_okstype['вид недвижимости (метод TermExtractor)'] = df_okstype['вид недвижимости (метод TermExtractor)'].fillna('не определено')
df_okstype['вид недвижимости (словари по тексту)'] = df_okstype['вид недвижимости (словари по тексту)'].fillna('не определено')
df_okstype['Тип ОКС (картографы)'] = df_okstype['Тип ОКС (картографы)'].fillna('не определено')
df_okstype['вид недвижимости (02:051, 02:061)'] = df_okstype['вид недвижимости (02:051, 02:061)'].fillna('не определено')
df_okstype['Тип ОКС (сопоставление адресов)'] = df_okstype['Тип ОКС (сопоставление адресов)'].fillna('не определено')
 
# %%
def count_occurrences(row, target):
    return sum(row.str.contains(target))
 
# %%
## Считаем интересные нам слова в столбцах методов и создаем 4 столбца, соответствующие определенным ранее типам помещения
for okstype in okstypes:
    df_okstype[okstype] = df_okstype[methods].apply(lambda row: count_occurrences(row, okstype), axis=1)
 
## Результирующий вид недвижимости - тот, количество баллов у которого максимально
df_okstype['вид недвижимости'] = df_okstype[okstypes].idxmax(axis=1)
 
# %%
## Убираем вид в строках, где определилось более 1 вида разными методами, либо не определилось вообще. Добавляем "вес" определенного вида по количеству методов
df_okstype['zero_count'] = df_okstype.apply(lambda row: (row == 0).sum(), axis=1)
df_okstype.loc[df_okstype['zero_count'] != 3, 'вид недвижимости'] = 'не определено'
df_okstype['max_value'] = df_okstype[okstypes].max(axis=1)
 
# %%
## Добавляем необходимые данные в исходный датафрейм и сохраняем
df = pd.concat([df, df_okstype[['помещение', 'здание', 'павильон', 'комплекс', 'вид недвижимости', 'max_value']]], axis=1)
#df.drop('Unnamed: 0', axis=1, inplace=True)
df.rename(columns={'max_value': 'Вес вида недвижимости'}, inplace=True)
 
# %%
#df.to_excel('проверить_v09.xlsx')
 
# %% [markdown]
# ## Укрупненная классификация ПЗЗ
 
# %%
#df = pd.read_excel('проверить_v09.xlsx')
df_pzz = pd.read_excel('ПЗЗ Зоны в ГО, МО и МР.xlsx', sheet_name='ПЗЗ')
 
# %%
df_pzz['Код территориалной зоны (бук. Знач.)'] = df_pzz['Код территориалной зоны (бук. Знач.)'].str.replace(' ', '', regex=True)
df_pzz['ПЗЗ сцепка'] = df_pzz['ГО МР'] + df_pzz['Код территориалной зоны (бук. Знач.)']
 
df_temp = df[['Region_Kray_(Polygon)_cut','Зона ПЗЗ']].copy()
df_temp['Зона ПЗЗ'] = df_temp['Зона ПЗЗ'].str.replace(' ', '', regex=True)
df_temp['ПЗЗ сцепка'] = df_temp['Region_Kray_(Polygon)_cut'] + df_temp['Зона ПЗЗ']
 
df_temp = df_temp.merge(df_pzz.groupby(by='ПЗЗ сцепка').agg({'Наименование территориальной зоны': 'first'}), on='ПЗЗ сцепка', how='left')
 
# %%
df_pzz_dict = pd.read_excel('коммерция пзз временно.xlsx', sheet_name='Лист1')
dict_pzz = dict(zip(df_pzz_dict['Наименование территориальной зоны'].to_list(), df_pzz_dict['Укрупненно зона'].to_list()))
df_temp['Зона укрупненно'] = df_temp['Наименование территориальной зоны'].map(dict_pzz)
df['Наименование территориальной зоны'] = df_temp['Наименование территориальной зоны']
df['Зона укрупненно'] = df_temp['Зона укрупненно']
 
# %%
#df.to_excel('проверить_v10.xlsx')
 
# %%
df = pd.read_excel('проверить_v10.xlsx')
 
# %% [markdown]
# # Вид бизнеса
 
# %%
#df = pd.read_excel('проверить_v09.xlsx')
 
# %%
#df
 
# %% [markdown]
# # Вид бизнеса
 
# %%
#df = pd.read_excel('проверить_v04.xlsx')
 
# %%
len(df)
 
# %%
## создадим временный столбец с заголовком текста объявления
## переведем все в нижний регистр
## разделим по словам и переведем слова в начальную форму
 
df['Заголовок_temp'] = df['Заголовок'].str.lower()
df['Заголовок_temp'] = df['Заголовок_temp'].apply(lambda x: re.sub("[^А-Яа-я ]", "", x))
df['Заголовок_temp'] = df['Заголовок_temp'].apply(lambda x: x.split(' '))
 
# %%
df['Заголовок'] = df['Заголовок'].str.lower()
df['Текст объявления'] = df['Текст объявления'].str.lower()
 
# %%
list_total_temp = []
 
for i in tqdm(range(0, len(df))):
    list_temp = []
    if len(df['Заголовок_temp'][i]) > 1:
        for j in df['Заголовок_temp'][i]:
            list_temp.append(morph.parse(j)[0].normal_form)
    else:
        list_temp.append('-')
        
    list_total_temp.append(list_temp)
 
# %%
df['Заголовок_temp'] = list_total_temp
 
# %%
 
# %% [markdown]
# ## ПР-СК
 
# %%
df = pd.read_excel('проверить_v10.xlsx')
 
# %%
dict_temp = pd.read_excel('support_files\Словари\Коммерция_Словари_Определенный вид бизнеса.xlsx', sheet_name='ПР-СК')
 
# %%
word_in_head = list(dict_temp[dict_temp['Шапка объявления'].notnull()]['Шапка объявления'])
word_in_head_not = list(dict_temp[dict_temp['НО НЕ'].notnull()]['НО НЕ'])
 
word_in_head_not_set = []
for word in word_in_head_not:
    word_in_head_not_set.append(set(word.split('+')))
 
word_in_body_not = list(dict_temp[dict_temp['НОНЕ текст СКЛАД шапка'].notnull()]['НОНЕ текст СКЛАД шапка'])
 
# %%
df_temp_1 = df[df['Заголовок'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_head))][['Заголовок', 'Текст объявления', 'уникальный номер']]
 
# %%
word_in_head_2 = re.compile("|".join(word_in_head))
df_temp_1['слова-маркеры'] = df_temp_1['Заголовок'].apply(lambda x: word_in_head_2.search(x.lower()).group())
 
# %%
def contains_all_parts_of_any_set(text, word_sets):
    for word_set in word_sets:
        if all(re.search(part, text) for part in word_set):
            return True
    return False
 
# %%
df_temp_1 = df_temp_1[~df_temp_1['Заголовок'].str.lower().apply(lambda x: contains_all_parts_of_any_set(x, word_in_head_not_set))]
 
# %%
mask_marker = df_temp_1['слова-маркеры'] == 'склад'
mask_text = df_temp_1['Текст объявления'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_body_not) if pd.notna(x) else False)
combined_mask = mask_marker & mask_text
df_temp_1 = df_temp_1[~combined_mask]
 
# %%
df_temp_2 = df[['Заголовок', 'Текст объявления', 'уникальный номер', 'Имя файла', 'Зона укрупненно']]
df_temp_2 = df_temp_2[(df_temp_2['Имя файла'].str.contains('Other') == True) & (df_temp_2['Зона укрупненно'] == 'Производственная') & (df_temp_2['Текст объявления'].str.contains('производ', case=False))][['Заголовок', 'Текст объявления', 'уникальный номер']]
df_temp_2['слова-маркеры'] = 'производ'
df_temp_2 = df_temp_2[~df_temp_2['Текст объявления'].str.contains('административн')]
 
# %%
## объединение
df_temp = pd.concat([df_temp_1, df_temp_2], ignore_index=True)
df_temp['temp'] = df_temp['Заголовок'].astype('str')+'_'+df_temp['Текст объявления'].astype('str')+'_'+df_temp['уникальный номер'].astype('str')
df_temp = df_temp.drop_duplicates('temp').reset_index(drop=True)
dict_df_temp = dict(df_temp[['temp', 'слова-маркеры']].values)
 
df['temp'] = df['Заголовок'].astype('str')+'_'+df['Текст объявления'].astype('str')+'_'+df['уникальный номер'].astype('str')
df['слова-маркеры'] = df['temp'].apply(lambda x: dict_df_temp.get(x, '-'))
 
df = df.drop(columns = ['temp'])
 
# %%
df.loc[df['слова-маркеры'] != '-', 'Вид бизнеса'] = 'ПР-СК'
 
# %% [markdown]
# ## ОФ-ТОРГ В БЦ_ТЦ
 
# %%
dict_temp = pd.read_excel('support_files\Словари\Коммерция_Словари_Определенный вид бизнеса.xlsx', sheet_name='ОФ-ТОРГ В БЦ_ТЦ')
 
df['Шапка+Текст'] = df['Заголовок'].fillna('') + ' ' + df['Текст объявления'].fillna('')
 
# %%
word_in_ad = list(dict_temp[dict_temp['Шапка/Текст объявления'].notnull()]['Шапка/Текст объявления'])
word_in_ad_not = list(dict_temp[dict_temp['НО НЕ'].notnull()]['НО НЕ'])
 
word_in_ad = list(map(lambda s: s.replace('_', ' '), word_in_ad))
 
# %%
df_temp = df[df['Шапка+Текст'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_ad))][['Шапка+Текст', 'Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_ad = re.compile("|".join(word_in_ad))
df_temp['слова-маркеры'] = df_temp['Шапка+Текст'].apply(lambda x: word_in_ad.search(x.lower()).group())
df_temp = df_temp[['Шапка+Текст', 'Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
# %%
df_temp = df_temp[~df_temp['Шапка+Текст'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_ad_not))]
 
# %%
df_temp = df_temp.drop(columns = ['Шапка+Текст'])
df = df.drop(columns = ['Шапка+Текст'])
 
# %%
df_temp['temp'] = df_temp['Заголовок'].astype('str')+'_'+df_temp['Текст объявления'].astype('str')+'_'+df_temp['уникальный номер'].astype('str')
dict_df_temp = dict(df_temp[['temp', 'слова-маркеры']].values)
 
df['temp'] = df['Заголовок'].astype('str')+'_'+df['Текст объявления'].astype('str')+'_'+df['уникальный номер'].astype('str')
df['слова-маркеры (1)'] = df['temp'].apply(lambda x: dict_df_temp.get(x, '-'))
 
df = df.drop(columns = ['temp'])
 
# %%
df.loc[df['слова-маркеры'] == '-', 'слова-маркеры'] = df['слова-маркеры (1)']
df.loc[(df['Вид бизнеса'].astype('str') == 'nan') & (df['слова-маркеры'] != '-'), 'Вид бизнеса'] = 'ОФ-ТОРГ В БЦ_ТЦ'
df = df.drop(columns = ['слова-маркеры (1)'])
 
# %% [markdown]
# ## АДМ ЗД(ПР-СК)
 
# %%
df_temp = df[['Заголовок', 'Текст объявления', 'уникальный номер', 'Имя файла', 'Зона укрупненно']]
df_temp = df_temp[(df_temp['Имя файла'].str.contains('Other') == True) & (df_temp['Зона укрупненно'] == 'Производственная') & (df_temp['Текст объявления'].str.contains('административно', case=False))][['Заголовок', 'Текст объявления', 'уникальный номер']]
df_temp['слова-маркеры'] = 'административно'
 
# %%
df_temp['temp'] = df_temp['Заголовок'].astype('str')+'_'+df_temp['Текст объявления'].astype('str')+'_'+df_temp['уникальный номер'].astype('str')
dict_df_temp = dict(df_temp[['temp', 'слова-маркеры']].values)
 
df['temp'] = df['Заголовок'].astype('str')+'_'+df['Текст объявления'].astype('str')+'_'+df['уникальный номер'].astype('str')
df['слова-маркеры (1)'] = df['temp'].apply(lambda x: dict_df_temp.get(x, '-'))
 
df = df.drop(columns = ['temp'])
 
# %%
df.loc[df['слова-маркеры'] == '-', 'слова-маркеры'] = df['слова-маркеры (1)']
df.loc[(df['Вид бизнеса'].astype('str') == 'nan') & (df['слова-маркеры'] != '-'), 'Вид бизнеса'] = 'АДМ ЗД(ПР-СК)'
df = df.drop(columns = ['слова-маркеры (1)'])
 
# %% [markdown]
# ## АДМ ЗД(ОФ-ТОРГ)
 
# %%
dict_temp = pd.read_excel('support_files\Словари\Коммерция_Словари_Определенный вид бизнеса.xlsx', sheet_name='АДМ ЗД(ОФ-ТОРГ)')
 
word_in_head = list(dict_temp[dict_temp['Шапка объявления'].notnull()]['Шапка объявления'])
word_in_body = list(dict_temp[dict_temp['Текст объявления'].notnull()]['Текст объявления'])
 
word_in_head = list(map(lambda s: s.replace('_', ' '), word_in_head))
 
df_temp_1 = df[df['Заголовок'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_head))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_head = re.compile("|".join(word_in_head))
df_temp_1['слова-маркеры'] = df_temp_1['Заголовок'].apply(lambda x: word_in_head.search(x.lower()).group())
df_temp_1 = df_temp_1[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
df_temp_2 = df[df['Текст объявления'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_body))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_body = re.compile("|".join(word_in_body))
df_temp_2['слова-маркеры'] = df_temp_2['Текст объявления'].apply(lambda x: word_in_body.search(x.lower()).group())
df_temp_2 = df_temp_2[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
## объединение
df_temp = pd.concat([df_temp_1, df_temp_2], ignore_index=True)
df_temp['temp'] = df_temp['Заголовок'].astype('str')+'_'+df_temp['Текст объявления'].astype('str')+'_'+df_temp['уникальный номер'].astype('str')
df_temp = df_temp.drop_duplicates('temp').reset_index(drop=True)
dict_df_temp = dict(df_temp[['temp', 'слова-маркеры']].values)
 
df['temp'] = df['Заголовок'].astype('str')+'_'+df['Текст объявления'].astype('str')+'_'+df['уникальный номер'].astype('str')
df['слова-маркеры (1)'] = df['temp'].apply(lambda x: dict_df_temp.get(x, '-'))
 
df = df.drop(columns = ['temp'])
 
# %%
df.loc[df['слова-маркеры'] == '-', 'слова-маркеры'] = df['слова-маркеры (1)']
df.loc[(df['Вид бизнеса'].astype('str') == 'nan') & (df['слова-маркеры'] != '-'), 'Вид бизнеса'] = 'АДМ ЗД(ОФ-ТОРГ)'
df = df.drop(columns = ['слова-маркеры (1)'])
 
# %% [markdown]
# ## Павильон
 
# %%
dict_temp = pd.read_excel('support_files\Словари\Коммерция_Словари_Определенный вид бизнеса.xlsx', sheet_name='Павильон')
 
word_in_head = list(dict_temp[dict_temp['Шапка объявления'].notnull()]['Шапка объявления'])
word_in_body = list(dict_temp[dict_temp['Текст объявления'].notnull()]['Текст объявления'])
 
word_in_head_not = list(dict_temp[dict_temp['Шапка НО НЕ'].notnull()]['Шапка НО НЕ'])
word_in_body_not = list(dict_temp[dict_temp['Текст НО НЕ'].notnull()]['Текст НО НЕ'])
 
word_in_head = list(map(lambda s: s.replace('_', ' '), word_in_head))
word_in_body = list(map(lambda s: s.replace('_', ' '), word_in_body))
word_in_body_not = list(map(lambda s: s.replace('_', ' '), word_in_body_not))
 
word_in_head_set = []
for word in word_in_head:
    word_in_head_set.append(set(word.split('+')))
 
word_in_body_not_set = []
for word in word_in_body_not:
    word_in_body_not_set.append(set(word.split('+')))
 
def contains_all_parts_of_any_set(text, word_sets):
    for word_set in word_sets:
        if all(re.search(part, text) for part in word_set):
            return True
    return False
 
df_temp_1 = df[df['Заголовок'].str.lower().apply(lambda x: contains_all_parts_of_any_set(x, word_in_head_set))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_head_set = [word for s in word_in_head_set for word in s]
word_in_head_set = re.compile("|".join(word_in_head_set))
df_temp_1['слова-маркеры'] = df_temp_1['Заголовок'].apply(lambda x: word_in_head_set.search(x.lower()).group())
df_temp_1 = df_temp_1[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
df_temp_1 = df_temp_1[~df_temp_1['Заголовок'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_head_not))]
df_temp_1.loc[df_temp_1['слова-маркеры'] == 'торг', 'слова-маркеры'] = 'торг+точк'
df_temp_1.loc[df_temp_1['слова-маркеры'] == 'точк', 'слова-маркеры'] = 'торг+точк'
 
df_temp_2 = df[df['Текст объявления'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_body))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_body = re.compile("|".join(word_in_body))
df_temp_2['слова-маркеры'] = df_temp_2['Текст объявления'].apply(lambda x: word_in_body.search(x.lower()).group())
df_temp_2 = df_temp_2[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
df_temp_2 = df_temp_2[~df_temp_2['Текст объявления'].str.lower().apply(lambda x: contains_all_parts_of_any_set(x, word_in_body_not_set))]
 
## объединение
df_temp = pd.concat([df_temp_1, df_temp_2], ignore_index=True)
df_temp['temp'] = df_temp['Заголовок'].astype('str')+'_'+df_temp['Текст объявления'].astype('str')+'_'+df_temp['уникальный номер'].astype('str')
df_temp = df_temp.drop_duplicates('temp').reset_index(drop=True)
dict_df_temp = dict(df_temp[['temp', 'слова-маркеры']].values)
 
df['temp'] = df['Заголовок'].astype('str')+'_'+df['Текст объявления'].astype('str')+'_'+df['уникальный номер'].astype('str')
df['слова-маркеры (1)'] = df['temp'].apply(lambda x: dict_df_temp.get(x, '-'))
 
df = df.drop(columns = ['temp'])
 
# %%
df.loc[df['слова-маркеры'] == '-', 'слова-маркеры'] = df['слова-маркеры (1)']
df.loc[(df['Вид бизнеса'].astype('str') == 'nan') & (df['слова-маркеры'] != '-'), 'Вид бизнеса'] = 'Павильон'
df = df.drop(columns = ['слова-маркеры (1)'])
 
# %% [markdown]
# ## Кладовые
 
# %%
dict_temp = pd.read_excel('support_files\Словари\Коммерция_Словари_Определенный вид бизнеса.xlsx', sheet_name='Кладовые')
 
word_in_head = list(dict_temp[dict_temp['Шапка объявления'].notnull()]['Шапка объявления'])
word_in_body = list(dict_temp[dict_temp['Текст объявления'].notnull()]['Текст объявления'])
 
df_temp_1 = df[df['Заголовок'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_head))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_head = re.compile("|".join(word_in_head))
df_temp_1['слова-маркеры'] = df_temp_1['Заголовок'].apply(lambda x: word_in_head.search(x.lower()).group())
df_temp_1 = df_temp_1[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
df_temp_2 = df[df['Текст объявления'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_body))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_body = re.compile("|".join(word_in_body))
df_temp_2['слова-маркеры'] = df_temp_2['Текст объявления'].apply(lambda x: word_in_body.search(x.lower()).group())
df_temp_2 = df_temp_2[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
## объединение
df_temp = pd.concat([df_temp_1, df_temp_2], ignore_index=True)
df_temp['temp'] = df_temp['Заголовок'].astype('str')+'_'+df_temp['Текст объявления'].astype('str')+'_'+df_temp['уникальный номер'].astype('str')
df_temp = df_temp.drop_duplicates('temp').reset_index(drop=True)
dict_df_temp = dict(df_temp[['temp', 'слова-маркеры']].values)
 
df['temp'] = df['Заголовок'].astype('str')+'_'+df['Текст объявления'].astype('str')+'_'+df['уникальный номер'].astype('str')
df['слова-маркеры (1)'] = df['temp'].apply(lambda x: dict_df_temp.get(x, '-'))
 
df = df.drop(columns = ['temp'])
 
# %%
df.loc[df['слова-маркеры'] == '-', 'слова-маркеры'] = df['слова-маркеры (1)']
df.loc[(df['Вид бизнеса'].astype('str') == 'nan') & (df['слова-маркеры'] != '-'), 'Вид бизнеса'] = 'Кладовые'
df = df.drop(columns = ['слова-маркеры (1)'])
 
# %% [markdown]
# ## Дома отдых, санатории
 
# %%
dict_temp = pd.read_excel('support_files\Словари\Коммерция_Словари_Определенный вид бизнеса.xlsx', sheet_name='Дома отдых, санатории')
 
word_in_head = list(dict_temp[dict_temp['Шапка объявления'].notnull()]['Шапка объявления'])
word_in_body = list(dict_temp[dict_temp['Текст объявления'].notnull()]['Текст объявления'])
word_in_var = (dict_temp[dict_temp['Шапка объявления (с вариативностью)'].notnull()])
 
df_temp_1 = df[df['Заголовок'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_head))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_head = re.compile("|".join(word_in_head))
df_temp_1['слова-маркеры'] = df_temp_1['Заголовок'].apply(lambda x: word_in_head.search(x.lower()).group())
df_temp_1 = df_temp_1[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
df_temp_2 = df[df['Текст объявления'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_body))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_body = re.compile("|".join(word_in_body))
df_temp_2['слова-маркеры'] = df_temp_2['Текст объявления'].apply(lambda x: word_in_body.search(x.lower()).group())
df_temp_2 = df_temp_2[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
word_in_var['temp'] = word_in_var['Шапка объявления (с вариативностью)'].apply(lambda x: x.split(';'))
word_in_var = list(word_in_var['temp'])
l = []
for i in word_in_var:
    t = df[(df['Заголовок_temp'].astype('str').apply(lambda x: i[0] in x.lower())) & (df['Заголовок_temp'].astype('str').apply(lambda x: i[1] in x.lower()))]
    t['слова-маркеры'] = str(i[0])+';'+str(i[1])
    
    l.append(t[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']])  
df_temp_3 = pd.concat(l)
 
## объединение
df_temp = pd.concat([df_temp_1, df_temp_2, df_temp_3], ignore_index=True)
df_temp['temp'] = df_temp['Заголовок'].astype('str')+'_'+df_temp['Текст объявления'].astype('str')+'_'+df_temp['уникальный номер'].astype('str')
df_temp = df_temp.drop_duplicates('temp').reset_index(drop=True)
dict_df_temp = dict(df_temp[['temp', 'слова-маркеры']].values)
 
df['temp'] = df['Заголовок'].astype('str')+'_'+df['Текст объявления'].astype('str')+'_'+df['уникальный номер'].astype('str')
df['слова-маркеры (1)'] = df['temp'].apply(lambda x: dict_df_temp.get(x, '-'))
 
df = df.drop(columns = ['temp'])
 
# %%
df.loc[df['слова-маркеры'] == '-', 'слова-маркеры'] = df['слова-маркеры (1)']
df.loc[(df['Вид бизнеса'].astype('str') == 'nan') & (df['слова-маркеры'] != '-'), 'Вид бизнеса'] = 'Дома отдых, санатории'
df = df.drop(columns = ['слова-маркеры (1)'])
 
# %% [markdown]
# ## Апартаменты
 
# %%
dict_temp = pd.read_excel('support_files\Словари\Коммерция_Словари_Определенный вид бизнеса.xlsx', sheet_name='Апартаменты')
 
word_in_head = list(dict_temp[dict_temp['Шапка объявления'].notnull()]['Шапка объявления'])
word_in_body = list(dict_temp[dict_temp['Текст объявления'].notnull()]['Текст объявления'])
 
df_temp_1 = df[df['Заголовок'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_head))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_head = re.compile("|".join(word_in_head))
df_temp_1['слова-маркеры'] = df_temp_1['Заголовок'].apply(lambda x: word_in_head.search(x.lower()).group())
df_temp_1 = df_temp_1[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
df_temp_2 = df[df['Текст объявления'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_body))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_body = re.compile("|".join(word_in_body))
df_temp_2['слова-маркеры'] = df_temp_2['Текст объявления'].apply(lambda x: word_in_body.search(x.lower()).group())
df_temp_2 = df_temp_2[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
## объединение
df_temp = pd.concat([df_temp_1, df_temp_2], ignore_index=True)
df_temp['temp'] = df_temp['Заголовок'].astype('str')+'_'+df_temp['Текст объявления'].astype('str')+'_'+df_temp['уникальный номер'].astype('str')
df_temp = df_temp.drop_duplicates('temp').reset_index(drop=True)
dict_df_temp = dict(df_temp[['temp', 'слова-маркеры']].values)
 
df['temp'] = df['Заголовок'].astype('str')+'_'+df['Текст объявления'].astype('str')+'_'+df['уникальный номер'].astype('str')
df['слова-маркеры (1)'] = df['temp'].apply(lambda x: dict_df_temp.get(x, '-'))
 
df = df.drop(columns = ['temp'])
 
# %%
df.loc[df['слова-маркеры'] == '-', 'слова-маркеры'] = df['слова-маркеры (1)']
df.loc[(df['Вид бизнеса'].astype('str') == 'nan') & (df['слова-маркеры'] != '-'), 'Вид бизнеса'] = 'Апартаменты'
df = df.drop(columns = ['слова-маркеры (1)'])
 
# %% [markdown]
# ## Гостиницы
 
# %%
dict_temp = pd.read_excel('support_files\Словари\Коммерция_Словари_Определенный вид бизнеса.xlsx', sheet_name='Гостиницы')
 
word_in_head = list(dict_temp[dict_temp['Шапка объявления'].notnull()]['Шапка объявления'])
word_in_body = list(dict_temp[dict_temp['Текст объявления'].notnull()]['Текст объявления'])
word_in_head_not = list(dict_temp[dict_temp['НО НЕ'].notnull()]['НО НЕ'])
 
word_in_head = list(map(lambda s: s.replace('_', ' '), word_in_head))
 
word_in_head_not_set = []
for word in word_in_head_not:
    word_in_head_not_set.append(set(word.split('+')))
 
def contains_all_parts_of_any_set(text, word_sets):
    for word_set in word_sets:
        if all(re.search(part, text) for part in word_set):
            return True
    return False
 
df_temp_1 = df[df['Заголовок'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_head))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_head = re.compile("|".join(word_in_head))
df_temp_1['слова-маркеры'] = df_temp_1['Заголовок'].apply(lambda x: word_in_head.search(x.lower()).group())
df_temp_1 = df_temp_1[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
df_temp_1 = df_temp_1[~df_temp_1['Заголовок'].str.lower().apply(lambda x: contains_all_parts_of_any_set(x, word_in_head_not_set))]
 
df_temp_2 = df[df['Текст объявления'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_body))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_body = re.compile("|".join(word_in_body))
df_temp_2['слова-маркеры'] = df_temp_2['Текст объявления'].apply(lambda x: word_in_body.search(x.lower()).group())
df_temp_2 = df_temp_2[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
## объединение
df_temp = pd.concat([df_temp_1, df_temp_2], ignore_index=True)
df_temp['temp'] = df_temp['Заголовок'].astype('str')+'_'+df_temp['Текст объявления'].astype('str')+'_'+df_temp['уникальный номер'].astype('str')
df_temp = df_temp.drop_duplicates('temp').reset_index(drop=True)
dict_df_temp = dict(df_temp[['temp', 'слова-маркеры']].values)
 
df['temp'] = df['Заголовок'].astype('str')+'_'+df['Текст объявления'].astype('str')+'_'+df['уникальный номер'].astype('str')
df['слова-маркеры (1)'] = df['temp'].apply(lambda x: dict_df_temp.get(x, '-'))
 
df = df.drop(columns = ['temp'])
 
# %%
df.loc[df['слова-маркеры'] == '-', 'слова-маркеры'] = df['слова-маркеры (1)']
df.loc[(df['Вид бизнеса'].astype('str') == 'nan') & (df['слова-маркеры'] != '-'), 'Вид бизнеса'] = 'Гостиницы'
df = df.drop(columns = ['слова-маркеры (1)'])
 
# %% [markdown]
# ## Бани, сауны
 
# %%
dict_temp = pd.read_excel('support_files\Словари\Коммерция_Словари_Определенный вид бизнеса.xlsx', sheet_name='Бани, сауны')
 
word_in_head = list(dict_temp[dict_temp['Шапка объявления'].notnull()]['Шапка объявления'])
word_in_body = list(dict_temp[dict_temp['Текст объявления'].notnull()]['Текст объявления'])
 
word_in_body = list(map(lambda s: s.replace('_', ' '), word_in_body))
 
df_temp_1 = df[df['Заголовок'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_head))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_head = re.compile("|".join(word_in_head))
df_temp_1['слова-маркеры'] = df_temp_1['Заголовок'].apply(lambda x: word_in_head.search(x.lower()).group())
df_temp_1 = df_temp_1[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
df_temp_2 = df[df['Текст объявления'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_body))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_body = re.compile("|".join(word_in_body))
df_temp_2['слова-маркеры'] = df_temp_2['Текст объявления'].apply(lambda x: word_in_body.search(x.lower()).group() if word_in_body.search(x.lower()) is not None else None)
df_temp_2 = df_temp_2[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
## объединение
df_temp = pd.concat([df_temp_1, df_temp_2], ignore_index=True)
df_temp['temp'] = df_temp['Заголовок'].astype('str')+'_'+df_temp['Текст объявления'].astype('str')+'_'+df_temp['уникальный номер'].astype('str')
df_temp = df_temp.drop_duplicates('temp').reset_index(drop=True)
dict_df_temp = dict(df_temp[['temp', 'слова-маркеры']].values)
 
df['temp'] = df['Заголовок'].astype('str')+'_'+df['Текст объявления'].astype('str')+'_'+df['уникальный номер'].astype('str')
df['слова-маркеры (1)'] = df['temp'].apply(lambda x: dict_df_temp.get(x, '-'))
 
df = df.drop(columns = ['temp'])
 
# %%
df.loc[df['слова-маркеры'] == '-', 'слова-маркеры'] = df['слова-маркеры (1)']
df.loc[(df['Вид бизнеса'].astype('str') == 'nan') & (df['слова-маркеры'] != '-'), 'Вид бизнеса'] = 'Бани, сауны'
df = df.drop(columns = ['слова-маркеры (1)'])
 
# %% [markdown]
# ## Общепит
 
# %%
dict_temp = pd.read_excel('support_files\Словари\Коммерция_Словари_Определенный вид бизнеса.xlsx', sheet_name='Общепит')
 
word_in_head = list(dict_temp[dict_temp['Шапка объявления'].notnull()]['Шапка объявления'])
word_in_body = list(dict_temp[dict_temp['Текст объявления'].notnull()]['Текст объявления'])
word_in_head_not = list(dict_temp[dict_temp['НО НЕ'].notnull()]['НО НЕ'])
 
df_temp_1 = df[df['Заголовок'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_head))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_head = re.compile("|".join(word_in_head))
df_temp_1['слова-маркеры'] = df_temp_1['Заголовок'].apply(lambda x: word_in_head.search(x.lower()).group())
df_temp_1 = df_temp_1[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
df_temp_1 = df_temp_1[~df_temp_1['Заголовок'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_head_not))]
 
df_temp_2 = df[df['Текст объявления'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_body))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_body = re.compile("|".join(word_in_body))
df_temp_2['слова-маркеры'] = df_temp_2['Текст объявления'].apply(lambda x: word_in_body.search(x.lower()).group())
df_temp_2 = df_temp_2[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
## объединение
df_temp = pd.concat([df_temp_1, df_temp_2], ignore_index=True)
df_temp['temp'] = df_temp['Заголовок'].astype('str')+'_'+df_temp['Текст объявления'].astype('str')+'_'+df_temp['уникальный номер'].astype('str')
df_temp = df_temp.drop_duplicates('temp').reset_index(drop=True)
dict_df_temp = dict(df_temp[['temp', 'слова-маркеры']].values)
 
df['temp'] = df['Заголовок'].astype('str')+'_'+df['Текст объявления'].astype('str')+'_'+df['уникальный номер'].astype('str')
df['слова-маркеры (1)'] = df['temp'].apply(lambda x: dict_df_temp.get(x, '-'))
 
df = df.drop(columns = ['temp'])
 
# %%
df.loc[df['слова-маркеры'] == '-', 'слова-маркеры'] = df['слова-маркеры (1)']
df.loc[(df['Вид бизнеса'].astype('str') == 'nan') & (df['слова-маркеры'] != '-'), 'Вид бизнеса'] = 'Общепит'
df = df.drop(columns = ['слова-маркеры (1)'])
 
# %% [markdown]
# ## Объекты автосервис
 
# %%
dict_temp = pd.read_excel('support_files\Словари\Коммерция_Словари_Определенный вид бизнеса.xlsx', sheet_name='Объекты автосервис')
 
word_in_head = list(dict_temp[dict_temp['Шапка объявления'].notnull()]['Шапка объявления'])
word_in_body = list(dict_temp[dict_temp['Текст объявления'].notnull()]['Текст объявления'])
word_in_var = (dict_temp[dict_temp['Шапка объявления (с вариативностью)'].notnull()])
word_in_var_body = (dict_temp[dict_temp['Текст объявления (с вариативностью)'].notnull()])
word_in_ad_not = list(dict_temp[dict_temp['НО НЕ'].notnull()]['НО НЕ'])
 
word_in_head = list(map(lambda s: s.replace('_', ' '), word_in_head))
word_in_body = list(map(lambda s: s.replace('_', ' '), word_in_body))
 
df_temp_1 = df[df['Заголовок'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_head))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_head = re.compile("|".join(word_in_head))
df_temp_1['слова-маркеры'] = df_temp_1['Заголовок'].apply(lambda x: word_in_head.search(x.lower()).group())
df_temp_1 = df_temp_1[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
df_temp_2 = df[df['Текст объявления'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_body))][['Заголовок', 'Текст объявления', 'уникальный номер']]
word_in_body = re.compile("|".join(word_in_body))
df_temp_2['слова-маркеры'] = df_temp_2['Текст объявления'].apply(lambda x: word_in_body.search(x.lower()).group())
df_temp_2 = df_temp_2[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']]
 
word_in_var['temp'] = word_in_var['Шапка объявления (с вариативностью)'].apply(lambda x: x.split(';'))
word_in_var = list(word_in_var['temp'])
l = []
for i in word_in_var:
    t = df[(df['Заголовок_temp'].astype('str').apply(lambda x: i[0] in x.lower())) & (df['Заголовок_temp'].astype('str').apply(lambda x: i[1] in x.lower()))].reset_index(drop=True)
    idx_temp = []
    for j in range(0, len(t)):
        idx_first_word = t['Заголовок_temp'].astype('str')[j].index(i[0])+len(i[0])
        idx_second_word = t['Заголовок_temp'].astype('str')[j].index(i[1])
        if ((idx_second_word-idx_first_word>=0) & (idx_second_word-idx_first_word<4)):
            idx_temp.append(j)
    t = t[t.index.isin(idx_temp)].reset_index(drop=True)
    t['слова-маркеры'] = str(i[0])+';'+str(i[1])
    
    l.append(t[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']])  
df_temp_3 = pd.concat(l)
 
word_in_var_body['temp'] = word_in_var_body['Текст объявления (с вариативностью)'].apply(lambda x: x.split(';'))
word_in_var_body = list(word_in_var_body['temp'])
l = []
for i in word_in_var_body:
    t = df[(df['Текст объявления'].astype('str').apply(lambda x: i[0] in x.lower())) & (df['Текст объявления'].astype('str').apply(lambda x: i[1] in x.lower()))].reset_index(drop=True)
    idx_temp = []
    for j in range(0, len(t)):
        idx_first_word = t['Текст объявления'].astype('str')[j].index(i[0])+len(i[0])
        idx_second_word = t['Текст объявления'].astype('str')[j].index(i[1])
        if ((idx_second_word-idx_first_word>=0) & (idx_second_word-idx_first_word<4)):
            idx_temp.append(j)
    t = t[t.index.isin(idx_temp)].reset_index(drop=True)
    t['слова-маркеры'] = str(i[0])+';'+str(i[1])
    
    l.append(t[['Заголовок', 'Текст объявления', 'уникальный номер', 'слова-маркеры']])  
df_temp_4 = pd.concat(l)
 
## объединение
df_temp = pd.concat([df_temp_1, df_temp_2, df_temp_3, df_temp_4], ignore_index=True)
df_temp['temp'] = df_temp['Заголовок'].astype('str')+'_'+df_temp['Текст объявления'].astype('str')+'_'+df_temp['уникальный номер'].astype('str')
df_temp = df_temp.drop_duplicates('temp').reset_index(drop=True)
df_temp = df_temp[~df_temp['Заголовок'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_ad_not))]
df_temp = df_temp[~df_temp['Текст объявления'].apply(lambda x: any(word.lower() in x.lower() for word in word_in_ad_not))]
dict_df_temp = dict(df_temp[['temp', 'слова-маркеры']].values)
 
df['temp'] = df['Заголовок'].astype('str')+'_'+df['Текст объявления'].astype('str')+'_'+df['уникальный номер'].astype('str')
df['слова-маркеры (1)'] = df['temp'].apply(lambda x: dict_df_temp.get(x, '-'))
 
df = df.drop(columns = ['temp'])
 
# %%
df.loc[df['слова-маркеры'] == '-', 'слова-маркеры'] = df['слова-маркеры (1)']
df.loc[(df['Вид бизнеса'].astype('str') == 'nan') & (df['слова-маркеры'] != '-'), 'Вид бизнеса'] = 'Объекты автосервис'
df = df.drop(columns = ['слова-маркеры (1)'])
df = df.drop(columns = ['Заголовок_temp'])
 
# %%
df.to_excel('проверить_v06.xlsx')
 
# %%
len(df[df['Вид бизнеса'].notna()])
 
# %%
xls = pd.ExcelFile('support_files\Словари\Коммерция Словари Вид бизнеса (бетта версия).xlsx')
for sheet_name in xls.sheet_names:
    print(sheet_name + ' ' + str(len(df[df['Вид бизнеса'] == sheet_name])))
 
# %% [markdown]
# # Подбиваем итоги по выбросам (нужен будет в конце работы)
 
# %%
control_count_result['сумма по выбросам'] = control_count_result[list(control_count_result.columns[1:])].sum(axis=1)
 
# %%
control_count_result['итого'] = control_count_result['исходное количество записей']-control_count_result['сумма по выбросам']
 
# %%
temp = pd.DataFrame(control_count_result[control_count_result.columns].sum(), columns = ['итого'])
temp = temp.T
 
# %%
control_count_result = pd.concat([control_count_result, temp])
 
# %%
control_count_result = control_count_result.reset_index().rename(columns = {'index': 'Имя файла'})
 
# %%
control_count_result['Дата выгрузки'] = control_count_result['Имя файла'].apply(lambda x: x.split('_')[1] if len(x.split('_')) > 1 else '')
 
# %%
control_count_result = control_count_result[['Имя файла', 'Дата выгрузки']+ list(control_count_result.columns[1:-1])]
 
# %%
control_count_result['Дата выгрузки'] = pd.to_datetime(control_count_result['Дата выгрузки'], dayfirst=True).dt.date
 
# %%
control_count_result.to_excel('not_use_files_for_statistic//статистика по выбросам.xlsx', index=False)
 
# %%
 
# %%
 
# %%
 
# %%
import psycopg2
import os
import pandas as pd
import numpy as np
import warnings; warnings.filterwarnings(action='ignore')
from rosreestr2coord import Area
import time
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from shapely import geometry
import pickle
import re
import datetime
import shapely.wkt
import osmnx as ox
from geopy.distance import geodesic as GD
from shapely import geometry
 
from geopy.geocoders import Nominatim
import geocoder as gc
import random
import time
from datetime import datetime
from collections import Counter
 
import pandas as pd
from collections import OrderedDict
from geopy.extra.rate_limiter import RateLimiter
from geopy.point import Point
import Levenshtein
from fuzzywuzzy import fuzz
import re
import shapely.wkt
 
from tqdm.auto import tqdm, trange
tqdm.pandas()
 
# %%
df_region_Vl_polygon = pd.read_excel('support_files//Полигоны/Населенные_пункты_полигоны.xlsx')
 
df_region_Vl_polygon['Координаты'] = (list(zip(df_region_Vl_polygon['POINT_X'], df_region_Vl_polygon['POINT_Y'])))
 
list_name = []
list_micro_region = []
 
for i in df_region_Vl_polygon['NAME_new'].unique():
    polygon = Polygon(df_region_Vl_polygon[df_region_Vl_polygon['NAME_new'] == i].reset_index()['Координаты'])
    list_name.append(i)
    list_micro_region.append(polygon)
    
df_region_Vl_polygon = pd.DataFrame({'Наименование микрорайона': list_name,
                                     'Полигон': list_micro_region})
 
# %%
df_region_Vl_polygon
 
# %%
df_region_Vl_polygon = df_region_Vl_polygon.rename(columns = {'Наименование микрорайона': 'Name',
                                                              'Полигон': 'geometry'})
 
# %%
import geopandas
df_region_Vl_polygon = geopandas.GeoDataFrame(df_region_Vl_polygon, geometry='geometry')
 
# %%
df_region_Vl_polygon.to_file('MyGeometries.shp', driver='ESRI Shapefile')
 
# %% [markdown]
# ## Проба городской район
 
# %%
df = pd.read_excel('проверить_v03.xlsx')
 
# %%
df = df.drop(columns= ['temp_1'])
 
# %%
df['Город'] = df['Город'].str.replace('ё', ' е')
df['Городской район'] = df['Городской район'].str.replace('ё', ' е')
 
# %%
#temp_np = pd.read_excel('D:/Dashboard_flats/support_files/Полигоны/Населенные_пункты_полигоны.xlsx')
temp_np = pd.read_excel('support_files/Полигоны/Населенные_пункты_полигоны.xlsx')
 
# %%
list_temp = list(temp_np['NAME'].unique()) + ['Заводской']
list_temp = [x.lower() for x in list_temp]
 
# %%
df['Городской район'] = df['Городской район'].fillna('неопределенно')
 
# %%
df['temp'] = df['Городской район'].progress_apply(lambda x: x.lower() in list_temp)
 
# %%
df['temp_1'] = df['Город'].str.lower() == df['Городской район'].str.lower()
 
# %%
df.loc[(df['Населенный пункт'].isnull()) & (df['temp'] == True) & (df['temp_1'] == False), 'temp_2'] = 'проверить'
 
# %%
df = df.drop(columns = ['temp', 'temp_1'])
 
# %%
df_temp = df[df['temp_2'] == 'проверить'].reset_index(drop=True)
 
# %%
temp = []
 
for i in tqdm(range(0, len(df_temp))):
    coord_temp = df_temp['Координаты объекта (округленные)'][i].replace('(', '').replace(')', '')
    geolocator = Nominatim(user_agent="my_request_"+str(random.randint(0,10000)))
    temp.append((geolocator.reverse(coord_temp).raw['address'].get('suburb', '-')))
 
# %%
df_temp['suburb из координат'] = temp
 
# %%
df_temp['temp_3'] = df_temp['suburb из координат'].str.lower() == df_temp['Городской район'].str.lower()
 
# %%
df_temp.to_excel('проверить.xlsx')
 
# %%
len(df)
 
# %%
 
# %%
# Подключение к бд
connection = psycopg2.connect(
    host = "192.168.1.12",
    user = "select_user",
    password = "12345",
    dbname = "BaseOKS_TEST_2")
 
# Создание курсора отправляющего зарпосы в бд
cur = connection.cursor()
 
# %%
def gkr_info(cad_num):
    cur.execute(f"""
                select cadastralnum,
                       okstype,
                       name,
                       area,
                       codesubgroup,
                       note,
                       districtname,
                       cityname,
                       localityname,
                       streetname,
                       level1name,
                       level2name
                from bufferbase b""")
    result = cur.fetchall()
    return result
 
# %%
list_cadastralnum = []
list_okstypename = []
list_name = []
list_area = []
list_codesubgroup = []
list_note = []
list_districtname = []
list_cityname = []
list_localityname = []
list_streetname = []
list_level1name = []
list_level2name = []
 
for j in gkr_info('1'):
    list_cadastralnum.append(j[0])
    list_okstypename.append(j[1])
    list_name.append(j[2])
    list_area.append(j[3])
    list_codesubgroup.append(j[4])
    list_note.append(j[5])
    list_districtname.append(j[6])
    list_cityname.append(j[7])
    list_localityname.append(j[8])
    list_streetname.append(j[9])
    list_level1name.append(j[10])
    list_level2name.append(j[11])
 
# %%
df_gkr = pd.DataFrame({'cadastralnum': list_cadastralnum,
                       'okstype': list_okstypename,
                       'name': list_name,
                       'area': list_area,
                       'codesubgroup': list_codesubgroup,
                       'note': list_note,
                       'districtname': list_districtname,
                       'cityname': list_cityname,
                       'localityname': list_localityname,
                       'streetname': list_streetname,
                       'level1name': list_level1name,
                       'level2name': list_level2name})
 
# %%
df_gkr.to_pickle('base_oks_test_2.pickle')
 
# %%
 
# %%
 
# %%
 
# %%
 
# %%
 
# %%
 
# %%
 
# %%
df_temp = df[df['Город'] == 'Владивосток'].reset_index(drop=True)
 
# %%
df_temp['Номер дома исход'] = df_temp['Номер дома исход'].astype('str')
df_temp['Номер дома исход'] = df_temp['Номер дома исход'].str.lower()
 
df_gkr['level1name'] = df_gkr['level1name'].astype('str')
df_gkr['level1name'] = df_gkr['level1name'].str.lower()
 
# %%
l_1 = []
l_2 = []
 
for i in tqdm(range(0, len(df_temp))):
    t = df_gkr[(df_gkr['cityname'] == df_temp['Город'][i]) & (df_gkr['streetname'] == df_temp['Улица'][i]) & (df_gkr['level1name'] == df_temp['Номер дома исход'][i]) & ((df_gkr['area']*1.01 >= df_temp['Площадь объекта(м²)'][i]) & (df_gkr['area']*0.99 <= df_temp['Площадь объекта(м²)'][i]))].reset_index(drop=True)
    if len(t) > 0:
        l_1.append(list(t['cadastralnum']))
        l_2.append(list(t['okstype']))
    else:
        l_1.append('-')
        l_2.append('-')
 
# %%
t = pd.DataFrame({'cadastralnum': l_1,
                  'okstype': l_2})
 
# %%
df_temp.join(t).to_excel('проверить_2.xlsx')
 
# %%
# df[(df['cityname'] == 'Владивосток') & (df['streetname'] == 'Кирова')].to_excel('проверить.xlsx') #& (df['level1name'].astype('str') == '26')] # & (df['area'].astype('str') == '49')]
 
# %%
len(df_temp)
 
# %%
pd.merge(df_temp, df_gkr,
         left_on = ['Город', 'Улица', 'Номер дома исход', 'Площадь объекта(м²)'],
         right_on = ['cityname', 'streetname', 'level1name', 'area'],
         how = 'left').to_excel('проверить_1.xlsx')
 
# %%
df[df.index == 0][['Площадь объекта(м²)', 'Регион',
       'Район субъекта РФ', 'Город', 'Городской район', 'Населенный пункт',
       'Территория', 'Улица', 'Тип дома', 'Номер дома исход', 'Тип блока',
       'Блок']]
 
# %%
df[df.index == 0]['Блок'][0].strip()
 
# %%
df_gkr[(df_gkr['cityname'] == df[df.index == 0]['Город'][0]) & (df_gkr['streetname'] == df[df.index == 0]['Улица'][0]) & (df_gkr['level1name'] == str(df[df.index == 0]['Номер дома исход'][0])) & (df_gkr['level2name'] == str(df[df.index == 0]['Блок'][0])) & (df_gkr['area'] == 4.2)]
 
# %%
df_gkr[(df_gkr['cityname'] == 'Владивосток') & (df_gkr['streetname'] == 'Стрелковая') & (df_gkr['level1name'] == '18') & (df_gkr['level2name'] == '2') & (df_gkr['area'] == 4.2)]
 
# %%
df_without_all_duplicates = pd.read_pickle(
        "D://Dashboard_common//dashboard_zu//page_sale//working_files_for_dashboard//pickle//market_without_all_duplicates.pickle"
    )
 
# %%
df_without_all_duplicates.to_excel('проверить_дашборд.xlsx')
 
# %%
df_temp_0 = df_without_all_duplicates[df_without_all_duplicates["segment_cut"] == "ИЖС"].reset_index(drop=True)
 
# %%
"{0:,}".format(round(df_temp_0["price_sqm"][i], 2)).replace(",", " ")
 
# %%
for i in tqdm(range(0, len(df_temp_0))):
    print("{0:,}".format(round(float(df_temp_0["price_sqm"][i]), 2)).replace(",", " "))
 
# %%
type(df_temp_0[df_temp_0.index == 630]["price_sqm"])
 
# %%
df_temp_0['price_sqm'][629]
 
# %%
 
# %%
 
# %%
 
# %%
 
# %%
 
# %%
 
# %% [markdown]
# ## Получение графики по БЦ
 
# %%
import geopandas
from folium.features import GeoJsonTooltip
 
# %%
df = pd.read_excel('БЦ_Ярина_230510.xlsx', usecols = ['Название','Адрес', 'Город', 'Поселок', 'Тип ул./пр.', 'Название улицы', '№ дома'])
 
# %%
df_1 = df[df['Поселок'].isnull()].reset_index(drop=True)
df_2 = df[~df['Поселок'].isnull()].reset_index(drop=True)
 
# %%
df_1['сцепка'] = df_1['Город'].astype('str')+', '+df_1['Название улицы'].astype('str')+', '+df_1['№ дома'].astype('str')
 
# %%
l_x = []
l_y = []
 
for i in tqdm(range(0, len(df_1))):
    geolocator = Nominatim(user_agent="my_request_"+str(random.randint(0,10000)))
    location = geolocator.geocode(df_1['сцепка'][i], exactly_one=False, timeout=20)
   
    if location != None:
        for j in range(0, len(location)):
            m = 0
            if (str(location[j]).split(',')[0][0].isdigit()):
                l_x.append(location[j].latitude)
                l_y.append(location[j].longitude)
                m = 1
                break
            
            if m == 0:
                l_x.append(location[0].latitude)
                l_y.append(location[0].longitude)
                break
    else:
        l_x.append('-')
        l_y.append('-')
 
# %%
temp = pd.DataFrame({'x': l_x,
                     'y': l_y})
 
# %%
df_1 = df_1.join(temp)
 
# %%
df_1
 
# %%
l = []
tags = {'building': True}
 
for i in tqdm(range(0, len(df_1))):
    if df_1['x'][i] != '-':
        t = ox.geometries.geometries_from_point((df_1['x'][i], df_1['y'][i]), tags, dist=20)
        polygon = t.iloc[0]['geometry']
        l.append((t.iloc[0]['geometry']))
    else:
        l.append('-')
 
# %%
df_1['geometry'] = l
 
# %%
df_1
 
# %%
df_1 = df_1.rename(columns = {'Название': 'Name'})
 
# %%
df_1_temp = df_1[df_1['geometry'] !='-'].reset_index(drop=True)
 
# %%
df_1_temp = geopandas.GeoDataFrame(df_1_temp)[['Name', 'Адрес', 'geometry']]
 
# %%
df_1_temp.crs = "epsg:4326"
 
# %%
map_group_Kray_pzz = folium.Map(location=[43.1160304, 131.8830161], tiles="openstreetmap", zoom_start=11)
 
tooltip = GeoJsonTooltip(
    fields=["Name"],
    labels=True
    )
 
folium.GeoJson(df_1_temp,
               style_function=lambda x: {"fillColor": "orange"},
               tooltip=tooltip).add_to(map_group_Kray_pzz)
 
map_group_Kray_pzz
 
# %% [markdown]
# https://gis.stackexchange.com/questions/378431/mapping-multiple-polygons-on-folium
 
# %%
df_1_temp.to_excel('проверить.xlsx')
 
# %%
i = 'Владивосток, Светланская, 83'
geolocator = Nominatim(user_agent="my_request_"+str(random.randint(0,10000)))
location = geolocator.geocode(i, exactly_one=False, timeout=20)
location
 
# %%
import osmnx as ox
import geopandas
from folium.features import GeoJsonTooltip
 
# %%
p = (43.802556, 131.944279)
 
# %%
tags = {'building': True}
 
# %%
t = ox.geometries.geometries_from_point((p[0], p[1]), tags, dist=14)
polygon = t.iloc[0]['geometry']
 
# %%
temp_df = pd.DataFrame({'name': [1]})
 
# %%
temp_df['geometry'] = polygon
 
# %%
temp_df = geopandas.GeoDataFrame(temp_df)
 
# %%
temp_df.crs = "epsg:4326"
 
# %%
map_group_Kray_pzz = folium.Map(location=[p[0], p[1]], tiles="openstreetmap", zoom_start=20)
 
folium.GeoJson(temp_df,
               style_function=lambda x: {"fillColor": "orange"}).add_to(map_group_Kray_pzz)
 
map_group_Kray_pzz
 
# %%
temp_df
 
# %%```
