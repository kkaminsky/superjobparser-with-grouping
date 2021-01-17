import requests
from datetime import datetime
import time
import pandas as pd
import numpy as np

seconds_in_day = 24 * 60 * 60

DELTA = 8640  # 1/10 of one day (UNIX)

START_TIME = int(time.time())

headers = {
    'content-type': 'application/json',
    'X-Api-App-Id': 'v3.r.133119248.d945118aae68aa8589b45fb090d41ff93cc758cf.454152c20c258f0d62b6179c02aa11103d0ab9a8'
}

params = {
    'catalogues': 33,  # категория IT, Интернет, связь,...
    'no_agreement': 1,  # без зп "по договоренности"
    'period': 0,  # показ активных вакансий за весь период
    'count': 100,
    'page': 0,
    # т.к api superjob не позволяет вытягивать одним запросом больше 500 сущностей даже с учетом пагинации,
    # то придется менять параметр времени в запросе
    # то есть, если резальтат запроса содержит 2800 результатов, вытянуть получится лишь 5 страниц по 100 сущностей
    # поэтому придется менять запрос по времени
    'date_published_from': START_TIME - DELTA,
    'date_published_to': START_TIME
}

URL = "https://api.superjob.ru/2.33/vacancies/"

data = []

# api для получения списка ближайших соседей
# слова, с помощью Russian Distributional Thesaurus (граф подобия слов)
synonyms_url = "http://www.serelex.org/find/ru-skipgram-librusec/"

# ключевые слова для IT групп
# к этим словам добавятся еще подобные слова (с другими окончаниями, синонимы) с помощью графа подобия
it_groups = {
    'managers': ['менеджер', 'продаж', 'клиент', 'продавец', 'агент'],
    'analytics': ['аналитик', 'анализ', 'analysts'],
    'programmer': ['программист', 'web-разработчик', 'web-программист', 'junior', 'middle', 'senior', 'java', 'c#',
                   'python', 'javascript', 'it-специалист', '1c', 'web', 'php', 'frontend', 'backend', 'back'],
    'engineer': ['инженер', 'коммутация', 'техник', 'ремонт'],
    'admin': ['системный', 'администратор'],
    'team lead': ['руководитель', 'начальник', 'главный'],
    'support': ['поддержка', 'диспетчер', 'колл', 'call', 'оператор', 'обращение'],
    'tester': ['тестировщик', 'тестирование'],
    'data scientist': ['data']
}

# добавлние списка ближайших слов и синонимов к каждому ключевому слову в каждой группе
for key, value in it_groups.items():
    extended_words = []
    for keyword in value:
        response = requests.get(url=synonyms_url + keyword).json()
        if 'relations' in response:
            synonyms_word = [i['word'] for i in response['relations'][:5]]
            extended_words.extend(synonyms_word)
        extended_words.append(keyword)
    it_groups[key] = extended_words


def find_keywords_group(words):
    for word in words:
        group = find_keyword_group(word)
        if group != "other":
            return group
    return "other"


def find_keyword_group(word):
    for group_key, group_keywords in it_groups.items():
        if word.lower() in group_keywords:
            return group_key
    return "other"


def get_mean_salary(_df):
    _df["min_salary"] = _df["min_salary"].fillna(_df.groupby("town")["min_salary"].transform("mean"))
    _df["max_salary"] = _df["max_salary"].fillna(_df.groupby("town")["max_salary"].transform("mean"))
    return _df


def map_vacancy(v):
    min_salary = v['payment_from']
    max_salary = v['payment_to']

    if min_salary == 0 and max_salary == 0:
        max_salary, min_salary = np.nan, np.nan
    if min_salary == 0:
        min_salary = max_salary
    if max_salary == 0:
        max_salary = min_salary

    return {
        'name': v['profession'],
        'it_group': find_keywords_group(v['profession'].replace('-', ' ').split(' ')),
        'town': v['town']['title'],
        'min_salary': min_salary,
        'max_salary': max_salary,
        'company_name': v['firm_name'],
        'date_published': datetime.utcfromtimestamp(v['date_published']).strftime('%Y-%m-%d %H:%M:%S'),
        'date_delta': datetime.now() - datetime.utcfromtimestamp(v['date_published']),
        'experience': v['experience']['title'],  # всегда указан
        'type_of_work': v['type_of_work']['title'],  # всегда указан
        'description': v['vacancyRichText'],
        'duties': v['candidat'],  # обязаности и требования
        'conditions': v['compensation'],  # условия
        'key_skills': [y['title'] for x in v['catalogues'] for y in x['positions'] if x['id'] == 33]
    }


while True:
    response = requests.get(url=URL, headers=headers, params=params)
    data_1 = response.json()
    data.extend(map(map_vacancy, data_1['objects']))
    print(datetime.utcfromtimestamp(params['date_published_to']).strftime('%Y-%m-%d %H:%M:%S'))
    print(datetime.utcfromtimestamp(params['date_published_from']).strftime('%Y-%m-%d %H:%M:%S'))
    print(len(data_1['objects']))
    print(len(data))
    params['page'] += 1
    if not data_1['more']:
        params['page'] = 0
        params['date_published_from'] -= DELTA
        params['date_published_to'] -= DELTA
        if params['date_published_to'] < START_TIME - DELTA * 10 * 10:
            break

data_frame = pd.DataFrame.from_records(data)

data_frame.to_csv('lab04.csv', encoding='utf-8-sig')

# группировка по IT группам и вычилсение срденей зп где необходимо зп
df_dict_1 = {}
for name, grouped_data_frame in data_frame.groupby('it_group'):
    _df = grouped_data_frame
    _df = get_mean_salary(_df)
    df_dict_1[name] = _df
