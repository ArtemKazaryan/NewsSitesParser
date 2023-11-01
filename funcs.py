import sqlite3
import requests
from bs4 import BeautifulSoup
import datetime
import time


def parse_news(resource_name, resource_url, top_tag, bottom_tag, title_cut, date_cut):
    # Распаковка структур(списков) из таблицы resources:

    # Список стуктуры параметра resource_url
    resource_url_args = resource_url.split(', ')

    # Список стуктуры параметра top_tag
    top_tag_args = top_tag.split(', ')

    # Список стуктуры параметра bottom_tag
    bottom_tag_args = bottom_tag.split(', ')

    # Список стуктуры параметра title_cut
    title_cut_args = title_cut.split(', ')

    # Список стуктуры параметра date_cut
    date_cut_args = date_cut.split(', ')

    # Уровень новостного меню (счётчик)
    level_count = int(resource_url_args[2])

    # Переменная для сравнения со счётчиком
    count = level_count - 1

    # Базовая ссылка на новостное меню
    news_menu_url = resource_url_args[0] + resource_name + resource_url_args[1]

    # Выходной список, формирующий таблицу items
    news = []

    while level_count > 0:  # Для получения ссылок второго уровня
        # Получение запроса на получение меню новостей второго (следующего) уровня (для сайта с пагинацией страниц)
        if level_count == count:
            response = requests.get(news_menu_url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                paginator_a_tags = soup.find(top_tag_args[6], class_=top_tag_args[7])
                paginator_a_tag = paginator_a_tags.find(bottom_tag_args[6], class_=bottom_tag_args[7])
                news_menu_url = resource_url_args[0] + resource_name + paginator_a_tag['href']

        # Получение запроса на получение меню новостей
        response = requests.get(news_menu_url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            div_tags = soup.find_all(top_tag_args[0], class_=top_tag_args[1])

            # Получение ссылок текущего уровня
            a_hrefs = []
            for div_tag in div_tags:
                a_tags = div_tag.find_all(bottom_tag_args[0], class_=bottom_tag_args[1])
                # Получение ссылок и проверка их на полноту перед добавлением в список a_hrefs
                for a_tag in a_tags:
                    if (resource_url_args[0] + resource_name) not in a_tag['href']:
                        a_href = resource_url_args[0] + resource_name + a_tag['href']
                        a_hrefs.append(a_href)
                    else:
                        a_hrefs.append(a_tag['href'])

            # Получение запроса на получение ссылок на новости
            for i in range(len(a_hrefs)):
                response = requests.get(a_hrefs[i])
                if response.status_code == 200:
                    # Используем BeautifulSoup для парсинга HTML-контента
                    soup = BeautifulSoup(response.content, 'html.parser')
                    title = soup.find(title_cut_args[1], class_=title_cut_args[0]).text.strip()

                    # Получение даты и времени со страницы с новостью
                    newsdate = soup.find(date_cut_args[0], class_=date_cut_args[1])
                    news_datetime_str = newsdate.get('datetime')

                    # Получение содержимого со страницы с новостью
                    content = soup.find(top_tag_args[2], class_=top_tag_args[3]).text.strip()

                    # Получение даты и времени со страницы с новостью в формате Unix time
                    datetime_obj = datetime.datetime.strptime(news_datetime_str, date_cut_args[2])
                    nd_date = int(datetime_obj.timestamp())

                    # Получение даты со страницы с новостью в формате Год-Месяц-День
                    datetime_obj = datetime.datetime.strptime(news_datetime_str, date_cut_args[2])
                    not_date = datetime_obj.strftime('%Y-%m-%d')

                    # Формируем выходной список функции parse_news()
                    news.append(a_hrefs[i])
                    news.append(title)
                    news.append(content)
                    news.append(news_datetime_str)
                    news.append(nd_date)
                    news.append(not_date)

            level_count -= 1

    return news


def create_tables_and_add_resources(resources):
    conn = sqlite3.connect('parsenews.db')

    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS resources(
        resource_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        resource_name TEXT,
        resource_url TEXT,
        top_tag TEXT,
        bottom_tag TEXT,
        title_cut TEXT,
        date_cut TEXT);
    """)
    conn.commit()

    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS items(
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        res_id INT,
        link TEXT,
        title TEXT,
        content TEXT,
        nd_date TEXT,
        s_date TEXT,
        not_date TEXT);
    """)
    conn.commit()

    cur.executemany("INSERT INTO resources (resource_name, resource_url, top_tag, bottom_tag, title_cut, date_cut)"
                    " VALUES(?, ?, ?, ?, ?, ?);", resources)
    conn.commit()


def get_resource(resource_id, resource_name):
    select = f"SELECT * FROM resources WHERE resource_id={resource_id};"
    conn = sqlite3.connect('parsenews.db')

    cur = conn.cursor()
    cur.execute(select)
    result = cur.fetchone()
    resources = list(result)

    resource_name = resources[1]
    resource_url = resources[2]
    top_tag = resources[3]
    bottom_tag = resources[4]
    title_cut = resources[5]
    date_cut = resources[6]

    news = parse_news(resource_name, resource_url, top_tag, bottom_tag, title_cut, date_cut)

    from datetime import datetime
    current_datetime = datetime.now()
    items = []
    for i in range(0, len(news), 6):
        res_id = resources[0]
        link = news[i]
        title = news[i + 1]
        content = news[i + 2]
        nd_date = news[i + 4]
        s_date = int(current_datetime.timestamp())  # в формате UnixTime
        not_date = news[i + 5]
        news_item_obj = (res_id, link, title, content, nd_date, s_date, not_date)
        items.append(news_item_obj)

    cur.executemany("INSERT INTO items (res_id, link, title, content, nd_date, s_date, not_date)"
                    " VALUES(?, ?, ?, ?, ?, ?, ?);", items)
    conn.commit()

    print(f'Новости от {resource_name} получены!')
