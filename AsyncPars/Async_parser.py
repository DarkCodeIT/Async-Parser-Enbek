import json, lxml, time, asyncio, aiohttp, aiofiles
from icecream import ic
from bs4 import BeautifulSoup
from All_data_parser import all_city_data,all_prof_data

#Основной словарь всех нужных данных сайта:
data = {'Машинист': [], 'Менеджер': [], 'Аналитик': [], 'Дизайнер': [], 'Програмист': [], 'Оператор': [],'Мастер': []
    , 'Автомеханик': [], 'Администратор': [], 'Косметолог': [], 'Мастер декоративных работ': []
    , 'Метролог': [], 'Оптик-механик': [], 'Парикмахер': [], 'Строитель': [], 'Разнорабочий': [], 'Повар': [],'Кондитер': []
    , 'Сантехник': [], 'Сварщик': [], 'Слесарь': [], 'Специалист': [], 'Техник': [],
    "Учитель" : [], "Тренер" : [], "Санитар" : [], "Врач" : [], "Бухгалтер" : [], "Агроном" : [],
    "Психолог" : [], "Швея" : [], "Кассир" : [], "Мойщик" : [], "Уборщик" : [], "Дворник" : [],
    "Инженер" : [], "Рекламный агент" : [], "IT" : [], "Секретарь" : [], "Ведущий специалист" : [],
    "Юрист" : [],"Адвокат" : []    
    }
headers = {"Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}



#Получение информации о вакансии :
async def get_data(link,prof,city_id):
    try:
        #Создание сессии подключения :
        session = aiohttp.ClientSession()
        async with session.get(url=link,headers=headers) as response:
            response_text = await response.text()

            #Начнем парсить все необходимые данные
            soup = BeautifulSoup(response_text,'lxml')
            #В переменной data содержится вся нужная инфа
            data_html = soup.find('div',class_='col-lg-9 order-0')

            #Название профессии
            name = data_html.find('h4',class_='title')
            name = name.text

            #ЗП професии
            price = data_html.find('div',class_='price')
            price = price.text

            #Тип занятости професии
            type_work = data_html.find('ul',class_='info d-flex flex-column').find('li').find_all('span')
            type_work = type_work[1].text

            #Обязанности професии
            rslit = data_html.find_all('div',class_='single-line')[2].find('div',class_='value').text.replace("\n","").strip().split(';')
            for u in range(len(rslit)):
                rslit[u] = rslit[u].strip()

            #Дата публикации
            date = data_html.find('ul',class_='info small mb-2').find('li',class_='mb-0').text.strip()
            #Парсинг данных окончен
            #Сохраняем данные
            prof_dict = {'name' : name,'price' : price,'type_work' : type_work,'responsibilities' : rslit,'date' : date,'link_vac' : link,'prof' : prof,'city_id' : city_id}
            data[prof].append(prof_dict)
            
    except Exception as ex:
        print(ex)
        
    finally:
        await session.close()


#Получение ссылок на страницу вакансии :
async def gather_data():
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        i=0
        # Получаем ссылки на профессии во всех городах
        for city in all_city_data.keys():
            city_id = all_city_data[city]
            
            for prof in all_prof_data:
                
                for page in range(1,50):

                    url = f"https://www.enbek.kz/ru/search/vacancy?prof={prof}&except[subsidized]=subsidized&region_id={city_id}&page={page}"
                    #Отправляю запрос на сайт
                    try:
                        async with session.get(url=url, headers=headers) as response:
                        # response = await session.get(url=url, headers=headers)
                            
                            #Создаю объект соупа для парсинга данных
                            soup = BeautifulSoup(await response.text(),'lxml')
    
                            #Проверка на наличие на странице вакансий
                            text = soup.find('div',class_="container mb-5").find('div',class_='row').find('div',class_='col-lg-8 col-xxl-9 position-relative content-search-vacancy').find('div',class_="text-center")
                            
                            if str(text) == 'None':
                                i += 1
                                ic(i)
                                    
                                #Продолжааем парсить данные
                                #Ссылки на вакансию содержатся в классе item-list:
                                item_list = soup.find('div', class_="container mb-5").find('div', class_='row').find('div',class_='col-lg-8 col-xxl-9 position-relative content-search-vacancy').find_all('div', class_='item-list')

                                for item in item_list:
                                    link = f"https://www.enbek.kz" + item.find('a',class_='stretched').get('href')
                                    task = asyncio.create_task(get_data(link,prof,city_id))
                                    tasks.append(task)
                                    
                            else:
                                #На странице нету вакансий останавливаем цикл
                                break

                    except Exception as ex:
                            print(ex)

                    finally:
                        pass
    ic("Запускаем собранные задачи...")
    await asyncio.gather(*tasks)


#Главная функция :
def main():
    asyncio.run(gather_data())
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)


#Точка входа в программу :
if __name__ == "__main__":
    main()
