import json, lxml, time, asyncio, aiohttp, aiofiles
from icecream import ic
from bs4 import BeautifulSoup
from const_data import all_city_data,all_prof_data, data, headers


#variables
tasks_gather_links_to_vac = []
tasks_get_data = []
#Получение информации о вакансии :
async def get_data(url,prof,city_id):

    try:
        #Создание сессии подключения :
        async with aiohttp.ClientSession() as session:
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

                #Сохраняем данные
                prof_dict = {'name' : name,'price' : price,'type_work' : type_work,'responsibilities' : rslit,'date' : date,'link_vac' : link,'prof' : prof,'city_id' : city_id}
                data[prof].append(prof_dict)
            
    except Exception as ex:
        ic(f"Problem in get_data()\n{ex}")


#Получение ссылок на страницу вакансии :
async def gather_links_to_vac(url: str, session: aiohttp.ClientSession):
    ic("Start")
    try:
        async with session.get(url=url, headers=headers) as response:
                            
                #Создаю объект соупа для парсинга данных
                soup = BeautifulSoup(await response.text(),'lxml')

                #Ссылки на вакансию содержатся в классе item-list:
                item_list = soup.find('div', class_="container mb-5").find('div', class_='row').find('div',class_='col-lg-8 col-xxl-9 position-relative content-search-vacancy').find_all('div', class_='item-list')

                for item in item_list:
                    link = f"https://www.enbek.kz" + item.find('a',class_='stretched').get('href')
                    tasks_get_data.append(get_data(url=link))
                    
                                    
    except Exception as ex:
        ic(f"Problem in gather_data()\n{ex}")

    ic("end")
async def create_main_link(city_id: int, session: aiohttp.ClientSession) -> None:

    for prof in all_prof_data:
        url = f"https://www.enbek.kz/ru/search/vacancy?prof={prof}&except[subsidized]=subsidized&region_id={city_id}"

        # async with aiohttp.ClientSession() as session:
        async with session.get(url=url, headers=headers) as response:

            soup = BeautifulSoup(await response.text(), "lxml")

            #Смотрим есть ли пагинация на странице
            try:
                max_page_num = soup.find("div", class_="container mb-5").find("div", class_="col-lg-8 col-xxl-9 position-relative content-search-vacancy").find('ul', class_="pagination").find_all("li", class_="page")[-1].text

            except AttributeError as attr:
                ic(attr)

                #Проверяем есть ли вакансии на странице
                text = soup.find('div',class_="container mb-5").find('div',class_='row').find('div',class_='col-lg-8 col-xxl-9 position-relative content-search-vacancy').find_all('div', class_="item-list")
                    
                if not text:
                    continue
                else:
                    tasks_gather_links_to_vac.append(
                        gather_links_to_vac(url=url, session=session)
                    )
                    continue
                
            #Пагинация на странице есть ->
            try:
                for i in range(1, int(max_page_num) + 1):
                    tasks_gather_links_to_vac.append(
                        gather_links_to_vac(
                            url=f"https://www.enbek.kz/ru/search/vacancy?prof={prof}&except[subsidized]=subsidized&region_id={city_id}&page={i}",
                            session=session
                        )
                    )

            except Exception as ex:
                ic(f"Problem in create_main_link()\n{ex}")
                continue


#Главная функция :
async def main() -> None:
    async with aiohttp.ClientSession() as session:
        main_link_task = []

        for key in all_city_data.keys():
            main_link_task.append(create_main_link(city_id=all_city_data[key], session=session))

        await asyncio.gather(*main_link_task)
        await asyncio.gather(*tasks_gather_links_to_vac)



    #создаем дамп данных
    # async with aiofiles.open("Collected_data.json", 'w', encoding='utf-8') as file:
    #     json.dump(data, file, ensure_ascii=False)


#Точка входа в программу :
if __name__ == "__main__":
    asyncio.run(main())
