from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common import exceptions
from webdriver_setup import get_webdriver_for
from selenium import webdriver
from fake_useragent import UserAgent
from bs4 import BeautifulSoup, SoupStrainer
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram import Bot, Dispatcher, types, executor
import re, os, requests, time, random
from dotenv import load_dotenv
from utils import convert_uzs_to_eur

import psycopg2

# It is used to load env
load_dotenv()


# Connecting to PostgreSQL database
conn = psycopg2.connect(
    dbname=os.environ.get("POSTGRES_DB"),
    user=os.environ.get("POSTGRES_USER"),
    password=os.environ.get("POSTGRES_PASS"),
    host=os.environ.get("POSTGRES_HOST"),
    port=5432
)

# Connecting to Telegram bot we created
bot = Bot(token=os.environ.get("API_TOKEN"))
dp = Dispatcher(bot)


# List of districts in Tashkent city used to search apartments
bektemir = InlineKeyboardButton(text='Bektemir tumani', callback_data='Bektemir tumani')
mirobod = InlineKeyboardButton(text="Mirobod tumani", callback_data='Mirobod tumani')
mirzo_ulugbek = InlineKeyboardButton(text="Mirzo-Ulug‘bek tumani", callback_data='Mirzo-Ulug‘bek tumani')
sirgali = InlineKeyboardButton(text="Sirg‘ali tumani", callback_data='Sirg‘ali tumani')
olmazor = InlineKeyboardButton(text="Olmazor tumani", callback_data='Olmazor tumani')
uchtepa = InlineKeyboardButton(text="Uchtepa tumani", callback_data='Uchtepa tumani')
shayxontohur = InlineKeyboardButton(text="Shayxontohur tumani", callback_data='Shayxontohur tumani')
chilonzor = InlineKeyboardButton(text="Chilonzor tumani", callback_data="Chilonzor tumani")
yunusobod = InlineKeyboardButton(text="Yunusobod tumani", callback_data="Yunusobod tumani")
yakkasaroy = InlineKeyboardButton(text="Yakkasaroy tumani", callback_data="Yakkasaroy tumani")
yashnobod = InlineKeyboardButton(text="Yashnobod tumani", callback_data="Yashnobod tumani")
ahamiyatsiz_loc = InlineKeyboardButton(text="Ahamiyatsiz", callback_data="ahamiyatsiz_loc")
districts = InlineKeyboardMarkup().add(bektemir, mirobod, mirzo_ulugbek, sirgali, olmazor, uchtepa, \
                                    shayxontohur, yashnobod, chilonzor, yunusobod, yakkasaroy, ahamiyatsiz_loc)


# Number of rooms per apartment
bir = InlineKeyboardButton(text='1', callback_data='1')
ikki = InlineKeyboardButton(text='2', callback_data='2')
uch = InlineKeyboardButton(text='3', callback_data='3')
tort = InlineKeyboardButton(text='4', callback_data='4')
besh = InlineKeyboardButton(text='5', callback_data='5')
ahamiyatsiz_room = InlineKeyboardButton(text='Ahamiyatsiz', callback_data='ahamiyatsiz_room')
room_num = InlineKeyboardMarkup().add(bir, ikki, uch, tort, besh, ahamiyatsiz_room)


# Rent prices of apartment
fifty_to_hundred = InlineKeyboardButton(text='50-100', callback_data='50-100')
hundred_to_twohundred = InlineKeyboardButton(text='100-200', callback_data='100-200')
twohundred_to_threehundred = InlineKeyboardButton(text='200-300', callback_data='200-300')
threehundred_to_fourhundred = InlineKeyboardButton(text='300-400', callback_data='300-400')
fourhundred_to_fivehundred = InlineKeyboardButton(text='400-500', callback_data='400-500')
fivehundred_to_sixhundred = InlineKeyboardButton(text='500-600', callback_data='500-600')
sixhundred_to_sevenhundred = InlineKeyboardButton(text='600-700', callback_data='600-700')
sevenhundred_to_more = InlineKeyboardButton(text='700-more', callback_data='700-3000')
ahamiyatsiz_price = InlineKeyboardButton(text='Ahamiyatsiz', callback_data='ahamiyatsiz_price')
price = InlineKeyboardMarkup().add(fifty_to_hundred, hundred_to_twohundred, twohundred_to_threehundred, 
                                threehundred_to_fourhundred, fourhundred_to_fivehundred, fivehundred_to_sixhundred,
                                sixhundred_to_sevenhundred, sevenhundred_to_more, ahamiyatsiz_price)


@dp.message_handler(commands=['start', ])
async def greet(message: types.Message):
    await message.answer(f"Assalomu Alaykum {message.from_user.full_name}!\nKvartira qidirish uchun /qidiruv komandasidan foydalaning.")


@dp.message_handler(commands=['qidiruv', ])
async def get_address_info(message: types.Message):
    await message.answer("Qaysi tumandan kvartira izlayapsiz?.\n\n<i>Joylashuvning farqi yo'q bo'lsa <b>Ahamiyatsiz</b> tugmasini bosing</i>", reply_markup=districts, parse_mode='html')


@dp.callback_query_handler(text=['Bektemir tumani', 'Mirobod tumani', 'Mirzo-Ulug‘bek tumani', 'Sirg‘ali tumani', 'Olmazor tumani', \
                                'Uchtepa tumani', 'Shayxontohur tumani', 'Yashnobod tumani', 'Chilonzor tumani', 'Yunusobod tumani', \
                                'Yakkasaroy tumani', 'ahamiyatsiz_loc'])
async def get_room_info(call: types.CallbackQuery):
    global search_dict
    search_dict = {}
    search_dict['address'] = None if call.data == 'ahamiyatsiz_loc' else call.data
    await call.message.answer("Nechchi xonali kvartira qidiryapsiz?.\n\n<i>Kvartira nechchi xonali ekanligini farqi yo'q bo'lsa <b>Ahamiyatsiz</b> tugmasini bosing</i>", reply_markup=room_num, parse_mode='html')


@dp.callback_query_handler(text=['1', '2', '3', '4', '5', 'ahamiyatsiz_room'])
async def get_price_info(call: types.CallbackQuery):
    global search_dict
    search_dict['room_no'] = None if call.data == 'ahamiyatsiz_room' else int(call.data)
    await call.message.answer("Ijara narxi qancha yevro atrofida bo'lishini istaysiz?", reply_markup=price)


@dp.callback_query_handler(text=['50-100', '100-200', '200-300', '300-400', '400-500', 
                                '500-600', '600-700', '700-3000', 'ahamiyatsiz_price'])
async def return_records(call: types.CallbackQuery):
    global search_dict
    search_dict['price'] = None if call.data == 'ahamiyatsiz_price' else [int(val) for val in call.data.split('-')]
    search_dict = {key: val for key, val in search_dict.items() if val != None}
    search_dict_keys = list(search_dict.keys())
    print(search_dict)
    with conn.cursor() as cursor:
        if len(search_dict_keys) == 3:
            cursor.execute("SELECT * FROM boshpana_table WHERE address = %s and room_no = %s and price >= %s and price <= %s;", 
                        (search_dict['address'], search_dict['room_no'], search_dict['price'][0], search_dict['price'][-1]))
            result = cursor.fetchall()
        elif len(search_dict_keys) == 2:
            if 'price' in search_dict_keys:
                cursor.execute(f"SELECT * FROM boshpana_table WHERE {search_dict_keys[0]} = '{search_dict[search_dict_keys[0]]}' and price >= {search_dict['price'][0]} and price <= {search_dict['price'][-1]};")
            else:
                cursor.execute(f"SELECT * FROM boshpana_table WHERE {search_dict_keys[0]} = '{search_dict[search_dict_keys[0]]}' and {search_dict_keys[-1]} = '{search_dict[search_dict_keys[-1]]}';")
            result = cursor.fetchall()
        elif len(search_dict_keys) == 1:
            if 'price' in search_dict_keys:
                cursor.execute("SELECT * FROM boshpana_table WHERE price >= %s and price <= %s;", 
                            (search_dict['price'][0], search_dict['price'][-1]))
            else:
                cursor.execute(f"SELECT * FROM boshpana_table WHERE {search_dict_keys[0]} = '{search_dict[search_dict_keys[0]]}';")
            result = cursor.fetchall()
        else:
            await call.message.answer("Siz qidirgan kriteriya bo'yicha e'lonlar bizda mavjud emas...")
        
        if len(result) == 0:
            await call.message.answer("Siz qidirgan kriteriya bo'yicha e'lonlar bizda mavjud emas...!")
        else:
            await call.message.answer(f"Biroz kuting...\nSiz kiritgan kriteriya bo'yicha <b>{len(result)}</b> ta e'lon topildi.", parse_mode='HTML')
            for ad in result:
                images = [types.InputMediaPhoto(image) for image in ad[7]]
                title, price, room_no, broker, characteristics, ad_description, district, ad_url = ad[1], ad[2], ad[3], ad[4], ad[5], ad[6], ad[8], ad[9]
                if len(district) > 0:
                    images[-1].caption = f"<b>⚡️E'lon:</b> {title}\n\n<b>💶Ijara Narxi: {price} y.e\n🤝{broker}\n📍Manzil: {district}</b>\n\n"
                else:
                    images[-1].caption = f"<b>⚡️E'lon:</b> {title}\n\n<b>💶Ijara Narxi: {price} y.e\n🤝{broker}</b>\n\n"
                images[-1].parse_mode = 'HTML'
                
                for characteristic in ["Xonalar soni: " + str(room_no)] + characteristics:
                    images[-1].caption += f"<b>- {characteristic}</b>\n"
                images[-1].caption += f"\n{ad_description}\n\n"
                
                # The maximum length of caption should be 1024 characters, we can't add more. Here I am checking 
                # if the length of caption is more than 880(we must add anchor tags which redirects to detail of add,
                # that's why i am taking 880 character. If we add anchor tag also length might be longer than 1024) character
                if len(images[-1].caption) >= 880:
                    images[-1].caption = images[-1].caption.replace(images[-1].caption[880-len(images[-1].caption):], '...\n\n')
                images[-1].caption += f"<a href='{ad_url}'>Batafsil ma'lumot</a>"
                
                # Telegram does not allow us to send each ad per second, so I need to sleep program for some time
                # time.sleep(50)
                
                # Sending entire ad to bot and then bot will sent it to Boshpana channel
                await call.message.answer_media_group(media=images, disable_notification=True)


@dp.message_handler(commands=['send',])
async def send_ads(message: types.Message):
    with conn.cursor() as cursor:
        cursor.execute("""CREATE TABLE IF NOT EXISTS boshpana_table (
            id SERIAL PRIMARY KEY,
            title VARCHAR(150) NOT NULL,
            price INTEGER NOT NULL, 
            room_no INTEGER NOT NULL,
            broker VARCHAR(50) NOT NULL,
            characteristics TEXT[] NOT NULL,
            description TEXT NOT NULL, 
            images TEXT[],
            address VARCHAR(100) NOT NULL,
            url VARCHAR(300) NOT NULL,
            created_date date DEFAULT CURRENT_DATE
            );
            DELETE FROM boshpana_table WHERE created_date < NOW() - INTERVAL '5 DAY';""")
        conn.commit()
    
        # Taking only tags which has value of "l-card" in `data-cy` attribute
        only_taking_l_cards = SoupStrainer(attrs={"data-cy": "l-card"})
        
        for i in range(1, 26):
            # Getting accesss to this url
            base_url = f"https://www.olx.uz/d/oz/nedvizhimost/kvartiry/arenda-dolgosrochnaya/?currency=UZS&page={i}&search%5Bfilter_enum_comission%5D%5B0%5D=no" if i > 1 else "https://www.olx.uz/d/oz/nedvizhimost/kvartiry/arenda-dolgosrochnaya/?currency=UZS&search%5Bfilter_enum_comission%5D%5B0%5D=no"
            request = requests.get(base_url).text

            # Passing it to BeautifulSoup contructor in order to parse the content
            soup = BeautifulSoup(request, "lxml", parse_only=only_taking_l_cards)
            
            # Taking ads which is posted today
            ads = soup.find_all(string=re.compile('Bugun'))
            
            # Ad detail
            for ad in ads:
                
                # Getting url of each ad 
                ad_url = "https://www.olx.uz" + ad.find_parent('a')['href']
                time.sleep(0.01)
                ad_request = requests.get(ad_url).text
                
                print(ad.text, ad_url)
                
                # Parsing using beautifulsoup
                ad_soup = BeautifulSoup(ad_request, "lxml")

            
                try:
                    # Getting title of ad
                    title = ad_soup.find('h1').text
                    
                    # Getting rent price of ad
                    price = ad_soup.find(attrs={"data-testid": "ad-price-container"}).h3.text
                    if price.endswith('сум'):
                        price = ''.join([num for num in price.split() if num.isdigit()])
                        price = convert_uzs_to_eur(price)
                    else:
                        price = ''.join([num for num in price.split() if num.isdigit()])
                    
                    # Getting negotiation status of ad
                    # negotiation = ad_soup.find(attrs={"data-testid": "ad-price-container"})
                    # negotiation = negotiation.p.text if len(negotiation.select('p')) > 0 else "Kelishilmagan"
                    
                    # Getting characteristics of ad
                    filters = ('Yashash maydoni', 'Oshxona maydoni', 'Qurilish turi', 'Rejasi', 'Qurilgan yili', 'Topshiriladigan yil', 
                            'Shiftining balandligi', 'Yaqinida joylashgan')
                    characteristics = ad_soup.select('ul li p')
                    # Getting mediation status
                    broker = characteristics[-1].get_text()
                    room_no = characteristics[1].get_text().split()[-1]
                    characteristics = [characteristic.get_text() for characteristic in characteristics[2:-1] if not characteristic.get_text().startswith(filters)]
                    
                    # Getting description of ad
                    ad_description = ad_soup.find(attrs={"data-cy": "ad_description"}).div.text
                    
                    # Getting all images of ad
                    first_image = ad_soup.find_all(attrs={"data-testid": "swiper-image"})[0]['src'] if len(ad_soup.find_all(attrs={"data-testid": "swiper-image"})) > 0 else "https://previews.123rf.com/images/pratyaksa/pratyaksa1701/pratyaksa170100068/70178095-no-home-sign-prohibition-sign-for-house-.jpg"
                    images = ad_soup.find_all(attrs={"data-testid": "swiper-image-lazy"})
                    image_urls = [image['data-src'] for image in images[:9]] + [first_image]
                    images = [types.InputMediaPhoto(image) for image in image_urls]
                    
                    # Getting address info using Selenium
                    with get_webdriver_for('chrome') as driver:
                        driver.get(ad_url)
                        district = driver.find_element(By.XPATH, "//*[@id='root']/div[1]/div[3]/div[3]/div[2]/div[2]/div/section/div[1]/div/p[1]/span").text
                    print(district, len(district), type(district))
                                
                    # Checking whether this add is availablle in Database or not
                    cursor.execute("SELECT * FROM boshpana_table where url = %s", (ad_url, ))
                    
                    # If ad does not exist
                    if cursor.rowcount == 0:
                        # Writing content of the ad, there is no way of attaching content to group of images in telegram, 
                        # except captions, so I am adding content to the last image and it will look like the content of whole ad
                        if len(district) > 0:
                            images[-1].caption = f"<b>⚡️E'lon:</b> {title}\n\n<b>💶Ijara Narxi: {price} y.e\n🤝{broker}\n📍Manzil: {district}</b>\n\n"
                        else:
                            images[-1].caption = f"<b>⚡️E'lon:</b> {title}\n\n<b>💶Ijara Narxi: {price} y.e\n🤝{broker}</b>\n\n"
                        images[-1].parse_mode = 'HTML'
                        for characteristic in ["Xonalar soni: " + room_no] + characteristics:
                            images[-1].caption += f"<b>- {characteristic}</b>\n"
                        images[-1].caption += f"\n{ad_description}\n\n"
                        
                        # The maximum length of caption should be 1024 characters, we can't add more. Here I am checking 
                        # if the length of caption is more than 880(we must add anchor tags which redirects to detail of add,
                        # that's why i am taking 880 character. If we add anchor tag also length might be longer than 1024) character
                        if len(images[-1].caption) >= 880:
                            images[-1].caption = images[-1].caption.replace(images[-1].caption[880-len(images[-1].caption):], '...\n\n')
                        images[-1].caption += f"<a href='{ad_url}'>Batafsil ma'lumot</a>"
                        
                        # Telegram does not allow us to send each ad per second, so I need to sleep program for some time
                        time.sleep(50)
                        
                        # Sending entire ad to bot and then bot will sent it to Boshpana channel
                        await bot.send_media_group(chat_id='-1001801063927', media=images, disable_notification=True)
                        
                        cursor.execute("INSERT INTO boshpana_table (title, price, room_no, broker, characteristics, description, images, address, url) \
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);", (title, price, int(room_no), broker, characteristics, ad_description, image_urls, district, ad_url))
                        conn.commit()
                    else:
                        print("skipped", ad_url)
                except:
                    pass


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)