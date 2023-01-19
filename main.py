from bs4 import BeautifulSoup, SoupStrainer
from aiogram import Bot, Dispatcher, types, executor
import re, os, psycopg2, requests, time
from dotenv import load_dotenv

# It is used to load env
load_dotenv()

# Connecting to PostgreSQL database
conn = psycopg2.connect(
    dbname=os.environ.get("POSTGRES_DB"),
    user=os.environ.get("POSTGRES_USER"),
    password=os.environ.get("POSTGRES_PASS"),
    host=os.environ.get("POSTGRES_HOST"),
    port=os.environ.get("POSTGRES_PORT")
)

bot = Bot(token=os.environ.get("API_TOKEN"))
dp = Dispatcher(bot)

@dp.message_handler(commands=['start',])
async def send_ads(message: types.Message):
    
    # Creating table called `ad_table`
    with conn.cursor() as cursor:
        cursor.execute('''CREATE TABLE IF NOT EXISTS ad_table (
        id SERIAL PRIMARY KEY, 
        url VARCHAR(300) NOT NULL,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        DELETE FROM ad_table WHERE created_date < CURRENT_DATE;
        ''')
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
                ad_request = requests.get(ad_url).text
                ad_soup = BeautifulSoup(ad_request, "lxml")
                print(ad.text, ad_url)
                try:
                    # Getting title of ad
                    title = ad_soup.find('h1').text
                    
                    # Getting rent price of ad
                    price = ad_soup.find(attrs={"data-testid": "ad-price-container"}).h3.text
                    
                    # Getting negotiation status of ad
                    negotiation = ad_soup.find(attrs={"data-testid": "ad-price-container"})
                    negotiation = negotiation.p.text if len(negotiation.select('p')) > 0 else "Kelishilmagan"
                    
                    # Getting characteristics of ad
                    characteristics = ad_soup.select('ul li p')
                    characteristics = [characteristic.get_text() for characteristic in characteristics][1:]
                    
                    # Getting description of ad
                    ad_description = ad_soup.find(attrs={"data-cy": "ad_description"}).div.text
                    
                    # Getting all images of ad
                    first_image = ad_soup.find_all(attrs={"data-testid": "swiper-image"})[0]['src'] if len(ad_soup.find_all(attrs={"data-testid": "swiper-image"})) > 0 else "https://previews.123rf.com/images/pratyaksa/pratyaksa1701/pratyaksa170100068/70178095-no-home-sign-prohibition-sign-for-house-.jpg"
                    images = ad_soup.find_all(attrs={"data-testid": "swiper-image-lazy"})
                    images = [image['data-src'] for image in images[:9]] + [first_image]
                    images = [types.InputMediaPhoto(image) for image in images]
                    
                    # Checking whether this add is availablle in Database or not
                    cursor.execute("SELECT * FROM ad_table where url = %s", (ad_url, ))

                    # If ad does not exist
                    if cursor.rowcount == 0:
                        # Writing content of the ad, there is no way of attaching content to group of images in telegram, 
                        # except captions, so I am adding content to the last image and it will look like the content of whole ad
                        images[-1].caption = f"<b>E'lon</b>: {title}\n<b>Ijara Narxi:</b> {price}\n<b>Holat:</b> <i>{negotiation}</i>\n"
                        images[-1].parse_mode = 'HTML'
                        for characteristic in characteristics:
                            images[-1].caption += f"- {characteristic}\n"
                        images[-1].caption += f"\n{ad_description}\n\n"
                        
                        # The maximum length of caption should be 1024 characters, we can't add more. Here I am checking 
                        # if the length of caption is more than 880(we must add anchor tags which redirects to detail of add,
                        # that's why i am taking 880 character. If we add anchor tag also length might be longer than 1024) character
                        if len(images[-1].caption) >= 880:
                            images[-1].caption = images[-1].caption.replace(images[-1].caption[880-len(images[-1].caption):], '...\n\n')
                        images[-1].caption += f"<a href='{ad_url}'>Batafsil ma'lumot</a>"
                                            
                        # Telegram does not allow us to send each ad per second, so I need to sleep program for some time
                        time.sleep(60)
                        
                        # Sending entire ad to bot and then bot will sent it to Boshpana channel
                        await bot.send_media_group('@bosh_pana', media=images)
                        
                        # Adding current ad to db in order to prevent duplication
                        cursor.execute("INSERT INTO ad_table (url) VALUES (%s);", (ad_url, ))
                        conn.commit()
                except:
                    pass


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)