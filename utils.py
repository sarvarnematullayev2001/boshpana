from bs4 import BeautifulSoup, SoupStrainer
import requests


def convert_uzs_to_eur(amount):
    base_url = f"https://www.xe.com/currencyconverter/convert/?Amount={amount}&From=UZS&To=EUR"
    request = requests.get(base_url).text
    soup = BeautifulSoup(request, 'lxml')
    price = soup.find(attrs={"class": "faded-digits"}).previous_sibling.text
    return int(float(str(price)))