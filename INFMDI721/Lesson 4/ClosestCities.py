import requests
from bs4 import BeautifulSoup

import itertools


def scrapWikipedia(url):
    content = requests.get(url).content
    bs = BeautifulSoup(content, 'html.parser')

    cities = []

    table = bs.find("tbody");
    trs = table.find_all("tr")
    for tr in trs[1:11] :
        city = tr.find_all("td")[1].text;
        city = city.strip()
        index = city.find('[')
        if index >=0:
            city = city[:index]
        cities.append(city)
    return cities


cities = scrapWikipedia("https://fr.wikipedia.org/wiki/Liste_des_communes_de_France_les_plus_peupl%C3%A9es")

cct = [cities, cities]

distance = {}

for element in itertools.product(*cct):

    if element[0] > element[1]:
        element = (element[1], element[0])

    if not distance.get(element):

        result = requests.get("https://fr.distance24.org/route.json?stops="+element[0]+"|"+element[1]).json()
        distance[element[0]+"|"+element[1]] = result["distance"]

print(distance)