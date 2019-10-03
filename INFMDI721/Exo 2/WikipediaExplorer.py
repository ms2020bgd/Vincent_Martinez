from bs4 import BeautifulSoup
import requests
import wikipediaapi
wiki_wiki = wikipediaapi.Wikipedia('fr')


philosophie = "Philosophie"
startingWorld = "France"
wikipediaUrl = "https://fr.wikipedia.org/wiki/"
short_url ="/wiki/"
max_level = 3
not_found = -1


## Scrap the wikipedia url with BeautifulSoup. Take only <a href items and clean the link.
def recurse_parseLink(url, alreadyParse, actualLevel):
    actualWorld = url[len(wikipediaUrl):];
    if actualWorld in alreadyParse :
        return not_found
    elif actualWorld == philosophie:
        return actualLevel
    else :
        actualLevel +=1
        alreadyParse.append(actualWorld)
        if actualLevel > max_level:
            return not_found;
        html = requests.get(wikipediaUrl + actualWorld)
        bs = BeautifulSoup(html.text, 'html.parser')
        for link in bs.find_all('a', href=True):
            url = link.get('href')
            if wikipediaUrl in url:
                level = recurse_parseLink(url, alreadyParse, actualLevel)
                if level != not_found:
                    return level
            elif url.startswith(short_url) and ":" not in url:
                level = recurse_parseLink("https://fr.wikipedia.org"+url, alreadyParse, actualLevel)
                if level != not_found:
                    print(level)
    return not_found


## Scrap the wikipedia throw the wikipedia API. Simple and faster than the BS try
def recurse_parse_link_api(actualWorld, alreadyParse, actualLevel):

    # Show some stats

    if len(alreadyParse)%10 ==0:
        print(str(len(alreadyParse)) + " links analysed")

    if actualWorld in alreadyParse :
        return not_found
    elif actualWorld == philosophie:
        return actualLevel
    else :
        actualLevel +=1
        alreadyParse.append(actualWorld)
        if actualLevel > max_level:
            return not_found;


        page = wiki_wiki.page(actualWorld)
        links = page.links
        for title in sorted(links.keys()):
            if title not in alreadyParse:
                level = recurse_parse_link_api(title, alreadyParse, actualLevel)
                if level != not_found:
                    return level
    return not_found

## Scrap the wikipedia with a full tree exploration. Too long, not usable!
def recurse_parseLinkOneLevelAtOnce(urls, alreadyParse, actualLevel):


    actualwords = []
    for url in urls :
        actualWorld = url[len(wikipediaUrl):];

        print(actualWorld)

        if actualWorld not in alreadyParse :
            actualwords.append(actualWorld)
        elif actualWorld == philosophie:
            return actualLevel



    actualLevel +=1
    if actualLevel > max_level:
        return not_found;
    nextUrls = []

    for nextWord in actualwords :
        html = requests.get(wikipediaUrl + nextWord)
        bs = BeautifulSoup(html.text, 'html.parser')
        alreadyParse.append(nextWord)
        for link in bs.find_all('a', href=True):
            url = link.get('href')
            if wikipediaUrl in url and url not in nextUrls:
                nextUrls.append(url)
            if url.startswith(short_url) and ":" not in url:
                if "https://fr.wikipedia.org"+url not in nextUrls:
                    nextUrls.append("https://fr.wikipedia.org"+url)
        print("Next Word size = " + str(len(nextUrls)))
    return recurse_parseLinkOneLevelAtOnce(nextUrls, alreadyParse, actualLevel)


level = recurse_parse_link_api(startingWorld, [], 0)

if level != not_found:
    print("We found "+philosophie+" at level "+str(level))
else:
    print("We didn't find the "+philosophie+ " page")


