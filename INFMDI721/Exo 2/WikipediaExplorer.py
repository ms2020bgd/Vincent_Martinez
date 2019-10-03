from bs4 import BeautifulSoup
import requests
import wikipediaapi
wiki_wiki = wikipediaapi.Wikipedia('fr')


philosophie = "Philosophie"
starting_world = "Anglais"
WIKIPEDIA_URL = "https://fr.wikipedia.org/wiki/"
short_url ="/wiki/"
max_level = 3
not_found = -1

##
# Loop on the all page link, go at max deep specified by max_level.
#
##


# Scrap the wikipedia url with BeautifulSoup. Take only <a href items and clean the link.
def recurse_parse_link_using_bs(url, already_parse, actual_level):
    actualWorld = url[len(WIKIPEDIA_URL):];
    if actualWorld in already_parse :
        return not_found
    elif actualWorld == philosophie:
        return actual_level
    else :
        actual_level +=1
        already_parse.append(actualWorld)
        if actual_level > max_level:
            return not_found;
        html = requests.get(WIKIPEDIA_URL + actualWorld)
        bs = BeautifulSoup(html.text, 'html.parser')
        for link in bs.find_all('a', href=True): # Loop over all the links, not only the first one.
            url = link.get('href')
            if WIKIPEDIA_URL in url:
                level = recurse_parse_link_using_bs(url, already_parse, actual_level)
                if level != not_found:
                    return level
            elif url.startswith(short_url) and ":" not in url:
                level = recurse_parse_link_using_bs("https://fr.wikipedia.org" + url, already_parse, actual_level)
                if level != not_found:
                    print(level)
    return not_found


# Scrap the wikipedia throw the wikipedia API. Simple and faster than the BS try
def recurse_parse_link_api(actual_world, already_parse, actual_level):

    # Show some stats
    if len(already_parse)%10 ==0:
        print(str(len(already_parse)) + " links analysed")

    if actual_world in already_parse :
        return not_found
    elif actual_world == philosophie:
        return actual_level
    else :
        actual_level +=1
        already_parse.append(actual_world)
        if actual_level > max_level:
            return not_found;
        page = wiki_wiki.page(actual_world)
        links = page.links
        for title in sorted(links.keys()):
            if title not in already_parse:
                level = recurse_parse_link_api(title, already_parse, actual_level)
                if level != not_found:
                    return level
    return not_found


# Scrap the wikipedia with a full tree exploration. Too long, not usable!
def recurse_parse_link_full_exploration(urls, already_parse, actual_level):
    actual_words = []
    for url in urls :
        actual_world = url[len(WIKIPEDIA_URL):];

        print(actual_world)

        if actual_world not in already_parse :
            actual_words.append(actual_world)
        elif actual_world == philosophie:
            return actual_level
    actual_level +=1
    if actual_level > max_level:
        return not_found;
    nextUrls = []

    for nextWord in actual_words :
        html = requests.get(WIKIPEDIA_URL + nextWord)
        bs = BeautifulSoup(html.text, 'html.parser')
        already_parse.append(nextWord)
        for link in bs.find_all('a', href=True):
            url = link.get('href')
            if WIKIPEDIA_URL in url and url not in nextUrls:
                nextUrls.append(url)
            if url.startswith(short_url) and ":" not in url:
                if "https://fr.wikipedia.org"+url not in nextUrls:
                    nextUrls.append("https://fr.wikipedia.org"+url)
        print("Next Word size = " + str(len(nextUrls)))
    return recurse_parse_link_full_exploration(nextUrls, already_parse, actual_level)


parsed_links = []
level = recurse_parse_link_api(starting_world, parsed_links, 0)

if level != not_found:
    print("We found "+philosophie+" at level "+str(level))
    print("We parse "+ str(len(parsed_links)) + " to find it")
else:
    print("We didn't find the "+philosophie+ " page")


