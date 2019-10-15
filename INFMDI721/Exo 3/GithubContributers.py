from multiprocessing import Pool
import asyncio
from pyppeteer import launch  # pip install pyppeteer
import requests
from bs4 import BeautifulSoup
from github import Github
import concurrent.futures

github = Github("47143283dfb91edf2b70188c07e4af91d045f3be")

async def githubScrapping(url) :
    # Github seems to be a javascript  generated page
    # Got a issue twice, choose the async way

    # Read and wait 5 s
    print('Starting browser...\n')
    browser = await launch()
    page = await browser.newPage()

    # Go to the url:
    await page.goto(url)

    print("\nWait 1 seconds...\n")
    await asyncio.sleep(1)
    # Get the content again:
    content = await page.evaluate('document.body.outerHTML', force_expr=True)

    bs = BeautifulSoup(content, 'html.parser')
    table = bs.find("tbody")
    contrib = []
    trs = table.find_all("tr")
    for tr in trs:
        td = tr.find("td")
        contrib.append(td.text)

    await browser.close()

    # Let's cut if

    users = {}
    for c in contrib:
        user_id = c.split(" ")[0]
        name = c.replace(user_id, '').replace('(', '').replace(')', '').strip();
        users[user_id] = name

    return users

def github_user_star(user_id):
    #repositories = requests.get("https://api.github.com/users/"+user_id+"/repos").json()
    #print(repositories)

    user = github.get_user(user_id)

    star = 0
    repos = user.get_repos()
    for repo in repos:
        star += repo.stargazers_count

    star_mean = 0
    if(repos.totalCount !=0):
        star_mean = star / float(repos.totalCount)

    print(user_id +" have "+str(star_mean)+" score")
    return {user_id:star_mean}

async def github_api(contribs):
    contributors = {}

    # Can't have more than 2 threads; github stop with a abuse detection mechanism
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    futures = [asyncio.get_event_loop().run_in_executor(executor, github_user_star, contrib) for contrib in contribs.keys()]
    star_mean = await asyncio.gather(*futures)
    out = {}
    for d in star_mean:
        for k,v in d.items():
           out[k] = v

    print(star_mean)
    contributors = sorted(out, key=out.get, reverse=True)

    return contributors



cts = asyncio.run(githubScrapping("https://gist.github.com/paulmillr/2657075"))


print(cts)

ctt = asyncio.run(github_api(cts))

print(ctt)


