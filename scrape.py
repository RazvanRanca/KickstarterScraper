import requests
from bs4 import BeautifulSoup
import time as tm
from datetime import timedelta
from calendar import timegm
import cPickle
import math

def retrieveProjects(categ="technology", sort="magic", noProjs = 20):
  projs = {}
  page = 0
  if not noProjs:
    noProjs = float("inf")
  while noProjs > len(projs):
    page += 1
    r = requests.get("https://www.kickstarter.com/discover/categories/" + categ, params = {"sort":sort,"page":page})
    soup = BeautifulSoup(r.text)

    if page == 1:
      totalProjs = int(soup.find("b", "count green").text.split()[0].encode('utf-8').translate(None,','))
      if noProjs > totalProjs:
        noProjs = totalProjs
        print "Total projects:", noProjs 

    maxProjs = min(20, noProjs - len(projs))
    print "Processing page", page, "- extracted", len(projs), "projects"
    for proj in soup.find_all("div", "project-card-wrap")[:maxProjs]:
      pid = int(proj["data-project"][6:-1]) # TODO: check if this id is actually unique per project
      if pid in projs: # since the pages of results are queried one at a time a project can be read twice if it shifts pages
        print "skipped"
        continue
      bn = proj.find("h2", "bbcard_name")
      name = bn.find("a").text.strip()
      author = bn.find("span").text[3:].strip()
      blurb = proj.find("p", "bbcard_blurb").text.strip()
      loc = proj.find("span", "location-name").text.strip()
      percFund = float(proj.find("div", "project-pledged")["style"].split()[1][:-1])
      money = proj.find("li", "pledged").contents[1]
      currency =  money.contents[0]["class"][1]
      amount = float(money.text[1:].encode('utf-8').translate(None,','))
      time = proj.find("li", "last ksr_page_timer")["data-end_time"]
      offHours, offMins = time[-5:].split(':')
      offSecs = float(offHours)*3600 + float(offMins)*60
      if time[-6] == "+":
        offSecs *= -1
      else:
        assert(time[-6] == "-")
      secsLeft = timegm(tm.strptime(time[:-6], "%Y-%m-%dT%H:%M:%S")) + offSecs - tm.mktime(tm.gmtime())
      timeLeft = timedelta(seconds = secsLeft)

      projs[pid] = {"name":name, "author":author, "blurb":blurb, "location":loc, "pledged":(currency, amount), "percentFunded":percFund, "timeLeft":timeLeft}


  return projs

def storeProjects(projs, categ = None, fn = None):
  if not fn:
    fn = categ + str(len(projs))
  with open("projectStores/" + fn, 'w') as f:
    for i in projs.items():
      cPickle.dump(i, f, cPickle.HIGHEST_PROTOCOL)

def loadProjects(fn):
  projs = {}
  with open("projectStores/" + fn, 'r') as f:
    while True:
      try:
        k,v = cPickle.load(f)
        projs[k] = v
      except:
        break
  return projs

def totalPledged(projs, targetCurr = "GBP"):
  currencies = [proj["pledged"][0].upper() for proj in projs.values()]
  currencyPairs = ','.join(['"' + c + targetCurr + '"' for c in set(currencies) if c != targetCurr])
  r = requests.get("http://query.yahooapis.com/v1/public/yql?q=select * from yahoo.finance.xchange where pair in (" + currencyPairs + ") &env=store://datatables.org/alltableswithkeys")
  soup = BeautifulSoup(r.text)

  exchange = {targetCurr:1.0}
  for curr in soup.find_all(lambda tag: tag.name == "rate" and tag.get("id") != None):
    exchange[curr.get("id")[:len(targetCurr)]] = float(curr.find("rate").text)

  return sum([exchange[curr.upper()] * amm for (curr, amm) in [proj["pledged"] for proj in projs.values()]])


if __name__ == "__main__":
  categ = "art"
  projs = retrieveProjects(categ, noProjs=None, sort="end_date")
  print len(projs)
  #storeProjects(projs, categ = categ)
  #projs = loadProjects(categ + "20")
  #print totalPledged(projs)
