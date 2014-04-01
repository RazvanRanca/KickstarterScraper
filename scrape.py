import requests
from bs4 import BeautifulSoup
import time as tm
from datetime import timedelta
from calendar import timegm
import cPickle
import math
from multiprocessing import Pool
import os

def retrieveProjects(categ="technology", sort="end_date", noProjs = 20):
  page = 0
  extProjs = 0
  seenPids = set()
  if not noProjs:
    noProjs = float("inf")
  while noProjs > extProjs:
    page += 1
    r = requests.get("https://www.kickstarter.com/discover/categories/" + categ, params = {"sort":sort,"page":page})
    while r.status_code != 200:
      tm.sleep(1)
      r = requests.get("https://www.kickstarter.com/discover/categories/" + categ, params = {"sort":sort,"page":page})
    soup = BeautifulSoup(r.text)
    if page == 1:
      totalProjs = int(soup.find("b", "count green").text.split()[0].encode('utf-8').translate(None,','))
      if noProjs > totalProjs:
        noProjs = totalProjs
        print "Total projects:", noProjs 
    maxProjs = min(20, noProjs - extProjs)
    for proj in soup.find_all("div", "project-card-wrap")[:maxProjs]:
      try:
        pid = int(proj["data-project"][6:-1]) # TODO: check if this id is actually unique per project
        if pid in seenPids: # since the pages of results are queried one at a time a project can be read twice if it shifts pages
          print "skipped"
          continue
        bn = proj.find("h2", "bbcard_name")
        name = bn.find("a").text.strip()
        author = bn.find("span").text[3:].strip()
        blurb = proj.find("p", "bbcard_blurb").text.strip()
        loc = proj.find("span", "location-name").text.strip()
        if proj.find("div", "project-status project-failed"):
          projStatus = "Failed"
          timeLeft = 0
          percFund = 0           #This could have the amount pledged before the project was cancelled, but would have to load another page to retrieve it
          pledged = ("USD", 0)
        elif proj.find("div", "project-status project-canceled"):
          projStatus = "Canceled"
          timeLeft = 0
          percFund = 0
          pledged = ("USD", 0)
        else:
          percFund = float(proj.find("div", "project-pledged")["style"].split()[1][:-1])
          if percFund >= 100:
            projStatus = "Successful"
          else:
            projStatus = "Live"

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
          #print projStatus, timeLeft
          if projStatus == "Live":
            assert(timeLeft.total_seconds() >= 0)
          

        yield (pid, {"name":name, "author":author, "blurb":blurb, "location":loc, "pledged":(currency, amount), "percentFunded":percFund, "timeLeft":timeLeft, "status":projStatus})
        seenPids.add(pid)
        extProjs += 1
      except:
        print "Exception", categ, page, proj
      #  assert(False)

    if page % 10 == 0:
      print categ, " - page", page, "- extracted", extProjs, "/", noProjs


def storeProjects(projs, fn):
  with open("projectStores/" + fn, 'w') as f:
    for i in projs:
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

def retrieveAndStore(categ, tp = "Par"):
  storeProjects(retrieveProjects(categ, sort="end_date", noProjs=None), fn = categ + tp)

def runSerial(categs):
  rezs = []
  for categ in categs:
    retrieveAndStore(categ, "Ser")

def runParallel(categs):
  pool = Pool(10)
  rez = pool.map_async(retrieveAndStore, categs)
  rez.get()

def compareVersions(categs):
  timeStart = tm.time()
  runSerial(categs)
  print "Serial ran in:", tm.time() - timeStart

  timeStart = tm.time()
  runParallel(categs)
  print "Parallel ran in:", tm.time() - timeStart

  for categ in categs:
    parProjs = loadProjects(categ + "Par")
    serProjs = loadProjects(categ + "Ser")
    for k in parProjs.keys():
      del parProjs[k]["timeLeft"] 
    for k in serProjs.keys():
      del serProjs[k]["timeLeft"]
    print categ, "Matches:", parProjs == serProjs


if __name__ == "__main__":
  categs = ["art", "comics", "dance", "design", "fashion", "film%20&%20video", "food", "games", "music", "photography", "publishing", "technology", "theater"]
  #runParallel(categs)
  print totalPledged(loadProjects("technologyPar"))

