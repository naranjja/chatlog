from unidecode import unidecode

import csv
import json
import requests
import datetime
import pandas as pd


class Scraper:
    def __init__(self, page, output, token):
        self.token = token
        self.output = output
        self.uri = self.build_url("{}/conversations?fields=participants,link&limit=100", page)


    def build_url(self, endpoint, *params):
        return "https://graph.facebook.com/v2.6/" + endpoint.format(*params) + "&access_token={}".format(self.token)


    def scrape_thread(self, url, lst):
        messages = requests.get(url).json()
        for m in messages["data"]:
            time = datetime.datetime.strptime(m["created_time"], "%Y-%m-%dT%H:%M:%S+0000").replace(tzinfo=datetime.timezone.utc).timestamp()
            lst.append({
                "time": m["created_time"].replace("+0000", "").replace("T", " "),
                "message": m["message"],
                "attachments": m.get("attachments", {}).get("data", [{}])[0].get("image_data", {}).get("url", ""),
                "shares": m.get("shares", {}).get("data", [{}])[0].get("name", ""),
                "from_id": m["from"]["id"]
            })
        if messages["data"]:
            print(" +", len(messages["data"]))
        next = messages.get("paging", {}).get("next", "")
        if next:
            self.scrape_thread(next, lst)
        return lst
        
        
    def scrape_thread_list(self, threads, count):
        for t in threads["data"]:
            url = self.build_url("{}/messages?fields=from,created_time,message,shares,attachments&limit=400" + extra_params, t["id"])
            print("GET", unidecode.unidecode(t["participants"]["data"][0]["name"]), t["id"])
            
            thread = self.scrape_thread(url, [])
            if thread:
                self.writer.writerow({
                    "url": t["link"],
                })
            id_map = {p["id"]: p["name"] for p in t["participants"]["data"]}
            for message in reversed(thread):
                message["from"] = id_map[message["from_id"]]
                self.writer.writerow(message)

        next = threads.get("paging", {}).get("next", "")
        if next and count > 1:
            self.scrape_thread_list(requests.get(next).json(), count - 1)
        

    def run(self):
        output = open(self.output, "w", newline="\n", encoding="utf-8")
        threads = requests.get(self.uri).json()

        if "error" in threads:
            print(threads)
            return

        self.writer = csv.DictWriter(output, dialect="excel", fieldnames=fieldnames, extrasaction="ignore", quoting=csv.QUOTE_NONNUMERIC)
        self.writer.writerow(dict((n, n) for n in fieldnames))
        self.scrape_thread_list(threads, 5)

        output.close()


def main():

    page_id = ""  # get it from https://developers.facebook.com/apps/
    page_access_token = ""  # get it from https://developers.facebook.com/tools/explorer/
    output_file = "result.txt"

    fieldnames = ["from_id", "from", "time", "message", "attachments", "shares", "url"]

    Scraper(page_id, output_file, page_access_token).run()

if __name__ == "__main__":
    main()
