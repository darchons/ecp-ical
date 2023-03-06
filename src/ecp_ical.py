#!/usr/bin/env python

from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil.parser import isoparse
from flask import Flask, Response, request
from google.cloud import datastore
import hjson
import html
from icalendar import Calendar, Event
import requests
import zlib

EVENTS_URI = "https://pittecp.org/Calendar?EventViewMode=1&EventListViewMode=2&SelectedDate={date}&CalendarViewType=0"
EVENT_BASE_URI = "https://pittecp.org/event-"
CLUB_EVENT_TAG = "ecp club event"
EXPIRATION = 7
JSON_EVENT_START = "const event = {"
JSON_EVENT_END = "};"
JSON_EVENT_HREF = "window.location.href"
JSON_EVENT_DESC = ".replace(/\\r+/g, '')"
JSON_EVENT_ALLDAY_TRUE = "'False' === 'False'"
JSON_EVENT_ALLDAY_FALSE = "'True' === 'False'"

app = Flask(__name__)
ds = datastore.Client()

def extract_links(content):
    bs = BeautifulSoup(content, "html.parser")
    return {
        a.get("href").partition("?")[0]: {
            "tags": [t.strip() for t in
                     a.get("data-tags", CLUB_EVENT_TAG).split(",")],
            } for a in bs.find_all("a", title=True, href=(
                lambda attr: attr and attr.startswith(EVENT_BASE_URI)))
    }

def get_page(url, filt, params={}):
    key = ds.key("page", url)
    page = ds.get(key)

    if page:
        content = page["content"]
        if not isinstance(content, bytes):
            return hjson.loads(content)
        return hjson.loads(zlib.decompress(content).decode("utf-8", "ignore"))

    response = requests.get(url=url, params=params)
    response.raise_for_status()

    content = filt(response.content)
    json = hjson.dumpsJSON(content)
    page = datastore.Entity(key, exclude_from_indexes=(
        "expiration",
        "content",
    ))
    page["expiration"] = datetime.now() + timedelta(days=EXPIRATION)
    page["content"] = zlib.compress(json.encode("utf-8", "ignore"), level=9)
    ds.put(page)
    return content

def get_all_events():
    query = ds.query(kind="event")
    query.projection = ["content"]

    out = {}
    for entity in query.fetch():
        if not entity["content"]:
            entity = ds.get(entity.key)
        json = zlib.decompress(entity["content"] or entity["fullcontent"])
        out[entity.key.name] = hjson.loads(json.decode("utf-8", "ignore"))
    return out

def fetch_event(href, params={}):
    response = requests.get(url=href, params=params)
    response.raise_for_status()

    bs = BeautifulSoup(response.content, "html.parser")
    for script in bs.find_all("script"):
        if script.string and JSON_EVENT_START in script.string:
            break
    else:
        return {}

    script = script.string
    start = script.find(JSON_EVENT_START) + len(JSON_EVENT_START) - 1
    end = script.find(JSON_EVENT_END, start) + 1
    json = script[start: end]
    json = json.replace(JSON_EVENT_HREF, f'"{href}"')
    json = json.replace(JSON_EVENT_DESC, "")
    json = json.replace(JSON_EVENT_ALLDAY_TRUE, "true")
    json = json.replace(JSON_EVENT_ALLDAY_FALSE, "false")

    content = hjson.loads(json)
    body = bs.find("div", {"class": "boxBodyContentOuterContainer"})
    if body:
        content["description"] = body.text.strip()

    json = hjson.dumpsJSON(content)
    json = zlib.compress(json.encode("utf-8", "ignore"), level=9)

    key = ds.key("event", href)
    event = datastore.Entity(key, exclude_from_indexes=(
        "expiration",
        "fullcontent",
    ))
    event["expiration"] = datetime.now() + timedelta(days=EXPIRATION)
    event["content"] = json if (len(json) < 1500) else None
    event["fullcontent"] = json if not event["content"] else None
    ds.put(event)
    return content

def normalize_event(event):
    if "title" in event:
        event["title"] = html.unescape(event["title"])
    if "location" in event:
        for _ in range(2):
            event["location"] = html.unescape(event["location"])
    start = isoparse(event["start"])
    end = isoparse(event.get("end", start))

    if end.astimezone(tz=None).replace(tzinfo=None) <= datetime.now(tz=None):
        return None

    if not event.get("allDay", False):
        event["start"] = start
        event["end"] = end
        event["allDay"] = (end - start).total_seconds() >= (8 * 60 * 60)
        event["multiDay"] = (end - start).total_seconds() > (24 * 60 * 60)
        return event

    start = start.astimezone(tz=None).date()
    end = end.astimezone(tz=None).date()

    event["start"] = start
    event["end"] = end
    event["multiDay"] = (end - start).days > 1
    return event

@app.route("/cal/", methods=["GET"], strict_slashes=False)
@app.route("/cal/<all_tags>", methods=["GET"])
def events(all_tags=None):
    all_tags = [t.replace("+", " ").strip()
                for t in all_tags.split(",")] if all_tags else []
    tags = set(all_tags)
    opts = {}

    for tag in {"allday", "multiday"}:
        opts[tag] = tag in tags
        if opts[tag]:
            all_tags.remove(tag)
            tags.remove(tag)

    page = {}
    for date in [
            f"1/1/{datetime.now().year}",
            f"1/1/{datetime.now().year + 1}"]:
        page.update(get_page(
            EVENTS_URI.replace("{date}", date), filt=extract_links))
    events = get_all_events()
    out = []
    fetch_left = (len(set(page.keys())) // (24 // 12) // EXPIRATION + 1)

    for href, attrs in page.items():
        if tags & set(attrs["tags"]) != tags:
            continue
        if href in events:
            out.append(events[href])
            continue
        # Need to fetch.
        if not fetch_left:
            # But already done fetching.
            continue
        out.append(fetch_event(href))
        fetch_left -= 1

    title = all_tags[0].strip().title() if all_tags else ""
    title += "..." if len(tags) > 1 else ""
    title = f"ECP ({title})" if title else "ECP"
    title = title.replace("Ecp", "ECP")
    desc = ", ".join(t.title() for t in tags)
    desc = desc.replace("Ecp", "ECP")

    cal = Calendar()
    cal.add("PRODID", "-//ECP-iCal//pittecp.org//EN")
    cal.add("VERSION", "2.0")
    cal.add("NAME", title)
    cal.add("DESCRIPTION", desc)
    cal.add("URL", request.url)
    cal.add("COLOR", "167:155:142")
    cal.add("METHOD", "PUBLISH")

    key_mapping = {
        "title": "SUMMARY",
        "description": "DESCRIPTION",
        "location": "LOCATION",
        "url": "URL",
        "start": "DTSTART",
        "end": "DTEND",
    }

    for event in out:
        if "sessions" not in event:
            event["sessions"] = [{}]
        for idx, session in enumerate(event["sessions"]):
            props = normalize_event({**event, **session})
            if not props:
                continue
            if (not (opts["allday"] or opts["multiday"]) and
                    props.get("allDay", False)):
                continue
            if not opts["multiday"] and props.get("multiDay", False):
                continue
            evt = Event()
            for key, val in props.items():
                if key in key_mapping:
                    evt.add(key_mapping[key], val)
                    continue
                if key == "id":
                    evt.add("UID", f"{val}_{idx}")
                    continue
            cal.add_component(evt)

    return Response(cal.to_ical(), mimetype="text/calendar")

if __name__ == "__main__":
    app.run()
