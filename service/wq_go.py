from datetime import datetime
from controllers.controller import getWebquotesGo
import pandas as pd

from data.models.wq_go import WebquotesGoResponse, fromDictRecursive

def generateWebquotesGoDf(start: str, end: str) -> pd.DataFrame:
    webquotesGoJson = getWebquotesGo(start, end)

    if not webquotesGoJson:
        raise Exception(f"No webquotes found from {start} to {end}")

    webquotesGoModel = [fromDictRecursive(WebquotesGoResponse, wq) for wq in webquotesGoJson]
    webquotesList = formatWebquotesModel(webquotesGoModel)
    webquotesGoDf = pd.DataFrame(webquotesList)
    webquotesGoDf["date_sold"] = pd.to_datetime(webquotesGoDf["date_sold"], format="mixed", errors="coerce")

    return webquotesGoDf

def formatWebquotesModel(wwebquotesGoModel: list[WebquotesGoResponse]) -> list[dict]:
    webquotes = []

    for wq in wwebquotesGoModel:
        firstname = wq.customer.firstname
        lastname = wq.customer.lastname
        dateCreated = wq.submission_date
        initialMaritalStatus = wq.customer.marital_status
        initialGender = wq.customer.gender

        id = wq.id
        form = wq.lead_source
        name = f"{upperFirstLetter(firstname)} {upperFirstLetter(lastname)}"
        email = wq.customer.email
        phone = wq.customer.phonenumber
        submissionDate = wq.createdAt
        submissionOnTime = formatTime(dateCreated)
        birthday = wq.customer.birthday

        if len(wq.cars) == 0:
            continue

        year = wq.cars[0].year
        make = wq.cars[0].make
        model = wq.cars[0].model
        notes = wq.notes
        status = wq.status
        agent = wq.agent
        zip = wq.customer.zipcode
        state = wq.customer.state
        preference = wq.customer.language_pref
        maritalStatus = formatMaritalStatus(initialMaritalStatus)
        licenseStatus = "N/A"
        gender = formatGender(initialGender)
        residenceStatus = "N/A"
        garaged = "N/A"
        assignedTo = wq.assigned_to
        toAll = wq.toall
        workedAt = wq.office_worked
        regionWorkedAt = wq.region_worked
        soldAt = wq.sold_date
        dateSold = wq.sold_date
        referer = wq.referer
        campaignId = wq.full_link
        gclid = wq.gclid
        calls = wq.calls

        wqModel = {
            "id": id,
            "form": form,
            "name": name,
            "email": email,
            "phone": phone,
            "submission_date": submissionDate,
            "submission_on_time": submissionOnTime,
            "birthday": birthday,
            "model_year": year,
            "make": make,
            "model": model,
            "notes": notes,
            "status": status,
            "agent": agent,
            "zip": zip,
            "state": state,
            "preference": preference,
            "marital_status": maritalStatus,
            "licence_status": licenseStatus,
            "gender": gender,
            "residence_status": residenceStatus,
            "garaged": garaged,
            "assigned_to": assignedTo,
            "to_all": toAll,
            "worked_at": workedAt,
            "region_worked_at": regionWorkedAt,
            "sold_at": soldAt,
            "date_sold": dateSold,
            "referer": referer,
            "campaign_id": campaignId,
            "gclid": gclid,
            "calls": calls
        }

        webquotes.append(wqModel)

    return webquotes

def upperFirstLetter(string: str) -> str:
    return string[0].upper() + string[1:]

def formatTime(stringDate: str) -> str:
    dt = datetime.strptime(stringDate, "%b %d %Y %I:%M %p")
    return dt.strftime("%Y-%m-%d %H:%M").split(" ")[1]

def formatMaritalStatus(status: str) -> str:
    if status == "S":
        return "Single"
    elif status == "M":
        return "Married"
    elif status == "D":
        return "Divorced"
    elif status == "W":
        return "Widowed"
    else:
        return status

def formatGender(gender: str) -> str:
    if gender == "F":
        return "Female"
    elif gender == "M":
        return "Male"
    else:
        return gender
