from controllers.controller import getDynamicForm
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def generateDynamicFormDf() -> list[dict]:
    """ Create Home Owners Dynamic FormDataFrame with API response.

    Returns
        {pandas.DataFrame} resulting DataFrame.
    """

    homeOwners = []
    
    try:
        response = getDynamicForm()
    except Exception:
        logger.error("An error occurred while fetching the dynamic form.")
        raise
    else:
        if not response:
            raise Exception("No dynamic form found")

        for data in response:
            homeOwner = renameDfAttributes(data)
            homeOwners.append(homeOwner)
        
        df = pd.DataFrame(homeOwners)
        df.drop_duplicates(subset="phoneNumber", inplace=True)

        dynamicForm = df.to_dict(orient="records")

        return dynamicForm

def renameDfAttributes(dynamicForm: dict) -> dict:
    MAPPING = {
        "lead_source": "leadSource",
        "campaign_id": "campaignId",
        "customer.firstname": "firstname",
        "customer.lastname": "lastname",
        "customer.email": "email",
        "customer.phonenumber": "phoneNumber"
    }

    for oldKey, newKey in MAPPING.items():
        dynamicForm[newKey] = dynamicForm.pop(oldKey)
    dynamicForm.pop("user.lae_agent_id")

    return dynamicForm
