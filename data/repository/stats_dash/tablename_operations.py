from data.repository.calls.compliance_repo import Compliance
from flask import jsonify, Response, request
from datetime import datetime
import json, logging

logger = logging.getLogger(__name__)

def saveTableName() -> tuple[Response | int]:
    """ Generate Overtime or Immigration Report data to be saved in
    it's table.
    
    Returns
        {tuple[Response | int]} JSON message if succeed or not.
    """

    tableName = request.json["table_name"]
    data = request.json["data"]
    table = request.json["table"]
    compliance = Compliance()

    dataToInsert = generateSavedImmDataValues(tableName, data)
      
    if table == "saved_data":        
        dataToInsert = generateSavedDataValues(tableName, data)

    try:
        response = compliance.getTableName(table, tableName)
    except Exception as e:
        logger.error(f"Error in saveTableName: {str(e)}")
        return jsonify({"success": False, "error": "Error searching report name in database"}), 500

    if response:
        return jsonify({
            "success": False,
            "error": "Table name already exists. Please choose a different name."
        }), 400

    try:
        compliance.insertTableName(table, dataToInsert)
        return jsonify({"success": True}), 200
    except Exception as e:
        logger.error(f"Error in saveTableName: {str(e)}")
        return jsonify({"success": False, "error": "Error saving report from database"}), 500

def generateSavedImmDataValues(tableName: str, data: list[dict]) -> dict:
    """ Generate object to save in 'saved_immdata' table.
    
    Parameters
        - tableName {str} database table name.
        - data {list[dict]} one of the parts to be saved in the table.
    
    Returns
        {dict} object with all the data that needs to be saved.   
    """

    dateCreated = datetime.now().date()
    jsonData = json.dumps(data)

    return {
        "table_name": tableName,
        "data": jsonData, 
        "datecreated": dateCreated
    }

def generateSavedDataValues(tableName: str, data: list[dict]) -> dict:
    """ Generate object to save in 'saved_data' table.
    
    Parameters
        - tableName {str} database table name.
        - data {list[dict]} one of the parts to be saved in the table.
    
    Returns
        {dict} object with all the data that needs to be saved.   
    """

    weekData = request.json["week_data"]
    jsonData = json.dumps({"data": data})
    jsonWeekData = json.dumps({"weekdata": weekData})
    dateCreated = datetime.now()
        
    return {
        "table_name": tableName,
        "data": jsonData,
        "weekdata": jsonWeekData,
        "datecreated": dateCreated
    }

def deleteTableName() -> tuple[Response | int]:
    """ Delete Overtime or Immigration Report data from it's table.
    
    Returns
        {tuple[Response | int]} JSON message if succeed or not.
    """

    viewName = request.form.get("view_name")
    table = request.form.get("table")

    compliance = Compliance()

    try:
        compliance.deleteTableName(table, viewName)

        return jsonify({"success": True}), 200
    except Exception as e:
        logger.error(f"Error in deleteTableName: {str(e)}")
        return jsonify({"success": False, "error": "Error deleting report from database"}), 500

def retrieveTableNames() -> tuple[Response | int]:
    """ Gets Overtime or Immigration Report data from it's table.
    
    Returns
        {tuple[Response | int]} JSON message if succeed or not.
    """

    viewName = request.args.get("view_name")
    table = request.args.get("table")
    compliance = Compliance()

    if not viewName:
        return jsonify({"success": False, "error": "View name is required"}), 400

    try:
        response = compliance.getTableName(table, viewName)
    except Exception as e:
        logger.error(f"Error in retrieveTableNames: {str(e)}")
        return jsonify({"success": False, "error": "Error retrieving report data from database"}), 500
    else:
        if not response:
            return jsonify({"success": False, "error": f'View "{viewName}" not found'}), 404
        
        if table == "saved_data":
            row = response[0]

            data = row["data"]
            weekData = row["weekdata"]
            dateCreated = row["datecreated"]

            return jsonify({
                "success": True,
                "data": data,  
                "weekdata": weekData,
                "created": dateCreated
            }), 200

        data = response[0]["data"]

        return jsonify({"success": True, "data": data}), 200
