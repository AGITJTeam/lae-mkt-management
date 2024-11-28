from service.columnsTransformations import vehNewColumnNames
from service.helpers import renameColumns
from data.models.vehicles_model import VehicleInsuredModel
import pandas as pd

""" Create VehiclesInsured DataFrame with renamed columns with API
    response.

Parameters
    vehiclesInsured {pd.Series or List} an iterable with vehicles
    insured from the PoliciesDetails DataFrame.    

Returns
    {DataFrame} resulting DataFrame.

"""
def generateVehiclesDf(vehiclesInsured: pd.Series) -> pd.DataFrame:
    dataclassVehicles = []
    
    for vehicles in vehiclesInsured:
        for vehicle in vehicles:
            if not vehicle:
                continue

            vehicleModel = VehicleInsuredModel(**vehicle)
            dataclassVehicles.append(vehicleModel)
    
    vehiclesDf = pd.DataFrame(dataclassVehicles)
    vehiclesDf["dateCreated"] = pd.to_datetime(vehiclesDf["dateCreated"])
    vehiclesDf["lastUpdated"] = pd.to_datetime(vehiclesDf["lastUpdated"])
    renamedVehiclesDf = renameColumns(vehiclesDf, vehNewColumnNames)

    return renamedVehiclesDf