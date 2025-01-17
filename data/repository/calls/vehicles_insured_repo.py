from data.repository.interfaces.i_vehicles_insured import IVehiclesInsured
from data.repository.calls.helpers import executeOperation

class VehiclesInsured(IVehiclesInsured):
    def deleteByIds(self, ids):
        """ Delete customers by its ids.

        Parameters
            ids {list} list of customers ids.
        """

        values = ", ".join(str(id) for id in ids)
        query = f"DELETE FROM vehicles_insured WHERE id_vehicle_insured IN ({values})"

        return executeOperation(query)
