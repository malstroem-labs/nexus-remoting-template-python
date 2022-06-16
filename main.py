import asyncio
import sys
from datetime import datetime, timedelta
from typing import Callable

from nexus_extensibility import (CatalogRegistration, DataSourceContext,
                                 IDataSource, NexusDataType, ReadDataHandler,
                                 ReadRequest, Representation, ResourceBuilder,
                                 ResourceCatalogBuilder)
from nexus_remoting import RemoteCommunicator


class PythonDataSource(IDataSource):
    
    async def set_context(self, context, logger):
        
        self._context: DataSourceContext = context

        if (context.resource_locator.scheme != "file"):
            raise Exception(f"Expected 'file' URI scheme, but got '{context.resource_locator.scheme}'.")

    async def get_catalog_registrations(self, path: str):

        if path == "/":
            return [
                CatalogRegistration("/A/B/C", "Test catalog /A/B/C.")
            ]

        else:
            return []

    async def get_catalog(self, catalog_id: str):

        if (catalog_id == "/A/B/C"):

            representation = Representation(NexusDataType.INT64, timedelta(seconds=1))

            resource1 = ResourceBuilder("resource1") \
                .with_unit("°C") \
                .with_groups(["group1"]) \
                .add_representation(representation) \
                .build()

            representation = Representation(NexusDataType.FLOAT64, timedelta(seconds=1))

            resource2 = ResourceBuilder("resource2") \
                .with_unit("bar") \
                .with_groups(["group2"]) \
                .add_representation(representation) \
                .build()

            catalog = ResourceCatalogBuilder("/A/B/C") \
                .with_property("a", "b") \
                .add_resources([resource1, resource2]) \
                .build()

        else:
            raise Exception("Unknown catalog ID.")

        return catalog

    async def get_time_range(self, catalog_id: str):
        return (datetime.min, datetime.max)

    async def get_availability(self, catalog_id: str, begin: datetime, end: datetime):
        return 1

    def read(self, 
        begin: datetime, 
        end: datetime,
        requests: list[ReadRequest], 
        read_data: ReadDataHandler,
        report_progress: Callable[[float], None]):

        for request in requests:
            double_data = request.data.cast("d")

            for i in range(0, len(double_data)):
                double_data[i] = i

            for i in range(0, len(request.status)):
                request.status[i] = 1

# args
if len(sys.argv) < 3:
    raise Exception(f"No argument for address and/or port was specified.")

# get address
address = sys.argv[1]

# get port
try:
    port = int(sys.argv[2])
except Exception as ex:
    raise Exception(f"The second command line argument must be a valid port number. Inner error: {str(ex)}")

# run
print ("Hello from the sample python remoting client!")
asyncio.run(RemoteCommunicator(PythonDataSource(), address, port).run())
