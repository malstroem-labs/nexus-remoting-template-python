import asyncio
import sys
from datetime import datetime, timedelta
from typing import Callable

from nexus_extensibility import (CatalogRegistration, NexusDataType,
                                 ReadDataHandler, ReadRequest, Representation,
                                 ResourceBuilder, ResourceCatalogBuilder,
                                 SimpleDataSource)
from nexus_remoting import RemoteCommunicator


class PythonDataSource(SimpleDataSource):
    
    async def get_catalog_registrations(self, path: str):

        if path == "/":
            return [
                CatalogRegistration("/A/B/C", "Test catalog /A/B/C.")
            ]

        else:
            return []

    async def get_catalog(self, catalog_id: str):

        if (catalog_id == "/A/B/C"):

            representation = Representation(NexusDataType.FLOAT64, timedelta(seconds=1))

            resource = ResourceBuilder("resource1") \
                .with_unit("°C") \
                .with_groups(["group1"]) \
                .add_representation(representation) \
                .build()

            catalog = ResourceCatalogBuilder("/A/B/C") \
                .add_resources([resource]) \
                .build()

            return catalog

        else:
            raise Exception("Unknown catalog identifier.")

    async def read(self, 
        begin: datetime, 
        end: datetime,
        requests: list[ReadRequest], 
        read_data: ReadDataHandler,
        report_progress: Callable[[float], None]):

        temperature_data = await read_data("/SAMPLE/LOCAL/T1", begin, end)

        for request in requests:

            # generate data
            result_buffer = request.data.cast("d")

            for i in range(0, len(result_buffer)):
                # example: multiply by two
                result_buffer[i] = temperature_data[i] * 2

            # mark all data as valid
            for i in range(0, len(request.status)):
                request.status[i] = 1

# args
if len(sys.argv) < 3:
    raise Exception("No argument for address and/or port was specified.")

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
