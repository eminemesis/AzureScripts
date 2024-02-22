"""
This script fetches DTU values of all the servers in a resource group.
"""

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.sql import SqlManagementClient
from datetime import datetime as dt, timedelta
from azure.mgmt.monitor import MonitorManagementClient

# variable declarations
tenantId = "<tenant id>"
subId = "<subscription id>"
rgName = "<resource group name>"
clientId = "<service principal app id>"
clientSecret = "<service principal secret>"
credential = ServicePrincipalCredentials(clientId, clientSecret, tenant=tenantId)

# function definitions
def listServersAndDBs(credential, subId, rgName):

    sqlClient = SqlManagementClient(credential, subId)
    servers = sqlClient.servers.list()
    serverNames = [server.as_dict()["name"] for server in servers]
    serverDbDict = {}
    serverDbNamesDict = {}

    for serverName in serverNames:
        serverDbDict[serverName] = sqlClient.databases.list_by_server(rgName, serverName)

    for serverName in serverNames:
        serverDbNamesDict[serverName] = [
            db.as_dict()["name"] 
            for db in serverDbDict[serverName] 
            if db.as_dict()["name"] != "master"
            ]

    return serverDbNamesDict


def calculateDtu(credential, subId, rgName, sqlServerName, sqlDbName):

    startTime = dt.now() - timedelta(days=30)  # durationTimeDelta
    endTime = dt.now()
    interval = timedelta(days=1)  # intervalTimeDelta
    sqlDbResourceId = f"/subscriptions/{subId}/resourceGroups/{rgName}/providers/Microsoft.Sql/servers/{sqlServerName}/databases/{sqlDbName}"

    mmClient = MonitorManagementClient(credential, subId)
    metrics = mmClient.metrics.list(
        resource_uri=sqlDbResourceId,
        timespan=f"{startTime}/{endTime}",
        interval=interval,
        metricnames="cpu_percent,physical_data_read_percent,log_write_percent",
        aggregation="average",
    ).as_dict()
    metricsDict = {}
    averagedMetricsDict = {}

    # metricsDict has metric names in keys and respective timeseries data in values
    for metric in metrics["value"]:
        metricsDict[metric["name"]["value"]] = metric["timeseries"][0]["data"]

    # averagedMetricsDict has metric names in keys and respective averaged value in values
    for metric in metricsDict.keys():
        filteredMetric = [
            float(i["average"]) 
            for i in metricsDict[metric] 
            if "average" in i
        ]
        averagedMetricsDict[metric] = sum(filteredMetric) / len(metricsDict[metric])

    # According to Microsoft, avg_dtu_percent = MAX(avg_cpu_percent, avg_data_io_percent, avg_log_write_percent). avg_data_io_percent is replaced with physical_data_read_percent here.
    dtu = max(averagedMetricsDict.values())
    return dtu


def main(credential, subId, rgName):

    serversAndDBs = listServersAndDBs(credential, subId, rgName)
    serversDtuUsages = {}
    for server in serversAndDBs.keys():
        if len(serversAndDBs[server]) > 0:
            DbDTUs = [
                calculateDtu(credential, subId, rgName, server, db)
                for db in serversAndDBs[server]
            ]
            serversDtuUsages[server] = sum(DbDTUs)
        else:
            serversDtuUsages[server] = 0.0

    return serversDtuUsages


# main execution
if __name__ == "__main__":
    main(credential, subId, rgName)
