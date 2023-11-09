# plot timeseries from the given csv file using matplotlib

import matplotlib.pyplot as plt
import pandas as pd
import json
from datetime import datetime, timedelta
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.ticker import FuncFormatter
import matplotlib.dates as mdates

UTILIZATION_GRAPH_COLOR = '#031230'
CAPACITY_GRAPH_COLOR = '#fa8155'
APPS_GRAPH_COLOR = '#2d4217'
OPTIMIZATION_LINE_COLOR = '#d90f23'

def gib_format(y, pos):
    gib = y / (1024 ** 3)  # Convert bytes to GiB
    return f'{gib:.2f} GiB'


totalCpuUsageMap={}
totalMemoryUsageMap={}
totalCpuCapacityMap={}
totalMemoryCapacityMap={}
totalAppsMap={}

def get_node_group_utilization_details(nodes_list, cpuMap, memMap, appsMap, time):
    
    for nodeGroup in nodes_list:

        nodeGroupName = nodeGroup["nodeGroupName"]
        instanceType = nodeGroup["instanceType"]
        totalMemoryCapacity = nodeGroup["memoryInBytes"] * nodeGroup["numberOfNodes"]
        totalCpuCapacity = nodeGroup["vcpus"] * nodeGroup["numberOfNodes"]

        appsWithResourceDetails = nodeGroup["appsWithResourceDetails"]
        
        memoryOfWorkloads = 0
        cpuOfWorkloads = 0
        for workload in appsWithResourceDetails:
            memoryOfWorkloads += workload["appTotalMemory"]
            cpuOfWorkloads += workload["appTotalCPU"]

        print("Node Group Name: ", nodeGroupName)
        print("Instance Type: ", instanceType)
        print("Total Memory Capacity: ", totalMemoryCapacity)
        print("Memory of Workloads: ", memoryOfWorkloads)    
        print("Total CPU Capacity", totalCpuCapacity)
        print("CPU of Workloads: ", cpuOfWorkloads)
        print("Number of nodes:", nodeGroup["numberOfNodes"])
        print("Number of Apps:", len(appsWithResourceDetails))

        if nodeGroupName not in cpuMap.keys():
            cpuMap[nodeGroupName] = {}
        if nodeGroupName not in memMap.keys():
            memMap[nodeGroupName] = {}
        if nodeGroupName not in appsMap.keys():
            appsMap[nodeGroupName] = {}
    
        cpuMap[nodeGroupName][time] = (cpuOfWorkloads/totalCpuCapacity)*100
        memMap[nodeGroupName][time] = (memoryOfWorkloads/totalMemoryCapacity)*100
        appsMap[nodeGroupName][time] = len(appsWithResourceDetails)


def get_node_group_details(nodes_list, cpuMap, memMap, appsMap, cpuCapacityMap, memoryCapacityMap, time):
    
    for nodeGroup in nodes_list:

        nodeGroupName = nodeGroup["nodeGroupName"]
        instanceType = nodeGroup["instanceType"]
        totalMemoryCapacity = nodeGroup["memoryInBytes"] * nodeGroup["numberOfNodes"]
        totalCpuCapacity = nodeGroup["vcpus"] * nodeGroup["numberOfNodes"]

        appsWithResourceDetails = nodeGroup["appsWithResourceDetails"]
        
        memoryOfWorkloads = 0
        cpuOfWorkloads = 0
        for workload in appsWithResourceDetails:
            memoryOfWorkloads += workload["appTotalMemory"]
            cpuOfWorkloads += workload["appTotalCPU"]

        print("Node Group Name: ", nodeGroupName)
        print("Instance Type: ", instanceType)
        print("Total Memory Capacity: ", totalMemoryCapacity)
        print("Memory of Workloads: ", memoryOfWorkloads)    
        print("Total CPU Capacity", totalCpuCapacity)
        print("CPU of Workloads: ", cpuOfWorkloads)
        print("Number of nodes:", nodeGroup["numberOfNodes"])
        print("Number of Apps:", len(appsWithResourceDetails))

        if nodeGroupName not in cpuMap.keys():
            cpuMap[nodeGroupName] = {}
        if nodeGroupName not in memMap.keys():
            memMap[nodeGroupName] = {}
        if nodeGroupName not in appsMap.keys():
            appsMap[nodeGroupName] = {}
        if nodeGroupName not in cpuCapacityMap.keys():
            cpuCapacityMap[nodeGroupName] = {}
        if nodeGroupName not in memoryCapacityMap.keys():
            memoryCapacityMap[nodeGroupName] = {}    
  
    
        cpuMap[nodeGroupName][time] = cpuOfWorkloads
        memMap[nodeGroupName][time] = memoryOfWorkloads
        appsMap[nodeGroupName][time] = len(appsWithResourceDetails) 
        cpuCapacityMap[nodeGroupName][time] = totalCpuCapacity
        memoryCapacityMap[nodeGroupName][time] = totalMemoryCapacity     

        if time not in totalCpuUsageMap.keys():
            totalCpuUsageMap[time] = 0
        if time not in totalMemoryUsageMap.keys():
            totalMemoryUsageMap[time] = 0
        if time not in totalCpuCapacityMap.keys():
            totalCpuCapacityMap[time] = 0
        if time not in totalMemoryCapacityMap.keys():
            totalMemoryCapacityMap[time] = 0
        if time not in totalAppsMap.keys():
            totalAppsMap[time] = 0    

        totalCpuUsageMap[time] += cpuOfWorkloads
        totalMemoryUsageMap[time] += memoryOfWorkloads
        totalCpuCapacityMap[time] += totalCpuCapacity
        totalMemoryCapacityMap[time] += totalMemoryCapacity
        totalAppsMap[time] += len(appsWithResourceDetails)


# plot time series
def plot_utilization_timeseries():
    df = pd.read_csv('sedai_cluster_optimization_projections.csv')
    account_list = df['account_id'].unique()

    for account in account_list:
        account_df = df[df['account_id'] == account]
        acc_details = account_df['details']
        timeseries = account_df['updated_time']
        dict_data = acc_details.to_dict()

        nodeGroupsJson = [json.loads(item) for item in dict_data.values()]
        nodeGroups = { time: item['optimizationRecommendations']['current']['nodeGroups'] for time,item in zip(timeseries, nodeGroupsJson) }

        cpuMap = {}
        memMap = {}
        appsMap = {}
        for time, details in nodeGroups.items():           
            get_node_group_utilization_details(details, cpuMap, memMap, appsMap, time)  

        plot_graph(cpuMap, account, 'CPU Timeseries Data', 'CPU Utilization Percentage')    
        plot_graph(memMap, account, 'Memory Timeseries Data', 'Memory Utilization Percentage')    

        #plot_graph_per_nodeGroup(cpuMap, memMap, appsMap, account, 'Timeseries Data')  

        break    # we need dev cluster info only for now


def plot_capacity_timeseries(daysForChart=30):
    df1 = pd.read_csv('sedai_cluster_optimization_projections.csv')
    df2 = pd.read_csv('current_nodegroups.csv')
    # Concatenate vertically (stacking one dataframe on top of the other)

    # Convert 'created_time' column to datetime if it's not already
    df1['updated_time'] = pd.to_datetime(df1['updated_time'], utc=True)
    df2['updated_time'] = pd.to_datetime(df2['updated_time'], utc=True)

    filter_date = datetime(2023, 10, 14)
    filter_date = pd.Timestamp(filter_date, tz='UTC')
    # Filter dataframes based on date
    df1_filtered = df1[df1['updated_time'].dt.date <= filter_date.date()]
    df2_filtered = df2[df2['updated_time'].dt.date > filter_date.date()]
    df = pd.concat([df1_filtered, df2_filtered], axis=0)
    account_list = df['account_id'].unique()

    for account in account_list:
        account_df = df[df['account_id'] == account]
        # Get current date
        current_date = datetime.now()

        # Calculate the date 30 days ago
        thirty_days_ago = current_date - timedelta(days=daysForChart)

        account_df['updated_time'] = pd.to_datetime(account_df['updated_time'])
        
        # Remove records from a specific date (e.g., October 26th, 2023)
        specific_date_1 = datetime(2023, 10, 26)
        specific_date_1 = pd.to_datetime(specific_date_1)

        specific_date_2 = datetime(2023, 10, 27)
        specific_date_2 = pd.to_datetime(specific_date_2)

        thirty_days_ago = pd.Timestamp(current_date - pd.Timedelta(days=daysForChart), tz='UTC')
        current_date = pd.Timestamp(current_date, tz='UTC')

        account_df = account_df[account_df['updated_time'].dt.date != specific_date_1.date()]
        account_df = account_df[account_df['updated_time'].dt.date != specific_date_2.date()]
        account_df = account_df[(account_df['updated_time'] >= thirty_days_ago) & (account_df['updated_time'] <= current_date)]

        acc_details = account_df['details']
        timeseries = account_df['updated_time']
        dict_data = acc_details.to_dict()

        nodeGroupsJson = [json.loads(item) for item in dict_data.values()]
        nodeGroups = { time: item['optimizationRecommendations']['current']['nodeGroups'] for time,item in zip(timeseries, nodeGroupsJson) }

        cpuMap = {}
        memMap = {}
        appsMap = {}
        cpuCapacityMap = {}
        memCapacityMap = {}
        for time, details in nodeGroups.items():           
            get_node_group_details(details, cpuMap, memMap, appsMap, cpuCapacityMap, memCapacityMap, time)  
   
        pdf_pages = PdfPages('time_series_plots.pdf')
        #plot_nodegroup_capacity_vs_utilization(pdf_pages, cpuMap, cpuCapacityMap, account, 'CPU Timeseries Data', 'Cores', False, appsMap) 
        #plot_nodegroup_capacity_vs_utilization(pdf_pages, memMap, memCapacityMap, account, 'Memory Timeseries Data', 'GibiBytes', True, appsMap) 
        plot_ng_utilization_apps_in_single_graph(pdf_pages, cpuMap, cpuCapacityMap, account, 'CPU Timeseries Data', 'Cores', False, appsMap)
        plot_ng_utilization_apps_in_single_graph(pdf_pages, memMap, memCapacityMap, account, 'Memory Timeseries Data', 'GibiBytes', True, appsMap)
        #plot_app_graph(pdf_pages, appsMap, account, 'Apps Timeseries Data', 'Number of Apps')
        
        #node_group_summary(pdf_pages, totalCpuUsageMap, totalCpuCapacityMap, account, 'Node Group CPU Utilization summary', 'Cores', False)
        #node_group_summary(pdf_pages, totalMemoryUsageMap, totalMemoryCapacityMap, account, 'Node Group Memory Utilization summary', 'GibiBytes', True)
        summary(pdf_pages, totalCpuUsageMap, totalCpuCapacityMap, account, 'Cluster CPU Utilization summary', 'Cores', False)
        summary(pdf_pages, totalMemoryUsageMap, totalMemoryCapacityMap, account, 'Cluster Memory Utilization summary', 'GibiBytes', True)
        pdf_pages.close()

        break    # we need dev cluster info only for now

    
def plot_graph(data_map, account, title, unit):
    # Create a single figure
    plt.figure(figsize=(10, 6))

    for nodeGroup in data_map.keys():
        data = data_map[nodeGroup]
        dates = [datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f %z') for date_str in data.keys()]
        values = list(data.values())

        # Filter data for dates starting from Oct 1
        oct_1_index = next((i for i, date in enumerate(dates) if date.day >= 1 and date.month >= 10), None)
        if oct_1_index is not None:
            dates = dates[oct_1_index:]
            values = values[oct_1_index:]

            # Plotting
            plt.plot(dates, values, marker='o', label=str(account + "_" + nodeGroup))

    plt.xlabel('Time')
    plt.ylabel(unit)
    plt.title(title)
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.legend()

    plt.show()


def plot_graph_per_nodeGroup(cpu_map, memory_map, apps_map, account, title):
    # Create a figure for each node group  

    for nodeGroup in cpu_map.keys():
        plt.figure(figsize=(10, 6))
        cpu_data = cpu_map[nodeGroup]
        memory_data = memory_map[nodeGroup]
        apps_data = apps_map[nodeGroup]
        dates = [datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f %z') for date_str in cpu_data.keys()]
        cpu_values = list(cpu_data.values())
        mem_values = list(memory_data.values())
        app_values = list(apps_data.values())

        # Filter data for dates starting from Oct 1
        oct_1_index = next((i for i, date in enumerate(dates) if date.day >= 1 and date.month >= 10), None)
        if oct_1_index is not None:
            dates = dates[oct_1_index:]
            cpu_values = cpu_values[oct_1_index:]
            mem_values = mem_values[oct_1_index:]
            app_values = app_values[oct_1_index:]

            # Plotting
            plt.plot(dates, cpu_values, marker='o', label='CPU')
            plt.plot(dates, mem_values, marker='o', label='MEMORY')
            plt.plot(dates, app_values, marker='o', label='APPS')

        plt.xlabel('Time')
        plt.ylabel('Utilization percentage')
        plt.title(title + " for " + nodeGroup)
        plt.xticks(rotation=20)
        plt.tight_layout()
        plt.legend()

    plt.show()    


    # bring a chart to show nodegroup capacity vs node utilization
def plot_capacity_vs_utilization(utilization_map, capacity_map, account, title, unit):
    # Create a single figure
    plt.figure(figsize=(10, 6))

    for nodeGroup in utilization_map.keys():
        util_data = utilization_map[nodeGroup]
        capacity_data = capacity_map[nodeGroup]
        dates = [datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f %z') for date_str in util_data.keys()]
        util_values = list(util_data.values())
        capacity_values = list(capacity_data.values())

        # Filter data for dates starting from Oct 1
        oct_1_index = next((i for i, date in enumerate(dates) if date.day >= 1 and date.month >= 10), None)
        if oct_1_index is not None:
            dates = dates[oct_1_index:]
            util_values = util_values[oct_1_index:]
            capacity_values = capacity_values[oct_1_index:]

            # Plotting
            plt.plot(dates, util_values, marker='o', label=str(account + "_" + nodeGroup))
            plt.plot(dates, capacity_values, marker='o', label=str(account + "_" + nodeGroup + "_capacity"))

    plt.xlabel('Time')
    plt.ylabel(unit)
    plt.title(title)
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.legend()

    plt.show()

def plot_app_graph(pdf_pages, appsMap, account, title, unit):
    plt.figure(figsize=(10, 6))
    for nodeGroup in appsMap.keys():
        apps_data = appsMap[nodeGroup]
        dates = [date_str.to_pydatetime() for date_str in apps_data.keys()]
        apps_values = list(apps_data.values())

        plt.plot(dates, apps_values, label=str(nodeGroup))
              
    # TODO Take the correct date from DB
    target_date = datetime(2023, 10, 15)

    # Add a vertical dotted line at the target date
    plt.axvline(x=target_date, color='b', linestyle='--', label='Optimization Started Date')

    plt.xlabel('Time')
    plt.ylabel(unit)
    plt.title(title)
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.legend()  

    # Add the plot to the PDF
    pdf_pages.savefig()

    #plt.show()    


def node_group_summary(pdf_pages, utilization_map, capacity_map, account, title, unit, isMemory=True):
    plt.figure(figsize=(20, 6))
    util_values = []
    capacity_values = []
    apps_count = []
    times = utilization_map.keys()
    for time in sorted(times):
        util_data = utilization_map[time]
        capacity_data = capacity_map[time]
        util_values.append(util_data)
        capacity_values.append(capacity_data)
        apps_count.append(totalAppsMap[time])
    
    plt.subplot(1, 2, 1)  # 2 rows, 1 column, 1st plot
    plt.stackplot(times, util_values, capacity_values, labels=['Utilization', 'Capacity'])
    # TODO Take the correct date from DB
    target_date = datetime(2023, 10, 13)
    plt.axvline(x=target_date, color='r', linestyle='--', label='Optimization Started Date')

    plt.xlabel('Time')
    plt.ylabel(unit)
    plt.title(title)
    plt.xticks(rotation=20)
    if isMemory:
        plt.gca().yaxis.set_major_formatter(FuncFormatter(gib_format))

    plt.subplot(1, 2, 2)  # 2 rows, 1 column, 2nd plot    
    plt.plot(times, apps_count, label='Total Number of Apps')
    plt.xlabel('Time')
    plt.ylabel('Total no of apps count')
    plt.xticks(rotation=20)
    plt.title('Total no of apps across node group')     
    plt.tight_layout()
    plt.legend(loc='upper right')  

    # Add the plot to the PDF
    pdf_pages.savefig()

def summary(pdf_pages, utilization_map, capacity_map, account, title, unit, isMemory=True):
    util_values = []
    capacity_values = []
    apps_count = []
    times = utilization_map.keys()
    for time in sorted(times):
        util_data = utilization_map[time]
        capacity_data = capacity_map[time]
        util_values.append(util_data)
        capacity_values.append(capacity_data)
        apps_count.append(totalAppsMap[time])

    fig, ax1 = plt.subplots(figsize=(20,10)) 

    colors = [UTILIZATION_GRAPH_COLOR, CAPACITY_GRAPH_COLOR]
    
    ax1.stackplot(times, util_values, capacity_values, colors=colors, labels=['Utilization', 'Capacity'])
    # TODO Take the correct date from DB
    target_date = datetime(2023, 10, 13)
    ax1.axvline(x=target_date, color=OPTIMIZATION_LINE_COLOR, linestyle='--', label='Optimization Started Date')

    ax1.set_xlabel('Time')
    ax1.set_ylabel(unit)
    if isMemory:
        ax1.yaxis.set_major_formatter(FuncFormatter(gib_format))

    ax1.set_xticklabels(times, rotation=20)

    # Format x-axis to show only dates
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=3))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))    

    ax2 = ax1.twinx()
   
    ax2.plot(times, apps_count, label='Total Number of Apps', color=APPS_GRAPH_COLOR)
    ax2.set_ylabel('Apps in cluster', color='green')

    plt.title(title)     
    plt.tight_layout()
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

    # Add the plot to the PDF
    pdf_pages.savefig()    


def plot_nodegroup_capacity_vs_utilization(pdf_pages, utilization_map, capacity_map, account, title, unit, isMemory=True, appsMap=None):

    for nodeGroup in utilization_map.keys():
        plt.figure(figsize=(20, 6))
        util_data = utilization_map[nodeGroup]
        capacity_data = capacity_map[nodeGroup]
        apps_data = appsMap[nodeGroup]
        dates = [date_str.to_pydatetime() for date_str in util_data.keys()]
        util_values = list(util_data.values())
        capacity_values = list(capacity_data.values())

        plt.subplot(1, 2, 1) 
        plt.stackplot(dates, util_values, capacity_values, labels=['Utilization', 'Capacity'])
              
        # TODO Take the correct date from DB
        target_date = datetime(2023, 10, 15)

        # Add a vertical dotted line at the target date
        plt.axvline(x=target_date, color='r', linestyle='--', label='Optimization Started Date')

        
        plt.xlabel('Time')
        plt.ylabel(unit)
        plt.title(title + " - " + nodeGroup)
        plt.xticks(rotation=20)
        if isMemory:
            plt.gca().yaxis.set_major_formatter(FuncFormatter(gib_format))
           
        plt.legend(loc='upper right')  
        
        
        plt.subplot(1, 2, 2)  
        plt.plot(dates, list(appsMap[nodeGroup].values()), label='Number of Apps') 
        plt.xlabel('Time')
        plt.ylabel('App Count')
        plt.title('Time vs App Count')
        plt.legend()

        
        plt.tight_layout()
        # Add the plot to the PDF
        pdf_pages.savefig()

    #plt.show()    

def plot_ng_utilization_apps_in_single_graph(pdf_pages, utilization_map, capacity_map, account, title, unit, isMemory=True, appsMap=None):
    plt.figure(figsize=(40, 20))
    for nodeGroup in utilization_map.keys():
        
        util_data = utilization_map[nodeGroup]
        capacity_data = capacity_map[nodeGroup]
        apps_data = appsMap[nodeGroup]
        dates = [date_str.to_pydatetime() for date_str in util_data.keys()]
        util_values = list(util_data.values())
        capacity_values = list(capacity_data.values())

        colors = [UTILIZATION_GRAPH_COLOR, CAPACITY_GRAPH_COLOR]

        fig, ax1 = plt.subplots(figsize=(20,10))
        ax1.stackplot(dates, util_values, capacity_values, colors=colors, labels=['Utilization', 'Capacity'])
              
        # TODO Take the correct date from DB
        target_date = datetime(2023, 10, 13)

        # Add a vertical dotted line at the target date
        ax1.axvline(x=target_date, color=OPTIMIZATION_LINE_COLOR, linestyle='--', label='Optimization Started Date')

        ax1.set_xlabel('Time')
        ax1.set_ylabel(unit)
        #ax1.set_xticks(ticks = target_date, rotation=20)
        if isMemory:
            ax1.yaxis.set_major_formatter(FuncFormatter(gib_format))
        ax1.set_xticklabels(dates, rotation=20)

        # Format x-axis to show only dates
        ax1.xaxis.set_major_locator(mdates.DayLocator(interval=3))
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
           
        #ax1.legend(loc='upper right')  

        # Create the second axis (on the right)
        ax2 = ax1.twinx()
        ax2.plot(dates, list(apps_data.values()), color=APPS_GRAPH_COLOR, label='Number of Apps') 
        ax2.set_ylabel('Apps in Nodegroup', color='green')

        if isMemory:
            plt.title('Memory Utilization for ' + nodeGroup)
        else:
            plt.title('CPU Utilization for ' + nodeGroup)

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

        plt.tight_layout()
        # Add the plot to the PDF
        pdf_pages.savefig()

    #plt.show()    
    pass    
   

#plot_utilization_timeseries()
plot_capacity_timeseries(45)